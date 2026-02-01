from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
import requests
import time
from src.helpers.Entity import Entity
from src.helpers.setup_logger import logger
from src.helpers.datadog_instrumentation import (
    trace_function,
    get_statsd,
    get_tracer,
    Metrics,
)
import datetime

# Constants
DEFAULT_TIMEOUT = 30
ERROR_TIMEOUT = 300
REQUEST_TIMEOUT = 10
MAX_ENTITIES = 1000  # Memory safety limit

statsd = get_statsd()
tracer = get_tracer()


class VehiclePositionFeed:
    def __init__(
        self,
        url,
        file_path,
        s3_bucket,
        headers=None,
        query_params=None,
        https_verify=True,
        timeout=DEFAULT_TIMEOUT,
    ):
        self.entities = []
        self.url = url
        self.headers = headers
        self.query_params = query_params
        self.file_path = file_path
        self.s3_bucket = s3_bucket
        self.https_verify = https_verify
        self.timeout = timeout

    def find_entity(self, entity_id):
        return next((e for e in self.entities if e.entity_id == entity_id), None)

    def update_timeout(self, timeout):
        self.timeout = timeout

    def _check_memory_limit(self):
        """Remove oldest entities if memory limit exceeded."""
        if len(self.entities) > MAX_ENTITIES:
            # Sort by creation time and remove oldest
            oldest = min(self.entities, key=lambda e: e.created)
            logger.debug(f"Memory limit exceeded ({len(self.entities)}/{MAX_ENTITIES})")
            if len(oldest.updated_at) > 1:
                oldest.save(self.file_path)
            self.entities.remove(oldest)

            # Record memory culling metric
            statsd.increment(Metrics.ENTITY_MEMORY_CULLED)

            logger.warning(
                f"Entity memory limit exceeded. Removed {oldest.entity_id}. "
                f"Current entities: {len(self.entities)}"
            )

    @trace_function("feed.get_entities", resource="VehiclePositionFeed")
    def get_entities(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        vehicles = []
        start_time = time.time()

        # Increment fetch count
        statsd.increment(Metrics.FEED_FETCH_COUNT)

        try:
            # Build headers with User-Agent
            headers = self.headers or {}
            if "User-Agent" not in headers:
                headers["User-Agent"] = "gtfspb-2-mfjson/1.0"

            logger.debug(f"Fetching feed from {self.url}")
            response = requests.get(
                self.url,
                headers=headers,
                params=self.query_params,
                verify=self.https_verify,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            # Record success and response time
            duration_ms = (time.time() - start_time) * 1000
            statsd.increment(Metrics.FEED_FETCH_SUCCESS)
            statsd.histogram(Metrics.FEED_FETCH_DURATION, duration_ms)

            # Add span tags for debugging
            span = tracer.current_span()
            if span:
                span.set_tag("http.status_code", response.status_code)
                span.set_tag("http.url", self.url)
                span.set_tag("response.size_bytes", len(response.content))

            logger.debug(f"Feed request successful. Status: {response.status_code}")

            feed.ParseFromString(response.content)
            logger.debug("Successfully parsed protobuf feed")
        except DecodeError as e:
            statsd.increment(Metrics.FEED_FETCH_FAILURE, tags=["error:decode_error"])
            logger.warning(f"Protobuf decode error for {self.url}: {e}")
        except requests.exceptions.Timeout:
            statsd.increment(Metrics.FEED_FETCH_FAILURE, tags=["error:timeout"])
            logger.warning(
                f"Request timeout for {self.url} (timeout={REQUEST_TIMEOUT}s)"
            )
        except requests.exceptions.TooManyRedirects:
            statsd.increment(
                Metrics.FEED_FETCH_FAILURE, tags=["error:too_many_redirects"]
            )
            logger.warning(f"Too many redirects for {self.url}")
        except requests.exceptions.SSLError as e:
            statsd.increment(Metrics.FEED_FETCH_FAILURE, tags=["error:ssl_error"])
            logger.warning(f"SSL error for {self.url}: {e}")
        except requests.exceptions.RequestException as e:
            statsd.increment(
                Metrics.FEED_FETCH_FAILURE, tags=["error:request_exception"]
            )
            logger.error(f"Request failed for {self.url}: {e}")
            raise SystemExit(e)
        except Exception as e:
            statsd.increment(Metrics.FEED_FETCH_FAILURE, tags=["error:unknown"])
            self.update_timeout(ERROR_TIMEOUT)
            logger.exception(f"Unexpected error fetching feed from {self.url}: {e}")

        # Extract vehicle entities from feed
        try:
            vehicles = [e for e in feed.entity if e.HasField("vehicle")]
            statsd.histogram(Metrics.FEED_ENTITY_COUNT, len(vehicles))
            logger.debug(f"Extracted {len(vehicles)} vehicle entities from feed")
        except Exception as e:
            logger.error(f"Failed to extract vehicle entities: {e}")

        return vehicles

    @trace_function("feed.consume", resource="VehiclePositionFeed")
    def consume_pb(self):
        # Check memory limits before processing
        self._check_memory_limit()

        feed_entities = self.get_entities()

        # Record active entity gauge
        statsd.gauge(Metrics.ENTITY_ACTIVE_COUNT, len(self.entities))

        if len(feed_entities) == 0:
            logger.warning(f"Empty feed for {self.url}")
            statsd.increment(Metrics.FEED_EMPTY_COUNT)
            self.update_timeout(ERROR_TIMEOUT)
            return

        logger.debug(f"Processing {len(feed_entities)} feed entities")

        # Track counts for metrics
        created_count = 0
        updated_count = 0
        direction_changed_count = 0
        saved_count = 0
        discarded_count = 0

        if len(self.entities) == 0:
            # create all new objects
            logger.info(
                f"No existing entities. Creating {len(feed_entities)} new entities"
            )
            for feed_entity in feed_entities:
                entity = Entity(feed_entity)
                self.entities.append(entity)
                created_count += 1
            logger.debug(f"Total entities after creation: {len(self.entities)}")
        else:
            current_ids = []

            # find and update entity
            for feed_entity in feed_entities:
                entity = self.find_entity(feed_entity.id)
                if entity:
                    # check if new direction and old direction are same
                    # check if last updated date is equivalent to new date, to prevent duplication
                    if (
                        entity.updated_at[-1]
                        != datetime.datetime.fromtimestamp(
                            feed_entity.vehicle.timestamp
                        ).isoformat()
                    ):
                        if entity.direction_id == feed_entity.vehicle.trip.direction_id:
                            entity.update(feed_entity)
                            current_ids.append(feed_entity.id)
                            updated_count += 1
                        else:
                            # Direction changed - save old and create new
                            direction_changed_count += 1
                            if len(entity.updated_at) > 1:
                                entity.save(self.file_path)
                                saved_count += 1
                                logger.debug(
                                    f"Saved entity {entity.entity_id} (direction changed)"
                                )
                            self.entities.remove(entity)
                            # now create new
                            entity = Entity(feed_entity)
                            self.entities.append(entity)
                            current_ids.append(feed_entity.id)
                            created_count += 1
                    else:
                        current_ids.append(feed_entity.id)
                else:
                    # New entity not seen before - create it
                    entity = Entity(feed_entity)
                    self.entities.append(entity)
                    current_ids.append(feed_entity.id)
                    created_count += 1

            # remove and save finished entities
            old_ids = {e.entity_id for e in self.entities}
            ids_to_remove = old_ids - set(current_ids)

            for id in ids_to_remove:
                entity = self.find_entity(id)
                if entity:
                    if len(entity.updated_at) > 1:
                        entity.save(self.file_path)
                        saved_count += 1
                        logger.debug(f"Saved finished entity {entity.entity_id}")
                    else:
                        discarded_count += 1
                        logger.debug(
                            f"Discarded entity {entity.entity_id} "
                            f"({len(entity.updated_at)} observation)"
                        )
                    self.entities.remove(entity)

        # Submit batch metrics
        if created_count > 0:
            statsd.increment(Metrics.ENTITY_CREATED, created_count)
        if updated_count > 0:
            statsd.increment(Metrics.ENTITY_UPDATED, updated_count)
        if direction_changed_count > 0:
            statsd.increment(Metrics.ENTITY_DIRECTION_CHANGED, direction_changed_count)
        if saved_count > 0:
            statsd.increment(Metrics.ENTITY_SAVED, saved_count)
        if discarded_count > 0:
            statsd.increment(Metrics.ENTITY_DISCARDED, discarded_count)

        # Update active count gauge
        statsd.gauge(Metrics.ENTITY_ACTIVE_COUNT, len(self.entities))

        logger.debug(
            f"Feed processing complete: {updated_count} updated, "
            f"{direction_changed_count} direction changes, {saved_count} saved, "
            f"{discarded_count} discarded"
        )
