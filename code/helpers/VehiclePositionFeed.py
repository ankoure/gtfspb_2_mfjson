from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
import requests
from code.helpers.Entity import Entity
from code.helpers.setup_logger import logger
import datetime

# Constants
DEFAULT_TIMEOUT = 30
ERROR_TIMEOUT = 300
REQUEST_TIMEOUT = 10
MAX_ENTITIES = 1000  # Memory safety limit


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
            logger.warning(
                f"Entity memory limit exceeded. Removed {oldest.entity_id}. "
                f"Current entities: {len(self.entities)}"
            )

    def get_entities(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        vehicles = []

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
            logger.debug(f"Feed request successful. Status: {response.status_code}")

            feed.ParseFromString(response.content)
            logger.debug("Successfully parsed protobuf feed")
        except DecodeError as e:
            logger.warning(f"Protobuf decode error for {self.url}: {e}")
        except requests.exceptions.Timeout:
            logger.warning(
                f"Request timeout for {self.url} (timeout={REQUEST_TIMEOUT}s)"
            )
        except requests.exceptions.TooManyRedirects:
            logger.warning(f"Too many redirects for {self.url}")
        except requests.exceptions.SSLError as e:
            logger.warning(f"SSL error for {self.url}: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {self.url}: {e}")
            raise SystemExit(e)
        except Exception as e:
            self.update_timeout(ERROR_TIMEOUT)
            logger.exception(f"Unexpected error fetching feed from {self.url}: {e}")

        # Extract vehicle entities from feed
        try:
            vehicles = [e for e in feed.entity if e.HasField("vehicle")]
            logger.debug(f"Extracted {len(vehicles)} vehicle entities from feed")
        except Exception as e:
            logger.error(f"Failed to extract vehicle entities: {e}")

        return vehicles

    def consume_pb(self):
        # Check memory limits before processing
        self._check_memory_limit()

        feed_entities = self.get_entities()

        if len(feed_entities) == 0:
            logger.warning(f"Empty feed for {self.url}")
            self.update_timeout(ERROR_TIMEOUT)
            return

        logger.debug(f"Processing {len(feed_entities)} feed entities")

        if len(self.entities) == 0:
            # create all new objects
            logger.info(
                f"No existing entities. Creating {len(feed_entities)} new entities"
            )
            for feed_entity in feed_entities:
                entity = Entity(feed_entity)
                self.entities.append(entity)
            logger.debug(f"Total entities after creation: {len(self.entities)}")
        else:
            current_ids = []
            updated_count = 0
            direction_changed_count = 0

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
                                logger.debug(
                                    f"Saved entity {entity.entity_id} (direction changed)"
                                )
                            self.entities.remove(entity)
                            # now create new
                            entity = Entity(feed_entity)
                            self.entities.append(entity)
                            current_ids.append(feed_entity.id)
                    else:
                        current_ids.append(feed_entity.id)
                else:
                    # New entity not seen before - create it
                    entity = Entity(feed_entity)
                    self.entities.append(entity)
                    current_ids.append(feed_entity.id)
                    updated_count += 1

            # remove and save finished entities
            old_ids = {e.entity_id for e in self.entities}
            ids_to_remove = old_ids - set(current_ids)
            saved_count = 0
            discarded_count = 0

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

            logger.debug(
                f"Feed processing complete: {updated_count} updated, "
                f"{direction_changed_count} direction changes, {saved_count} saved, "
                f"{discarded_count} discarded"
            )
