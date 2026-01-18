"""
GTFS Static Data Manager

Loads and manages GTFS static feeds (stops.txt, stop_times.txt, trips.txt, routes.txt).
Builds stop-to-stop segment indexes for map matching GPS trajectories.
"""

import json
import logging
from pathlib import Path
from typing import Optional
from collections import defaultdict
import gtfs_kit as gk

logger = logging.getLogger(__name__)


class GTFSStaticManager:
    """Manages GTFS static data and provides segment index for map matching."""

    def __init__(self, gtfs_path: str | Path):
        """
        Initialize GTFS Static Manager.

        Args:
            gtfs_path: Path to directory containing GTFS static files
        """
        self.gtfs_path = Path(gtfs_path)
        self.feed: Optional[gk.Feed] = None
        self.segment_index: dict = {}
        self.stops_lookup: dict = {}

    def load_gtfs_bundle(self) -> bool:
        """
        Load GTFS static bundle from directory.

        Returns:
            True if successful, False otherwise
        """
        if not self.gtfs_path.exists():
            logger.error(f"GTFS path does not exist: {self.gtfs_path}")
            return False

        try:
            logger.info(f"Loading GTFS bundle from {self.gtfs_path}")
            self.feed = gk.read_feed(str(self.gtfs_path), dist_units="km")

            if self.feed is None:
                logger.error("Failed to load GTFS feed")
                return False

            # Validate required files
            required_tables = ["stops", "stop_times", "trips", "routes"]
            for table in required_tables:
                if not hasattr(self.feed, table) or getattr(self.feed, table) is None:
                    logger.error(f"GTFS feed missing required table: {table}")
                    return False

            logger.info(
                f"GTFS loaded: {len(self.feed.routes)} routes, "
                f"{len(self.feed.stops)} stops, {len(self.feed.trips)} trips"
            )

            # Build lookup tables
            self._build_stops_lookup()

            return True

        except Exception as e:
            logger.error(f"Error loading GTFS bundle: {e}")
            return False

    def _build_stops_lookup(self):
        """Build stop ID to stop name lookup dictionary."""
        if self.feed is None or self.feed.stops is None:
            return

        self.stops_lookup = dict(
            zip(self.feed.stops["stop_id"], self.feed.stops["stop_name"])
        )
        logger.debug(f"Built stops lookup with {len(self.stops_lookup)} stops")

    def build_segment_index(self) -> bool:
        """
        Build segment index from GTFS stop_times.

        Creates a hierarchical index:
        {
            route_id: {
                direction_id: [
                    {segment_id, from_stop_id, to_stop_id, ...},
                    ...
                ]
            }
        }

        Returns:
            True if successful, False otherwise
        """
        if self.feed is None:
            logger.error("GTFS feed not loaded")
            return False

        try:
            logger.info("Building segment index...")

            # Merge trips with stop_times to get route_id and direction_id
            stop_times_with_trips = self.feed.stop_times.merge(
                self.feed.trips[["trip_id", "route_id", "direction_id"]],
                on="trip_id",
                how="left",
            )

            # Group by route_id and direction_id
            grouped = stop_times_with_trips.groupby(["route_id", "direction_id"])

            segment_index = defaultdict(lambda: defaultdict(list))
            total_segments = 0

            for (route_id, direction_id), group in grouped:
                # Find the canonical trip pattern (trip with most stops)
                trip_stop_counts = group.groupby("trip_id").size()
                canonical_trip_id = trip_stop_counts.idxmax()

                # Get stop sequence for canonical trip
                canonical_stops = group[
                    group["trip_id"] == canonical_trip_id
                ].sort_values("stop_sequence")

                # Create segments from consecutive stops
                stops_list = canonical_stops[
                    ["stop_id", "stop_sequence"]
                ].values.tolist()

                for i in range(len(stops_list) - 1):
                    from_stop_id, from_seq = stops_list[i]
                    to_stop_id, to_seq = stops_list[i + 1]

                    # Get stop names
                    from_stop_name = self.stops_lookup.get(from_stop_id, from_stop_id)
                    to_stop_name = self.stops_lookup.get(to_stop_id, to_stop_id)

                    # Create segment
                    segment = {
                        "segment_id": f"{route_id}_{direction_id}_{int(from_seq)}-{int(to_seq)}",
                        "route_id": route_id,
                        "direction_id": int(direction_id),
                        "from_stop_id": from_stop_id,
                        "to_stop_id": to_stop_id,
                        "from_stop_sequence": int(from_seq),
                        "to_stop_sequence": int(to_seq),
                        "from_stop_name": from_stop_name,
                        "to_stop_name": to_stop_name,
                        "segment_order": i,
                    }

                    segment_index[route_id][int(direction_id)].append(segment)
                    total_segments += 1

            self.segment_index = dict(segment_index)
            logger.info(
                f"Built segment index: {len(self.segment_index)} routes, "
                f"{total_segments} total segments"
            )

            return True

        except Exception as e:
            logger.error(f"Error building segment index: {e}")
            return False

    def get_segments_for_route(self, route_id: str, direction_id: int) -> list[dict]:
        """
        Get all segments for a specific route and direction.

        Args:
            route_id: GTFS route ID
            direction_id: Direction (0 or 1)

        Returns:
            List of segment dictionaries
        """
        if route_id not in self.segment_index:
            return []

        # Handle both int and string keys (JSON converts int keys to strings)
        route_segments = self.segment_index.get(route_id, {})
        return route_segments.get(
            direction_id, route_segments.get(str(direction_id), [])
        )

    def find_segment(
        self, route_id: str, direction_id: int, from_seq: int, to_seq: int
    ) -> Optional[dict]:
        """
        Find a specific segment by stop sequences.

        Args:
            route_id: GTFS route ID
            direction_id: Direction (0 or 1)
            from_seq: Starting stop sequence
            to_seq: Ending stop sequence

        Returns:
            Segment dictionary if found, None otherwise
        """
        segments = self.get_segments_for_route(route_id, direction_id)

        for segment in segments:
            if (
                segment["from_stop_sequence"] == from_seq
                and segment["to_stop_sequence"] == to_seq
            ):
                return segment

        return None

    def save_segment_index(self, output_path: Path) -> bool:
        """
        Save segment index to JSON file for caching.

        Args:
            output_path: Path to output JSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w") as f:
                json.dump(self.segment_index, f, indent=2)

            logger.info(f"Saved segment index to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving segment index: {e}")
            return False

    def load_segment_index(self, input_path: Path) -> bool:
        """
        Load segment index from JSON file.

        Args:
            input_path: Path to JSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            if not input_path.exists():
                logger.warning(f"Segment index file not found: {input_path}")
                return False

            with open(input_path, "r") as f:
                self.segment_index = json.load(f)

            total_segments = sum(
                len(directions[str(dir_id)])
                for route in self.segment_index.values()
                for dir_id, directions in [(0, route), (1, route)]
                if str(dir_id) in route
            )

            logger.info(
                f"Loaded segment index from {input_path}: "
                f"{len(self.segment_index)} routes, {total_segments} segments"
            )
            return True

        except Exception as e:
            logger.error(f"Error loading segment index: {e}")
            return False

    def get_stats(self) -> dict:
        """
        Get statistics about the loaded GTFS data.

        Returns:
            Dictionary with statistics
        """
        stats: dict[str, bool | int] = {
            "gtfs_loaded": self.feed is not None,
            "segment_index_built": len(self.segment_index) > 0,
        }

        if self.feed is not None:
            stats.update(
                {
                    "routes": len(self.feed.routes),
                    "stops": len(self.feed.stops),
                    "trips": len(self.feed.trips),
                }
            )

        if self.segment_index:
            # Handle both int and string keys (JSON saves int keys as strings)
            total_segments = 0
            for route_data in self.segment_index.values():
                for key in route_data.keys():
                    total_segments += len(route_data[key])

            stats.update(
                {
                    "indexed_routes": len(self.segment_index),
                    "total_segments": total_segments,
                }
            )

        return stats
