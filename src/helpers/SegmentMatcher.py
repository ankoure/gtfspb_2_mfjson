"""
Segment Matcher

Matches GPS trajectories to stop-to-stop segments using GTFS-RT stop_sequence data.
Computes segment-level statistics for demand analysis.
"""

import logging
from datetime import datetime
from collections import Counter, defaultdict
from src.helpers.GTFSStaticManager import GTFSStaticManager

logger = logging.getLogger(__name__)


class SegmentMatcher:
    """Matches trajectories to GTFS segments and computes statistics."""

    # GTFS-RT occupancy status enum values
    OCCUPANCY_STATUS_NAMES = {
        0: "EMPTY",
        1: "MANY_SEATS_AVAILABLE",
        2: "FEW_SEATS_AVAILABLE",
        3: "STANDING_ROOM_ONLY",
        4: "CRUSHED_STANDING_ROOM_ONLY",
        5: "FULL",
        6: "NOT_ACCEPTING_PASSENGERS",
    }

    def __init__(self, gtfs_manager: GTFSStaticManager):
        """
        Initialize Segment Matcher.

        Args:
            gtfs_manager: GTFSStaticManager with loaded GTFS data
        """
        self.gtfs_manager = gtfs_manager
        self.matched_count = 0
        self.null_count = 0

    def match_trajectory(self, feature: dict) -> bool:
        """
        Add segment_id to a trajectory feature's temporalProperties.

        Modifies the feature in-place by adding segment_id temporal property.

        Args:
            feature: MFJSON feature dictionary

        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract route and direction
            properties = feature.get("properties", {})
            route_id = properties.get("route_id")
            direction_id = properties.get("direction_id")

            if route_id is None or direction_id is None:
                logger.warning("Feature missing route_id or direction_id")
                return False

            # Get temporal properties
            temporal_props = feature.get("temporalProperties", [])
            if not temporal_props or len(temporal_props) == 0:
                logger.warning("Feature has no temporalProperties")
                return False

            temporal_data = temporal_props[0]

            # Extract stop sequences
            stop_sequence_data = temporal_data.get("current_stop_sequence", {})
            stop_sequences = stop_sequence_data.get("values", [])

            if not stop_sequences:
                logger.warning("Feature has no stop_sequence data")
                return False

            # Assign segments based on stop_sequence
            segment_ids = []

            for i, stop_seq in enumerate(stop_sequences):
                segment_id = None

                # current_stop_sequence indicates the NEXT stop the vehicle is approaching
                # So the vehicle is on segment FROM (stop_seq - 1) TO stop_seq
                if stop_seq is not None and stop_seq > 0:
                    from_seq = int(stop_seq) - 1
                    to_seq = int(stop_seq)

                    # Find segment from GTFS index
                    segment = self.gtfs_manager.find_segment(
                        route_id, int(direction_id), from_seq, to_seq
                    )

                    if segment:
                        segment_id = segment["segment_id"]
                        self.matched_count += 1
                    else:
                        self.null_count += 1
                else:
                    self.null_count += 1

                segment_ids.append(segment_id)

            # Add segment_id to temporalProperties
            temporal_data["segment_id"] = {
                "type": "Measure",
                "values": segment_ids,
                "interpolation": "Discrete",
            }

            return True

        except Exception as e:
            logger.error(f"Error matching trajectory: {e}")
            return False

    def compute_segment_statistics(
        self, features: list[dict], date: str, agency: str
    ) -> dict:
        """
        Compute segment-level statistics from matched trajectories.

        Args:
            features: List of MFJSON features with segment_id assigned
            date: Date string (YYYY-MM-DD)
            agency: Agency name

        Returns:
            Dictionary with segment statistics
        """
        try:
            # Group data by segment
            segment_data = defaultdict(
                lambda: {
                    "observations": [],
                    "trips": set(),
                    "route_id": None,
                    "direction_id": None,
                    "from_stop_name": None,
                    "to_stop_name": None,
                }
            )

            # Process each trajectory
            for feature in features:
                properties = feature.get("properties", {})
                route_id = properties.get("route_id")
                direction_id = properties.get("direction_id")
                trip_id = properties.get("trip_id")

                temporal_props = feature.get("temporalProperties", [])
                if not temporal_props:
                    continue

                temporal_data = temporal_props[0]
                datetimes = temporal_data.get("datetimes", [])
                segment_ids = temporal_data.get("segment_id", {}).get("values", [])
                occupancy_percentages = temporal_data.get(
                    "occupancy_percentage", {}
                ).get("values", [])
                occupancy_statuses = temporal_data.get("occupancy_status", {}).get(
                    "values", []
                )

                # Process each observation
                for i, segment_id in enumerate(segment_ids):
                    if segment_id is None:
                        continue

                    # Get occupancy data
                    occupancy_pct = (
                        occupancy_percentages[i]
                        if i < len(occupancy_percentages)
                        else None
                    )
                    occupancy_status = (
                        occupancy_statuses[i] if i < len(occupancy_statuses) else None
                    )
                    timestamp = datetimes[i] if i < len(datetimes) else None

                    # Store observation
                    segment_data[segment_id]["observations"].append(
                        {
                            "timestamp": timestamp,
                            "occupancy_percentage": occupancy_pct,
                            "occupancy_status": occupancy_status,
                            "trip_id": trip_id,
                        }
                    )

                    segment_data[segment_id]["trips"].add(trip_id)

                    # Store metadata (from first observation)
                    if segment_data[segment_id]["route_id"] is None:
                        segment = self.gtfs_manager.find_segment(
                            route_id,
                            int(direction_id),
                            int(segment_id.split("_")[2].split("-")[0]),
                            int(segment_id.split("_")[2].split("-")[1]),
                        )
                        if segment:
                            segment_data[segment_id]["route_id"] = route_id
                            segment_data[segment_id]["direction_id"] = direction_id
                            segment_data[segment_id]["from_stop_name"] = segment[
                                "from_stop_name"
                            ]
                            segment_data[segment_id]["to_stop_name"] = segment[
                                "to_stop_name"
                            ]

            # Compute statistics for each segment
            segments_stats = []

            for segment_id, data in segment_data.items():
                observations = data["observations"]

                if not observations:
                    continue

                # Extract occupancy percentages (filter None values)
                occupancy_pcts = [
                    obs["occupancy_percentage"]
                    for obs in observations
                    if obs["occupancy_percentage"] is not None
                ]

                # Extract occupancy statuses
                occupancy_statuses = [
                    obs["occupancy_status"]
                    for obs in observations
                    if obs["occupancy_status"] is not None
                ]

                # Compute occupancy statistics
                occupancy_stats = {}
                if occupancy_pcts:
                    # Convert to 0-1 range if needed
                    normalized_pcts = [
                        pct / 100.0 if pct > 1 else pct for pct in occupancy_pcts
                    ]

                    occupancy_stats = {
                        "mode": self._compute_mode(normalized_pcts),
                        "median": sorted(normalized_pcts)[len(normalized_pcts) // 2],
                        "min": min(normalized_pcts),
                        "max": max(normalized_pcts),
                        "p95": sorted(normalized_pcts)[int(len(normalized_pcts) * 0.95)]
                        if len(normalized_pcts) > 20
                        else max(normalized_pcts),
                    }

                    # Distribution by status
                    status_distribution = {}
                    for status in occupancy_statuses:
                        status_name = self.OCCUPANCY_STATUS_NAMES.get(
                            status, f"UNKNOWN_{status}"
                        )
                        status_distribution[status_name] = (
                            status_distribution.get(status_name, 0) + 1
                        )

                    occupancy_stats["distribution"] = status_distribution

                # Compute temporal patterns (hourly buckets)
                temporal_patterns = self._compute_temporal_patterns(observations)

                # Build segment statistics
                segment_stats = {
                    "segment_id": segment_id,
                    "from_stop_name": data["from_stop_name"],
                    "to_stop_name": data["to_stop_name"],
                    "observations": len(observations),
                    "trips": len(data["trips"]),
                    "occupancy_stats": occupancy_stats,
                    "temporal_patterns": temporal_patterns,
                }

                segments_stats.append(segment_stats)

            # Sort by segment_id
            segments_stats.sort(key=lambda s: s["segment_id"])

            # Build final statistics object
            stats = {
                "date": date,
                "agency": agency,
                "route_id": segments_stats[0]["segment_id"].split("_")[0]
                if segments_stats
                else None,
                "direction_id": int(segments_stats[0]["segment_id"].split("_")[1])
                if segments_stats
                else None,
                "segments": segments_stats,
            }

            logger.info(
                f"Computed statistics for {len(segments_stats)} segments "
                f"({self.matched_count} matched, {self.null_count} null)"
            )

            return stats

        except Exception as e:
            logger.error(f"Error computing segment statistics: {e}")
            return {}

    def _compute_temporal_patterns(self, observations: list[dict]) -> dict:
        """
        Compute temporal patterns (time-of-day analysis) using occupancy status.

        Args:
            observations: List of observation dictionaries

        Returns:
            Dictionary with temporal patterns
        """
        # Define time buckets (hourly windows)
        buckets = {f"{h:02d}:00-{h + 1:02d}:00": [] for h in range(24)}
        bucket_ranges = [(h, h + 1) for h in range(24)]

        # Group observations by time bucket
        for obs in observations:
            timestamp = obs.get("timestamp")
            occupancy_status = obs.get("occupancy_status")

            if timestamp is None or occupancy_status is None:
                continue

            try:
                dt = datetime.fromisoformat(timestamp)
                hour = dt.hour

                # Find bucket
                for i, (start, end) in enumerate(bucket_ranges):
                    if start <= hour < end:
                        bucket_key = list(buckets.keys())[i]
                        buckets[bucket_key].append(occupancy_status)
                        break

            except Exception as e:
                logger.debug(f"Error parsing timestamp {timestamp}: {e}")
                continue

        # Compute statistics for each bucket
        patterns = {}
        for bucket_key, statuses in buckets.items():
            if statuses:
                # Compute distribution of occupancy statuses
                status_counts = Counter(statuses)
                total = len(statuses)
                distribution = {
                    self.OCCUPANCY_STATUS_NAMES.get(status, f"UNKNOWN_{status}"): count
                    for status, count in status_counts.items()
                }

                # Find mode (most common status)
                mode_status = status_counts.most_common(1)[0][0]

                patterns[bucket_key] = {
                    "mode_status": self.OCCUPANCY_STATUS_NAMES.get(
                        mode_status, f"UNKNOWN_{mode_status}"
                    ),
                    "min_status": self.OCCUPANCY_STATUS_NAMES.get(
                        min(statuses), f"UNKNOWN_{min(statuses)}"
                    ),
                    "max_status": self.OCCUPANCY_STATUS_NAMES.get(
                        max(statuses), f"UNKNOWN_{max(statuses)}"
                    ),
                    "distribution": distribution,
                    "observations": total,
                }

        return patterns

    def _compute_mode(self, values: list[float]) -> float:
        """
        Compute the mode (most frequent value) of a list.

        Args:
            values: List of numeric values

        Returns:
            The most frequent value, or the smallest if there's a tie
        """
        if not values:
            return 0.0

        counts = Counter(values)
        max_count = max(counts.values())
        modes = [val for val, count in counts.items() if count == max_count]
        return min(modes)  # Return smallest mode if there's a tie

    def get_stats(self) -> dict:
        """
        Get matching statistics.

        Returns:
            Dictionary with statistics
        """
        total = self.matched_count + self.null_count
        coverage = (self.matched_count / total * 100) if total > 0 else 0

        return {
            "matched": self.matched_count,
            "null": self.null_count,
            "total": total,
            "coverage_percent": coverage,
        }
