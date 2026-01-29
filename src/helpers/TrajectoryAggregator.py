"""
Trajectory Aggregation Logic

Core functions for combining individual trajectory files into daily route summaries.
Supports optional segment matching with GTFS data and S3 uploads.
"""

import json
import logging
from pathlib import Path
from typing import Optional
from collections import defaultdict

from src.helpers.s3Uploader import upload_file

logger = logging.getLogger(__name__)


def load_mfjson(file_path: Path) -> dict:
    """Load and parse MFJSON file."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        raise


def aggregate_trajectories(features: list[dict]) -> dict:
    """
    Combine multiple trajectory features into a single FeatureCollection.

    Assigns sequential trajectory_id values (0, 1, 2, ...).
    """
    # Assign sequential trajectory IDs
    for idx, feature in enumerate(features):
        if "properties" in feature:
            feature["properties"]["trajectory_id"] = idx

    return {"type": "FeatureCollection", "features": features}


def aggregate_day(
    data_dir: Path,
    agency: str,
    route_id: str,
    year: int,
    month: int,
    day: int,
) -> tuple[Optional[dict], list[Path]]:
    """
    Aggregate all trajectories for a specific route and day.

    Args:
        data_dir: Data directory path
        agency: Agency name
        route_id: Route ID
        year: Year
        month: Month
        day: Day

    Returns:
        Tuple of (Aggregated MFJSON or None, file paths)
    """
    day_dir = (
        data_dir
        / agency
        / "raw"
        / route_id
        / f"Year={year}"
        / f"Month={month:02d}"
        / f"Day={day:02d}"
    )

    if not day_dir.exists():
        logger.debug(f"Directory not found: {day_dir}")
        return None, []

    # Find all mfjson files
    mfjson_files = sorted(day_dir.glob("*.mfjson"))

    if not mfjson_files:
        logger.debug(f"No MFJSON files in {day_dir}")
        return None, []

    logger.debug(f"Found {len(mfjson_files)} files in {day_dir}")

    all_features = []

    for file_path in mfjson_files:
        try:
            mfjson = load_mfjson(file_path)

            # Extract features from FeatureCollection
            if isinstance(mfjson, dict) and mfjson.get("type") == "FeatureCollection":
                features = mfjson.get("features", [])
                all_features.extend(features)
            else:
                logger.warning(
                    f"Invalid format in {file_path}: expected FeatureCollection"
                )

        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            continue

    if not all_features:
        logger.debug(
            f"No features collected for {agency}/{route_id}/{year}-{month:02d}-{day:02d}"
        )
        return None, []

    logger.info(
        f"  Aggregated {len(all_features)} trajectories for "
        f"{agency}/{route_id}/{year}-{month:02d}-{day:02d}"
    )

    return aggregate_trajectories(all_features), mfjson_files


def save_aggregated(
    aggregated_data: dict,
    output_dir: Path,
    s3_bucket: Optional[str] = None,
    delete_after_upload: bool = False,
) -> tuple[bool, Optional[str]]:
    """Save aggregated MFJSON to file and optionally upload to S3.

    Returns:
        Tuple of (success: bool, file_path: str or None)
    """
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "aggregated.mfjson"

        with open(output_file, "w") as f:
            json.dump(aggregated_data, f, indent=2)

        logger.debug(f"Saved aggregated data to {output_file}")

        # Upload to S3 if bucket is specified
        if s3_bucket:
            # Build S3 path: aggregated/{agency}/{route}/{date}/aggregated.mfjson
            relative_path = output_file.relative_to(output_file.parents[6])
            s3_path = f"aggregated/{relative_path}"

            with open(output_file, "r") as f:
                file_data = f.read()

            if upload_file(file_data, s3_bucket, str(s3_path)):
                logger.info(f"Uploaded to S3: s3://{s3_bucket}/{s3_path}")

                # Delete local file if requested
                if delete_after_upload:
                    try:
                        output_file.unlink()
                        logger.debug(f"Deleted local file: {output_file}")
                    except Exception as e:
                        logger.error(f"Failed to delete {output_file}: {e}")
                        return False, str(output_file)
            else:
                logger.error(f"Failed to upload {output_file} to S3")
                return False, str(output_file)

        return True, str(output_file)

    except Exception as e:
        logger.error(f"Failed to save aggregated data to {output_dir}: {e}")
        return False, None


def find_date_ranges(
    data_dir: Path, agency: str, route_id: Optional[str] = None
) -> dict:
    """
    Find all available year/month/day combinations for routes.

    Returns dict: {route_id: set of (year, month, day) tuples}
    """
    date_ranges = defaultdict(set)

    raw_dir = data_dir / agency / "raw"

    if not raw_dir.exists():
        return date_ranges

    # Find all routes
    for route_dir in raw_dir.iterdir():
        if not route_dir.is_dir() or route_dir.name.startswith("."):
            continue

        current_route = route_dir.name

        if route_id and current_route != route_id:
            continue

        # Find all Year directories
        for year_dir in route_dir.glob("Year=*"):
            try:
                year = int(year_dir.name.split("=")[1])

                # Find all Month directories
                for month_dir in year_dir.glob("Month=*"):
                    try:
                        month = int(month_dir.name.split("=")[1])

                        # Find all Day directories
                        for day_dir in month_dir.glob("Day=*"):
                            try:
                                day = int(day_dir.name.split("=")[1])
                                date_ranges[current_route].add((year, month, day))
                            except ValueError:
                                continue

                    except ValueError:
                        continue

            except ValueError:
                continue

    return date_ranges


def aggregate_all(
    data_dir: Path = Path("./data"),
    agency: Optional[str] = None,
    route_id: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
    s3_bucket: Optional[str] = None,
    delete_after_upload: bool = False,
    delete_raw_files: bool = False,
) -> tuple[int, int]:
    """
    Programmatic interface for aggregating trajectories.

    Args:
        data_dir: Path to data directory
        agency: Specific agency to aggregate (None for all)
        route_id: Specific route_id to aggregate (None for all)
        year: Specific year to aggregate (None for all)
        month: Specific month to aggregate (None for all)
        day: Specific day to aggregate (None for all)
        s3_bucket: S3 bucket name for uploading (None to skip S3)
        delete_after_upload: Delete local aggregated files after S3 upload
        delete_raw_files: Delete raw individual files after aggregation

    Returns:
        Tuple of (total_aggregated, total_failed)
    """
    logger.info("Starting trajectory aggregation")
    logger.info(f"Data directory: {data_dir.absolute()}")

    if agency:
        logger.info(f"Agency filter: {agency}")
    if route_id:
        logger.info(f"Route filter: {route_id}")
    if year:
        logger.info(f"Year filter: {year}")
    if month:
        logger.info(f"Month filter: {month}")
    if day:
        logger.info(f"Day filter: {day}")
    if s3_bucket:
        logger.info(f"S3 bucket: {s3_bucket}")
        if delete_after_upload:
            logger.info("Delete after upload: True")

    logger.info("")

    # Determine agencies to process
    agencies_to_process = []

    if agency:
        agencies_to_process = [agency]
    else:
        # Find all agencies
        for item in data_dir.iterdir():
            if item.is_dir() and not item.name.startswith("."):
                agencies_to_process.append(item.name)

    if not agencies_to_process:
        logger.warning("No agencies found")
        return 0, 0

    total_aggregated = 0
    total_failed = 0

    for current_agency in sorted(agencies_to_process):
        logger.info(f"Processing agency: {current_agency}")

        # Find available date ranges for this agency
        date_ranges = find_date_ranges(data_dir, current_agency, route_id)

        if not date_ranges:
            logger.info(f"  No routes found for {current_agency}")
            continue

        for current_route_id in sorted(date_ranges.keys()):
            logger.info(f"  Route {current_route_id}:")

            for y, m, d in sorted(date_ranges[current_route_id]):
                # Apply filters
                if year and y != year:
                    continue
                if month and m != month:
                    continue
                if day and d != day:
                    continue

                # Aggregate
                aggregated, raw_files = aggregate_day(
                    data_dir, current_agency, current_route_id, y, m, d
                )

                if aggregated:
                    # Save to aggregated directory
                    output_dir = (
                        data_dir
                        / current_agency
                        / "aggregated"
                        / current_route_id
                        / f"Year={y}"
                        / f"Month={m:02d}"
                        / f"Day={d:02d}"
                    )

                    success, _ = save_aggregated(
                        aggregated,
                        output_dir,
                        s3_bucket=s3_bucket,
                        delete_after_upload=delete_after_upload,
                    )
                    if success:
                        total_aggregated += 1

                        # Delete raw files if requested and aggregation successful
                        if delete_raw_files and raw_files:
                            logger.info(f"    Deleting {len(raw_files)} raw files")
                            for raw_file in raw_files:
                                try:
                                    raw_file.unlink()
                                    logger.debug(f"      Deleted {raw_file.name}")
                                except Exception as e:
                                    logger.error(
                                        f"      Failed to delete {raw_file}: {e}"
                                    )
                    else:
                        total_failed += 1

                else:
                    logger.debug(f"    No data to aggregate for {y}-{m:02d}-{d:02d}")

        logger.info("")

    logger.info("Aggregation complete")
    logger.info(f"Total files created: {total_aggregated}")
    if total_failed > 0:
        logger.error(f"Total failures: {total_failed}")

    return total_aggregated, total_failed
