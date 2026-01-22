#!/usr/bin/env python3
"""
Trajectory Aggregation CLI

Workflow runner for combining individual trajectory files into daily route summaries.
Uses core logic from TrajectoryAggregator.

Usage:
    python aggregate_trajectories.py [OPTIONS]

Examples:
    # Aggregate all data
    python aggregate_trajectories.py

    # Aggregate specific agency
    python aggregate_trajectories.py --agency MBTA

    # Aggregate specific route and date
    python aggregate_trajectories.py --agency MBTA --route 57 --year 2025 --month 12 --day 19

    # Aggregate specific year/month
    python aggregate_trajectories.py --agency MBTA --year 2025 --month 12

    # Upload to S3
    python aggregate_trajectories.py --s3-bucket my-bucket

    # Upload to S3 and delete local aggregated files
    python aggregate_trajectories.py --s3-bucket my-bucket --delete-after-upload

    # Delete raw individual files after aggregation
    python aggregate_trajectories.py --delete-raw-files

    # Full cleanup: Upload to S3, delete aggregated files, delete raw files
    python aggregate_trajectories.py --s3-bucket my-bucket --delete-after-upload --delete-raw-files
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.helpers.TrajectoryAggregator import aggregate_all

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Setup logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Run trajectory aggregation as CLI."""
    parser = argparse.ArgumentParser(
        description="Aggregate individual trajectories into daily route summaries"
    )
    parser.add_argument(
        "--agency", type=str, help="Specific agency to aggregate (default: all)"
    )
    parser.add_argument(
        "--route", type=str, help="Specific route_id to aggregate (default: all)"
    )
    parser.add_argument(
        "--year", type=int, help="Specific year to aggregate (default: all)"
    )
    parser.add_argument(
        "--month", type=int, help="Specific month to aggregate (default: all)"
    )
    parser.add_argument(
        "--day", type=int, help="Specific day to aggregate (default: all)"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="Data directory (default: ./data)",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        help="S3 bucket for uploading aggregated files (optional)",
    )
    parser.add_argument(
        "--delete-after-upload",
        action="store_true",
        help="Delete local aggregated files after successful S3 upload",
    )
    parser.add_argument(
        "--delete-raw-files",
        action="store_true",
        help="Delete raw individual trajectory files after aggregation",
    )

    args = parser.parse_args()

    aggregate_all(
        data_dir=Path(args.data_dir),
        agency=args.agency,
        route_id=args.route,
        year=args.year,
        month=args.month,
        day=args.day,
        s3_bucket=args.s3_bucket,
        delete_after_upload=args.delete_after_upload,
        delete_raw_files=args.delete_raw_files,
    )


if __name__ == "__main__":
    main()
