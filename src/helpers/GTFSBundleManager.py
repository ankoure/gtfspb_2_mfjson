"""
GTFS Bundle Management Logic

Core functions for downloading, extracting, and managing GTFS static bundles.
Builds and caches segment indexes for map matching.
"""

import json
import logging
import shutil
import zipfile
from pathlib import Path
from datetime import datetime
import requests

from src.helpers.GTFSStaticManager import GTFSStaticManager

logger = logging.getLogger(__name__)


def download_gtfs_bundle(url: str, output_path: Path) -> bool:
    """
    Download GTFS bundle from URL.

    Args:
        url: URL to GTFS zip file
        output_path: Path to save downloaded file

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading GTFS bundle from {url}")

        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(
            f"Downloaded {output_path.stat().st_size / 1024:.1f} KB to {output_path}"
        )
        return True

    except Exception as e:
        logger.error(f"Error downloading GTFS bundle: {e}")
        return False


def extract_gtfs_bundle(zip_path: Path, extract_to: Path) -> bool:
    """
    Extract GTFS zip file to directory.

    Args:
        zip_path: Path to GTFS zip file
        extract_to: Directory to extract to

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Extracting GTFS bundle to {extract_to}")

        # Create extraction directory
        extract_to.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)

        # List extracted files
        extracted_files = list(extract_to.glob("*.txt"))
        logger.info(
            f"Extracted {len(extracted_files)} files: {[f.name for f in extracted_files]}"
        )

        return True

    except Exception as e:
        logger.error(f"Error extracting GTFS bundle: {e}")
        return False


def archive_current_bundle(current_path: Path, archive_path: Path) -> bool:
    """
    Archive current GTFS bundle before replacing.

    Args:
        current_path: Path to current bundle
        archive_path: Path to archive directory

    Returns:
        True if successful, False otherwise
    """
    if not current_path.exists():
        logger.info("No current bundle to archive")
        return True

    try:
        # Create archive with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        archive_dir = archive_path / timestamp

        logger.info(f"Archiving current bundle to {archive_dir}")
        shutil.copytree(current_path, archive_dir)

        return True

    except Exception as e:
        logger.error(f"Error archiving current bundle: {e}")
        return False


def save_metadata(bundle_path: Path, url: str = None, build_date: str = None) -> bool:
    """
    Save metadata about GTFS bundle.

    Args:
        bundle_path: Path to bundle directory
        url: URL where bundle was downloaded from
        build_date: Date bundle was built

    Returns:
        True if successful, False otherwise
    """
    try:
        metadata = {
            "installed_date": datetime.now().isoformat(),
            "url": url,
            "build_date": build_date,
        }

        metadata_file = bundle_path / "metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Saved metadata to {metadata_file}")
        return True

    except Exception as e:
        logger.error(f"Error saving metadata: {e}")
        return False


def build_segment_index(agency: str, data_dir: Path) -> bool:
    """
    Build segment index from current GTFS bundle.

    Args:
        agency: Agency name
        data_dir: Data directory path

    Returns:
        True if successful, False otherwise
    """
    try:
        gtfs_path = data_dir / agency / "gtfs_static" / "current"

        if not gtfs_path.exists():
            logger.error(f"GTFS bundle not found at {gtfs_path}")
            return False

        # Initialize manager and load bundle
        manager = GTFSStaticManager(gtfs_path)

        if not manager.load_gtfs_bundle():
            logger.error("Failed to load GTFS bundle")
            return False

        # Build segment index
        if not manager.build_segment_index():
            logger.error("Failed to build segment index")
            return False

        # Save segment index
        index_path = data_dir / agency / "gtfs_static" / "segments.json"
        if not manager.save_segment_index(index_path):
            logger.error("Failed to save segment index")
            return False

        # Print statistics
        stats = manager.get_stats()
        logger.info("=" * 60)
        logger.info("GTFS Bundle Statistics:")
        logger.info(f"  Routes: {stats.get('routes', 0)}")
        logger.info(f"  Stops: {stats.get('stops', 0)}")
        logger.info(f"  Trips: {stats.get('trips', 0)}")
        logger.info(f"  Indexed Routes: {stats.get('indexed_routes', 0)}")
        logger.info(f"  Total Segments: {stats.get('total_segments', 0)}")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error(f"Error building segment index: {e}")
        return False


def show_stats(agency: str, data_dir: Path) -> bool:
    """
    Show statistics about current GTFS bundle.

    Args:
        agency: Agency name
        data_dir: Data directory path

    Returns:
        True if successful, False otherwise
    """
    try:
        gtfs_path = data_dir / agency / "gtfs_static" / "current"

        if not gtfs_path.exists():
            logger.error(f"GTFS bundle not found at {gtfs_path}")
            return False

        # Load metadata if available
        metadata_file = gtfs_path / "metadata.json"
        if metadata_file.exists():
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
            logger.info("=" * 60)
            logger.info("Bundle Metadata:")
            logger.info(f"  Installed: {metadata.get('installed_date', 'Unknown')}")
            logger.info(f"  Source URL: {metadata.get('url', 'Unknown')}")
            logger.info(f"  Build Date: {metadata.get('build_date', 'Unknown')}")
            logger.info("=" * 60)

        # Initialize manager and load bundle
        manager = GTFSStaticManager(gtfs_path)

        if not manager.load_gtfs_bundle():
            logger.error("Failed to load GTFS bundle")
            return False

        # Load segment index if available
        index_path = data_dir / agency / "gtfs_static" / "segments.json"
        if index_path.exists():
            manager.load_segment_index(index_path)

        # Print statistics
        stats = manager.get_stats()
        logger.info("=" * 60)
        logger.info("GTFS Bundle Statistics:")
        logger.info(f"  Routes: {stats.get('routes', 0)}")
        logger.info(f"  Stops: {stats.get('stops', 0)}")
        logger.info(f"  Trips: {stats.get('trips', 0)}")
        logger.info(f"  Segment Index Built: {stats.get('segment_index_built', False)}")
        if stats.get("segment_index_built"):
            logger.info(f"  Indexed Routes: {stats.get('indexed_routes', 0)}")
            logger.info(f"  Total Segments: {stats.get('total_segments', 0)}")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error(f"Error showing statistics: {e}")
        return False
