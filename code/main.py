import signal
import threading
import datetime
from pathlib import Path
from code.helpers.config import Config
from code.helpers.VehiclePositionFeed import VehiclePositionFeed
from code.helpers.setup_logger import logger
from code.helpers.TrajectoryAggregator import aggregate_all


_shutdown_event = threading.Event()


def signal_handler(sig, frame):  # noqa: ARG001
    """Handle shutdown signals gracefully."""
    logger.info("Shutdown signal received. Exiting...")
    _shutdown_event.set()


def collection_thread(config: Config):
    """Thread for continuous feed data collection."""
    try:
        feed = VehiclePositionFeed(
            config.feed_url,
            f"./data/{config.provider}",
            s3_bucket=config.s3_bucket,
            headers=config.get_headers(),
            query_params=config.get_query_params(),
            timeout=30,
        )
        logger.info(f"Feed initialized for provider: {config.provider}")

        logger.info("Starting feed consumption loop...")
        cycle_count = 0

        while not _shutdown_event.is_set():
            try:
                cycle_count += 1
                logger.debug(f"Feed consumption cycle #{cycle_count}")
                logger.info("Preparing to consume data...")
                feed.consume_pb()
                logger.info("Data consumed...preparing to sleep...")
                logger.debug(f"Sleeping for {feed.timeout}s before next cycle")
                _shutdown_event.wait(feed.timeout)
            except Exception as e:
                logger.exception(f"Error in feed consumption cycle #{cycle_count}: {e}")
                _shutdown_event.wait(feed.timeout)
    except Exception as e:
        logger.exception(f"Fatal error in collection thread: {e}")


def aggregation_thread(config: Config):
    """Thread for periodic trajectory aggregation."""
    try:
        data_dir = Path("./data")
        last_aggregation = None
        aggregation_hour = 2  # Run at 2 AM

        logger.info(
            f"Aggregation thread started. Will run daily at {aggregation_hour}:00 AM"
        )

        while not _shutdown_event.is_set():
            now = datetime.datetime.now()

            # Check if it's time to run aggregation
            should_run = now.hour == aggregation_hour and (
                last_aggregation is None or (now - last_aggregation).days >= 1
            )

            if should_run:
                logger.info(f"Starting scheduled aggregation at {now}")
                try:
                    # Aggregate previous day's data
                    yesterday = now - datetime.timedelta(days=1)
                    agg_count, fail_count = aggregate_all(
                        data_dir=data_dir,
                        agency=config.provider,
                        year=yesterday.year,
                        month=yesterday.month,
                        day=yesterday.day,
                        s3_bucket=config.s3_bucket,
                        delete_after_upload=True,
                        delete_raw_files=True,
                    )
                    logger.info(
                        f"Aggregation complete: {agg_count} created, {fail_count} failed"
                    )
                    last_aggregation = now
                except Exception as e:
                    logger.exception(f"Error during aggregation: {e}")

            # Sleep for 5 minutes before checking again
            _shutdown_event.wait(300)

    except Exception as e:
        logger.exception(f"Fatal error in aggregation thread: {e}")


if __name__ == "__main__":
    logger.info("Starting GTFS PB to MFJSON converter")

    try:
        config = Config()
        logger.info(
            f"Configuration loaded successfully. Provider: {config.provider}, "
            f"Feed URL: {config.feed_url}"
        )
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        raise SystemExit(1)

    try:
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Create and start threads
        t_collection = threading.Thread(
            target=collection_thread, args=(config,), daemon=False
        )
        t_aggregation = threading.Thread(
            target=aggregation_thread, args=(config,), daemon=False
        )

        logger.info("Starting collection and aggregation threads...")
        t_collection.start()
        t_aggregation.start()

        # Wait for threads to complete
        t_collection.join()
        t_aggregation.join()

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        _shutdown_event.set()
    except SystemExit as e:
        logger.error(f"System exit: {e}")
        _shutdown_event.set()
        raise
    except Exception as e:
        logger.exception(f"Unexpected error in main loop: {e}")
        _shutdown_event.set()
        raise SystemExit(1)
    finally:
        logger.info("GTFS PB to MFJSON converter stopped")
