import time
import signal
from helpers.config import Config
from helpers.VehiclePositionFeed import VehiclePositionFeed
from helpers.setup_logger import logger


def signal_handler(sig, frame):  # noqa: ARG001
    """Handle shutdown signals gracefully."""
    logger.info("Shutdown signal received. Exiting...")
    raise KeyboardInterrupt


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting GTFS PB to MFJSON converter")
    logger.info("=" * 60)

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
        feed = VehiclePositionFeed(
            config.feed_url,
            f"./data/{config.provider}",
            s3_bucket=config.s3_bucket,
            headers=config.get_headers(),
            query_params=config.get_query_params(),
            timeout=30,
        )
        logger.info(f"Feed initialized for provider: {config.provider}")

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("Starting feed consumption loop...")
        running = True
        cycle_count = 0

        while running:
            try:
                cycle_count += 1
                logger.debug(f"Feed consumption cycle #{cycle_count}")
                logger.info("Preparing to consume data...")
                feed.consume_pb()
                logger.info("Data consumed...preparing to sleep...")
                logger.debug(f"Sleeping for {feed.timeout}s before next cycle")
                time.sleep(feed.timeout)
            except Exception as e:
                logger.exception(f"Error in feed consumption cycle #{cycle_count}: {e}")
                # Continue processing despite errors
                time.sleep(feed.timeout)

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except SystemExit as e:
        logger.error(f"System exit: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error in main loop: {e}")
        raise SystemExit(1)
    finally:
        logger.info("=" * 60)
        logger.info("GTFS PB to MFJSON converter stopped")
        logger.info("=" * 60)
