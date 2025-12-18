import time
from helpers.config import Config
from helpers.VehiclePositionFeed import VehiclePositionFeed
from helpers.setup_logger import logger

if __name__ == "__main__":
    try:
        config = Config()
    except ValueError as e:
        logger.error(str(e))
        raise SystemExit(1)

    x = VehiclePositionFeed(
        config.feed_url,
        config.provider,
        f"./data/{config.provider}",
        s3_bucket=config.s3_bucket,
        headers=config.get_headers(),
        query_params=config.get_query_params(),
        timeout=30,
    )
    running = True
    while running:
        x.consume_pb()
        time.sleep(x.timeout)
