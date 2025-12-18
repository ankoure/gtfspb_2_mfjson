import logging
from logging.handlers import TimedRotatingFileHandler
import os

log_file = os.getenv("LOG_FILE")
log_dir = os.path.dirname(log_file)

os.makedirs(log_dir, exist_ok=True)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

file_handler = TimedRotatingFileHandler(
    "app.log", when="midnight", interval=1, backupCount=7
)
file_handler.suffix = "%Y-%m-%d"

formatter = logging.Formatter(
    "%(processName)s - %(levelname)s - %(message)s - %(asctime)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
