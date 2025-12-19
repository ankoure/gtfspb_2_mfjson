import logging
from logging.handlers import TimedRotatingFileHandler
import os
from helpers.config import Config

config = Config()
log_file = config.log_file
log_dir = os.path.dirname(log_file)

if log_dir:
    os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# More detailed formatter with module and function information
detailed_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Simpler formatter for console output
console_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

# File handler with rotation (daily)
file_handler = TimedRotatingFileHandler(
    log_file, when="midnight", interval=1, backupCount=7
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(detailed_formatter)
file_handler.suffix = "%Y-%m-%d"

# Console handler for user-visible output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
