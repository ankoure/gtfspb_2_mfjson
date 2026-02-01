import logging
import json
from logging.handlers import TimedRotatingFileHandler
import os
from src.helpers.config import Config

# Import ddtrace for log injection (adds trace_id, span_id to logs)
try:
    from ddtrace import patch

    patch(logging=True)
except ImportError:
    pass

_logger = None


class DataDogJSONFormatter(logging.Formatter):
    """
    JSON formatter that includes DataDog trace correlation fields.
    Enables trace-log correlation in DataDog UI.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "filename": record.filename,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "agency": os.getenv("PROVIDER", "unknown"),
        }

        # Add trace correlation if available (injected by ddtrace)
        if hasattr(record, "dd"):
            log_record["dd"] = record.dd
        else:
            # Check for individual attributes
            trace_id = getattr(record, "dd.trace_id", None)
            span_id = getattr(record, "dd.span_id", None)
            if trace_id or span_id:
                log_record["dd"] = {
                    "trace_id": str(trace_id) if trace_id else "",
                    "span_id": str(span_id) if span_id else "",
                }

        # Add exception info if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


def _create_logger():
    config = Config()
    log_file = config.log_file
    log_dir = os.path.dirname(log_file)

    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers (important for pytest)
    if logger.handlers:
        return logger

    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - "
        "[%(filename)s:%(funcName)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=7
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    file_handler.suffix = "%Y-%m-%d"

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Use JSON formatter if DD_LOGS_INJECTION is enabled
    if os.getenv("DD_LOGS_INJECTION", "false").lower() == "true":
        console_handler.setFormatter(DataDogJSONFormatter())
    else:
        console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_logger():
    global _logger
    if _logger is None:
        _logger = _create_logger()
    return _logger


# Backward-compatible export
logger = logging.getLogger(__name__)
