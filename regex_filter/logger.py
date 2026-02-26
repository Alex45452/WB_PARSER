import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logging() -> logging.Logger:
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    log_path = os.environ.get("LOG_PATH", "regex_pipeline.log")
    max_bytes = int(os.environ.get("LOG_MAX_BYTES", str(50 * 1024 * 1024)))  # 50MB
    backup_count = int(os.environ.get("LOG_BACKUP_COUNT", "5"))

    logger = logging.getLogger("regex_pipeline")
    logger.setLevel(level)
    logger.propagate = False

    # avoid duplicate handlers if imported twice
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler with rotation
    fh = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger