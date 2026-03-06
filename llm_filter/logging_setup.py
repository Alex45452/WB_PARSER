from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Any, Mapping


@dataclass(frozen=True)
class LogContext:
    run_id: str


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "lvl": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        extra = getattr(record, "extra", None)
        if isinstance(extra, Mapping):
            payload.update(extra)

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def setup_logger(app_name: str = "llm_filter") -> tuple[logging.Logger, LogContext]:
    """
    Creates:
      - stdout JSON logs
      - logs/llm_filter.log (rotating)
      - logs/llm_filter.error.log (rotating, ERROR+)
    Env:
      LOG_DIR, LOG_LEVEL, LOG_MAX_BYTES, LOG_BACKUP_COUNT, RUN_ID
    """
    log_dir = os.environ.get("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)

    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    run_id = os.environ.get("RUN_ID") or uuid.uuid4().hex[:10]
    ctx = LogContext(run_id=run_id)

    logger = logging.getLogger(app_name)
    logger.setLevel(level)
    logger.propagate = False

    # Clear previous handlers
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = JsonFormatter()

    # stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    max_bytes = int(os.environ.get("LOG_MAX_BYTES", str(50 * 1024 * 1024)))  # 50MB
    backups = int(os.environ.get("LOG_BACKUP_COUNT", "5"))

    # file handler
    fh = RotatingFileHandler(
        os.path.join(log_dir, f"{app_name}.log"),
        maxBytes=max_bytes,
        backupCount=backups,
        encoding="utf-8",
    )
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # error file handler
    eh = RotatingFileHandler(
        os.path.join(log_dir, f"{app_name}.error.log"),
        maxBytes=max_bytes,
        backupCount=backups,
        encoding="utf-8",
    )
    eh.setLevel(logging.ERROR)
    eh.setFormatter(fmt)
    logger.addHandler(eh)

    logger.info(
        "logger_ready",
        extra={"extra": {"run_id": run_id, "level": level_name, "log_dir": log_dir}},
    )
    return logger, ctx


class StepTimer:
    def __init__(self) -> None:
        self.t0 = time.perf_counter()

    def ms(self) -> int:
        return int((time.perf_counter() - self.t0) * 1000)


def log(logger: logging.Logger, ctx: LogContext, event: str, **fields: Any) -> None:
    logger.info(event, extra={"extra": {"run_id": ctx.run_id, **fields}})


def log_err(logger: logging.Logger, ctx: LogContext, event: str, **fields: Any) -> None:
    logger.error(event, extra={"extra": {"run_id": ctx.run_id, **fields}})