from dataclasses import dataclass
import os

def _f(name: str, default: str) -> float:
    return float(os.getenv(name, default))

def _i(name: str, default: str) -> int:
    return int(os.getenv(name, default))

@dataclass(frozen=True)
class Settings:
    db_dsn: str = os.getenv("DB_DSN", "postgresql://wb:wb_pass_change_me@127.0.0.1:6432/wbdb")
    worker_id: str = os.getenv("WORKER_ID", "worker-1")

    concurrency: int = _i("CONCURRENCY", "300")

    # Раздельные таймауты aiohttp (сек)
    http_connect_timeout_s: float = _f("HTTP_CONNECT_TIMEOUT", "2.0")
    http_sock_connect_timeout_s: float = _f("HTTP_SOCK_CONNECT_TIMEOUT", "2.0")
    http_sock_read_timeout_s: float = _f("HTTP_SOCK_READ_TIMEOUT", "6.0")
    # total=None => не режем весь запрос общим total, иначе p99 будет “упираться” в total
    http_total_timeout_s: float = _f("HTTP_TOTAL_TIMEOUT", "0")  # 0 => отключить total

    # Ограничения коннектора
    connector_limit: int = _i("CONNECTOR_LIMIT", "300")
    limit_per_host: int = _i("LIMIT_PER_HOST", "100")

    job_batch: int = _i("JOB_BATCH", "1")
    insert_batch: int = _i("INSERT_BATCH", "2000")

    # очередь writer-а (чтобы не раздувать RAM)
    queue_max: int = _i("QUEUE_MAX", "20000")

    # метрики
    metrics_every_s: float = _f("METRICS_EVERY", "2.0")

    # фильтр
    enable_filter: bool = os.getenv("ENABLE_FILTER", "0") == "1"
