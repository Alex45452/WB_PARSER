from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    db_dsn: str = os.getenv("DB_DSN", "postgresql://wb:wb_pass_change_me@127.0.0.1:6432/wbdb")
    worker_id: str = os.getenv("WORKER_ID", "worker-1")

    concurrency: int = int(os.getenv("CONCURRENCY", "300"))
    timeout_s: float = float(os.getenv("TIMEOUT", "8"))

    job_batch: int = int(os.getenv("JOB_BATCH", "1"))         # сколько jobs брать за раз
    insert_batch: int = int(os.getenv("INSERT_BATCH", "2000"))# сколько строк в батч INSERT

    # фильтр
    enable_filter: bool = os.getenv("ENABLE_FILTER", "1") == "1"
