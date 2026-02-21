from __future__ import annotations

import asyncpg
from typing import List


CLAIM_JOBS_SQL = """
WITH picked AS (
    SELECT job_id
    FROM scan_jobs
    WHERE status = 'queued'
    ORDER BY job_id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE scan_jobs j
SET
    status = 'running',
    worker_id = $2,
    attempts = COALESCE(attempts, 0) + 1,
    updated_at = now()
FROM picked
WHERE j.job_id = picked.job_id
RETURNING j.job_id, j.start_nm, j.end_nm, j.basket;
"""


MARK_DONE_SQL = """
UPDATE scan_jobs
SET
    status = 'done',
    done_at = now(),
    updated_at = now()
WHERE job_id = $1;
"""


MARK_FAILED_SQL = """
UPDATE scan_jobs
SET
    status = 'failed',
    failed_at = now(),
    updated_at = now(),
    fail_reason = $2,
    fail_trace = $3
WHERE job_id = $1;
"""


REQUEUE_JOB_SQL = """
UPDATE scan_jobs
SET
    status = 'queued',
    worker_id = NULL,
    updated_at = now()
WHERE job_id = $1;
"""


async def claim_jobs(conn: asyncpg.Connection, cfg) -> List[asyncpg.Record]:
    """
    Забираем пачку queued задач и отмечаем running.
    Требования к cfg:
      - cfg.job_batch: int (сколько задач брать за раз)
      - cfg.worker_id: str (идентификатор воркера)
    """
    batch = int(getattr(cfg, "job_batch", 1))
    worker_id = str(getattr(cfg, "worker_id", "worker-unknown"))
    rows = await conn.fetch(CLAIM_JOBS_SQL, batch, worker_id)
    return list(rows)


async def mark_done(conn: asyncpg.Connection, job_id: int) -> None:
    await conn.execute(MARK_DONE_SQL, int(job_id))


async def mark_failed(conn: asyncpg.Connection, job_id: int, reason: str, trace: str) -> None:
    # ограничиваем размер, чтобы не залить в БД мегабайты
    reason = (reason or "")[:500]
    trace = (trace or "")[:8000]
    await conn.execute(MARK_FAILED_SQL, int(job_id), reason, trace)


async def requeue_job(conn: asyncpg.Connection, job_id: int) -> None:
    """
    Полезно для ручного переисполнения сомнительных jobs:
    возвращает задачу обратно в queued.
    """
    await conn.execute(REQUEUE_JOB_SQL, int(job_id))