import asyncpg
from typing import Optional, Dict, Any
from .config import Settings


MARK_FAILED_SQL = """
UPDATE scan_jobs
SET status = 'failed',
    updated_at = now(),
    failed_at = now(),
    fail_reason = $2,
    fail_trace = $3
WHERE job_id = $1;
"""

CLAIM_SQL = """
WITH cte AS (
  SELECT job_id
  FROM scan_jobs
  WHERE status='queued'
  ORDER BY job_id
  FOR UPDATE SKIP LOCKED
  LIMIT $1
)
UPDATE scan_jobs j
SET status='running', worker_id=$2, updated_at=now(), attempts=attempts+1
FROM cte
WHERE j.job_id = cte.job_id
RETURNING j.job_id, j.start_nm, j.end_nm, j.basket;
"""

DONE_SQL = "UPDATE scan_jobs SET status='done', updated_at=now() WHERE job_id=$1;"
FAIL_SQL = "UPDATE scan_jobs SET status='failed', updated_at=now() WHERE job_id=$1;"

async def claim_jobs(conn: asyncpg.Connection, cfg: Settings):
    return await conn.fetch(CLAIM_SQL, cfg.job_batch, cfg.worker_id)

async def mark_done(conn: asyncpg.Connection, job_id: int):
    await conn.execute(DONE_SQL, job_id)

async def mark_failed(conn: asyncpg.Connection, job_id: int, reason: str, trace: str) -> None:
    # Ограничим размер, чтобы случайно не залить в БД мегабайты
    reason = (reason or "")[:500]
    trace = (trace or "")[:8000]
    await conn.execute(MARK_FAILED_SQL, job_id, reason, trace)

