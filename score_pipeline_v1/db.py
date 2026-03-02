import asyncpg
from typing import List, Tuple, Optional, Any, Dict

SRC_DECISIONS = ("accept", "unknown")

async def create_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=1, max_size=6)

async def select_batch(conn: asyncpg.Connection, regex_ver: int, score_ver: int, batch_size: int):
    q = """
    WITH pick AS (
      SELECT r.nm_id, r.category, r.target, r.decision
      FROM nm_regex_result r
      WHERE r.ver = $1
        AND r.decision = ANY($2::text[])
        AND NOT EXISTS (
          SELECT 1 FROM wb_score_result s
          WHERE s.nm_id = r.nm_id AND s.score_ver = $3
        )
      ORDER BY r.nm_id
      LIMIT $4
    )
    SELECT
      p.nm_id, p.category, p.target, p.decision AS regex_decision,
      c.title, c.description, c.url
    FROM pick p
    JOIN wb_cards c ON c.nm_id = p.nm_id
    ORDER BY p.nm_id;
    """
    return await conn.fetch(q, regex_ver, list(SRC_DECISIONS), score_ver, batch_size)

async def insert_results(conn: asyncpg.Connection, rows: List[Tuple[int,int,float,str,Optional[str],Optional[str],str]]):
    if not rows:
        return
    q = """
    INSERT INTO wb_score_result (nm_id, score_ver, score, decision, category, target, explain)
    VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
    ON CONFLICT (nm_id, score_ver) DO NOTHING;
    """
    await conn.executemany(q, rows)