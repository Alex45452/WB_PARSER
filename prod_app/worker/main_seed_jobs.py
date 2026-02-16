import asyncio
import asyncpg
from .config import Settings
from .basket_ranges import RANGES_EXACT

UPSERT_RANGE = """
INSERT INTO basket_ranges(basket,start_nm,end_nm)
VALUES ($1,$2,$3)
ON CONFLICT (basket) DO UPDATE SET start_nm=EXCLUDED.start_nm, end_nm=EXCLUDED.end_nm, updated_at=now();
"""

INSERT_JOB = """
INSERT INTO scan_jobs(start_nm,end_nm,basket,status)
VALUES ($1,$2,$3,'queued');
"""

CHUNK = 2_000_000

async def main():
    cfg = Settings()
    conn = await asyncpg.connect(cfg.db_dsn)

    # ranges
    for s,e,b in RANGES_EXACT:
        await conn.execute(UPSERT_RANGE, b, s, e)

    # jobs
    for s,e,b in RANGES_EXACT:
        cur = s
        while cur <= e:
            end = min(cur + CHUNK - 1, e)
            await conn.execute(INSERT_JOB, cur, end, b)
            cur = end + 1

    await conn.close()
    print("Seeded basket_ranges and scan_jobs")

if __name__ == "__main__":
    asyncio.run(main())
