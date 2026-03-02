import asyncpg
import asyncio
from db_config import DB_DSN


async def create_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=1, max_size=6)

async def main():
    pool = await create_pool(DB_DSN)
    async with pool.acquire() as conn:
        batch = """
            UPDATE wb_score_result s
            SET decision = 'reject'
            FROM nm_regex_result r
            WHERE s.nm_id = r.nm_id
            AND s.score_ver = 1
            AND s.decision = 'unknown'
            AND r.ver = 2
            AND r.reject_reason = 'NO_MATCH';
        """
        res = await conn.execute(batch)
        print(res)

if __name__ == "__main__":
    asyncio.run(main())