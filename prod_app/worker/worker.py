import asyncio
import aiohttp
import asyncpg
import uvloop
from typing import List, Tuple

from .config import Settings
from .wb import card_url
from .filter import fast_filter

INSERT_SQL = """
INSERT INTO wb_cards (nm_id, basket, supplier_id, title, description, product_key, score, url)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (nm_id) DO NOTHING;
"""

CHECK_SQL = """
UPDATE scan_jobs
SET status='queued', worker_id=NULL
WHERE status='running' AND updated_at < now() - interval '30 minutes';
"""

def extract_fields(obj: dict) -> tuple[int | None, str | None, str | None]:
    # WB JSON может отличаться; оставим максимально безопасно
    supplier_id = obj.get("supplierId") or obj.get("supplier_id") or obj.get("selling", {}).get("supplierId")
    title = obj.get("imt_name") or obj.get("title") or obj.get("name") or obj.get("goodsName")
    desc = obj.get("description") or obj.get("desc") or obj.get("content", {}).get("description")
    return supplier_id, title, desc

async def run_job(cfg: Settings, conn: asyncpg.Connection, job: asyncpg.Record):
    job_id = job["job_id"]
    start_nm = int(job["start_nm"])
    end_nm = int(job["end_nm"])
    basket = int(job["basket"])

    sem = asyncio.Semaphore(cfg.concurrency)
    timeout = aiohttp.ClientTimeout(total=cfg.timeout_s)
    connector = aiohttp.TCPConnector(limit=cfg.concurrency, ttl_dns_cache=300)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    buffer: List[Tuple] = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:

        async def handle_nm(nm_id: int):
            url = card_url(nm_id, basket)
            async with sem:
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            return
                        data = await resp.json(content_type=None)
                except Exception:
                    return

            supplier_id, title, desc = extract_fields(data)
            if not title:
                return

            if cfg.enable_filter:
                fr = fast_filter(title, desc or "")
                if not fr.accept:
                    return
                product_key, score = fr.product_key, fr.score
            else:
                product_key, score = None, None

            buffer.append((nm_id, basket, supplier_id, title, desc, product_key, score, url))

            if len(buffer) >= cfg.insert_batch:
                await flush()

        async def flush():
            nonlocal buffer
            if not buffer:
                return
            # executemany быстрее и проще
            await conn.executemany(INSERT_SQL, buffer)
            buffer = []

        tasks = []
        for nm_id in range(start_nm, end_nm + 1):
            tasks.append(asyncio.create_task(handle_nm(nm_id)))
            if len(tasks) >= cfg.concurrency * 4:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)

        await flush()

    return job_id

async def main_loop():
    uvloop.install()
    cfg = Settings()
    import asyncpg
    from .db import create_pool
    from .jobs import claim_jobs, mark_done, mark_failed
    
    pool = await create_pool(cfg)

    while True:
        async with pool.acquire() as conn:
            await conn.execute(CHECK_SQL)
            jobs = await claim_jobs(conn, cfg)
            if not jobs:
                await asyncio.sleep(2)
                continue

            for job in jobs:
                try:
                    job_id = await run_job(cfg, conn, job)
                    await mark_done(conn, job_id)
                except Exception:
                    await mark_failed(conn, int(job["job_id"]))

if __name__ == "__main__":
    asyncio.run(main_loop())
