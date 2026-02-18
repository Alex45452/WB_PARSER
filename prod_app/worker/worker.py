import asyncio
import time
import logging
from typing import List, Tuple, Optional

import aiohttp
import asyncpg
import uvloop

from .config import Settings
from .wb import card_url
from .filter import fast_filter

CHECK_SQL = """
UPDATE scan_jobs
SET status='queued', worker_id=NULL
WHERE status='running' AND updated_at < now() - interval '30 minutes';
"""

# Один bulk INSERT на батч (решение проблемы №2)
# ВАЖНО: типы под таблицу wb_cards из твоего SQL.
BULK_INSERT_SQL = """
INSERT INTO wb_cards (nm_id, basket, supplier_id, title, description, product_key, score, url)
SELECT *
FROM unnest(
  $1::bigint[],
  $2::int[],
  $3::int[],
  $4::text[],
  $5::text[],
  $6::text[],
  $7::real[],
  $8::text[]
)
ON CONFLICT (nm_id) DO NOTHING;
"""

def extract_fields(obj: dict) -> tuple[Optional[int], Optional[str], Optional[str]]:
    supplier_id = obj.get("supplierId") or obj.get("supplier_id") or obj.get("selling", {}).get("supplierId")
    title = obj.get("imt_name") or obj.get("title") or obj.get("name") or obj.get("goodsName")
    desc = obj.get("description") or obj.get("desc") or obj.get("content", {}).get("description")
    return supplier_id, title, desc

def _make_timeout(cfg: Settings) -> aiohttp.ClientTimeout:
    # total=0 => отключаем total
    total = None if cfg.http_total_timeout_s == 0 else cfg.http_total_timeout_s
    return aiohttp.ClientTimeout(
        total=total,
        connect=cfg.http_connect_timeout_s,
        sock_connect=cfg.http_sock_connect_timeout_s,
        sock_read=cfg.http_sock_read_timeout_s,
    )

class Metrics:
    __slots__ = ("t0", "req", "ok200", "not200", "timeouts", "errors",
                 "json_errors", "accepted", "queued", "inserted_batches",
                 "inserted_rows", "last_report")

    def __init__(self):
        self.t0 = time.time()
        self.req = 0
        self.ok200 = 0
        self.not200 = 0
        self.timeouts = 0
        self.errors = 0
        self.json_errors = 0
        self.accepted = 0
        self.queued = 0
        self.inserted_batches = 0
        self.inserted_rows = 0
        self.last_report = time.time()

async def bulk_insert(conn: asyncpg.Connection, rows: List[Tuple]):
    # rows: (nm_id, basket, supplier_id, title, desc, product_key, score, url)
    nm_ids = []
    baskets = []
    supplier_ids = []
    titles = []
    descs = []
    product_keys = []
    scores = []
    urls = []

    for (nm_id, basket, supplier_id, title, desc, product_key, score, url) in rows:
        nm_ids.append(int(nm_id))
        baskets.append(int(basket))
        supplier_ids.append(int(supplier_id) if supplier_id is not None else None)
        titles.append(title if title is not None else "")
        descs.append(desc if desc is not None else None)
        product_keys.append(product_key if product_key is not None else None)
        scores.append(float(score) if score is not None else None)
        urls.append(url)

    await conn.execute(
        BULK_INSERT_SQL,
        nm_ids, baskets, supplier_ids, titles, descs, product_keys, scores, urls
    )

async def run_job(cfg: Settings, pool: asyncpg.Pool, job: asyncpg.Record, log: logging.Logger):
    job_id = int(job["job_id"])
    start_nm = int(job["start_nm"])
    end_nm = int(job["end_nm"])
    basket = int(job["basket"])

    metrics = Metrics()

    sem = asyncio.Semaphore(cfg.concurrency)

    timeout = _make_timeout(cfg)
    connector = aiohttp.TCPConnector(
        limit=cfg.connector_limit,
        limit_per_host=cfg.limit_per_host,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_max)
    stop_sentinel = object()

    async def writer_task():
        # отдельное соединение под вставки (фикс проблемы №1 + не мешаем job-conn)
        async with pool.acquire() as wconn:
            batch: List[Tuple] = []
            last_flush = time.time()

            while True:
                # ждём элемент, но периодически флашим даже если батч не добился
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    item = None

                if item is stop_sentinel:
                    queue.task_done()
                    break

                if item is not None:
                    batch.append(item)
                    queue.task_done()

                now = time.time()
                if batch and (len(batch) >= cfg.insert_batch or (now - last_flush) >= 1.0):
                    await bulk_insert(wconn, batch)
                    metrics.inserted_batches += 1
                    metrics.inserted_rows += len(batch)
                    batch.clear()
                    last_flush = now

            if batch:
                await bulk_insert(wconn, batch)
                metrics.inserted_batches += 1
                metrics.inserted_rows += len(batch)
                batch.clear()

    async def metrics_task():
        while True:
            await asyncio.sleep(cfg.metrics_every_s)
            elapsed = time.time() - metrics.t0
            rps = metrics.req / elapsed if elapsed > 0 else 0
            log.info(
                "job=%s nm=[%s..%s] basket=%s | req=%s (rps=%.1f) 200=%s not200=%s timeouts=%s errors=%s json_err=%s accepted=%s queued=%s | db_batches=%s db_rows=%s | qsize=%s",
                job_id, start_nm, end_nm, basket,
                metrics.req, rps,
                metrics.ok200, metrics.not200, metrics.timeouts, metrics.errors, metrics.json_errors,
                metrics.accepted, metrics.queued,
                metrics.inserted_batches, metrics.inserted_rows,
                queue.qsize(),
            )

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        w = asyncio.create_task(writer_task())
        m = asyncio.create_task(metrics_task())

        async def handle_nm(nm_id: int):
            url = card_url(nm_id, basket)

            async with sem:
                metrics.req += 1
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            metrics.not200 += 1
                            return
                        metrics.ok200 += 1
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            metrics.json_errors += 1
                            return
                except asyncio.TimeoutError:
                    metrics.timeouts += 1
                    return
                except Exception as e:
                    metrics.errors += 1
                    # покажем первые 20 ошибок, дальше — раз в 1000
                    if metrics.errors <= 20 or metrics.errors % 1000 == 0:
                        log.warning("http error: %r (%s)", e, type(e).__name__)
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
                metrics.accepted += 1  # для тестов считаем “принято”

            row = (nm_id, basket, supplier_id, title, desc, product_key, score, url)

            # backpressure: если writer не успевает — тут притормозим, зато не сожрём RAM
            await queue.put(row)
            metrics.queued += 1

        tasks = []
        for nm_id in range(start_nm, end_nm + 1):
            tasks.append(asyncio.create_task(handle_nm(nm_id)))
            if len(tasks) >= cfg.concurrency * 4:
                await asyncio.gather(*tasks)
                tasks.clear()

        if tasks:
            await asyncio.gather(*tasks)

        # Дожимаем очередь и writer
        await queue.put(stop_sentinel)
        await queue.join()
        await w

        m.cancel()
        try:
            await m
        except asyncio.CancelledError:
            pass

    return job_id

async def main_loop():
    uvloop.install()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log = logging.getLogger("worker")

    cfg = Settings()

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
                job_id = await run_job(cfg, pool, job, log)
                async with pool.acquire() as conn:
                    await mark_done(conn, job_id)
            except Exception:
                log.exception("job failed: job_id=%s", int(job["job_id"]))
                async with pool.acquire() as conn:
                    await mark_failed(conn, int(job["job_id"]))

if __name__ == "__main__":
    asyncio.run(main_loop())
