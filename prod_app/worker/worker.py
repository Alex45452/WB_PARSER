from __future__ import annotations

import asyncio
import logging
import time
import traceback
from typing import List, Tuple, Optional, Dict

import aiohttp
import asyncpg
import uvloop

from .config import Settings
from .wb import card_url
from .filter import fast_filter


# Requeue "зависших" running. Теперь будет heartbeat, так что живые job не переочередятся.
CHECK_SQL = """
UPDATE scan_jobs
SET status='queued', worker_id=NULL
WHERE status='running' AND updated_at < now() - interval '30 minutes';
"""

HEARTBEAT_SQL = """
UPDATE scan_jobs
SET updated_at = now()
WHERE job_id = $1 AND status='running';
"""

# Пишем job-статы + suspicious (ты уже сделал миграцию)
UPDATE_JOB_STATS_SQL = """
UPDATE scan_jobs
SET
  updated_at = now(),
  done_at = COALESCE(done_at, now()),
  stat_req = $2,
  stat_200 = $3,
  stat_not200 = $4,
  stat_timeouts = $5,
  stat_errors = $6,
  stat_accepted = $7,
  stat_db_rows = $8,
  suspicious = $9,
  suspicious_reason = $10
WHERE job_id = $1;
"""

# Один bulk INSERT на батч
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


def _clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    # Postgres text не принимает \x00
    if "\x00" in s:
        s = s.replace("\x00", "")
    return s


def extract_fields(obj: dict) -> tuple[Optional[int], Optional[str], Optional[str]]:
    supplier_id = obj.get("supplierId") or obj.get("supplier_id") or obj.get("selling", {}).get("supplierId")
    title = obj.get("imt_name") or obj.get("title") or obj.get("name") or obj.get("goodsName")
    desc = obj.get("description") or obj.get("desc") or obj.get("content", {}).get("description")
    return supplier_id, title, desc


def _make_timeout(cfg: Settings) -> aiohttp.ClientTimeout:
    total = None if cfg.http_total_timeout_s == 0 else cfg.http_total_timeout_s
    return aiohttp.ClientTimeout(
        total=total,
        connect=cfg.http_connect_timeout_s,
        sock_connect=cfg.http_sock_connect_timeout_s,
        sock_read=cfg.http_sock_read_timeout_s,
    )


def _make_connector(cfg: Settings) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        limit=cfg.connector_limit,
        limit_per_host=cfg.limit_per_host,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )


def _make_headers() -> dict:
    # Убираем br, чтобы не зависеть от brotli и не ловить 400 decode
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }


class Metrics:
    __slots__ = (
        "t0",
        "req",
        "ok200",
        "not200",
        "timeouts",
        "errors",
        "json_errors",
        "passed_filter",
        "queued",
        "inserted_batches",
        "inserted_rows",
        # статусная детализация
        "st_400", "st_401", "st_403", "st_404", "st_429",
        "st_500", "st_502", "st_503", "st_504", "st_other",
    )

    def __init__(self):
        self.t0 = time.time()
        self.req = 0
        self.ok200 = 0
        self.not200 = 0
        self.timeouts = 0
        self.errors = 0
        self.json_errors = 0

        # passed_filter = сколько прошло фильтр (или фильтр выключен)
        self.passed_filter = 0
        self.queued = 0

        self.inserted_batches = 0
        self.inserted_rows = 0

        self.st_400 = 0
        self.st_401 = 0
        self.st_403 = 0
        self.st_404 = 0
        self.st_429 = 0
        self.st_500 = 0
        self.st_502 = 0
        self.st_503 = 0
        self.st_504 = 0
        self.st_other = 0

    def count_status(self, status: int) -> None:
        if status == 400:
            self.st_400 += 1
        elif status == 401:
            self.st_401 += 1
        elif status == 403:
            self.st_403 += 1
        elif status == 404:
            self.st_404 += 1
        elif status == 429:
            self.st_429 += 1
        elif status == 500:
            self.st_500 += 1
        elif status == 502:
            self.st_502 += 1
        elif status == 503:
            self.st_503 += 1
        elif status == 504:
            self.st_504 += 1
        else:
            self.st_other += 1

    def sum_5xx(self) -> int:
        return self.st_500 + self.st_502 + self.st_503 + self.st_504


async def bulk_insert(conn: asyncpg.Connection, rows: List[Tuple]):
    nm_ids: List[int] = []
    baskets: List[int] = []
    supplier_ids: List[Optional[int]] = []
    titles: List[str] = []
    descs: List[Optional[str]] = []
    product_keys: List[Optional[str]] = []
    scores: List[Optional[float]] = []
    urls: List[str] = []

    for (nm_id, basket, supplier_id, title, desc, product_key, score, url) in rows:
        nm_ids.append(int(nm_id))
        baskets.append(int(basket))
        supplier_ids.append(int(supplier_id) if supplier_id is not None else None)

        title = _clean_text(title) or ""
        desc = _clean_text(desc)
        product_key = _clean_text(product_key)

        titles.append(title)
        descs.append(desc)
        product_keys.append(product_key)
        scores.append(float(score) if score is not None else None)
        urls.append(url)

    await conn.execute(
        BULK_INSERT_SQL,
        nm_ids, baskets, supplier_ids, titles, descs, product_keys, scores, urls
    )


def _suspicious_reason(metrics: Metrics) -> str:
    req = max(1, metrics.req)
    ok_ratio = metrics.ok200 / req
    to_ratio = metrics.timeouts / req
    block_ratio = (metrics.st_403 + metrics.st_429) / req
    not_ratio = metrics.not200 / req
    return (
        f"req={metrics.req} ok200={metrics.ok200} not200={metrics.not200} "
        f"timeouts={metrics.timeouts} errors={metrics.errors} "
        f"ok_ratio={ok_ratio:.3f} not_ratio={not_ratio:.3f} to_ratio={to_ratio:.3f} block_ratio={block_ratio:.3f} "
        f"(404={metrics.st_404} 403={metrics.st_403} 429={metrics.st_429} 5xx={metrics.sum_5xx()} other={metrics.st_other})"
    )


async def _write_job_stats(pool: asyncpg.Pool, job_id: int, metrics: Metrics, suspicious: bool, susp_reason: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            UPDATE_JOB_STATS_SQL,
            job_id,
            metrics.req,
            metrics.ok200,
            metrics.not200,
            metrics.timeouts,
            metrics.errors,
            metrics.passed_filter,
            metrics.inserted_rows,
            suspicious,
            (susp_reason or "")[:1000],
        )


async def run_job(cfg: Settings, pool: asyncpg.Pool, job: asyncpg.Record, log: logging.Logger):
    job_id = int(job["job_id"])
    start_nm = int(job["start_nm"])
    end_nm = int(job["end_nm"])
    basket = int(job["basket"])

    metrics = Metrics()
    sem = asyncio.Semaphore(cfg.concurrency)

    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_max)
    stop_sentinel = object()

    suspicious = False
    suspicious_reason = ""

    # ---- heartbeat ----
    hb_stop = asyncio.Event()

    async def heartbeat_task():
        while not hb_stop.is_set():
            try:
                async with pool.acquire() as c:
                    await c.execute(HEARTBEAT_SQL, job_id)
            except Exception:
                log.exception("heartbeat failed job_id=%s", job_id)
            await asyncio.sleep(15)

    hb = asyncio.create_task(heartbeat_task())

    # ---- writer ----
    async def writer_task():
        async with pool.acquire() as wconn:
            batch: List[Tuple] = []
            last_flush = time.time()

            while True:
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
                    try:
                        await bulk_insert(wconn, batch)
                    except Exception:
                        # Главное: writer не должен умереть. Пробуем по одной строке.
                        log.exception("bulk_insert failed: job_id=%s batch_size=%s; fallback row-by-row", job_id, len(batch))
                        for row in batch:
                            try:
                                await bulk_insert(wconn, [row])
                            except Exception:
                                log.exception("drop row nm_id=%s due to insert error", row[0])

                    metrics.inserted_batches += 1
                    metrics.inserted_rows += len(batch)
                    batch.clear()
                    last_flush = now

            # добить остаток
            if batch:
                try:
                    await bulk_insert(wconn, batch)
                except Exception:
                    log.exception("final bulk_insert failed: job_id=%s batch_size=%s; fallback row-by-row", job_id, len(batch))
                    for row in batch:
                        try:
                            await bulk_insert(wconn, [row])
                        except Exception:
                            log.exception("drop row nm_id=%s due to insert error", row[0])

                metrics.inserted_batches += 1
                metrics.inserted_rows += len(batch)
                batch.clear()

    w = asyncio.create_task(writer_task())

    # ---- metrics logger ----
    async def metrics_task():
        while True:
            await asyncio.sleep(cfg.metrics_every_s)
            elapsed = time.time() - metrics.t0
            rps = metrics.req / elapsed if elapsed > 0 else 0.0
            log.info(
                "job=%s nm=[%s..%s] basket=%s | req=%s (rps=%.1f) "
                "200=%s not200=%s (404=%s 403=%s 429=%s 5xx=%s other=%s) "
                "timeouts=%s errors=%s json_err=%s passed=%s queued=%s | "
                "db_batches=%s db_rows=%s | qsize=%s",
                job_id, start_nm, end_nm, basket,
                metrics.req, rps,
                metrics.ok200, metrics.not200,
                metrics.st_404, metrics.st_403, metrics.st_429,
                metrics.sum_5xx(), metrics.st_other,
                metrics.timeouts, metrics.errors, metrics.json_errors,
                metrics.passed_filter, metrics.queued,
                metrics.inserted_batches, metrics.inserted_rows,
                queue.qsize(),
            )

    m = asyncio.create_task(metrics_task())

    # ---- circuit breaker window ----
    win_t0 = time.time()
    win_req = 0
    win_to = 0
    storm_event = asyncio.Event()

    def window_on_req():
        nonlocal win_req
        win_req += 1

    def window_on_timeout():
        nonlocal win_to
        win_to += 1

    async def maybe_trip_timeout_storm():
        nonlocal win_t0, win_req, win_to
        now = time.time()
        if now - win_t0 < 5.0:
            return
        if win_req >= 200:
            ratio = win_to / max(1, win_req)
            if ratio >= 0.80:
                storm_event.set()
                log.warning("timeout storm detected: %.0f%% timeouts in last 5s (req=%s)", ratio * 100, win_req)
        win_t0 = now
        win_req = 0
        win_to = 0

    timeout = _make_timeout(cfg)

    async def should_abort_suspicious() -> Optional[str]:
        # Вызываем после каких-то чекпоинтов (например, после каждой пачки gather)
        if metrics.req < 5000:
            return None

        req = max(1, metrics.req)
        ok_ratio = metrics.ok200 / req
        to_ratio = metrics.timeouts / req
        block_ratio = (metrics.st_403 + metrics.st_429) / req

        # 1) timeout-storm: почти все таймауты и 200 почти нет
        if metrics.ok200 == 0 and to_ratio > 0.95:
            return "timeout-storm: no 200 responses, mostly timeouts"

        # 2) http-storm: 200 очень мало и not200 доминирует
        if ok_ratio < 0.20 and metrics.not200 > metrics.ok200:
            return f"http-storm: ok_ratio={ok_ratio:.3f}, not200>{metrics.ok200}"

        # 3) block-storm: много 403/429
        if block_ratio > 0.30:
            return f"block-storm: (403+429)/req={block_ratio:.3f}"

        return None

    current_nm = start_nm

    try:
        while current_nm <= end_nm:
            connector = _make_connector(cfg)
            headers = _make_headers()

            async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
                storm_event.clear()

                async def handle_nm(nm_id: int):
                    url = card_url(nm_id, basket)

                    async with sem:
                        metrics.req += 1
                        window_on_req()

                        try:
                            async with session.get(url) as resp:
                                status = resp.status
                                if status != 200:
                                    metrics.not200 += 1
                                    metrics.count_status(status)
                                    return

                                metrics.ok200 += 1
                                try:
                                    data = await resp.json(content_type=None)
                                except Exception:
                                    metrics.json_errors += 1
                                    return

                        except asyncio.TimeoutError:
                            metrics.timeouts += 1
                            window_on_timeout()
                            return

                        except Exception as e:
                            metrics.errors += 1
                            # первые 20 + каждый 1000-ый
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
                        metrics.passed_filter += 1
                    else:
                        product_key, score = None, None
                        metrics.passed_filter += 1

                    # очередь — backpressure
                    row = (nm_id, basket, supplier_id, title, desc, product_key, score, url)
                    await queue.put(row)
                    metrics.queued += 1

                tasks: List[asyncio.Task] = []

                while current_nm <= end_nm and not storm_event.is_set():
                    tasks.append(asyncio.create_task(handle_nm(current_nm)))
                    current_nm += 1

                    if len(tasks) >= cfg.concurrency * 4:
                        await asyncio.gather(*tasks)
                        tasks.clear()

                        await maybe_trip_timeout_storm()

                        abort_reason = await should_abort_suspicious()
                        if abort_reason:
                            raise RuntimeError(abort_reason)

                if tasks:
                    await asyncio.gather(*tasks)
                    tasks.clear()

                    await maybe_trip_timeout_storm()
                    abort_reason = await should_abort_suspicious()
                    if abort_reason:
                        raise RuntimeError(abort_reason)

                # если таймаут-шторм — пауза + пересоздать session/connector
                if storm_event.is_set():
                    suspicious = True
                    suspicious_reason = "timeout-storm-window: 80%+ timeouts in 5s"
                    log.warning("sleep 2s + recreate session/connector due to timeout storm")
                    await asyncio.sleep(2.0)
                    continue

            # session отработала нормально → выходим
            break

        # корректное завершение: дожимаем очередь
        await queue.put(stop_sentinel)
        await queue.join()
        await w

        # финальная запись stats
        await _write_job_stats(pool, job_id, metrics, suspicious, suspicious_reason)

        m.cancel()
        try:
            await m
        except asyncio.CancelledError:
            pass

        return job_id

    except Exception as e:
        # Любая ошибка job: помечаем suspicious, пишем stats и пробрасываем дальше
        suspicious = True
        suspicious_reason = f"{type(e).__name__}: {e} | {_suspicious_reason(metrics)}"
        await _write_job_stats(pool, job_id, metrics, suspicious, suspicious_reason)
        raise

    finally:
        # Остановить heartbeat
        hb_stop.set()
        hb.cancel()
        try:
            await hb
        except asyncio.CancelledError:
            pass

        # На всякий случай: если writer ещё жив — попросим его завершиться
        # (если исключение случилось раньше)
        try:
            if not w.done():
                await queue.put(stop_sentinel)
        except Exception:
            pass

        # метрик-таск
        try:
            if not m.done():
                m.cancel()
        except Exception:
            pass


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
            job_id = int(job["job_id"])
            try:
                await run_job(cfg, pool, job, log)
                async with pool.acquire() as conn:
                    await mark_done(conn, job_id)
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
                trace = traceback.format_exc()
                log.exception("job failed: job_id=%s", job_id)

                # mark_failed мог быть как старый (job_id), так и новый (job_id, reason, trace)
                async with pool.acquire() as conn:
                    try:
                        await mark_failed(conn, job_id, reason, trace)
                    except TypeError:
                        # fallback на старую сигнатуру
                        await mark_failed(conn, job_id)


if __name__ == "__main__":
    asyncio.run(main_loop())