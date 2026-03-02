import json
import time
import asyncio
import logging
import signal
from collections import Counter

import aiohttp
import uvloop

from .config import load_config
from .db import create_pool, select_batch, insert_results
from .http_client import fetch_json
from .keyword_profiles import load_keyword_profiles, group_by_category
from .scoring import compute_score

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# ---------------- LOGGING ----------------

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("wb-score-worker")


# ---------------- WORKER ----------------

async def run() -> None:
    log = setup_logging()
    cfg = load_config()

    log.info("Starting worker")
    log.info(f"REGEX_VER={cfg.regex_ver} SCORE_VER={cfg.score_ver}")
    log.info(f"BATCH_SIZE={cfg.batch_size} HTTP_CONCURRENCY={cfg.http_concurrency}")

    profiles = load_keyword_profiles(cfg.keywords_path)
    prof_by_cat = group_by_category(profiles)
    log.info(f"Loaded {len(profiles)} keyword profiles")

    pool = await create_pool(cfg.db_dsn)
    sem = asyncio.Semaphore(cfg.http_concurrency)

    total_processed = 0
    total_fetch_ok = 0
    total_started_at = time.time()

    stop_event = asyncio.Event()

    def handle_sigterm():
        log.warning("Received shutdown signal, stopping after current batch...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_sigterm)
    loop.add_signal_handler(signal.SIGINT, handle_sigterm)

    async with aiohttp.ClientSession(
        headers={"User-Agent": "wb-score-worker/1.0"}
    ) as session:

        while not stop_event.is_set():

            async with pool.acquire() as conn:
                batch = await select_batch(
                    conn,
                    cfg.regex_ver,
                    cfg.score_ver,
                    cfg.batch_size,
                )

            if not batch:
                log.info("No more rows to score. Exiting.")
                break

            batch_started_at = time.time()
            decision_counter = Counter()
            batch_fetch_ok = 0
            out_rows = []

            async def process_one(rec):
                nonlocal batch_fetch_ok

                nm_id = int(rec["nm_id"])
                category = rec["category"]
                target = rec["target"]
                regex_decision = rec["regex_decision"]
                title = rec["title"] or ""
                desc = rec["description"] or ""
                url = rec["url"]

                card = None
                if url and url.startswith("http"):
                    async with sem:
                        card = await fetch_json(session, url, cfg.http_timeout_s)

                if card:
                    batch_fetch_ok += 1

                options = card.get("options") if isinstance(card, dict) else None

                profiles_for_category = prof_by_cat.get(category, [])
                res = compute_score(
                    nm_id=nm_id,
                    category=category,
                    target=target,
                    title=title,
                    description=desc,
                    options=options,
                    profiles_for_category=profiles_for_category,
                    regex_decision=regex_decision,
                )

                decision_counter[res.decision] += 1

                res.explain["card_fetch"] = {
                    "ok": bool(card),
                    "url_present": bool(url),
                }

                out_rows.append((
                    nm_id,
                    cfg.score_ver,
                    float(res.score),
                    res.decision,
                    category,
                    target,
                    json.dumps(res.explain, ensure_ascii=False),
                ))

            await asyncio.gather(*(process_one(r) for r in batch))

            async with pool.acquire() as conn:
                await insert_results(conn, out_rows)

            # ---- metrics ----
            batch_time = time.time() - batch_started_at
            batch_size = len(out_rows)
            batch_rps = batch_size / batch_time if batch_time > 0 else 0

            total_processed += batch_size
            total_fetch_ok += batch_fetch_ok

            total_time = time.time() - total_started_at
            total_rps = total_processed / total_time if total_time > 0 else 0
            fetch_rate = (
                (batch_fetch_ok / batch_size) * 100 if batch_size > 0 else 0
            )

            log.info(
                f"Batch done | size={batch_size} "
                f"time={batch_time:.2f}s rps={batch_rps:.1f} "
                f"fetch_ok={fetch_rate:.1f}% "
                f"decisions={dict(decision_counter)}"
            )

            log.info(
                f"TOTAL | processed={total_processed} "
                f"rps={total_rps:.1f}"
            )

    await pool.close()
    log.info("Worker stopped cleanly.")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()