import json
import asyncio
import aiohttp
import uvloop

from .config import load_config
from .db import create_pool, select_batch, insert_results
from .http_client import fetch_json
from .keyword_profiles import load_keyword_profiles, group_by_category
from .scoring import compute_score
from .slots import options_to_text

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def run() -> None:
    cfg = load_config()

    profiles = load_keyword_profiles(cfg.keywords_path)
    prof_by_cat = group_by_category(profiles)

    pool = await create_pool(cfg.db_dsn)
    sem = asyncio.Semaphore(cfg.http_concurrency)

    async with aiohttp.ClientSession(headers={"User-Agent": "wb-score-worker/1.0"}) as session:
        while True:
            async with pool.acquire() as conn:
                batch = await select_batch(conn, cfg.regex_ver, cfg.score_ver, cfg.batch_size)

            if not batch:
                print("No more rows. Exit.")
                break

            out_rows = []

            async def process_one(rec):
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

                options = card.get("options") if isinstance(card, dict) else None

                # compute score with keyword profiles
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

                res.explain["card_fetch"] = {"ok": bool(card), "url_present": bool(url)}

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

            print(f"Scored: {len(out_rows)}")

    await pool.close()

def main() -> None:
    asyncio.run(run())

if __name__ == "__main__":
    main()