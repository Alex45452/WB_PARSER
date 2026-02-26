# runner.py
# LIMIT-batch runner for 33M rows
# Reads unprocessed rows in nm_id order using LIMIT, applies regex rules, writes to result table, marks processed.

import time
from typing import List, Tuple

from config import (
    RULES_PATH,
    PARSE_VERSION,
    BATCH_SIZE,
    WRITE_CHUNK,
    ONLY_UNPROCESSED,
    REPROCESS_ALL,
)
from rules_loader import load_rules
from classifier import classify_one
from db import connect, upsert_results, mark_processed, mark_error
from logger import setup_logging


def fetch_batch(conn, last_nm_id: int, batch_size: int) -> List[Tuple[int, str, str]]:
    """
    Fetch next batch using LIMIT.
    IMPORTANT: This uses parse_status = 0 only (no OR with NULL).
    Make sure parse_status is NOT NULL DEFAULT 0, or prefill NULLs to 0.
    """
    where = []
    params = []

    if not REPROCESS_ALL:
        if ONLY_UNPROCESSED:
            where.append("parse_status = 0")
        else:
            where.append("parse_status = 0")

    where.append("nm_id > %s")
    params.append(last_nm_id)

    where_sql = "WHERE " + " AND ".join(where) if where else "WHERE nm_id > %s"

    sql = f"""
      SELECT nm_id, title, description
      FROM wb_cards
      {where_sql}
      ORDER BY nm_id
      LIMIT %s
    """
    params.append(batch_size)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [(int(r[0]), r[1], r[2]) for r in cur.fetchall()]


def run():
    logger = setup_logging()
    logger.info(
        "Starting LIMIT-batch regex pipeline ver=%s batch_size=%s write_chunk=%s only_unprocessed=%s reprocess_all=%s rules=%s",
        PARSE_VERSION, BATCH_SIZE, WRITE_CHUNK, ONLY_UNPROCESSED, REPROCESS_ALL, RULES_PATH,
    )

    rules = load_rules(RULES_PATH)

    processed = accepted = rejected = unknown = 0
    t0 = time.time()
    last_log_t = t0
    last_log_processed = 0

    last_nm_id = 0  # resume cursor; if you want resume after restart, persist this somewhere

    with connect() as conn:
        while True:
            rows = fetch_batch(conn, last_nm_id, BATCH_SIZE)
            if not rows:
                break

            batch_rows = []
            batch_ids = []

            batch_min_id = rows[0][0]
            batch_max_id = rows[-1][0]

            try:
                for nm_id, title, description in rows:
                    res = classify_one(rules, title, description)

                    dec = res["decision"]
                    if dec == "accept":
                        accepted += 1
                    elif dec == "reject":
                        rejected += 1
                    else:
                        unknown += 1

                    batch_rows.append((
                        nm_id,
                        PARSE_VERSION,
                        dec,
                        res["category"],
                        res["target"],
                        res["reject_reason"],
                        bool(res["title_used"]),
                        res["matched_in"],
                    ))
                    batch_ids.append(nm_id)

                # Write results in chunks
                for i in range(0, len(batch_rows), WRITE_CHUNK):
                    upsert_results(conn, batch_rows[i:i + WRITE_CHUNK])

                # Mark processed
                mark_processed(conn, batch_ids)

                conn.commit()
                processed += len(batch_rows)

                last_nm_id = batch_max_id  # advance cursor ONLY after commit

            except Exception as e:
                conn.rollback()
                logger.exception(
                    "Batch failed size=%d nm_id_range=%s..%s error=%s",
                    len(rows), batch_min_id, batch_max_id, repr(e),
                )
                try:
                    mark_error(conn, batch_ids)
                    conn.commit()
                    logger.warning("Marked batch as error parse_status=2 size=%d", len(batch_ids))
                except Exception as e2:
                    conn.rollback()
                    logger.exception("Failed to mark error batch. error=%s", repr(e2))
                raise

            # progress log every ~10s
            now = time.time()
            if now - last_log_t >= 10:
                dt_total = now - t0
                rps_total = processed / dt_total if dt_total > 0 else 0

                dt_window = now - last_log_t
                dp = processed - last_log_processed
                rps_window = dp / dt_window if dt_window > 0 else 0

                logger.info(
                    "progress processed=%d rps=%.1f (window %.1f) last_nm_id=%d accept=%d reject=%d unknown=%d",
                    processed, rps_total, rps_window, last_nm_id, accepted, rejected, unknown
                )
                last_log_t = now
                last_log_processed = processed

    dt = time.time() - t0
    rps = processed / dt if dt > 0 else 0
    logger.info("DONE processed=%d rps=%.1f accept=%d reject=%d unknown=%d last_nm_id=%d",
                processed, rps, accepted, rejected, unknown, last_nm_id)


if __name__ == "__main__":
    run()