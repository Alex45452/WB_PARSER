import time

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
from db import connect, stream_cards, upsert_results, mark_processed, mark_error
from logger import setup_logging


def run():
    logger = setup_logging()
    logger.info(
        "Starting regex pipeline ver=%s batch_size=%s write_chunk=%s only_unprocessed=%s reprocess_all=%s rules=%s",
        PARSE_VERSION, BATCH_SIZE, WRITE_CHUNK, ONLY_UNPROCESSED, REPROCESS_ALL, RULES_PATH,
    )

    rules = load_rules(RULES_PATH)

    processed = accepted = rejected = unknown = 0
    t0 = time.time()
    last_log_t = t0
    last_log_processed = 0

    with connect() as conn:
        cur = stream_cards(conn, ONLY_UNPROCESSED, REPROCESS_ALL, BATCH_SIZE)
        logger.info("DB stream cursor started.")

        batch_rows = []
        batch_ids = []
        # to help debug a failing batch
        batch_min_id = None
        batch_max_id = None

        def flush():
            nonlocal processed, batch_rows, batch_ids, batch_min_id, batch_max_id

            if not batch_rows:
                return

            try:
                for i in range(0, len(batch_rows), WRITE_CHUNK):
                    upsert_results(conn, batch_rows[i:i + WRITE_CHUNK])

                mark_processed(conn, batch_ids)
                conn.commit()

                processed += len(batch_rows)
                logger.debug("Committed batch size=%d nm_id_range=%s..%s", len(batch_rows), batch_min_id, batch_max_id)

            except Exception as e:
                conn.rollback()
                logger.exception(
                    "Batch failed size=%d nm_id_range=%s..%s error=%s",
                    len(batch_rows), batch_min_id, batch_max_id, repr(e),
                )
                try:
                    mark_error(conn, batch_ids)
                    conn.commit()
                    logger.warning("Marked batch as error parse_status=2 size=%d", len(batch_rows))
                except Exception as e2:
                    conn.rollback()
                    logger.exception("Failed to mark error batch. error=%s", repr(e2))
                raise
            finally:
                batch_rows = []
                batch_ids = []
                batch_min_id = None
                batch_max_id = None

        for nm_id, title, description in cur:
            nm_id = int(nm_id)
            if batch_min_id is None:
                batch_min_id = nm_id
            batch_max_id = nm_id

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

            if len(batch_rows) >= BATCH_SIZE:
                flush()

                now = time.time()
                if now - last_log_t >= 10:  # log every ~10s
                    dt_total = now - t0
                    rps_total = processed / dt_total if dt_total > 0 else 0

                    dt_window = now - last_log_t
                    dp = processed - last_log_processed
                    rps_window = dp / dt_window if dt_window > 0 else 0

                    logger.info(
                        "progress processed=%d rps=%.1f (window %.1f) accept=%d reject=%d unknown=%d",
                        processed, rps_total, rps_window, accepted, rejected, unknown
                    )
                    last_log_t = now
                    last_log_processed = processed

        flush()

    dt = time.time() - t0
    rps = processed / dt if dt > 0 else 0
    logger.info("DONE processed=%d rps=%.1f accept=%d reject=%d unknown=%d", processed, rps, accepted, rejected, unknown)


if __name__ == "__main__":
    run()