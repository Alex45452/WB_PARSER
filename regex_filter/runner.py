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


def run():
    rules = load_rules(RULES_PATH)

    processed = accepted = rejected = unknown = 0
    t0 = time.time()

    with connect() as conn:
        cur = stream_cards(conn, ONLY_UNPROCESSED, REPROCESS_ALL, BATCH_SIZE)

        batch_rows = []
        batch_ids = []

        def flush():
            nonlocal processed, batch_rows, batch_ids
            if not batch_rows:
                return
            try:
                # write results in chunks
                for i in range(0, len(batch_rows), WRITE_CHUNK):
                    upsert_results(conn, batch_rows[i:i + WRITE_CHUNK])

                mark_processed(conn, batch_ids)
                conn.commit()
                processed += len(batch_rows)
            except Exception:
                conn.rollback()
                try:
                    mark_error(conn, batch_ids)
                    conn.commit()
                except Exception:
                    conn.rollback()
                raise
            finally:
                batch_rows = []
                batch_ids = []

        for nm_id, title, description in cur:
            res = classify_one(rules, title, description)

            dec = res["decision"]
            if dec == "accept":
                accepted += 1
            elif dec == "reject":
                rejected += 1
            else:
                unknown += 1

            batch_rows.append((
                int(nm_id),
                PARSE_VERSION,
                dec,
                res["category"],
                res["target"],
                res["reject_reason"],
                bool(res["title_used"]),
                res["matched_in"],
            ))
            batch_ids.append(int(nm_id))

            if len(batch_rows) >= BATCH_SIZE:
                flush()
                dt = time.time() - t0
                rps = processed / dt if dt > 0 else 0
                print(f"processed={processed} rps={rps:.1f} accept={accepted} reject={rejected} unknown={unknown}")

        flush()

    dt = time.time() - t0
    rps = processed / dt if dt > 0 else 0
    print(f"DONE processed={processed} rps={rps:.1f} accept={accepted} reject={rejected} unknown={unknown}")


if __name__ == "__main__":
    run()