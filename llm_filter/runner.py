from __future__ import annotations
import asyncio
import json
import logging
from collections import Counter
from typing import Any

from .config import Config
from .db import DB
from .wb_http import WBHttp
from .supplier_checker import SupplierChecker
from .targets import load_targets_by_categories
from .enrich import (
    fetch_card_json, fetch_feedbacks, fetch_questions,
    squeeze_feedbacks, squeeze_questions, extract_model_from_options
)
from .llm_client import LLMClient
from .prompts import USER_TEMPLATE
from .fast_reject import fast_accessory_reject


def setup_json_logger() -> logging.Logger:
    logger = logging.getLogger("llm_filter")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler()

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "lvl": record.levelname,
                "msg": record.getMessage(),
                "name": record.name,
            }
            if hasattr(record, "extra") and isinstance(record.extra, dict):
                payload.update(record.extra)
            return json.dumps(payload, ensure_ascii=False)

    h.setFormatter(JsonFormatter())
    logger.addHandler(h)
    return logger


SELECT_CANDIDATES_SQL = """
with candidates as (
  select r.nm_id, r.category, r.target
  from wb_score_result r
  where r.score_ver = $1
    and r.decision = 'accept'
    and r.score >= $2
)
select c.nm_id, c.category,
       w.basket, w.supplier_id, w.title, w.description, w.url
from candidates c
join wb_cards w on w.nm_id = c.nm_id
left join wb_llm_result l
  on l.nm_id = c.nm_id and l.llm_ver = $3
where l.nm_id is null
order by c.nm_id
limit $4
"""


INSERT_RESULT_SQL = """
insert into wb_llm_result
(nm_id, llm_ver, status, supplier_decision, supplier_reason,
 decision, category, target, confidence, reject_reason, signals, explain,
 model, attempts, last_error, created_at, updated_at)
values
($1,$2,$3,$4,$5,
 $6,$7,$8,$9,$10,$11,$12,
 $13,$14,$15, now(), now())
on conflict (nm_id, llm_ver) do update set
  status=excluded.status,
  supplier_decision=excluded.supplier_decision,
  supplier_reason=excluded.supplier_reason,
  decision=excluded.decision,
  category=excluded.category,
  target=excluded.target,
  confidence=excluded.confidence,
  reject_reason=excluded.reject_reason,
  signals=excluded.signals,
  explain=excluded.explain,
  model=excluded.model,
  attempts=excluded.attempts,
  last_error=excluded.last_error,
  updated_at=now()
"""

UPSERT_BLACKLIST_SQL = """
insert into wb_supplier_name_lists(list_name, exact_name, note)
values ('blacklist', $1, $2)
on conflict (list_name, exact_name) do nothing
"""

def _short(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    t = text.strip()
    return t[:limit] if len(t) > limit else t

def build_user_payload(items: list[dict[str, Any]]) -> str:
    return USER_TEMPLATE + "\n" + json.dumps(items, ensure_ascii=False)

def detect_model_conflict(title: str | None, desc: str | None, opt_model: str | None) -> bool:
    # лёгкая эвристика: если в title есть "m4/ps5 pro/2025/16/512" и в options явно другое — конфликт.
    # Основной конфликт пусть ловит LLM, но на fallback очередь нам нужен триггер.
    if not title or not opt_model:
        return False
    t = title.lower()
    o = opt_model.lower()
    # если opt_model вообще не встречается в title и сильно отличается — считаем конфликтом
    if o and (o[:6] not in t) and (o not in t):
        return True
    return False

async def run_llm_and_store(db: DB, llm: LLMClient, cfg: Config, logger: logging.Logger,
                            batch: list[dict[str, Any]], model_name: str,
                            stats: Counter) -> None:
    payload = build_user_payload(batch)
    out = await llm.classify_batch(payload, retries=cfg.llm_retry)
    results = out.get("results") or []

    idx = {int(r["nm_id"]): r for r in results if "nm_id" in r}

    for it in batch:
        nm_id = it["nm_id"]
        r = idx.get(nm_id)
        if not r:
            stats["llm_missing_item"] += 1
            await db.execute(
                INSERT_RESULT_SQL,
                nm_id, cfg.llm_ver,
                "llm_done", it["seller"]["supplier_decision"], it["seller"]["supplier_reason"],
                "review", it["category"], None, 0.2, "missing_llm_output",
                json.dumps({"missing_llm_output": True}),
                json.dumps({"note": "No item in LLM response for this nm_id"}),
                model_name, 1, None
            )
            continue

        stats[f"llm_{r['decision']}"] += 1

        # If authenticity risk => blacklist seller
        signals = r.get("signals") or {}
        auth_risk = bool(signals.get("authenticity_risk") or signals.get("not_original") or False)
        if auth_risk:
            seller_name = it["seller"].get("seller_name") or ""
            if seller_name:
                await db.execute(UPSERT_BLACKLIST_SQL, seller_name, f"auto_blacklist: nm_id={nm_id}")

        await db.execute(
            INSERT_RESULT_SQL,
            nm_id, cfg.llm_ver,
            "llm_done", it["seller"]["supplier_decision"], it["seller"]["supplier_reason"],
            r["decision"], r["category"], r["target"], float(r["confidence"]), r["reject_reason"],
            json.dumps(signals),
            json.dumps(r.get("explain") or {}),
            model_name, 1, None
        )

async def main() -> None:
    cfg = Config()
    logger = setup_json_logger()

    db = DB(cfg.pg_dsn)
    await db.open()

    targets = load_targets_by_categories("/mnt/data/keywords_by_categories.txt").mapping

    wb = WBHttp(
        timeout_s=cfg.wb_timeout_s,
        concurrency=cfg.wb_concurrency,
        breaker_timeouts=20,
        breaker_sleep_s=120
    )

    supplier_checker = SupplierChecker(
        db=db, wb=wb,
        min_val=cfg.min_valuation,
        strict_min=cfg.strict_min, strict_max=cfg.strict_max,
        min_reviews=cfg.min_reviews,
        min_age_days=cfg.min_age_days,
        strict_age_days=180,
        strict_min_sales=5000,
        strict_min_supp_ratio=80,
        strict_min_ratio_mark=2
    )

    llm_main = LLMClient(api_key=cfg.openai_api_key, model=cfg.openai_model_main)
    llm_fb = LLMClient(api_key=cfg.openai_api_key, model=cfg.openai_model_fallback)

    stats = Counter()
    review_queue: list[dict[str, Any]] = []

    try:
        while True:
            rows = await db.fetch(
                SELECT_CANDIDATES_SQL,
                cfg.score_ver, cfg.score_min, cfg.llm_ver, cfg.llm_batch_size
            )
            if not rows:
                break

            batch_main: list[dict[str, Any]] = []

            for r in rows:
                nm_id = int(r["nm_id"])
                category = str(r["category"] or "")
                allowed = targets.get(category) or []

                if not allowed:
                    stats["category_not_mapped"] += 1
                    await db.execute(
                        INSERT_RESULT_SQL,
                        nm_id, cfg.llm_ver,
                        "llm_done", "pass", "category_not_in_mapping",
                        "review", category, None, 0.2, "category_not_mapped",
                        json.dumps({"category_mapped": False}),
                        json.dumps({"note": "Category not found in keywords_by_categories mapping"}),
                        cfg.openai_model_main, 1, None
                    )
                    continue

                # cheap accessory/game/spare-part reject
                is_rej, reason = fast_accessory_reject(r["title"], r["description"])
                if is_rej:
                    stats["fast_accessory_reject"] += 1
                    await db.execute(
                        INSERT_RESULT_SQL,
                        nm_id, cfg.llm_ver,
                        "llm_done", "pass", "fast_reject",
                        "reject", category, None, 0.95, "accessory_or_game",
                        json.dumps({"fast_reject": True, "reason": reason}),
                        json.dumps({}),
                        cfg.openai_model_main, 1, None
                    )
                    continue

                supplier_id = int(r["supplier_id"])
                sdec = await supplier_checker.check(supplier_id)

                if sdec.decision in ("blacklist_reject", "reject"):
                    stats["supplier_reject"] += 1
                    await db.execute(
                        INSERT_RESULT_SQL,
                        nm_id, cfg.llm_ver,
                        "supplier_reject", sdec.decision, sdec.reason,
                        "reject", category, None, 0.99, "seller_reject",
                        json.dumps({
                            "seller_name": sdec.seller_name_norm,
                            "supplier_decision": sdec.decision,
                            "supplier_reason": sdec.reason
                        }),
                        json.dumps({}),
                        cfg.openai_model_main, 1, None
                    )
                    continue

                card_json_url = str(r["url"])  # confirmed: card.json

                # enrich
                card = await fetch_card_json(
                    wb,
                    card_json_url
                )

                imt_id = int(card.get("imt_id") or card.get("imtId") or 0)
                opt_model = extract_model_from_options(card)

                fb_raw = await fetch_feedbacks(wb, imt_id) if imt_id else {}
                q_raw = await fetch_questions(wb, imt_id, take=30, skip=0) if imt_id else {}

                item = {
                    "nm_id": nm_id,
                    "category": category,
                    "allowed_targets": allowed,
                    "title": _short(r["title"], 260),
                    "description": _short(r["description"], 900),
                    "seller": {
                        "supplier_id": supplier_id,
                        "seller_name": sdec.seller_name_norm,
                        "supplier_decision": sdec.decision,
                        "supplier_reason": sdec.reason,
                        "profile": sdec.profile_raw and {
                            "valuation": sdec.profile_raw.get("valuationToHundredths") or sdec.profile_raw.get("valuation"),
                            "feedbacksCount": sdec.profile_raw.get("feedbacksCount"),
                            "registrationDate": sdec.profile_raw.get("registrationDate"),
                            "saleItemQuantity": sdec.profile_raw.get("saleItemQuantity"),
                            "suppRatio": sdec.profile_raw.get("suppRatio"),
                            "ratioMarkSupp": sdec.profile_raw.get("ratioMarkSupp"),
                            "deliveryDuration": sdec.profile_raw.get("deliveryDuration"),
                        } or None
                    },
                    "card_model_option": opt_model,
                    "feedbacks": squeeze_feedbacks(fb_raw, limit=5),
                    "questions": squeeze_questions(q_raw, limit=10),
                }

                # If whitelist seller: allow main model, but still can go review later
                # If model conflict heuristic => push to review queue for fallback model
                if detect_model_conflict(item["title"], item["description"], opt_model):
                    stats["model_conflict_hint"] += 1
                    review_queue.append(item)
                else:
                    batch_main.append(item)

            if batch_main:
                await run_llm_and_store(db, llm_main, cfg, logger, batch_main, cfg.openai_model_main, stats)

            # periodic log
            logger.info(
                "batch_done",
                extra={
                    "extra": {
                        "stats_top": stats.most_common(10),
                        "review_queue_len": len(review_queue),
                    }
                }
            )

        # SECOND PASS: fallback model for review queue
        # chunk smaller to keep context safe
        fb_batch_size = max(4, min(8, cfg.llm_batch_size // 2))
        for i in range(0, len(review_queue), fb_batch_size):
            chunk = review_queue[i:i + fb_batch_size]
            await run_llm_and_store(db, llm_fb, cfg, logger, chunk, cfg.openai_model_fallback, stats)

        logger.info("done", extra={"extra": {"stats_all": stats.most_common(50)}})

    finally:
        await wb.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())