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
    squeeze_feedbacks, squeeze_questions, extract_model_from_options,
    extract_product_rating,
)
from .llm_client import LLMClient
from .prompts import USER_TEMPLATE
from .fast_reject import fast_accessory_reject
from .logging_setup import setup_logger, log, log_err, StepTimer



# ---------- SQL ----------

SELECT_CANDIDATES_SQL = """
with candidates as (
  select r.nm_id, r.category
  from wb_score_result r
  where r.score_ver = $1
    and r.decision = 'accept'
    and r.score >= $2
)
select c.nm_id, c.category,
       w.basket, w.title, w.description, w.url
from candidates c
join wb_cards w on w.nm_id = c.nm_id
left join wb_llm_result l
  on l.nm_id = c.nm_id and l.llm_ver = $3
where l.nm_id is null
  and w.url is not null
order by c.nm_id
limit $4
"""

UPDATE_SUPPLIER_ID_SQL = """
update wb_cards
set supplier_id = $2
where nm_id = $1 and supplier_id is null
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

UPSERT_LIST_SQL = """
insert into wb_supplier_name_lists(list_name, exact_name, note)
values ($1, $2, $3)
on conflict (list_name, exact_name) do update set note = excluded.note
"""


# ---------- helpers ----------

def _short(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    t = text.strip()
    return t[:limit] if len(t) > limit else t


def build_user_payload(items: list[dict[str, Any]]) -> str:
    return USER_TEMPLATE + "\n" + json.dumps(items, ensure_ascii=False)


def norm_name(s: str) -> str:
    return " ".join(s.strip().upper().split())


def load_name_list_txt(path: str) -> set[str]:
    out: set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if not s or s.startswith("#"):
                continue
            out.add(norm_name(s))
    return out


def extract_supplier_id_from_card(card: dict[str, Any]) -> int | None:
    # Common WB shapes
    candidates = [
        ("selling", "supplier_id"),
        ("data", "supplier_id"),
        ("supplierId",),
        ("supplier_id",),
    ]
    for path in candidates:
        cur: Any = card
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and cur is not None:
            try:
                return int(cur)
            except Exception:
                pass
    return None


def detect_model_conflict(title: str | None, opt_model: str | None) -> bool:
    # lightweight heuristic to push to fallback queue
    if not title or not opt_model:
        return False
    t = title.lower()
    o = opt_model.lower()
    return bool(o) and (o not in t) and (o[:6] not in t)


# ---------- cached supplier checker wrapper ----------

class CachedSupplierChecker:
    """
    Wraps SupplierChecker but caches whitelist/blacklist sets in memory,
    so we don't hit DB per seller check.
    """
    def __init__(self, base: SupplierChecker, blacklist: set[str], whitelist: set[str]):
        self.base = base
        self.blacklist = blacklist
        self.whitelist = whitelist

    async def check(self, supplier_id: int):
        # Call base to get seller name + (maybe) profile
        # But we can short-circuit after name fetch if in list.
        # We do: fetch name cached via base internals by calling its private cache method via check()
        # The base.check already applies DB lists; however we also want in-memory lists.
        # So we replicate the initial part: call base.check but override decision if needed.
        dec = await self.base.check(supplier_id)

        # base.check returns seller_name_norm in dec, apply our lists:
        seller_name = dec.seller_name_norm or ""
        if seller_name:
            if seller_name in self.blacklist:
                return dec.__class__(
                    "blacklist_reject",
                    "seller_name_blacklisted(file)",
                    seller_name,
                    dec.name_raw,
                    dec.profile_raw,
                )
            if seller_name in self.whitelist:
                return dec.__class__(
                    "whitelist_pass",
                    "seller_name_whitelisted(file)",
                    seller_name,
                    dec.name_raw,
                    dec.profile_raw,
                )
        return dec


# ---------- LLM runner ----------

async def run_llm_and_store(
    db: DB,
    llm: LLMClient,
    cfg: Config,
    logger: logging.Logger,
    batch: list[dict[str, Any]],
    model_name: str,
    stats: Counter,
) -> list[dict[str, Any]]:
    """
    Run LLM, store results, return list of items with decision=review (2nd-wave fallback).
    """
    payload = build_user_payload(batch)
    out = await llm.classify_batch(payload, retries=cfg.llm_retry)
    results = out.get("results") or []
    idx = {int(r["nm_id"]): r for r in results if "nm_id" in r}

    review_items: list[dict[str, Any]] = []

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
            review_items.append(it)
            continue

        decision = r["decision"]
        stats[f"llm_{decision}"] += 1

        await db.execute(
            INSERT_RESULT_SQL,
            nm_id, cfg.llm_ver,
            "llm_done", it["seller"]["supplier_decision"], it["seller"]["supplier_reason"],
            decision, r["category"], r["target"], float(r["confidence"]), r["reject_reason"],
            json.dumps(r.get("signals") or {}),
            json.dumps(r.get("explain") or {}),
            model_name, 1, None
        )

        if decision == "review":
            review_items.append(it)

    return review_items


# ---------- main ----------

async def main() -> None:
    cfg = Config()
    logger = setup_json_logger()

    db = DB(cfg.pg_dsn)
    await db.open()

    # paths (safe defaults)
    keywords_path = getattr(cfg, "keywords_path", "./keywords_by_categories.yaml")
    bl_path = getattr(cfg, "blacklist_path", "./blacklist_suppliers.txt")
    wl_path = getattr(cfg, "whitelist_path", "./whitelist_suppliers.txt")

    # load targets mapping
    targets = load_targets_by_categories(keywords_path).mapping

    # load and normalize supplier name lists from files
    blacklist = load_name_list_txt(bl_path)
    whitelist = load_name_list_txt(wl_path)

    # upsert lists into DB for visibility/audit
    # (and also in case other components read from DB)
    for name in sorted(blacklist):
        await db.execute(UPSERT_LIST_SQL, "blacklist", name, "from_file:blacklist_suppliers.txt")
    for name in sorted(whitelist):
        await db.execute(UPSERT_LIST_SQL, "whitelist", name, "from_file:whitelist_suppliers.txt")

    wb = WBHttp(
        timeout_s=cfg.wb_timeout_s,
        concurrency=cfg.wb_concurrency,
        breaker_timeouts=20,  # 20 подряд таймаутов
        breaker_sleep_s=120,  # пауза 2 минуты
    )

    base_supplier_checker = SupplierChecker(
        db=db, wb=wb,
        min_val=cfg.min_valuation,
        strict_min=cfg.strict_min, strict_max=cfg.strict_max,
        min_reviews=cfg.min_reviews,
        min_age_days=cfg.min_age_days,
        strict_age_days=180,
        strict_min_sales=5000,
        strict_min_supp_ratio=80,
        strict_min_ratio_mark=2,
    )
    supplier_checker = CachedSupplierChecker(base_supplier_checker, blacklist, whitelist)

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

                card_json_url = str(r["url"])  # wb_cards.url is card.json

                # fetch card.json FIRST (we need supplier_id)
                card = await fetch_card_json(wb, card_json_url)
                supplier_id = extract_supplier_id_from_card(card)

                if supplier_id is None:
                    stats["missing_supplier_id_in_card"] += 1
                    await db.execute(
                        INSERT_RESULT_SQL,
                        nm_id, cfg.llm_ver,
                        "error", "reject", "missing_supplier_id_in_card_json",
                        "review", category, None, 0.0, "missing_supplier_id",
                        json.dumps({"card_url": card_json_url}),
                        json.dumps({}),
                        cfg.openai_model_main, 1, "supplier_id not found in card.json"
                    )
                    continue

                # persist supplier_id back to wb_cards
                await db.execute(UPDATE_SUPPLIER_ID_SQL, nm_id, int(supplier_id))

                # seller check
                sdec = await supplier_checker.check(int(supplier_id))
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

                # enrich feedbacks/questions
                imt_id = int(card.get("imt_id") or card.get("imtId") or 0)
                opt_model = extract_model_from_options(card)

                fb_raw = await fetch_feedbacks(wb, imt_id) if imt_id else {}
                product_rating = extract_product_rating(fb_raw)

                # PRODUCT RATING FILTER:
                # - rating > 0 and < 4.7 => reject
                # - rating == 0 => strict mode => fallback queue (not reject)
                if product_rating > 0.0 and product_rating < 4.7:
                    stats["product_rating_reject"] += 1
                    await db.execute(
                        INSERT_RESULT_SQL,
                        nm_id, cfg.llm_ver,
                        "llm_done", sdec.decision, sdec.reason,
                        "reject", category, None, 0.98, "low_product_rating",
                        json.dumps({"product_rating": product_rating, "rule": "rating<4.7"}),
                        json.dumps({}),
                        cfg.openai_model_main, 1, None
                    )
                    continue

                q_raw = await fetch_questions(wb, imt_id, take=30, skip=0) if imt_id else {}

                item = {
                    "nm_id": nm_id,
                    "category": category,
                    "allowed_targets": allowed,
                    "title": _short(r["title"], 260),
                    "description": _short(r["description"], 900),
                    "product_rating": product_rating,
                    "seller": {
                        "supplier_id": int(supplier_id),
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

                # strict mode for rating==0: immediately to fallback queue
                if product_rating == 0.0:
                    stats["product_rating_zero_strict"] += 1
                    review_queue.append(item)
                    continue

                # model conflict heuristic -> fallback queue
                if detect_model_conflict(item["title"], opt_model):
                    stats["model_conflict_hint"] += 1
                    review_queue.append(item)
                else:
                    batch_main.append(item)

            # MAIN LLM
            if batch_main:
                main_review = await run_llm_and_store(
                    db, llm_main, cfg, logger, batch_main, cfg.openai_model_main, stats
                )

                # SECOND-WAVE: everything review -> fallback queue
                if main_review:
                    stats["second_wave_review_to_fallback"] += len(main_review)
                    review_queue.extend(main_review)

            logger.info(
                "batch_done",
                extra={"extra": {"stats_top": stats.most_common(12), "review_queue_len": len(review_queue)}},
            )

        # FALLBACK LLM for review queue
        fb_batch_size = max(4, min(8, cfg.llm_batch_size // 2))
        for i in range(0, len(review_queue), fb_batch_size):
            chunk = review_queue[i:i + fb_batch_size]
            await run_llm_and_store(
                db, llm_fb, cfg, logger, chunk, cfg.openai_model_fallback, stats
            )

        logger.info("done", extra={"extra": {"stats_all": stats.most_common(50)}})

    finally:
        await wb.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())