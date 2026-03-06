from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from typing import Any

from .config import Config
from .db import DB
from .wb_http import WBHttp
from .supplier_checker import SupplierChecker
from .targets import load_targets_by_categories
from .enrich import (
    fetch_card_json,
    fetch_feedbacks,
    fetch_questions,
    squeeze_feedbacks,
    squeeze_questions,
    extract_model_from_options,
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

INSERT_UNPARSED_SUPPLIER_SQL = """
insert into wb_supplier_unparsed (supplier_id, nm_id, card_url, reason, http_status)
values ($1,$2,$3,$4,$5)
on conflict (supplier_id, nm_id) do update set
  reason = excluded.reason,
  http_status = excluded.http_status,
  created_at = now()
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


def parse_id_set(env_value: str | None) -> set[int]:
    if not env_value:
        return set()
    out: set[int] = set()
    for part in env_value.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.add(int(p))
        except Exception:
            pass
    return out


def extract_supplier_id_from_card(card: dict[str, Any]) -> int | None:
    paths = [
        ("selling", "supplier_id"),
        ("data", "supplier_id"),
        ("supplierId",),
        ("supplier_id",),
    ]
    for path in paths:
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
    if not title or not opt_model:
        return False
    t = title.lower()
    o = opt_model.lower()
    return bool(o) and (o not in t) and (o[:6] not in t)


class CachedSupplierChecker:
    """
    SupplierChecker uses DB name lists; we additionally keep file-based lists in memory,
    and also supports whitelist by supplier_id via SupplierChecker itself.
    """
    def __init__(self, base: SupplierChecker, blacklist: set[str], whitelist: set[str]):
        self.base = base
        self.blacklist = blacklist
        self.whitelist = whitelist

    async def check(self, supplier_id: int):
        dec = await self.base.check(supplier_id)
        seller_name = dec.seller_name_norm or ""

        if seller_name:
            if seller_name in self.blacklist:
                return dec.__class__(
                    "blacklist_reject",
                    "seller_name_blacklisted(file)",
                    seller_name,
                    dec.name_raw,
                    dec.profile_raw,
                    dec.http_status,
                )
            if seller_name in self.whitelist:
                return dec.__class__(
                    "whitelist_pass",
                    "seller_name_whitelisted(file)",
                    seller_name,
                    dec.name_raw,
                    dec.profile_raw,
                    dec.http_status,
                )
        return dec


async def run_llm_and_store(
    db: DB,
    llm: LLMClient,
    cfg: Config,
    logger,
    lctx,
    batch: list[dict[str, Any]],
    model_name: str,
    stats: Counter,
) -> list[dict[str, Any]]:
    t = StepTimer()
    log(logger, lctx, "llm_call", model=model_name, n_items=len(batch))

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
            log_err(logger, lctx, "llm_missing_item", nm_id=nm_id, model=model_name)

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

    log(
        logger, lctx, "llm_done",
        model=model_name,
        n_items=len(batch),
        elapsed_ms=t.ms(),
        accept=stats.get("llm_accept", 0),
        reject=stats.get("llm_reject", 0),
        review=stats.get("llm_review", 0),
    )
    return review_items


async def main() -> None:
    cfg = Config()
    logger, lctx = setup_logger("llm_filter")

    db = DB(cfg.pg_dsn)
    await db.open()

    keywords_path = getattr(cfg, "keywords_path", "/mnt/data/keywords_by_categories.yaml")
    bl_path = getattr(cfg, "blacklist_path", "/mnt/data/blacklist_suppliers.txt")
    wl_path = getattr(cfg, "whitelist_path", "/mnt/data/whitelist_suppliers.txt")

    targets = load_targets_by_categories(keywords_path).mapping

    blacklist = load_name_list_txt(bl_path)
    whitelist = load_name_list_txt(wl_path)

    for name in sorted(blacklist):
        await db.execute(UPSERT_LIST_SQL, "blacklist", name, "from_file:blacklist_suppliers.txt")
    for name in sorted(whitelist):
        await db.execute(UPSERT_LIST_SQL, "whitelist", name, "from_file:whitelist_suppliers.txt")

    # ✅ supplier_id whitelist via ENV + default for 28976
    supplier_id_whitelist = {28976}
    supplier_id_whitelist |= parse_id_set(os.environ.get("SUPPLIER_ID_WHITELIST"))

    log(
        logger, lctx, "startup",
        llm_ver=cfg.llm_ver,
        score_ver=cfg.score_ver,
        batch_size=cfg.llm_batch_size,
        wb_concurrency=cfg.wb_concurrency,
        targets_categories=len(targets),
        blacklist_size=len(blacklist),
        whitelist_size=len(whitelist),
        supplier_id_whitelist=sorted(list(supplier_id_whitelist))[:20],
        keywords_path=keywords_path,
    )

    wb = WBHttp(
        timeout_s=cfg.wb_timeout_s,
        concurrency=cfg.wb_concurrency,
        breaker_timeouts=20,
        breaker_sleep_s=120,
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
        supplier_id_whitelist=supplier_id_whitelist,
    )
    supplier_checker = CachedSupplierChecker(base_supplier_checker, blacklist, whitelist)

    llm_main = LLMClient(api_key=cfg.openai_api_key, model=cfg.openai_model_main)
    llm_fb = LLMClient(api_key=cfg.openai_api_key, model=cfg.openai_model_fallback)

    stats = Counter()
    review_queue: list[dict[str, Any]] = []

    try:
        batch_no = 0
        while True:
            rows = await db.fetch(
                SELECT_CANDIDATES_SQL,
                cfg.score_ver, cfg.score_min, cfg.llm_ver, cfg.llm_batch_size
            )
            if not rows:
                break

            batch_no += 1
            log(logger, lctx, "batch_start", batch_no=batch_no, n_rows=len(rows))

            batch_main: list[dict[str, Any]] = []
            batch_timer = StepTimer()

            for r in rows:
                nm_id = int(r["nm_id"])
                category = str(r["category"] or "")
                allowed = targets.get(category) or []

                card_timer = StepTimer()

                if not allowed:
                    stats["category_not_mapped"] += 1
                    log(logger, lctx, "category_not_mapped", nm_id=nm_id, category=category)
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

                # pre-reject accessories/games/spares
                is_rej, reason = fast_accessory_reject(r["title"], r["description"])
                if is_rej:
                    stats["fast_accessory_reject"] += 1
                    log(logger, lctx, "fast_reject", nm_id=nm_id, category=category, reason=reason)
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

                card_json_url = str(r["url"])
                log(logger, lctx, "card_fetch_start", nm_id=nm_id, url=card_json_url)

                try:
                    card = await fetch_card_json(wb, card_json_url)
                except Exception:
                    stats["card_fetch_error"] += 1
                    log_err(logger, lctx, "card_fetch_error", nm_id=nm_id, url=card_json_url)
                    await db.execute(
                        INSERT_RESULT_SQL,
                        nm_id, cfg.llm_ver,
                        "error", "reject", "card_fetch_error",
                        "review", category, None, 0.0, "card_fetch_error",
                        json.dumps({"card_url": card_json_url}),
                        json.dumps({}),
                        cfg.openai_model_main, 1, "card.json fetch failed"
                    )
                    continue

                supplier_id = extract_supplier_id_from_card(card)
                if supplier_id is None:
                    stats["missing_supplier_id_in_card"] += 1
                    log_err(logger, lctx, "missing_supplier_id_in_card", nm_id=nm_id, url=card_json_url)
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

                # save supplier_id back to wb_cards
                await db.execute(UPDATE_SUPPLIER_ID_SQL, nm_id, int(supplier_id))

                # seller check
                sdec = await supplier_checker.check(int(supplier_id))

                if sdec.decision == "unparsed":
                    stats["supplier_unparsed"] += 1
                    log(
                        logger, lctx, "supplier_unparsed",
                        nm_id=nm_id,
                        supplier_id=int(supplier_id),
                        reason=sdec.reason,
                        http_status=sdec.http_status,
                    )
                    await db.execute(
                        INSERT_UNPARSED_SUPPLIER_SQL,
                        int(supplier_id),
                        nm_id,
                        card_json_url,
                        sdec.reason,
                        sdec.http_status,
                    )
                    await db.execute(
                        INSERT_RESULT_SQL,
                        nm_id, cfg.llm_ver,
                        "error", "reject", sdec.reason,
                        "review", category, None, 0.0, "supplier_name_unavailable",
                        json.dumps({"supplier_id": int(supplier_id), "card_url": card_json_url, "reason": sdec.reason, "http_status": sdec.http_status}),
                        json.dumps({}),
                        cfg.openai_model_main, 1, sdec.reason
                    )
                    continue

                if sdec.decision in ("blacklist_reject", "reject"):
                    stats["supplier_reject"] += 1
                    log(
                        logger, lctx, "supplier_reject",
                        nm_id=nm_id,
                        supplier_id=int(supplier_id),
                        seller_name=sdec.seller_name_norm,
                        supplier_decision=sdec.decision,
                        reason=sdec.reason,
                        elapsed_ms=card_timer.ms(),
                    )
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

                # enrich product signals
                imt_id = int(card.get("imt_id") or card.get("imtId") or 0)
                opt_model = extract_model_from_options(card)

                fb_raw = await fetch_feedbacks(wb, imt_id) if imt_id else {}
                product_rating = extract_product_rating(fb_raw)

                # rating filter
                if product_rating > 0.0 and product_rating < 4.7:
                    stats["product_rating_reject"] += 1
                    log(
                        logger, lctx, "product_rating_reject",
                        nm_id=nm_id,
                        supplier_id=int(supplier_id),
                        imt_id=imt_id,
                        product_rating=product_rating,
                        elapsed_ms=card_timer.ms(),
                    )
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

                log(
                    logger, lctx, "card_enriched",
                    nm_id=nm_id,
                    supplier_id=int(supplier_id),
                    imt_id=imt_id,
                    product_rating=product_rating,
                    model_option=opt_model,
                    supplier_decision=sdec.decision,
                    elapsed_ms=card_timer.ms(),
                )

                # strict mode: rating==0 -> fallback queue
                if product_rating == 0.0:
                    stats["product_rating_zero_strict"] += 1
                    review_queue.append(item)
                    log(logger, lctx, "strict_rating_zero_to_fallback", nm_id=nm_id, supplier_id=int(supplier_id))
                    continue

                # model conflict -> fallback
                if detect_model_conflict(item["title"], opt_model):
                    stats["model_conflict_hint"] += 1
                    review_queue.append(item)
                    log(logger, lctx, "model_conflict_to_fallback", nm_id=nm_id, supplier_id=int(supplier_id))
                else:
                    batch_main.append(item)

            # main llm
            if batch_main:
                main_review = await run_llm_and_store(
                    db, llm_main, cfg, logger, lctx, batch_main, cfg.openai_model_main, stats
                )
                if main_review:
                    stats["second_wave_review_to_fallback"] += len(main_review)
                    review_queue.extend(main_review)
                    log(logger, lctx, "second_wave_enqueue", n_items=len(main_review))

            log(
                logger, lctx, "batch_done",
                batch_no=batch_no,
                batch_elapsed_ms=batch_timer.ms(),
                stats_top=stats.most_common(12),
                review_queue_len=len(review_queue),
            )

        # fallback llm
        fb_batch_size = max(4, min(8, cfg.llm_batch_size // 2))
        log(logger, lctx, "fallback_start", n_items=len(review_queue), fb_batch_size=fb_batch_size)
        for i in range(0, len(review_queue), fb_batch_size):
            chunk = review_queue[i:i + fb_batch_size]
            await run_llm_and_store(
                db, llm_fb, cfg, logger, lctx, chunk, cfg.openai_model_fallback, stats
            )

        log(logger, lctx, "done", stats_all=stats.most_common(50))

    finally:
        await wb.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())