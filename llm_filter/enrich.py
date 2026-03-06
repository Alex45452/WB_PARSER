from __future__ import annotations
from typing import Any

from .wb_http import WBHttp

def card_url(basket: int, nm_id: int) -> str:
    """
    You already have basket and know how WB URL is built.
    We'll reuse the stable pattern:
      https://basket-XX.wbbasket.ru/vol{vol}/part{part}/{nm}/info/ru/card.json
    BUT vol/part depends on nm_id and basket mapping.
    In your project you already store 'url' and probably know card.json link.
    So: pass in direct card_json_url if you have it.
    """
    raise NotImplementedError("Use your existing url builder or store card_json_url")

async def fetch_card_json(wb: WBHttp, card_json_url: str) -> dict[str, Any]:
    return await wb.get_json(card_json_url, retries=10)

async def fetch_feedbacks(wb: WBHttp, imt_id: int) -> dict[str, Any]:
    url = f"https://feedbacks2.wb.ru/feedbacks/v2/{imt_id}"
    return await wb.get_json(url, retries=10)

async def fetch_questions(wb: WBHttp, imt_id: int, take: int = 30, skip: int = 0) -> dict[str, Any]:
    url = f"https://questions.wildberries.ru/api/v1/questions?imtId={imt_id}&take={take}&skip={skip}"
    return await wb.get_json(url, retries=10)

def squeeze_feedbacks(raw: dict[str, Any], limit: int = 5) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for fb in (raw.get("feedbacks") or [])[:limit]:
        out.append({
            "rating": fb.get("productValuation"),
            "date": fb.get("createdDate"),
            "text": fb.get("text"),
            "pros": fb.get("pros"),
            "cons": fb.get("cons"),
            "seller_answer": (fb.get("answer") or {}).get("text"),
        })
    return out

def squeeze_questions(raw: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    qs = raw.get("questions") or []
    for q in qs[:limit]:
        out.append({
            "text": q.get("text"),
            "tag": q.get("tag") or q.get("tags"),
            "answer": (q.get("answer") or {}).get("text"),
        })
    return out

def extract_model_from_options(card: dict[str, Any]) -> str | None:
    # tries to find option name like "Модель"
    for opt in card.get("options") or []:
        if str(opt.get("name", "")).strip().lower() == "модель":
            return str(opt.get("value") or "").strip() or None
    # grouped_options
    for group in card.get("grouped_options") or []:
        for opt in group.get("options") or []:
            if str(opt.get("name", "")).strip().lower() == "модель":
                return str(opt.get("value") or "").strip() or None
    return None