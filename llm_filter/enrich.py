from __future__ import annotations
from typing import Any

from .wb_http import WBHttp

FEEDBACKS1 = "https://feedbacks1.wb.ru/feedbacks/v2/{imt_id}"
FEEDBACKS2 = "https://feedbacks2.wb.ru/feedbacks/v2/{imt_id}"


async def fetch_card_json(wb: WBHttp, card_json_url: str) -> dict[str, Any]:
    # card_json_url already stored in wb_cards.url (you confirmed)
    return await wb.get_json(card_json_url, timeout_retries=5, other_retries=10)


async def fetch_feedbacks(wb: WBHttp, imt_id: int) -> dict[str, Any]:
    # Prefer feedbacks1, fallback to feedbacks2
    try:
        return await wb.get_json(FEEDBACKS1.format(imt_id=imt_id), timeout_retries=5, other_retries=10)
    except Exception:
        return await wb.get_json(FEEDBACKS2.format(imt_id=imt_id), timeout_retries=5, other_retries=10)


async def fetch_questions(wb: WBHttp, imt_id: int, take: int = 30, skip: int = 0) -> dict[str, Any]:
    url = f"https://questions.wildberries.ru/api/v1/questions?imtId={imt_id}&take={take}&skip={skip}"
    return await wb.get_json(url, timeout_retries=5, other_retries=10)


def squeeze_feedbacks(raw: dict[str, Any], limit: int = 5) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for fb in (raw.get("feedbacks") or [])[:limit]:
        out.append(
            {
                "rating": fb.get("productValuation"),
                "date": fb.get("createdDate"),
                "text": fb.get("text"),
                "pros": fb.get("pros"),
                "cons": fb.get("cons"),
                "seller_answer": (fb.get("answer") or {}).get("text"),
            }
        )
    return out


def squeeze_questions(raw: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    qs = raw.get("questions") or []
    for q in qs[:limit]:
        out.append(
            {
                "text": q.get("text"),
                "tag": q.get("tag") or q.get("tags"),
                "answer": (q.get("answer") or {}).get("text"),
            }
        )
    return out


def extract_model_from_options(card: dict[str, Any]) -> str | None:
    # options (flat)
    for opt in card.get("options") or []:
        if str(opt.get("name", "")).strip().lower() == "модель":
            return str(opt.get("value") or "").strip() or None

    # grouped_options
    for group in card.get("grouped_options") or []:
        for opt in group.get("options") or []:
            if str(opt.get("name", "")).strip().lower() == "модель":
                return str(opt.get("value") or "").strip() or None

    return None


def extract_product_rating(feedbacks_raw: dict[str, Any]) -> float:
    """
    feedbacks v2 usually has:
      - "valuation": "4.8" (string)
    Can be absent or "0".
    """
    v = feedbacks_raw.get("valuation")
    if v is None:
        return 0.0
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).strip().replace(",", "."))
        except Exception:
            return 0.0
        
def extract_supplier_id(card: dict[str, Any]) -> int | None:
    # common WB shapes
    for path in (
        ("selling", "supplier_id"),
        ("data", "supplier_id"),
        ("supplierId",),
        ("supplier_id",),
    ):
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