from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from .db import DB
from .wb_http import WBHttp

NAME_URL = "https://static-basket-01.wbbasket.ru/vol0/data/supplier-by-id/{supplier_id}.json"
PROFILE_URL = "https://suppliers-shipment-2.wildberries.ru/api/v1/suppliers/{supplier_id}?curr=RUB"


@dataclass(frozen=True)
class SupplierDecision:
    decision: str  # pass|reject|whitelist_pass|blacklist_reject
    reason: str
    seller_name_norm: str
    name_raw: dict[str, Any] | None
    profile_raw: dict[str, Any] | None


def _norm_name(s: str) -> str:
    return " ".join(s.strip().upper().split())


def _parse_ts(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


class SupplierChecker:
    def __init__(
        self,
        db: DB,
        wb: WBHttp,
        min_val: float,
        strict_min: float,
        strict_max: float,
        min_reviews: int,
        min_age_days: int,
        strict_age_days: int = 180,
        strict_min_sales: int = 5000,   # "низкий" по умолчанию; можешь поменять env/config
        strict_min_supp_ratio: int = 80,
        strict_min_ratio_mark: int = 2,  # ratioMarkSupp <=1 => reject, значит min ok =2
    ):
        self.db = db
        self.wb = wb
        self.min_val = min_val
        self.strict_min = strict_min
        self.strict_max = strict_max
        self.min_reviews = min_reviews
        self.min_age_days = min_age_days

        self.strict_age_days = strict_age_days
        self.strict_min_sales = strict_min_sales
        self.strict_min_supp_ratio = strict_min_supp_ratio
        self.strict_min_ratio_mark = strict_min_ratio_mark

    async def _get_lists(self) -> tuple[set[str], set[str]]:
        rows = await self.db.fetch("select list_name, exact_name from wb_supplier_name_lists")
        bl, wl = set(), set()
        for r in rows:
            name = _norm_name(r["exact_name"])
            if r["list_name"] == "blacklist":
                bl.add(name)
            elif r["list_name"] == "whitelist":
                wl.add(name)
        return bl, wl

    async def _get_name_cached(self, supplier_id: int) -> dict[str, Any]:
        row = await self.db.fetchrow(
            "select raw from wb_supplier_name_cache where supplier_id=$1",
            supplier_id,
        )
        if row:
            return row["raw"]

        raw = await self.wb.get_json(NAME_URL.format(supplier_id=supplier_id), timeout_retries=5, other_retries=10)
        await self.db.execute(
            "insert into wb_supplier_name_cache (supplier_id, trademark, supplier_name, supplier_full_name, raw) "
            "values ($1,$2,$3,$4,$5) "
            "on conflict (supplier_id) do update set raw=excluded.raw, fetched_at=now(), "
            "trademark=excluded.trademark, supplier_name=excluded.supplier_name, supplier_full_name=excluded.supplier_full_name",
            supplier_id,
            raw.get("trademark"),
            raw.get("supplierName"),
            raw.get("supplierFullName"),
            raw,
        )
        return raw

    async def _get_profile_cached(self, supplier_id: int) -> dict[str, Any]:
        row = await self.db.fetchrow(
            "select raw from wb_supplier_profile_cache where supplier_id=$1",
            supplier_id,
        )
        if row:
            return row["raw"]

        headers = {"x-client-name": "site"}
        raw = await self.wb.get_json(
            PROFILE_URL.format(supplier_id=supplier_id),
            headers=headers,
            timeout_retries=5,
            other_retries=10,
        )

        reg_dt = _parse_ts(raw.get("registrationDate"))
        await self.db.execute(
            "insert into wb_supplier_profile_cache "
            "(supplier_id, valuation, feedbacks_count, registration_date, sale_item_quantity, rating, supp_ratio, ratio_mark_supp, delivery_duration, raw) "
            "values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) "
            "on conflict (supplier_id) do update set raw=excluded.raw, fetched_at=now(), "
            "valuation=excluded.valuation, feedbacks_count=excluded.feedbacks_count, registration_date=excluded.registration_date, "
            "sale_item_quantity=excluded.sale_item_quantity, rating=excluded.rating, supp_ratio=excluded.supp_ratio, "
            "ratio_mark_supp=excluded.ratio_mark_supp, delivery_duration=excluded.delivery_duration",
            supplier_id,
            float(raw.get("valuationToHundredths") or raw.get("valuation") or 0.0),
            int(raw.get("feedbacksCount") or 0),
            reg_dt,
            int(raw.get("saleItemQuantity") or 0),
            float(raw.get("rating") or 0.0),
            int(raw.get("suppRatio") or 0),
            int(raw.get("ratioMarkSupp") or 0),
            float(raw.get("deliveryDuration") or 0.0),
            raw,
        )
        return raw

    async def check(self, supplier_id: int) -> SupplierDecision:
        bl, wl = await self._get_lists()

        name_raw = await self._get_name_cached(supplier_id)
        seller_name = (name_raw.get("trademark") or name_raw.get("supplierName") or name_raw.get("supplierFullName") or "")
        seller_name_norm = _norm_name(seller_name) if seller_name else ""

        if seller_name_norm and seller_name_norm in bl:
            return SupplierDecision("blacklist_reject", "seller_name_blacklisted", seller_name_norm, name_raw, None)
        if seller_name_norm and seller_name_norm in wl:
            return SupplierDecision("whitelist_pass", "seller_name_whitelisted", seller_name_norm, name_raw, None)

        profile_raw = await self._get_profile_cached(supplier_id)

        valuation = float(profile_raw.get("valuationToHundredths") or profile_raw.get("valuation") or 0.0)
        feedbacks = int(profile_raw.get("feedbacksCount") or 0)
        sales = int(profile_raw.get("saleItemQuantity") or 0)
        supp_ratio = int(profile_raw.get("suppRatio") or 0)
        ratio_mark = int(profile_raw.get("ratioMarkSupp") or 0)

        reg_dt = _parse_ts(profile_raw.get("registrationDate"))
        age_days = 0
        if reg_dt:
            age_days = int((datetime.now(timezone.utc) - reg_dt).total_seconds() / 86400)

        # base thresholds
        if feedbacks < self.min_reviews:
            return SupplierDecision("reject", f"low_reviews<{self.min_reviews}", seller_name_norm, name_raw, profile_raw)
        if age_days < self.min_age_days:
            return SupplierDecision("reject", f"young_seller<{self.min_age_days}d", seller_name_norm, name_raw, profile_raw)

        # strict band logic (your rule)
        if self.strict_min <= valuation <= self.strict_max:
            if sales < self.strict_min_sales or age_days < self.strict_age_days:
                return SupplierDecision("reject", f"strict_band_low_sales_or_young(sales={sales},age={age_days})", seller_name_norm, name_raw, profile_raw)
            if supp_ratio < self.strict_min_supp_ratio or ratio_mark <= 1:
                return SupplierDecision("reject", f"strict_band_low_ratios(suppRatio={supp_ratio},ratioMark={ratio_mark})", seller_name_norm, name_raw, profile_raw)
            return SupplierDecision("pass", f"strict_band_pass(valuation={valuation})", seller_name_norm, name_raw, profile_raw)

        # main valuation check
        if valuation < self.min_val:
            return SupplierDecision("reject", f"low_valuation<{self.min_val} ({valuation})", seller_name_norm, name_raw, profile_raw)

        return SupplierDecision("pass", "ok", seller_name_norm, name_raw, profile_raw)