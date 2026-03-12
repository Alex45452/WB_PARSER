from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict

from .logging_setup import log


@dataclass
class Progress:
    started_at: float = field(default_factory=time.time)
    last_report_at: float = field(default_factory=time.time)

    # totals
    fetched_from_db: int = 0
    processed_total: int = 0

    # stage counters
    fast_reject: int = 0
    fast_accessory_reject: int = 0
    category_not_mapped: int = 0
    supplier_reject: int = 0
    supplier_unparsed: int = 0
    card_fetch_error: int = 0
    missing_supplier_id_in_card: int = 0

    product_rating_reject: int = 0
    product_rating_zero_strict: int = 0

    model_conflict_hint: int = 0

    # llm
    llm_calls_main: int = 0
    llm_calls_fallback: int = 0
    llm_items_main: int = 0
    llm_items_fallback: int = 0
    llm_missing_item: int = 0

    # decisions
    llm_accept: int = 0
    llm_reject: int = 0
    llm_review: int = 0
    llm_accept_unknown: int = 0

    def inc(self, name: str, n: int = 1) -> None:
        setattr(self, name, getattr(self, name) + n)

    def snapshot(self) -> Dict[str, int | float]:
        now = time.time()
        elapsed = max(now - self.started_at, 0.001)
        rate = self.processed_total / elapsed

        return {
            "elapsed_s": round(elapsed, 1),
            "processed_total": self.processed_total,
            "fetched_from_db": self.fetched_from_db,
            "rate_cards_per_s": round(rate, 3),

            "fast_reject": self.fast_reject,
            "fast_accessory_reject": self.fast_accessory_reject,
            "category_not_mapped": self.category_not_mapped,

            "supplier_reject": self.supplier_reject,
            "supplier_unparsed": self.supplier_unparsed,
            "card_fetch_error": self.card_fetch_error,
            "missing_supplier_id_in_card": self.missing_supplier_id_in_card,

            "product_rating_reject": self.product_rating_reject,
            "product_rating_zero_strict": self.product_rating_zero_strict,
            "model_conflict_hint": self.model_conflict_hint,

            "llm_calls_main": self.llm_calls_main,
            "llm_calls_fallback": self.llm_calls_fallback,
            "llm_items_main": self.llm_items_main,
            "llm_items_fallback": self.llm_items_fallback,
            "llm_missing_item": self.llm_missing_item,

            "llm_accept": self.llm_accept,
            "llm_reject": self.llm_reject,
            "llm_review": self.llm_review,
            "llm_accept_unknown": self.llm_accept_unknown,
        }


class ProgressReporter:
    def __init__(self, logger, lctx, progress: Progress, every_s: int = 15):
        self.logger = logger
        self.lctx = lctx
        self.progress = progress
        self.every_s = every_s
        self._task = None
        self._stop = False

    async def start(self) -> None:
        import asyncio

        async def loop():
            while not self._stop:
                await asyncio.sleep(self.every_s)
                log(self.logger, self.lctx, "progress", **self.progress.snapshot())

        self._task = asyncio.create_task(loop())

    async def stop(self) -> None:
        self._stop = True
        if self._task:
            try:
                await self._task
            except Exception:
                pass