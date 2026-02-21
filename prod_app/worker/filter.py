from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Optional


@dataclass
class FilterResult:
    accept: bool
    product_key: Optional[str] = None
    score: Optional[float] = None


_space_re = re.compile(r"\s+")


def _norm(s: str) -> str:
    # Очень дёшево: lower + ё->е + схлопывание пробелов
    s = (s or "").lower().replace("ё", "е")
    s = _space_re.sub(" ", s).strip()
    return s


# Минимальные якори, которые покрывают ВСЕ текущие строки в keywords.txt
# Порядок важен: специфичные раньше.
# PS5 ловим и по "ps5", и по "playstation"
ANCHORS = (
    ("steam_deck", ("steam deck", "steamdeck", "стим дек", "стимдек")),
    ("ps5", ("ps5", "playstation", "плейстейшн","play station")),
    ("yandex_station", ("яндекс", "yandex")),
    ("macbook", ("macbook", "макбук", "mac book")),
    ("ipad", ("ipad",)),
    ("iphone", ("iphone", "айфон",)),
    ("samsung", ("samsung", "самсунг")),
    ("huawei", ("huawei", "хуавей")),
    # если появится в keywords.txt — уже готово:
    ("nintendo_switch", ("nintendo", "нинтендо", "switch", "свитч")),
)

# Преднормализуем якори один раз (чтобы не нормализовать их на каждом вызове)
_ANCHORS_NORM = tuple((k, tuple(_norm(a) for a in arr)) for k, arr in ANCHORS)


def _validate_keywords_covered() -> None:

    if os.getenv("CHECK_KEYWORDS", "0") != "1":
        return

    path = os.getenv("KEYWORDS_FILE", "keywords.txt")
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            n = _norm(raw)
            ok = False
            for _, kws in _ANCHORS_NORM:
                for a in kws:
                    if a and a in n:
                        ok = True
                        break
                if ok:
                    break
            if not ok:
                raise RuntimeError(
                    f"keywords.txt содержит строку без якоря: {raw!r}. "
                    f"Добавь якорь в ANCHORS или поправь keyword."
                )


# Проверим покрытие один раз при импорте
_validate_keywords_covered()


def fast_filter(title: str, desc: str) -> FilterResult:
    """
    Самый быстрый фильтр: только якори, только title+desc.
    """
    blob = _norm(title) + " " + _norm(desc)

    for product_key, kws in _ANCHORS_NORM:
        # минимум операций: substring search
        for a in kws:
            if a and a in blob:
                return FilterResult(True, product_key=product_key, score=1.0)

    return FilterResult(False, None, None)
