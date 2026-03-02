import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .normalize import normalize_text
from .slots import (
    extract_storage_gb,
    extract_ram_gb,
    extract_iphone_sim_type,
    extract_mac_chip,
    extract_diagonal,
    extract_ipad_connectivity,
    extract_ipad_size,
    extract_year,
    extract_oled,
    extract_steamdeck_storage_bucket,
    extract_ps5_variant,
    extract_ps5_edition,
)

@dataclass(frozen=True)
class KeywordProfile:
    raw: str
    norm: str
    category: str
    target_key: str
    required: Tuple[str, ...]
    slots: Dict[str, object]

_SAMSUNG_HINT_RE = re.compile(
    r"\b(galaxy|s\d{2,3}|a\d{2,3}|m\d{2,3}|z\s*(fold|flip)|fold|flip|ultra|fe|plus|note|галакси|ультра|фолд|флип)\b",
    re.IGNORECASE,
)

def infer_category(norm: str) -> Optional[str]:
    if "steam deck" in norm or "стим дек" in norm:
        return "steam_deck"
    if "ps5" in norm or "playstation" in norm:
        return "ps5"
    if "macbook" in norm or "макбук" in norm:
        return "macbook"
    if "ipad" in norm or "айпад" in norm:
        return "ipad"
    if "iphone" in norm or "айфон" in norm:
        return "iphone"
    if "huawei" in norm or "хуавей" in norm:
        return "huawei"
    if "samsung" in norm or _SAMSUNG_HINT_RE.search(norm):
        return "samsung"
    if "яндекс" in norm or "yandex" in norm or "алиса" in norm or "станция" in norm:
        return "yandex_station"
    return None

def build_target_key(category: str, norm: str) -> str:
    core = norm.replace('"', "").replace("”", "")
    return f"{category}:{core}"

def parse_keyword_line(line: str) -> Optional[KeywordProfile]:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None

    norm = normalize_text(raw)
    cat = infer_category(norm)
    if not cat:
        return None

    slots: Dict[str, object] = {}
    req: List[str] = []

    if cat == "iphone":
        st = extract_storage_gb(norm)
        if st is not None:
            req.append("storage_gb"); slots["storage_gb"] = st

        sim = extract_iphone_sim_type(norm)
        if sim is not None:
            req.append("sim_type"); slots["sim_type"] = sim

    elif cat in ("samsung", "huawei"):
        ram = extract_ram_gb(norm)
        st = extract_storage_gb(norm)
        if ram is not None:
            req.append("ram_gb"); slots["ram_gb"] = ram
        if st is not None:
            req.append("storage_gb"); slots["storage_gb"] = st

    elif cat == "macbook":
        diag = extract_diagonal(norm)
        chip = extract_mac_chip(norm)
        ram = extract_ram_gb(norm)
        st = extract_storage_gb(norm)
        if diag is not None:
            req.append("diagonal"); slots["diagonal"] = diag
        if chip is not None:
            req.append("chip"); slots["chip"] = chip
        if st is not None:
            req.append("storage_gb"); slots["storage_gb"] = st
        if ram is not None:
            req.append("ram_gb"); slots["ram_gb"] = ram

    elif cat == "ipad":
        size = extract_ipad_size(norm)
        conn = extract_ipad_connectivity(norm)
        st = extract_storage_gb(norm)
        yr = extract_year(norm)

        if size is not None:
            req.append("ipad_size"); slots["ipad_size"] = size
        if conn is not None:
            req.append("ipad_connectivity"); slots["ipad_connectivity"] = conn
        if st is not None:
            req.append("storage_gb"); slots["storage_gb"] = st
        if yr is not None:
            req.append("year"); slots["year"] = yr

    elif cat == "steam_deck":
        req.append("oled"); slots["oled"] = bool(extract_oled(norm))
        sb = extract_steamdeck_storage_bucket(norm)
        if sb is not None:
            req.append("steamdeck_storage"); slots["steamdeck_storage"] = sb

    elif cat == "ps5":
        v = extract_ps5_variant(norm)
        e = extract_ps5_edition(norm)
        if v is not None:
            req.append("ps5_variant"); slots["ps5_variant"] = v
        if e is not None:
            req.append("ps5_edition"); slots["ps5_edition"] = e

    elif cat == "yandex_station":
        # пока слоты не используем
        pass

    return KeywordProfile(
        raw=raw,
        norm=norm,
        category=cat,
        target_key=build_target_key(cat, norm),
        required=tuple(req),
        slots=slots,
    )

def load_keyword_profiles(path: str) -> List[KeywordProfile]:
    out: List[KeywordProfile] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            p = parse_keyword_line(line)
            if p:
                out.append(p)
    return out

def group_by_category(profiles: List[KeywordProfile]) -> Dict[str, List[KeywordProfile]]:
    d: Dict[str, List[KeywordProfile]] = {}
    for p in profiles:
        d.setdefault(p.category, []).append(p)
    return d