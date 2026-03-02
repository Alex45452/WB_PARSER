import re
from dataclasses import dataclass
from typing import Any, Optional

from .normalize import normalize_text

# ---------- common numeric parsers ----------

_GB_RE = re.compile(r"\b(\d{2,4})\s*(gb|гб)\b", re.IGNORECASE)
_TB_RE = re.compile(r"\b(\d{1,2})\s*(tb|тб)\b", re.IGNORECASE)

_ONE_TB_ALIAS = re.compile(r"\b(1\s*(tb|тб)|1024\s*(gb|гб))\b", re.IGNORECASE)
_TWO_TB_ALIAS = re.compile(r"\b(2\s*(tb|тб)|2048\s*(gb|гб))\b", re.IGNORECASE)

# RAM/Storage like 8/256, 12/512, 16/1tb, 16/2tb
_RAM_STORAGE_RE = re.compile(
    r"\b(\d{1,2})\s*/\s*(\d{2,4}|1\s*tb|2\s*tb|1tb|2tb)\b",
    re.IGNORECASE
)

_YEAR_RE = re.compile(r"\b(20(1[8-9]|2[0-6]))\b")
_IPAD_DIAG_ANY_RE = re.compile(r"\b(10\.5|11|12\.9|13)\s*(?:[\"″]|дюйм|inch)?\b", re.IGNORECASE)

def extract_year(text: str) -> Optional[int]:
    if not text:
        return None
    m = _YEAR_RE.search(text)
    return int(m.group(1)) if m else None

def extract_ipad_diagonal_any(text: str) -> Optional[float]:
    if not text:
        return None
    m = _IPAD_DIAG_ANY_RE.search(text)
    if not m:
        return None
    v = m.group(1)
    return float(v) if "." in v else float(int(v))

def extract_storage_gb(text: str) -> Optional[int]:
    if not text:
        return None

    if _TWO_TB_ALIAS.search(text):
        return 2048
    if _ONE_TB_ALIAS.search(text):
        return 1024

    m = _TB_RE.search(text)
    if m:
        return int(m.group(1)) * 1024

    m = _GB_RE.search(text)
    if m:
        gb = int(m.group(1))
        if gb >= 64:
            return gb

    m = _RAM_STORAGE_RE.search(text)
    if m:
        raw = m.group(2).replace(" ", "").lower()
        if raw.endswith("tb"):
            try:
                return int(raw[:-2]) * 1024
            except ValueError:
                return None
        try:
            gb = int(raw)
            if gb >= 64:
                return gb
        except ValueError:
            return None

    return None

def extract_ram_gb(text: str) -> Optional[int]:
    if not text:
        return None
    m = _RAM_STORAGE_RE.search(text)
    if not m:
        return None
    ram = int(m.group(1))
    if 2 <= ram <= 64:
        return ram
    return None

# ---------- iphone sim ----------

_SIM_ESIM_RE = re.compile(
    r"\bsim\b.*\besim\b|\besim\b.*\bsim\b|\bsim\s*\+\s*esim\b|\b1\s*sim\b.*\besim\b",
    re.IGNORECASE
)
_ESIM_RE = re.compile(
    r"\besim\b|\besim\s*\+\s*esim\b|\bdual\s*esim\b|\b2\s*esim\b",
    re.IGNORECASE
)

def extract_iphone_sim_type(text: str) -> Optional[str]:
    if not text:
        return None
    if _SIM_ESIM_RE.search(text):
        return "SIM_ESIM"
    if _ESIM_RE.search(text):
        return "ESIM"
    return None

# ---------- mac chip + diagonal ----------

_MAC_CHIP_RE = re.compile(r"\b(m[1-5])\b", re.IGNORECASE)
_MAC_CHIP_PRO_RE = re.compile(r"\b(m[1-5])\s*pro\b", re.IGNORECASE)
_DIAG_RE = re.compile(r"\b(13|14|15|16)\s*(?:[\"″]|дюйм|inch)?\b", re.IGNORECASE)

def extract_mac_chip(text: str) -> Optional[str]:
    if not text:
        return None
    m = _MAC_CHIP_PRO_RE.search(text)
    if m:
        return f"{m.group(1).lower()}_pro"
    m = _MAC_CHIP_RE.search(text)
    return m.group(1).lower() if m else None

def extract_diagonal(text: str) -> Optional[int]:
    if not text:
        return None
    m = _DIAG_RE.search(text)
    return int(m.group(1)) if m else None

# ---------- ipad connectivity + size ----------

_IPAD_WIFI_RE = re.compile(r"\bwifi\b|\bwi-fi\b", re.IGNORECASE)
_IPAD_CELL_RE = re.compile(r"\b(lte|5g|cellular)\b", re.IGNORECASE)
_IPAD_SIZE_RE = re.compile(r'\b(11|13)\s*(?:["″]|дюйм|inch)?\b', re.IGNORECASE)

def extract_ipad_connectivity(text: str) -> Optional[str]:
    if not text:
        return None
    if _IPAD_CELL_RE.search(text):
        return "CELLULAR"
    if _IPAD_WIFI_RE.search(text):
        return "WIFI"
    return None

def extract_ipad_size(text: str) -> Optional[int]:
    if not text:
        return None
    m = _IPAD_SIZE_RE.search(text)
    if not m:
        return None
    v = int(m.group(1))
    return v if v in (11, 13) else None

# ---------- steam deck ----------

_OLED_RE = re.compile(r"\boled\b", re.IGNORECASE)

def extract_oled(text: str) -> bool:
    return bool(text and _OLED_RE.search(text))

def extract_steamdeck_storage_bucket(text: str) -> Optional[str]:
    if not text:
        return None
    st = extract_storage_gb(text)
    if st in (512, 1024):
        return "512" if st == 512 else "1024"
    return None

# ---------- ps5 ----------

_PS5_PRO_RE = re.compile(r"\bps5\s*pro\b", re.IGNORECASE)
_PS5_SLIM_RE = re.compile(r"\bps5\s*slim\b", re.IGNORECASE)
_PS5_DIGITAL_RE = re.compile(r"\bdigital\b", re.IGNORECASE)
_PS5_DISC_RE = re.compile(r"\b(disc|disk)\b", re.IGNORECASE)

def extract_ps5_variant(text: str) -> Optional[str]:
    if not text:
        return None
    if _PS5_PRO_RE.search(text):
        return "PRO"
    if _PS5_SLIM_RE.search(text):
        return "SLIM"
    if "ps5" in text.lower():
        return "BASE"
    return None

def extract_ps5_edition(text: str) -> Optional[str]:
    if not text:
        return None
    if _PS5_DIGITAL_RE.search(text):
        return "DIGITAL"
    if _PS5_DISC_RE.search(text):
        return "DISC"
    return None

# ---------- slots dispatcher ----------

def pick_slot(extractor, t: str, d: str, a: str):
    for s in (t, d, a):
        val = extractor(s)
        if val is not None and val is not False:
            return val
    return None

@dataclass
class CardSlots:
    storage_gb: Optional[int] = None
    ram_gb: Optional[int] = None
    year: Optional[int] = None

    sim_type: Optional[str] = None

    chip: Optional[str] = None
    diagonal: Optional[int] = None

    ipad_connectivity: Optional[str] = None
    ipad_size: Optional[int] = None
    ipad_diag_any: Optional[float] = None

    oled: Optional[bool] = None
    steamdeck_storage: Optional[str] = None

    ps5_variant: Optional[str] = None
    ps5_edition: Optional[str] = None


def extract_slots_for_category(category: str, title: str, description: str, options: Any) -> CardSlots:
    t = normalize_text(title or "")
    d = normalize_text(description or "")

    # attrs/options: делаем в строку “как есть”
    a = ""
    if isinstance(options, list):
        parts = []
        for o in options:
            if isinstance(o, dict):
                n = str(o.get("name", "") or "")
                v = str(o.get("value", "") or "")
                if n or v:
                    parts.append(f"{n}:{v}")
        a = normalize_text(" ".join(parts))

    s = CardSlots()

    if category == "iphone":
        s.storage_gb = pick_slot(extract_storage_gb, t, d, a)
        s.sim_type = pick_slot(extract_iphone_sim_type, t, d, a)

    elif category in ("samsung", "huawei"):
        s.ram_gb = pick_slot(extract_ram_gb, t, d, a)
        s.storage_gb = pick_slot(extract_storage_gb, t, d, a)

    elif category == "macbook":
        s.diagonal = pick_slot(extract_diagonal, t, d, a)
        s.chip = pick_slot(extract_mac_chip, t, d, a)
        s.ram_gb = pick_slot(extract_ram_gb, t, d, a)
        s.storage_gb = pick_slot(extract_storage_gb, t, d, a)

    elif category == "ipad":
        s.year = pick_slot(extract_year, t, d, a)
        s.ipad_diag_any = pick_slot(extract_ipad_diagonal_any, t, d, a)
        s.ipad_size = pick_slot(extract_ipad_size, t, d, a)
        s.ipad_connectivity = pick_slot(extract_ipad_connectivity, t, d, a)
        s.storage_gb = pick_slot(extract_storage_gb, t, d, a)

    elif category == "steam_deck":
        s.oled = bool(pick_slot(extract_oled, t, d, a))
        s.steamdeck_storage = pick_slot(extract_steamdeck_storage_bucket, t, d, a)

    elif category == "ps5":
        s.ps5_variant = pick_slot(extract_ps5_variant, t, d, a)
        s.ps5_edition = pick_slot(extract_ps5_edition, t, d, a)

    elif category == "yandex_station":
        # слоты можно расширить позже, пока используем только anchors/matcher
        pass

    return s