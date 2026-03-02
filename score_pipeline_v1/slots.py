import re
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from .normalize import normalize_text

# ---------- common numeric parsers ----------

_GB_RE = re.compile(r"\b(\d{2,4})\s*(gb|гб)\b", re.IGNORECASE)
_TB_RE = re.compile(r"\b(\d{1,2})\s*(tb|тб)\b", re.IGNORECASE)
_ONE_TB_ALIAS = re.compile(r"\b(1\s*(tb|тб)|1024\s*(gb|гб))\b", re.IGNORECASE)

# RAM/Storage like 8/256, 12/512, 16/1tb
_RAM_STORAGE_RE = re.compile(
    r"\b(\d{1,2})\s*/\s*(\d{2,4}|1\s*tb|2\s*tb|1tb|2tb)\b",
    re.IGNORECASE
)

def extract_storage_gb(text: str) -> Optional[int]:
    if not text:
        return None

    # Prefer explicit TB/GB
    m = _TB_RE.search(text)
    if m:
        return int(m.group(1)) * 1024

    m = _GB_RE.search(text)
    if m:
        gb = int(m.group(1))
        if gb >= 64:
            return gb

    if _ONE_TB_ALIAS.search(text):
        return 1024

    # Try RAM/Storage pattern where storage is second part
    m = _RAM_STORAGE_RE.search(text)
    if m:
        raw = m.group(2).replace(" ", "").lower()
        if raw.endswith("tb"):
            return int(raw[:-2]) * 1024
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
    # sanity
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
    # sim+esim отдельная категория
    if _SIM_ESIM_RE.search(text):
        return "SIM_ESIM"
    # esim включает esim+esim / dual esim / просто esim
    if _ESIM_RE.search(text):
        return "ESIM"
    return None
# ---------- mac chip + diagonal ----------

def extract_mac_chip(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"\bm([1-5])\b", text)
    return f"m{m.group(1)}" if m else None

def extract_diagonal(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"\b(13|14|15|16)\b", text)
    return int(m.group(1)) if m else None

# ---------- ipad ----------

def extract_ipad_connectivity(text: str) -> Optional[str]:
    if not text:
        return None
    if re.search(r"\bwifi\b", text):
        return "WIFI"
    if re.search(r"\b(lte|5g|cellular|sim|сотов)\b", text):
        return "CELLULAR"
    return None

def extract_ipad_size(text: str) -> Optional[int]:
    if not text:
        return None
    # keywords use iPad 11 / iPad Air 11 / 13
    m = re.search(r"\bipad\b.*\b(11|13)\b", text)
    return int(m.group(1)) if m else None

# ---------- steam deck ----------

def extract_oled(text: str) -> bool:
    return bool(text and re.search(r"\boled\b", text))

def extract_steamdeck_storage_bucket(text: str) -> Optional[str]:
    gb = extract_storage_gb(text)
    if gb is None:
        return None
    if 500 <= gb <= 600:
        return "512"
    if gb >= 900:
        return "1TB"
    return None

# ---------- ps5 ----------

def extract_ps5_variant(text: str) -> Optional[str]:
    if not text:
        return None
    if re.search(r"\bpro\b", text):
        return "PRO"
    if re.search(r"\bslim\b", text):
        return "SLIM"
    return None

def extract_ps5_edition(text: str) -> Optional[str]:
    if not text:
        return None
    if re.search(r"\bdigital\b", text):
        return "DIGITAL"
    if re.search(r"\bdisk\b|\bdisc\b", text):
        return "DISK"
    return None

# ---------- helpers ----------

def options_to_text(options: Any) -> str:
    if not isinstance(options, list):
        return ""
    parts: List[str] = []
    for it in options:
        if not isinstance(it, dict):
            continue
        n = str(it.get("name") or "")
        v = str(it.get("value") or "")
        if n or v:
            parts.append(f"{n} {v}".strip())
    return " ".join(parts)

def build_card_texts(title: str, description: str, options: Any) -> Tuple[str, str, str]:
    t = normalize_text(title)
    d = normalize_text(description)
    a = normalize_text(options_to_text(options))
    return t, d, a

def pick_slot(extractor, t: str, d: str, a: str):
    # приоритет: title > description > attrs
    for s in (t, d, a):
        val = extractor(s)
        if val is not None and val is not False:
            return val
    return None

@dataclass
class CardSlots:
    storage_gb: Optional[int] = None
    ram_gb: Optional[int] = None

    sim_type: Optional[str] = None

    chip: Optional[str] = None
    diagonal: Optional[int] = None

    ipad_connectivity: Optional[str] = None
    ipad_size: Optional[int] = None

    oled: Optional[bool] = None
    steamdeck_storage: Optional[str] = None

    ps5_variant: Optional[str] = None
    ps5_edition: Optional[str] = None

def extract_slots_for_category(category: str, title: str, description: str, options: Any) -> CardSlots:
    t, d, a = build_card_texts(title, description, options)
    s = CardSlots()

    if category == "iphone":
        s.sim_type = pick_slot(extract_iphone_sim_type, t, d, a)
        s.storage_gb = pick_slot(extract_storage_gb, t, d, a)

    elif category in ("samsung", "huawei"):
        s.ram_gb = pick_slot(extract_ram_gb, t, d, a)
        s.storage_gb = pick_slot(extract_storage_gb, t, d, a)

    elif category == "macbook":
        s.chip = pick_slot(extract_mac_chip, t, d, a)
        s.diagonal = pick_slot(extract_diagonal, t, d, a)
        s.ram_gb = pick_slot(extract_ram_gb, t, d, a)
        s.storage_gb = pick_slot(extract_storage_gb, t, d, a)

    elif category == "ipad":
        s.ipad_size = pick_slot(extract_ipad_size, t, d, a)
        s.ipad_connectivity = pick_slot(extract_ipad_connectivity, t, d, a)
        s.storage_gb = pick_slot(extract_storage_gb, t, d, a)

    elif category == "steam_deck":
        s.oled = True if extract_oled(t) else (True if extract_oled(d) else extract_oled(a))
        s.steamdeck_storage = pick_slot(extract_steamdeck_storage_bucket, t, d, a)

    elif category == "ps5":
        s.ps5_variant = pick_slot(extract_ps5_variant, t, d, a)
        s.ps5_edition = pick_slot(extract_ps5_edition, t, d, a)

    return s