import re
import unicodedata
from typing import Optional

_WS_RE = re.compile(r"\s+")
_BAD_PUNCT = re.compile(r"[^\w\s\+\"”\./-]+", re.UNICODE)

def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.lower().replace("ё", "е")
    s = s.replace("\u00a0", " ")
    s = s.replace("–", "-").replace("—", "-")

    # unify wifi variants
    s = s.replace("wi fi", "wi-fi").replace("wi-fi", "wifi")

    # ---- eSIM normalization (EN + RU variants) ----
    # english variants
    s = s.replace("e-sim", "esim").replace("e sim", "esim")
    # russian variants: есим / е сим / е-сим / и-сим / esim written in cyrillic-ish
    s = re.sub(r"\bе[\s-]*сим\b", "esim", s)  # е сим / е-сим / есим
    s = re.sub(r"\bи[\s-]*сим\b", "esim", s)  # и-сим
    s = re.sub(r"\bесим\b", "esim", s)        # есім/есим (часто слитно)

    # normalize SIM in RU
    s = re.sub(r"\bсим\b", "sim", s)

    # yandex lite variants
    s = re.sub(r"\bлайт\b", "lite", s)

    # keep '+' and '/'
    s = s.replace("+", " + ")

    s = _BAD_PUNCT.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s