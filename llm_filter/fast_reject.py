import re

# Минимальный, но полезный список "точно не то"
ANTI_PATTERNS = [
    r"\bчех(ол|лы|ла)\b",
    r"\bкейс\b",
    r"\bнакладк(а|и)\b",
    r"\bпленк(а|и)\b",
    r"\bстекл(о|а)\b",
    r"\bзащитн(ое|ая|ый)\b",
    r"\bбампер\b",
    r"\bремеш(ок|ки)\b",
    r"\bкабель\b",
    r"\bзарядк(а|и)\b",
    r"\bадаптер\b",
    r"\bпереходник\b",
    r"\bхаб\b",
    r"\bдок-станц(ия|ии)\b",
    r"\bкреплен(ие|ия)\b",
    r"\bстойк(а|и)\b",
    r"\bподставк(а|и)\b",
    r"\bдержател(ь|и)\b",
    r"\bзапчаст(ь|и)\b",
    r"\bшлейф\b",
    r"\bаккумулятор\b",
    r"\bбатаре(я|и)\b",
    r"\bконтроллер\b",  # часто как запчасть/модуль
    r"\bплата\b",
    r"\bджойстик\b",  # иногда именно аксессуар, лучше пусть LLM уточнит — но prefilter оставим мягким (см. ниже)
    r"\bигра\b",
    r"\bдиск\b",
    r"\bподписк(а|и)\b",
    r"\bкод\b",
    r"\bключ\b",
    r"\bкарта пополнения\b",
]

ANTI_RE = re.compile("|".join(ANTI_PATTERNS), re.IGNORECASE)

# "джойстик" может быть и в бандле/в теме консоли, поэтому не делаем 100% reject.
SOFT_WORDS = {"джойстик","геймпад"}

def fast_accessory_reject(title: str | None, description: str | None) -> tuple[bool, str | None]:
    text = f"{title or ''}\n{description or ''}".lower()

    m = ANTI_RE.search(text)
    if not m:
        return False, None

    w = m.group(0).lower()
    if w in SOFT_WORDS:
        return False, None  # отправим в LLM

    return True, f"anti_accessory:{w}"