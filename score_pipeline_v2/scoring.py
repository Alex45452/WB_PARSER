import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .normalize import normalize_text
from .slots import extract_slots_for_category, CardSlots
from .keyword_profiles import KeywordProfile

# ---------- Hard reject: non-original / refurbished ----------
HARD_NON_ORIG = re.compile(
    r"\b(копи[яи]|реплик[аи]|аналог|no\s*name|oem|refurb|refurbished|восстановлен|востановлен|обновлен|обновленный|как\s*новый|б/у|бу|used|подделк|fake|replica|не\s*оригинал|неоригинал)\b",
    re.IGNORECASE,
)

# ---------- Hard reject: accessories / parts / services ----------
HARD_ACCESSORY = re.compile(
    r"\b("
    r"чехол|чехлы|кейс|case|cover|бампер|накладк\w*|пленк\w*|стекл\w*|бронестекло|защитн\w*|гидрогел\w*"
    r"|накле\w*|стикер|skin|панель|корпус|крышк\w*|задн(яя|ую)\s*панел\w*|боков(ая|ую)\s*панел\w*"
    r"|держател\w*|подставк\w*|стенд|stand|кронштейн|ремеш(ок|к)|петля|strаp|strap"
    r"|стилус|pencil|пенсил|перо"
    r"|клавиатур\w*|keyboard|мыш(ь|ка)|трекпад|trackpad"
    r"|кабель|провод|зарядк\w*|адаптер|блок\s*питан|переходник|хаб|hub|dock|док"
    r"|аккум\w*|батаре\w*|powerbank|павербанк"
    r"|шлейф|диспле\w*|экран|тачскрин|разъем|камера|динамик|плата|контроллер|джойстик"
    r"|ремонт|замена|услуг\w*|страховк\w*|подписк\w*|dlc|код\s*(активац\w*|пополнен\w*)"
    r"|муляж|макет|копия\s*коробк\w*|коробк(а|и)\s*без\s*товара"
    r")\b",
    re.IGNORECASE,
)


PS5_FOR_CUE = re.compile(r"\b(для\s*ps5|for\s*ps5|compatible|совместим)\b", re.IGNORECASE)
PS5_CONSOLE_CUE = re.compile(r"\b(консоль|console)\b", re.IGNORECASE)
# PS5: parts/accessories/games/service — hard reject
PS5_ACCESSORY = re.compile(
    r"\b("
    # RU
    r"подставк\w*|держател\w*|органайзер|стойк\w*|креплен\w*|кронштейн|настенн\w*"
    r"|вентилятор|охладител\w*|охлажден\w*|кулер"
    r"|винил|vinyl"
    r"|заряд\w*|док-станц|докстанц\w*|dock|charging"
    r"|наушник|хедсет|headset"
    r"|контроллер|передач|база"
    r"|диск(и|ов)|игр(ы|)|дисков"
    r"|пылезащит\w*|защит\w*|пылевик|чехол|кейс"
    # EN
    r"|stand|holder|mount|wall\s*mount|organizer"
    r"|fan|cooling|cooler"

    # --- cue words (почти всегда аксессуар/часть) ---
    r"подход\w*|совместим|compatible|for|replace|replacem|replacement|shell|housing|kit|set"
    r"|oem|aftermark|custom"

    # --- games / codes ---
    r"|игр(а|)|game|games|карридж|код|code|dlc|активац\w*|subscription|подписк\w*|psn"

    # --- controllers / input ---
    r"|dualsense|dual\s*sense|controller|gamepad|pad\b|джойст|контрол|trigger|button|stick|thumbstick"
    r"|analog|аналог|кнопк\w*|стик"

    # --- docks / stands / mounts / storage ---
    r"|dock|док|charg|заряд|stand|стойк|подстав|держател|holder|mount|крепл|кронштейн|wall"
    r"|органайз|storage|хранен\w*|полк|rack|case\b|bag|сумк\w*|чехол|кейс"

    # --- skins / stickers / lights ---
    r"|skin|vinyl|wrap|наклейк\w*|стикер|пленк\w*|стекл\w*|protect|защит\w*"
    r"|rgb|led|light|подсвет"

    # --- cooling / dust ---
    r"|fan|cool|cooler|охлажд|кулер|вентилят\w*"
    r"|dust|пыл\w*|filter|фильтр|mesh|сетк\w*|grill|решет\w*"

    # --- power / cables / ports ---
    r"|psu|power\s*supply|питан|блок\s*питан|адаптер|adapter|charger|charging"
    r"|cable|кабел|провод|hdmi|usb|type-?c|lan|ethernet|порт|port|разъем|разъём|jack"

    # --- storage / drive / optical ---
    r"|drive|disc\s*drive|привод|лазер|laser|lens|линз\w*"

    # --- internals / electronics / repair ---
    r"|motherboard|mainboard|board\b|pcb|плат|chip|ic\b|микросхем|контакт|конденс\w*"
    r"|flex|шлейф|лейф|connector|коннект\w*|socket|слот"
    r"|hdmi\s*port|usb\s*port|port\s*repair|repair|ремонт|замен\w*|пайк\w*|service|услуг\w*"

    # --- enclosure / faceplate / casing ---
    r"|faceplate|plate|plates|панел\w*|крышк\w*|корпус|оболоч\w*|cover\b|frame|рамк\w*"
    r")\b",
    re.IGNORECASE,
)

# ---------- Anchors by category ----------
ANCHORS = {
    "yandex_station": re.compile(r"\b(яндекс|yandex|алиса|станци)\b", re.IGNORECASE),
    "ps5": re.compile(r"\b(ps5|playstation|sony|сон[иy])\b", re.IGNORECASE),
    "iphone": re.compile(r"\b(iphone|айфон|apple)\b", re.IGNORECASE),
    "macbook": re.compile(r"\b(macbook|макбук|apple)\b", re.IGNORECASE),
    "ipad": re.compile(r"\b(ipad|айпад|apple)\b", re.IGNORECASE),
    "steam_deck": re.compile(r"\b(steam\s*deck|стим\s*дек|valve)\b", re.IGNORECASE),
    "samsung": re.compile(
        r"\b(galaxy|s\d{2,3}|a\d{2,3}|m\d{2,3}|z\s*(fold|flip)|fold|flip|ultra|fe|plus|note|галакси|ультра|фолд|флип)\b",
        re.IGNORECASE,
    ),
    "huawei": re.compile(r"\b(huawei|хуавей|mate|pura|nova)\b", re.IGNORECASE),
}

# ---------- Model anchor helper ----------
_MODEL_CLEAN_RE = re.compile(
    r"\b("
    r"\d{1,2}\s*/\s*\d{2,4}"
    r"|\d{2,4}\s*(gb|гб)"
    r"|\d\s*(tb|тб)"
    r"|wifi|lte|5g|cellular"
    r"|sim|esim"
    r"|disk|disc|digital"
    r")\b",
    re.IGNORECASE,
)

def model_anchor(norm_keyword: str) -> str:
    s = _MODEL_CLEAN_RE.sub(" ", norm_keyword)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def anchor_in_title(anchor: str, title_norm: str) -> bool:
    if not anchor:
        return False
    toks = [t for t in anchor.split(" ") if t]
    return all(t in title_norm for t in toks)

def _anchors_in_text(category: str, text_norm: str) -> bool:
    rx = ANCHORS.get(category)
    return bool(rx and rx.search(text_norm))

def _desc_empty(desc: str) -> bool:
    return not desc or not desc.strip()

# ---------- Matching ----------
def _card_slot_value(card: CardSlots, slot: str) -> Any:
    return getattr(card, slot, None)

def match_profile(card: CardSlots, profile: KeywordProfile) -> Tuple[float, List[str], List[str]]:
    req = list(profile.required)
    if not req:
        return 1.0, [], []
    matched: List[str] = []
    missing: List[str] = []
    for s in req:
        pv = profile.slots.get(s, None)
        cv = _card_slot_value(card, s)
        if pv is None:
            missing.append(s)
            continue
        if isinstance(pv, bool):
            ok = bool(cv) is bool(pv)
        else:
            ok = (cv == pv)
        if ok:
            matched.append(s)
        else:
            missing.append(s)
    return len(matched) / len(req), matched, missing

@dataclass
class ScoreResult:
    score: float
    decision: str
    best_keyword: Optional[str]
    explain: Dict[str, Any]

def compute_score(
    nm_id: int,
    category: str,
    target: Optional[str],
    title: str,
    description: str,
    options: Any,
    profiles_for_category: List[KeywordProfile],
    regex_decision: str,
) -> ScoreResult:
    title_norm = normalize_text(title)
    desc_norm = normalize_text(description)

    explain: Dict[str, Any] = {
        "score_ver": 2,
        "nm_id": nm_id,
        "category": category,
        "regex_target": target,
        "regex_decision": regex_decision,
        "anchors_in_title": _anchors_in_text(category, title_norm),
        "gates": {},
        "best_match": {},
    }

    # Gate 1: description required
    if _desc_empty(description):
        explain["gates"]["no_description"] = True
        return ScoreResult(0.0, "reject", None, explain)
    explain["gates"]["no_description"] = False

    # Gate 2: non-original/refurb
    if HARD_NON_ORIG.search(title_norm) or HARD_NON_ORIG.search(desc_norm):
        explain["gates"]["non_original"] = True
        return ScoreResult(0.0, "reject", None, explain)
    explain["gates"]["non_original"] = False

    # Gate 3: accessories/parts/services
    if HARD_ACCESSORY.search(title_norm) or HARD_ACCESSORY.search(desc_norm):
        explain["gates"]["accessory_or_service"] = True
        return ScoreResult(0.0, "reject", None, explain)
    explain["gates"]["accessory_or_service"] = False

    # Gate 4: desc-only block (категория есть только в описании)
    if category in ANCHORS:
        if (not explain["anchors_in_title"]) and _anchors_in_text(category, desc_norm):
            explain["gates"]["desc_only_blocked"] = True
            return ScoreResult(0.0, "reject", None, explain)
    explain["gates"]["desc_only_blocked"] = False

    # Extract slots (title > desc > attrs)
    card_slots = extract_slots_for_category(category, title, description, options)

    # iPad hard rules: only 2025; iPad Air only 11/13
    if category == "ipad":
        if getattr(card_slots, "year", None) != 2025:
            explain["gates"]["ipad_year_not_2025"] = True
            explain["card_slots"] = _dump_card_slots(card_slots)
            return ScoreResult(0.0, "reject", None, explain)
        explain["gates"]["ipad_year_not_2025"] = False

        if (target == "ipad_air") or ("ipad air" in title_norm) or ("ipad air" in desc_norm):
            diag_any = getattr(card_slots, "ipad_diag_any", None)
            if diag_any not in (11.0, 13.0):
                explain["gates"]["ipad_air_bad_diagonal"] = True
                explain["ipad_air_diag_any"] = diag_any
                explain["card_slots"] = _dump_card_slots(card_slots)
                return ScoreResult(0.0, "reject", None, explain)
            explain["gates"]["ipad_air_bad_diagonal"] = False
    
    # PS5: accessories/parts killer
    if category == "ps5":
        t = title_norm
        d = desc_norm

        # Если явно аксессуарные слова — сразу reject
        if PS5_ACCESSORY.search(t):
            explain["gates"]["ps5_accessory_title"] = True
            return ScoreResult(0.0, "reject", None, explain)
        explain["gates"]["ps5_accessory_title"] = False

        # Страховка: "для PS5 / compatible" и при этом нет "консоль" — почти всегда аксессуар
        if (PS5_FOR_CUE.search(t) or PS5_FOR_CUE.search(d)) and not PS5_CONSOLE_CUE.search(t):
            explain["gates"]["ps5_for_but_not_console"] = True
            return ScoreResult(0.0, "reject", None, explain)
        explain["gates"]["ps5_for_but_not_console"] = False

    # Choose best keyword profile
    best: Optional[KeywordProfile] = None
    best_ratio = -1.0
    best_anchor_hit = False
    best_anchor_len = -1
    best_matched: List[str] = []
    best_missing: List[str] = []

    for p in profiles_for_category:
        ratio, matched, missing = match_profile(card_slots, p)

        anc = model_anchor(p.norm)
        anc_hit = anchor_in_title(anc, title_norm)
        anc_len = len(anc.split()) if anc else 0

        better = False
        if ratio > best_ratio:
            better = True
        elif ratio == best_ratio:
            if anc_hit and not best_anchor_hit:
                better = True
            elif anc_hit == best_anchor_hit and anc_len > best_anchor_len:
                better = True

        if better:
            best = p
            best_ratio = ratio
            best_anchor_hit = anc_hit
            best_anchor_len = anc_len
            best_matched = matched
            best_missing = missing

        if best_ratio == 1.0 and best_anchor_hit:
            break

    explain["card_slots"] = _dump_card_slots(card_slots)

    # Special disambiguation: Yandex "Pro" must not map to base model
    if category == "yandex_station":
        if (" pro " in f" {title_norm} ") or (" про " in f" {title_norm} "):
            explain["gates"]["yandex_pro_present"] = True
        else:
            explain["gates"]["yandex_pro_present"] = False

    # Special disambiguation: Samsung Fold7 must not accept Fold6/Fold5 etc
    if category == "samsung":
        if best and ("fold7" in best.norm):
            if ("fold6" in title_norm) or ("fold5" in title_norm) or ("fold 6" in title_norm) or ("fold 5" in title_norm):
                explain["gates"]["fold7_but_title_has_other_fold"] = True
                return ScoreResult(0.0, "reject", None, explain)
            explain["gates"]["fold7_but_title_has_other_fold"] = False

    # If no keyword found but card looks like a real model -> parsed_correct_but_not_in_keywords
    if best is None:
        looks_real = False
        if category == "macbook":
            looks_real = bool(card_slots.diagonal and card_slots.chip and card_slots.storage_gb)
        elif category == "ipad":
            looks_real = bool(card_slots.ipad_size and card_slots.storage_gb and card_slots.year == 2025)
        elif category in ("samsung", "huawei"):
            looks_real = bool(card_slots.ram_gb and card_slots.storage_gb)
        elif category == "ps5":
            looks_real = bool(card_slots.ps5_variant)
        elif category == "steam_deck":
            looks_real = bool(card_slots.oled and card_slots.steamdeck_storage)

        explain["parsed_correct_but_not_in_keywords"] = bool(looks_real)

        # FP=0 => если не уверены, не принимаем
        decision = "unknown" if looks_real else "reject"
        return ScoreResult(0.45 if looks_real else 0.0, decision, None, explain)

    # Build explain best_match
    explain["best_match"] = {
        "keyword": best.raw,
        "target_key": best.target_key,
        "required": list(best.required),
        "ratio": best_ratio,
        "matched": best_matched,
        "missing": best_missing,
        "model_anchor": model_anchor(best.norm),
        "model_anchor_in_title": best_anchor_hit,
    }

    # Yandex Pro rule: if title has pro but keyword doesn't => not accept (unknown bucket)
    if category == "yandex_station" and explain["gates"].get("yandex_pro_present"):
        kw = best.norm
        if (" pro " not in f" {kw} ") and (" про " not in f" {kw} "):
            explain["parsed_correct_but_not_in_keywords"] = True
            return ScoreResult(0.55, "unknown", best.raw, explain)

    # Score: conservative
    base = 0.70 if explain["anchors_in_title"] else 0.55
    score = base + 0.30 * best_ratio
    score = max(0.0, min(1.0, float(score)))


    # Decision rules (FP=0 bias):
    # accept only if strict slot match + model anchor hit in title
    if best_ratio == 1.0 and best_anchor_hit:
        decision = "accept"
    elif best_ratio >= 0.70 and explain["anchors_in_title"]:
        decision = "middle"
    else:
        decision = "reject"

    return ScoreResult(score, decision, best.raw, explain)


def _dump_card_slots(s: CardSlots) -> Dict[str, Any]:
    return {
        "year": getattr(s, "year", None),
        "storage_gb": getattr(s, "storage_gb", None),
        "ram_gb": getattr(s, "ram_gb", None),
        "sim_type": getattr(s, "sim_type", None),
        "chip": getattr(s, "chip", None),
        "diagonal": getattr(s, "diagonal", None),
        "ipad_connectivity": getattr(s, "ipad_connectivity", None),
        "ipad_size": getattr(s, "ipad_size", None),
        "ipad_diag_any": getattr(s, "ipad_diag_any", None),
        "oled": getattr(s, "oled", None),
        "steamdeck_storage": getattr(s, "steamdeck_storage", None),
        "ps5_variant": getattr(s, "ps5_variant", None),
        "ps5_edition": getattr(s, "ps5_edition", None),
    }