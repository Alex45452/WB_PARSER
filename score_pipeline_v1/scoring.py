# wb_parser/score_pipeline/scoring.py

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .normalize import normalize_text
from .slots import (
    extract_slots_for_category,
    CardSlots,
)

from .keyword_profiles import KeywordProfile


# ---------- Hard reject: non-original suspicion ----------
HARD_NON_ORIG = re.compile(
    r"\b(копи[яи]|реплик[аи]|аналог|no\s*name|oem|refurb|восстановлен|как\s*новый|подделк|fake|replica|не\s*оригинал|неоригинал)\b",
    re.IGNORECASE,
)

# ---------- Anchors by category (for desc-only gating / base boost) ----------
ANCHORS = {
    "yandex_station": re.compile(r"\b(яндекс|yandex|алиса|станци)\b"),
    "ps5": re.compile(r"\b(ps5|playstation|sony|сон[иy])\b"),
    "iphone": re.compile(r"\b(iphone|айфон|apple)\b"),
    "macbook": re.compile(r"\b(macbook|макбук|apple)\b"),
    "ipad": re.compile(r"\b(ipad|айпад|apple)\b"),
    "steam_deck": re.compile(r"\b(steam\s*deck|стим\s*дек|valve)\b"),
    # samsung often without "samsung"
    "samsung": re.compile(
        r"\b(galaxy|s\d{2,3}|a\d{2,3}|m\d{2,3}|z\s*(fold|flip)|fold|flip|ultra|fe|plus|note|галакси|ультра|фолд|флип)\b",
        re.IGNORECASE,
    ),
    "huawei": re.compile(r"\b(huawei|хуавей|mate|pura|nova)\b", re.IGNORECASE),
}

# ---------- Soft patterns (only penalty, not reject) ----------
SOFT_SPAM_DESC = re.compile(
    r"\b(описание\s*продукта|пакет\s*включает|мы\s*специализируемся|добро\s*пожаловать)\b",
    re.IGNORECASE,
)

# ---------- Model anchor helper (prevents slot-only collapse: e.g. 8/256 across models) ----------
_MODEL_CLEAN_RE = re.compile(
    r"\b("
    r"\d{1,2}\s*/\s*\d{2,4}"          # 8/256
    r"|\d{2,4}\s*(gb|гб)"             # 256gb
    r"|\d\s*(tb|тб)"                  # 1tb
    r"|wifi|lte|5g|cellular"
    r"|sim|esim"
    r"|disk|disc|digital"
    r")\b",
    re.IGNORECASE,
)


def model_anchor(norm_keyword: str) -> str:
    """
    Remove slot-like tokens, keep the model skeleton:
    'samsung s25 fe', 'huawei pura 80 pro', 'ps5 slim', 'steam deck oled'
    """
    s = _MODEL_CLEAN_RE.sub(" ", norm_keyword)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def anchor_in_title(anchor: str, title_norm: str) -> bool:
    if not anchor:
        return False
    toks = [t for t in anchor.split(" ") if t]
    return all(t in title_norm for t in toks)


# ---------- Matching ----------
def _card_slot_value(card: CardSlots, slot: str) -> Any:
    return getattr(card, slot, None)


def match_profile(card: CardSlots, profile: KeywordProfile) -> Tuple[float, List[str], List[str]]:
    """
    Strict match: value must equal.
    Returns (ratio over required slots, matched_slots, missing_slots).
    If keyword says slot is required but keyword slot is None => treat as missing (fail-closed).
    """
    req = list(profile.required)
    if not req:
        return 1.0, [], []

    matched: List[str] = []
    missing: List[str] = []

    for s in req:
        pv = profile.slots.get(s, None)
        cv = _card_slot_value(card, s)

        # fail-closed if we couldn't parse keyword slot
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


def _anchors_in_text(category: str, text_norm: str) -> bool:
    rx = ANCHORS.get(category)
    return bool(rx and rx.search(text_norm))


@dataclass
class ScoreResult:
    score: float
    decision: str  # accept | middle | reject | unknown
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
    """
    Decision logic:
      - hard reject for non-original suspicion
      - reject for "desc-only category signal but no title anchors" (main FP killer)
      - accept only if best_ratio == 1.0 and score >= 0.70
      - middle if score >= 0.40
      - reject otherwise
      - preserve upstream unknown (regex_decision == 'unknown') => decision 'unknown'
    """
    title_norm = normalize_text(title)
    desc_norm = normalize_text(description)

    explain: Dict[str, Any] = {
        "score_ver": 1,
        "nm_id": nm_id,
        "category": category,
        "target": target,
        "regex_decision": regex_decision,
        "anchors_in_title": _anchors_in_text(category, title_norm),
        "gates": {},
        "soft": {"description": []},
        "best_match": {},
    }

    # Gate: non-original suspicion
    if HARD_NON_ORIG.search(title_norm) or HARD_NON_ORIG.search(desc_norm):
        explain["gates"]["non_original"] = True
        return ScoreResult(0.0, "reject", None, explain)
    explain["gates"]["non_original"] = False

    # Gate: desc-only block (category signal in desc, but no anchors in title)
    # This is the main "карточка про будильник, а в описании яндекс станция" killer.
    if category in ANCHORS:
        if (not explain["anchors_in_title"]) and _anchors_in_text(category, desc_norm):
            explain["gates"]["desc_only_blocked"] = True
            return ScoreResult(0.0, "reject", None, explain)
    explain["gates"]["desc_only_blocked"] = False

    # Soft penalty: spammy description template
    soft_penalty = 0.0
    if SOFT_SPAM_DESC.search(desc_norm):
        explain["soft"]["description"].append("spam_desc")
        soft_penalty += 0.08

    # Extract card slots with priority: title > description > attrs (inside slots module)
    card_slots = extract_slots_for_category(category, title, description, options)

    # Choose best keyword profile by:
    #   1) slot ratio
    #   2) model-anchor hit in title
    #   3) anchor length (more specific)
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

        # early exit: perfect + anchor hit
        if best_ratio == 1.0 and best_anchor_hit:
            break

    # Base score (PS5 boosted for higher recall)
    anchors = explain["anchors_in_title"]
    if category == "ps5":
        base = 0.78 if anchors else 0.62
    else:
        base = 0.70 if anchors else 0.55

    base = max(0.0, base - soft_penalty)

    # If no keyword profiles => can't match; keep as middle/reject by base
    if best is None:
        score = max(0.0, min(1.0, base))
        decision = "middle" if score >= 0.40 else "reject"
        if regex_decision == "unknown":
            decision = "unknown"
        explain["best_match"] = {"keyword": None, "ratio": None}
        explain["card_slots"] = _dump_card_slots(card_slots)
        return ScoreResult(score, decision, None, explain)

    # score includes match ratio strongly
    score = base + 0.30 * best_ratio
    score = max(0.0, min(1.0, float(score)))

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
    explain["card_slots"] = _dump_card_slots(card_slots)

    # Decisions
    if score >= 0.70 and best_ratio == 1.0:
        decision = "accept"
    elif score >= 0.40:
        decision = "middle"
    else:
        decision = "reject"

    # Preserve upstream unknown
    if regex_decision == "unknown":
        decision = "unknown"

    return ScoreResult(score, decision, best.raw, explain)


def _dump_card_slots(s: CardSlots) -> Dict[str, Any]:
    return {
        "storage_gb": getattr(s, "storage_gb", None),
        "ram_gb": getattr(s, "ram_gb", None),
        "sim_type": getattr(s, "sim_type", None),
        "chip": getattr(s, "chip", None),
        "diagonal": getattr(s, "diagonal", None),
        "ipad_connectivity": getattr(s, "ipad_connectivity", None),
        "ipad_size": getattr(s, "ipad_size", None),
        "oled": getattr(s, "oled", None),
        "steamdeck_storage": getattr(s, "steamdeck_storage", None),
        "ps5_variant": getattr(s, "ps5_variant", None),
        "ps5_edition": getattr(s, "ps5_edition", None),
    }