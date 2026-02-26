from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from rules_loader import CompiledRules, CategoryRules


def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.lower().replace("ё", "е")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def any_match(patterns: List[re.Pattern], text: str) -> bool:
    for p in patterns:
        if p.search(text):
            return True
    return False


def classify_one(rules: CompiledRules, title_raw: Optional[str], desc_raw: Optional[str]) -> Dict[str, Any]:
    title = normalize_text(title_raw)
    desc = normalize_text(desc_raw)

    # 1) Global hard reject (title)
    if title and any_match(rules.global_hard_title, title):
        return {
            "decision": "reject",
            "category": None,
            "target": None,
            "reject_reason": "GLOBAL_HARD_TITLE",
            "title_used": True,
            "matched_in": "title",
        }

    # 1b) Global hard reject (description)
    if desc and any_match(rules.global_hard_desc, desc):
        return {
            "decision": "reject",
            "category": None,
            "target": None,
            "reject_reason": "GLOBAL_HARD_DESC",
            "title_used": True,
            "matched_in": "description",
        }

    # 2) Category by title
    for cname in rules.category_priority:
        c = rules.categories.get(cname)
        if not c:
            continue
        if title and any_match(c.positive_title, title):
            return decide_for_category(rules, c, title, desc, matched_in="title", title_used=True)

    # 3) Category by description (precision-first => unknown on desc-only accept)
    for cname in rules.category_priority:
        c = rules.categories.get(cname)
        if not c:
            continue
        if desc and any_match(c.positive_description, desc):
            res = decide_for_category(rules, c, title, desc, matched_in="description", title_used=False)
            if res["decision"] == "accept":
                res["decision"] = "unknown"
                res["reject_reason"] = "DESC_ONLY_MATCH"
            return res

    return {
        "decision": "unknown",
        "category": None,
        "target": None,
        "reject_reason": "NO_MATCH",
        "title_used": True,
        "matched_in": None,
    }


def decide_for_category(
    rules: CompiledRules,
    c: CategoryRules,
    title: str,
    desc: str,
    matched_in: str,
    title_used: bool,
) -> Dict[str, Any]:
    # category-specific negative (check TITLE always)
    if title and c.negative_title and any_match(c.negative_title, title):
        return {
            "decision": "reject",
            "category": c.name,
            "target": None,
            "reject_reason": "CATEGORY_NEG_TITLE",
            "title_used": True,
            "matched_in": matched_in,
        }

    # decision rules (e.g. yandex_station)
    check_text = title if title else desc
    if c.decision_require_any and not any_match(c.decision_require_any, check_text):
        return {
            "decision": "unknown",
            "category": c.name,
            "target": None,
            "reject_reason": "DECISION_RULES_FAIL",
            "title_used": title_used,
            "matched_in": matched_in,
        }
    if c.decision_and_any and not any_match(c.decision_and_any, check_text):
        return {
            "decision": "unknown",
            "category": c.name,
            "target": None,
            "reject_reason": "DECISION_RULES_FAIL",
            "title_used": title_used,
            "matched_in": matched_in,
        }

    target, conflict = disambiguate_target(c, title, desc)
    if conflict:
        return {
            "decision": "reject" if rules.disambig_mode == "reject_on_conflict" else "unknown",
            "category": c.name,
            "target": target,
            "reject_reason": "IN_CATEGORY_CONFLICT",
            "title_used": title_used,
            "matched_in": matched_in,
        }

    return {
        "decision": "accept",
        "category": c.name,
        "target": target,
        "reject_reason": None,
        "title_used": title_used,
        "matched_in": matched_in,
    }


def disambiguate_target(c: CategoryRules, title: str, desc: str) -> Tuple[Optional[str], bool]:
    if not c.targets:
        return None, False

    # title first
    if title:
        for t in c.targets:
            if t.match_any and any_match(t.match_any, title):
                conflict = bool(t.conflict_if_any and any_match(t.conflict_if_any, title))
                if not conflict and c.conflict_groups and conflict_group_triggers(c.conflict_groups, title):
                    conflict = True
                return t.target_id, conflict

    # then description
    if desc:
        for t in c.targets:
            if t.match_any and any_match(t.match_any, desc):
                conflict = bool(t.conflict_if_any and any_match(t.conflict_if_any, desc))
                if not conflict and c.conflict_groups and conflict_group_triggers(c.conflict_groups, desc):
                    conflict = True
                return t.target_id, conflict

    return None, False


def conflict_group_triggers(conflict_groups: List[Dict[str, Any]], text: str) -> bool:
    # minimal support for: unknown_if_multiple_distinct_matches
    for g in conflict_groups:
        if g.get("rule") != "unknown_if_multiple_distinct_matches":
            continue
        any_of = g.get("any_of", [])
        if not any_of:
            continue
        pats = [re.compile(p, re.IGNORECASE | re.UNICODE) for p in any_of]
        hits = sum(1 for p in pats if p.search(text))
        if hits >= 2:
            return True
    return False