from __future__ import annotations

import re
import yaml
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

FLAGS = re.IGNORECASE | re.UNICODE


def compile_list(patterns: Optional[List[str]]) -> List[re.Pattern]:
    if not patterns:
        return []
    return [re.compile(p, FLAGS) for p in patterns]


@dataclass
class RegexTarget:
    target_id: str
    match_any: List[re.Pattern]
    conflict_if_any: List[re.Pattern]


@dataclass
class CategoryRules:
    name: str
    positive_title: List[re.Pattern]
    positive_description: List[re.Pattern]
    negative_title: List[re.Pattern]
    decision_require_any: List[re.Pattern]
    decision_and_any: List[re.Pattern]
    targets: List[RegexTarget]
    conflict_groups: List[Dict[str, Any]]


@dataclass
class CompiledRules:
    ver: int
    global_hard_title: List[re.Pattern]
    global_hard_desc: List[re.Pattern]
    category_priority: List[str]
    categories: Dict[str, CategoryRules]
    disambig_mode: str
    disambig_fields: List[str]
    disambig_priority_field: str


def load_rules(path: str) -> CompiledRules:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    global_hard_title = compile_list(raw.get("global_hard_reject_title", []))

    # Ты сделал risk по описанию hard. Поддержим оба ключа:
    # - если ты переименовал блок в global_hard_reject_description -> берём его
    # - иначе берём global_soft_risk_description, но трактуем как hard
    hard_desc_src = raw.get("global_hard_reject_description")
    if hard_desc_src is None:
        hard_desc_src = raw.get("global_soft_risk_description", [])
    global_hard_desc = compile_list(hard_desc_src)

    dis = raw.get("disambiguation", {}) or {}
    dis_mode = dis.get("mode", "unknown_on_conflict")
    dis_fields = dis.get("fields", ["title", "description"])
    dis_priority = dis.get("priority_field", "title")

    cat_priority = raw.get("category_priority", [])

    categories: Dict[str, CategoryRules] = {}
    raw_cats = raw.get("categories", {}) or {}

    for cname, c in raw_cats.items():
        positive_title = compile_list(c.get("positive_title", []))
        positive_desc = compile_list(c.get("positive_description", []))
        negative_title = compile_list(c.get("negative_title", []))

        dr = c.get("decision_rules", {}) or {}
        require_any = compile_list(dr.get("require_any_of", []))
        and_any = compile_list(dr.get("and_any_of", []))

        targets: List[RegexTarget] = []
        raw_targets = (c.get("targets", {}) or {})
        for tid, t in raw_targets.items():
            t_match = compile_list(t.get("match_any", []))
            t_conf = compile_list(t.get("conflict_if_any", []))
            targets.append(RegexTarget(target_id=tid, match_any=t_match, conflict_if_any=t_conf))

        conflict_groups = c.get("conflict_groups", []) or []

        categories[cname] = CategoryRules(
            name=cname,
            positive_title=positive_title,
            positive_description=positive_desc,
            negative_title=negative_title,
            decision_require_any=require_any,
            decision_and_any=and_any,
            targets=targets,
            conflict_groups=conflict_groups,
        )

    return CompiledRules(
        ver=int(raw.get("version", 0)),
        global_hard_title=global_hard_title,
        global_hard_desc=global_hard_desc,
        category_priority=cat_priority,
        categories=categories,
        disambig_mode=dis_mode,
        disambig_fields=dis_fields,
        disambig_priority_field=dis_priority,
    )