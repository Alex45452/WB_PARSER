from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List

@dataclass(frozen=True)
class TargetsByCategory:
    mapping: Dict[str, List[str]]

def load_targets_by_categories(path: str) -> TargetsByCategory:
    """
    Expected format example:
      CategoryName:
        - target1
        - target2
    or any simple "category: target" lines.
    We'll parse a tolerant format:
      [category]
      target
      target
    """
    mapping: Dict[str, List[str]] = {}
    cur_cat: str | None = None

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            # YAML-like "Category:"
            if line.endswith(":") and len(line) > 1:
                cur_cat = line[:-1].strip()
                mapping.setdefault(cur_cat, [])
                continue

            # "- target" YAML bullet
            if line.startswith("- "):
                if not cur_cat:
                    raise ValueError("Target before category header")
                mapping[cur_cat].append(line[2:].strip())
                continue

            # INI-like "[Category]"
            if line.startswith("[") and line.endswith("]"):
                cur_cat = line[1:-1].strip()
                mapping.setdefault(cur_cat, [])
                continue

            # "category: target" single line
            if ":" in line and not line.startswith("http"):
                left, right = line.split(":", 1)
                cat = left.strip()
                tgt = right.strip()
                mapping.setdefault(cat, []).append(tgt)
                cur_cat = cat
                continue

            # plain target line under current category
            if cur_cat:
                mapping[cur_cat].append(line)
            else:
                raise ValueError(f"Unscoped target line: {line}")

    # de-dup
    for k in list(mapping.keys()):
        seen = set()
        out = []
        for t in mapping[k]:
            tt = t.strip()
            if tt and tt not in seen:
                seen.add(tt)
                out.append(tt)
        mapping[k] = out

    return TargetsByCategory(mapping=mapping)