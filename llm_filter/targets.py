from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class TargetsByCategory:
    mapping: Dict[str, List[str]]


def load_targets_by_categories(path: str) -> TargetsByCategory:
    """
    Supports:
      iphone:
        - target1
        - target2

    Also supports:
      [iphone]
      target1
      target2

    And:
      iphone: target1
      iphone: target2
    """
    mapping: Dict[str, List[str]] = {}
    cur_cat: str | None = None

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            # Keep indentation for YAML detection, but we also need stripped
            line = raw.rstrip("\n")
            s = line.strip()

            if not s or s.startswith("#"):
                continue

            # INI-like section
            if s.startswith("[") and s.endswith("]"):
                cur_cat = s[1:-1].strip()
                mapping.setdefault(cur_cat, [])
                continue

            # YAML header: "category:"
            if s.endswith(":") and len(s) > 1 and ":" not in s[:-1]:
                cur_cat = s[:-1].strip()
                mapping.setdefault(cur_cat, [])
                continue

            # YAML bullet: "- target" (after strip)
            if s.startswith("- "):
                if not cur_cat:
                    raise ValueError(f"Target before category header: {s}")
                mapping[cur_cat].append(s[2:].strip())
                continue

            # "category: target" one-liner
            if ":" in s and not s.startswith("http"):
                left, right = s.split(":", 1)
                cat = left.strip()
                tgt = right.strip()
                mapping.setdefault(cat, [])
                if tgt:
                    mapping[cat].append(tgt)
                cur_cat = cat
                continue

            # plain target line under current category
            if cur_cat:
                mapping[cur_cat].append(s)
            else:
                raise ValueError(f"Unscoped target line: {s}")

    # de-dup + cleanup
    for k in list(mapping.keys()):
        seen = set()
        out: list[str] = []
        for t in mapping[k]:
            tt = t.strip()
            if tt and tt not in seen:
                seen.add(tt)
                out.append(tt)
        mapping[k] = out

    return TargetsByCategory(mapping=mapping)