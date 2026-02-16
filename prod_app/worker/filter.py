from dataclasses import dataclass

@dataclass
class FilterResult:
    accept: bool
    product_key: str | None = None
    score: float | None = None

def fast_filter(title: str, desc: str) -> FilterResult:
    # TODO: сюда вставим твой keyword-first -> hard anti -> scoring
    return FilterResult(accept=True, product_key=None, score=None)
