"""Cheap, deterministic relevance pre-filter.

The spec calls for a local model (e.g. BERT) that throws away ~95% of incoming
items before the LLM is invoked. This is a transparent keyword/impact scorer
exposing the same ``select`` interface, so a real transformer can be dropped in
later without touching the rest of the layer.
"""
from __future__ import annotations

from typing import List, Sequence

from .models import NewsItem

RELEVANCE_TERMS = {
    "bitcoin": 3, "btc": 3, "ethereum": 2, "eth": 2, "crypto": 2, "sec": 3,
    "etf": 3, "fed": 3, "cpi": 3, "inflation": 2, "rate": 2, "hack": 3,
    "liquidation": 2, "regulation": 2, "approval": 2, "lawsuit": 2, "halving": 2,
}
IMPACT_TERMS = {
    "surge": 2, "soar": 2, "rally": 2, "plunge": 2, "crash": 3, "ban": 3,
    "approve": 2, "reject": 2, "breach": 3, "record": 1, "warning": 2,
}


class LocalRelevanceFilter:
    def __init__(self, threshold: float = 3.0, top_k: int = 5) -> None:
        self.threshold = threshold
        self.top_k = top_k

    def score(self, item: NewsItem) -> float:
        text = item.text.lower()
        s = sum(w for term, w in RELEVANCE_TERMS.items() if term in text)
        s += sum(w for term, w in IMPACT_TERMS.items() if term in text)
        return float(s)

    def select(self, items: Sequence[NewsItem]) -> List[NewsItem]:
        scored = [(self.score(it), it) for it in items]
        keep = [(sc, it) for sc, it in scored if sc >= self.threshold]
        keep.sort(key=lambda x: x[0], reverse=True)
        return [it for _, it in keep[: self.top_k]]
