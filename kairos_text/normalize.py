"""Normalize heterogeneous source items into clean, comparable NewsItems.

Sources speak different dialects (HTML in RSS descriptions, entities in headlines,
ragged whitespace). The normalizer unescapes HTML entities, strips tags, collapses
whitespace, bounds field length and drops empty-title items, so the downstream
dedup / relevance / LLM stages see uniform text regardless of provenance.
"""
from __future__ import annotations

import html
import re
from typing import Iterable, List

from .models import NewsItem

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def clean(value: str) -> str:
    value = html.unescape(value or "")
    value = _TAG.sub(" ", value)
    return _WS.sub(" ", value).strip()


class EventNormalizer:
    def __init__(self, *, max_title: int = 240, max_body: int = 600) -> None:
        self.max_title = max_title
        self.max_body = max_body

    def normalize(self, items: Iterable[NewsItem]) -> List[NewsItem]:
        out: List[NewsItem] = []
        for item in items:
            title = clean(item.title)[: self.max_title]
            if not title:
                continue
            item.title = title
            item.body = clean(item.body)[: self.max_body]
            item.url = (item.url or "").strip()
            item.source = (item.source or item.source_kind or "unknown").strip()
            out.append(item)
        return out
