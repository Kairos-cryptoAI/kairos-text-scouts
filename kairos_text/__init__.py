"""Kairos Layer 1B — Text Scouts.

A universal event aggregator over official APIs/feeds (GDELT news, RSS backstop,
Bright Data X/Reddit). It normalizes and deduplicates events, drops the noise with
a cheap local filter, then asks an LLM (low reasoning effort -> DeepSeek-V4-Flash,
non-thinking) to turn the few relevant items into structured ``SentimentSignal``s.
The expensive model only ever sees pre-filtered text; a local fallback keeps the
layer alive if the model is down.
"""
from __future__ import annotations

__version__ = "0.1.0"

from .dedup import EventDeduplicator
from .filter import LocalRelevanceFilter
from .models import NewsItem
from .normalize import EventNormalizer
from .sentiment import SentimentExtractor

__all__ = [
    "NewsItem",
    "LocalRelevanceFilter",
    "EventNormalizer",
    "EventDeduplicator",
    "SentimentExtractor",
    "__version__",
]
