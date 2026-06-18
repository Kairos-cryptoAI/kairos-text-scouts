"""Kairos Layer 1B — Text Scouts.

Monitors news / RSS / X, drops the noise with a cheap local filter, then asks an
LLM (low reasoning effort) to turn the few relevant items into a structured
``SentimentSignal``. The expensive model only ever sees pre-filtered text.
"""
from __future__ import annotations

__version__ = "0.1.0"

from .models import NewsItem
from .filter import LocalRelevanceFilter
from .sentiment import SentimentExtractor

__all__ = ["NewsItem", "LocalRelevanceFilter", "SentimentExtractor", "__version__"]
