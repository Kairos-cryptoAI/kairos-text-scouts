"""Text Scouts configuration."""
from __future__ import annotations

from typing import List

from kairos_core.config import CoreSettings


class TextSettings(CoreSettings):
    service_name: str = "kairos-text-scouts"
    poll_interval_s: float = 300.0          # batch every 5 minutes (spec)
    top_k: int = 5                           # keep ~5 of ~100 items
    relevance_threshold: float = 3.0
    rss_feeds: List[str] = [
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://feeds.reuters.com/reuters/businessNews",
    ]
