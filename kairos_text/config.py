"""Text Scouts configuration.

The 1B layer is an event aggregator over *official* APIs (no self-hosted scrapers
or proxies): GDELT for news, RSS as a backstop, and Bright Data for X / Reddit.
The Bright Data sources turn themselves on only when an API token + dataset id are
configured, so the layer runs fine for free out of the box.
"""
from __future__ import annotations

from typing import List

from kairos_core.config import CoreSettings


class TextSettings(CoreSettings):
    service_name: str = "kairos-text-scouts"
    poll_interval_s: float = 300.0          # aggregate every 5 minutes (spec)
    top_k: int = 5                           # keep ~5 of the incoming items
    relevance_threshold: float = 3.0
    dedup_window_s: float = 21600.0          # 6h rolling window for cross-poll dedup

    # --- News via GDELT (free, official; aggregates Reuters/Bloomberg/CNBC/...) ---
    enable_gdelt: bool = True
    gdelt_query: str = (
        '(bitcoin OR btc OR ethereum OR eth OR crypto OR etf OR sec OR cpi) sourcelang:english'
    )
    gdelt_timespan: str = "15min"
    gdelt_max_records: int = 75

    # --- RSS backstop (Reuters/Bloomberg dropped public RSS; GDELT covers them) ---
    enable_rss: bool = True
    rss_feeds: List[str] = [
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://cointelegraph.com/rss",
    ]

    # --- Social via Bright Data (managed API; no proxy/scrapers of our own) ---
    brightdata_api_token: str = ""
    brightdata_x_dataset_id: str = ""
    brightdata_reddit_dataset_id: str = ""
    brightdata_poll_timeout_s: float = 90.0
    x_accounts: List[str] = ["elonmusk", "lookonchain", "whale_alert", "cz_binance"]
    x_num_posts: int = 10
    subreddits: List[str] = ["CryptoCurrency", "Bitcoin", "ethfinance"]

    @property
    def enable_x(self) -> bool:
        return bool(self.brightdata_api_token and self.brightdata_x_dataset_id and self.x_accounts)

    @property
    def enable_reddit(self) -> bool:
        return bool(self.brightdata_api_token and self.brightdata_reddit_dataset_id and self.subreddits)
