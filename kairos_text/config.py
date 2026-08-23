"""Text Scouts configuration.

The 1B layer is an event aggregator over *official* APIs (no self-hosted scrapers
or proxies): GDELT for news, RSS as a backstop, and the official X and Reddit APIs.
"""

from __future__ import annotations

from kairos_core.config import CoreSettings
from pydantic import Field, SecretStr


class TextSettings(CoreSettings):
    service_name: str = "kairos-text-scouts"
    poll_interval_s: float = Field(default=300.0, gt=0)  # aggregate every 5 minutes (spec)
    top_k: int = Field(default=5, ge=1)  # keep ~5 of the incoming items
    relevance_threshold: float = Field(default=3.0, ge=0)
    dedup_window_s: float = Field(default=21600.0, gt=0)  # 6h rolling window for cross-poll dedup
    max_event_age_s: float = Field(default=1800.0, ge=0)  # accept evidence up to 30 minutes old
    max_future_skew_s: float = Field(default=5.0, ge=0)  # match downstream ingestion skew bound
    allow_estimated_timestamps: bool = False

    # --- News via GDELT (free, official; aggregates Reuters/Bloomberg/CNBC/...) ---
    enable_gdelt: bool = True
    gdelt_query: str = "(bitcoin OR btc OR ethereum OR eth OR crypto OR etf OR sec OR cpi) sourcelang:english"
    gdelt_timespan: str = "15min"
    gdelt_max_records: int = 75

    # --- RSS backstop (Reuters/Bloomberg dropped public RSS; GDELT covers them) ---
    enable_rss: bool = True
    rss_feeds: list[str] = [
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://cointelegraph.com/rss",
    ]

    # --- Official X API v2 (app-only bearer, pay per User/Post resource) ---
    x_bearer_token: SecretStr = SecretStr("")
    # Ordered by information quality. Secondary on-chain observers are last and
    # can never consume budget before regulators, macro, venues, or projects.
    x_accounts: list[str] = [
        "SECGov",
        "federalreserve",
        "CFTC",
        "binance",
        "bitcoincoreorg",
        "ethereum",
        "solana",
        "BNBCHAIN",
        "Ripple",
        "lookonchain",
        "whale_alert",
    ]
    x_max_results: int = Field(default=5, ge=5, le=100)
    x_max_pages: int = Field(default=2, ge=1, le=10)
    x_timeout_s: float = Field(default=30.0, gt=0)
    x_monthly_budget_microusd: int = Field(default=2_000_000, ge=0, le=2_000_000)
    x_post_read_unit_cost_microusd: int = Field(default=5_000, ge=5_000)
    x_user_read_unit_cost_microusd: int = Field(default=10_000, ge=10_000)

    # --- Reddit via the official Reddit API (OAuth2 application-only; free) ---
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "kairos-text-scouts/0.1 by Kairos-cryptoAI"
    reddit_listing: str = "new"  # freshest first
    reddit_limit: int = 25
    subreddits: list[str] = ["CryptoCurrency", "Bitcoin", "ethfinance"]

    @property
    def enable_x(self) -> bool:
        return bool(self.x_bearer_token.get_secret_value() and self.x_accounts)

    @property
    def enable_reddit(self) -> bool:
        return bool(self.reddit_client_id and self.reddit_client_secret and self.subreddits)
