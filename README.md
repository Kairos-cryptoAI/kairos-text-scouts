# kairos-text-scouts

**Layer 1B — Text Scouts.** A universal **event aggregator** over *official* APIs/feeds
(no self-hosted scrapers, no proxies, no 403/captcha juggling). It normalizes and
deduplicates events, drops the noise with a cheap **local filter**, then submits the
few relevant items through the explicit `TEXT_SCOUTS` LLM workload. Provider/model
selection belongs to `kairos-llm`; the workload currently maps to the low-cost,
non-thinking sentiment route. The model only ever sees pre-filtered text. Items
without a trustworthy publication time, older than 30 minutes, or more than 5
seconds in the future are rejected before deduplication. The same five-second
ingestion skew bound is used by Router; future evidence is still never scored
against an earlier market snapshot.

## Pipeline
```
sources ──▶ normalize ──▶ dedup ──▶ relevance (+top-K) ──▶ LLM (TEXT_SCOUTS) ──▶ SentimentSignal
```
Example output (matches the spec): `{"topic": "SEC ETF", "sentiment": 0.85, "impact": "bullish"}`.

## Sources
| source | provider | cost | enabled |
| --- | --- | --- | --- |
| **GDELT** DOC 2.0 | free official news API — already indexes Reuters, Bloomberg, CNBC, Coindesk… | $0 | always |
| **RSS** | Coindesk + Cointelegraph (crypto-native backstop) | $0 | always |
| **X / Twitter** | Bright Data **Web Scraper API** — live scrape of influencer handles (sync `/scrape`, async fallback) | metered | when token + dataset id set |
| **Reddit** | **official Reddit API** (OAuth2 application-only) — newest posts per subreddit | $0 | when client id + secret set |

> **X** uses Bright Data's **Web Scraper API** (on-demand live scraping, **not** the static Dataset
> Marketplace; sync `/scrape` with async fallback). **Reddit** uses the **official Reddit API**
> (free, OAuth2 application-only) — every poll fetches the newest posts.

Each source is isolated: one provider failing (e.g. GDELT rate-limiting) never blinds the
layer. Reuters/Bloomberg no longer publish public RSS, so GDELT covers them. A real
transformer (BERT/FinBERT) can replace `LocalRelevanceFilter` behind its `select()` interface.
If DeepSeek-Flash is down the layer degrades to a deterministic local keyword sentiment.
The degraded path emits only attributable directional evidence; neutral,
contradictory, stale, or provenance-free items are an abstention (no signal).

## Configuration (env, `KAIROS_` prefix)
```bash
# News (free, on by default)
KAIROS_GDELT_QUERY='(bitcoin OR btc OR ethereum OR eth OR crypto OR etf OR sec OR cpi) sourcelang:english'
KAIROS_GDELT_TIMESPAN=15min
KAIROS_MAX_EVENT_AGE_S=1800
KAIROS_MAX_FUTURE_SKEW_S=5
# X / Twitter (optional — Bright Data Web Scraper API)
KAIROS_BRIGHTDATA_API_TOKEN=...
KAIROS_BRIGHTDATA_X_DATASET_ID=...
# Reddit (optional — official Reddit API, free; register an app at reddit.com/prefs/apps)
KAIROS_REDDIT_CLIENT_ID=...
KAIROS_REDDIT_CLIENT_SECRET=...
```

## Local development
```powershell
winget install --id astral-sh.uv --exact
uv sync --locked
uv run --locked ruff check kairos_text tests
uv run --locked ruff format --check kairos_text tests
uv run --locked mypy kairos_text
uv run --locked bandit -q -r kairos_text -x tests
uv run --locked pytest -q --tb=short
uv build --no-sources
uv run --locked python -m kairos_text
```
Emits `kairos.sentiment.signal`. The LLM call goes through
[`kairos-llm`](https://github.com/Kairos-cryptoAI/kairos-llm).
Every emitted signal carries the URLs or source handles that support it; the
strict output schema rejects invented item references. `sources` is canonical,
unique, and lexicographically ordered. LLM item IDs are batch-local 1-based
references and never leave this service. A single-source LLM confidence is capped
at `0.65`; low-confidence, neutral, directionally inconsistent, or unattributed
results are abstentions and are not published. The bus `message_id` is deterministic
for source/topic/direction/provenance plus evidence time, and `produced_at` is the newest backing
event time, so a retry after partial publication is an exact downstream duplicate.

## Runtime delivery durability

The Redis backend uses `kairos-persistence`: publications are committed to a
PostgreSQL outbox before dispatch. Configure `KAIROS_PERSISTENCE_DATABASE_URL`
through the deployment secret provider. The in-memory backend intentionally
bypasses persistence and is limited to local tests.

---
Part of the [Kairos](https://github.com/Kairos-cryptoAI/kairos) system. MIT licensed.
