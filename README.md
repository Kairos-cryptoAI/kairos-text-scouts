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
| **X / Twitter** | official X API v2, User lookup then User Posts timeline | $0.010 per returned User once + $0.005 per returned Post at the registered 2026-08-18 rates | when Bearer Token is set |
| **Reddit** | **official Reddit API** (OAuth2 application-only) — newest posts per subreddit | $0 | when client id + secret set |

**X** resolves a configured handle through `GET /2/users/by/username/{username}` and
stores the immutable User ID durably. Subsequent polls use the documented
`GET /2/users/{id}/tweets` endpoint with app-only Bearer authentication, `since_id`,
and `exclude=replies,retweets`; broad search is never used. Registered prices are the
[official $0.010 per User and $0.005 per Post read](https://docs.x.com/x-api/getting-started/pricing).
Every request reserves its worst-case cost in PostgreSQL before touching X; the
reservation is then reduced to the resources actually returned. Per-account Post
cursors advance only after the complete normalize/filter/LLM/outbox pipeline succeeds.
The default cap is exactly `$10.000000` per UTC month. **Reddit** uses its official
OAuth2 application-only API.

Each source is isolated: one provider failing (e.g. GDELT rate-limiting) never blinds the
layer. Reuters/Bloomberg no longer publish public RSS, so GDELT covers them. A real
transformer (BERT/FinBERT) can replace `LocalRelevanceFilter` behind its `select()` interface.
If DeepSeek-Flash is down the layer degrades to a deterministic local keyword sentiment.
The degraded path emits only attributable directional evidence; neutral,
contradictory, stale, or provenance-free items are an abstention (no signal).

## Feed qualification

Run a source-only probe before shadow operation:

```powershell
uv run --locked kairos-feed-qualify `
  --samples 3 `
  --output $env:TEMP\kairos-feed-qualification.json `
  --overwrite
```

GDELT and each RSS feed are measured independently. Reddit credentials are read only
from `--reddit-client-id-file` / `--reddit-client-secret-file`. X is never called merely
because a token exists: the paid request additionally requires
`--x-bearer-token-file`, the explicit `--allow-metered-x-probe` flag, and an exact
`--maximum-x-cost-usd` hard cap. For the four default accounts with ten Posts each,
the first one-sample probe is bounded to `$0.24` (`$0.04` User resolution + `$0.20`
Posts). A narrower one-account probe is:

```powershell
uv run --locked kairos-feed-qualify `
  --samples 1 `
  --interval-s 0 `
  --x-bearer-token-file D:\secure\x_bearer_token `
  --x-account lookonchain `
  --allow-metered-x-probe `
  --maximum-x-cost-usd 0.06 `
  --output $env:TEMP\kairos-feed-qualification.json
```

Evidence contains only counts, freshness, latency, metered units and estimated cost—
never Post text, OAuth tokens, or provider secrets. Empty or stale content is
`BLOCKED` rather than misclassified as a transport failure; malformed attributable
evidence is `FAIL`. X rate-limit headers are observed explicitly. Other feeds without
provider quota evidence remain `BLOCKED`; every report always sets
`live_orders_allowed=false`.

## Configuration (env, `KAIROS_` prefix)
```bash
# News (free, on by default)
KAIROS_GDELT_QUERY='(bitcoin OR btc OR ethereum OR eth OR crypto OR etf OR sec OR cpi) sourcelang:english'
KAIROS_GDELT_TIMESPAN=15min
KAIROS_MAX_EVENT_AGE_S=1800
KAIROS_MAX_FUTURE_SKEW_S=5
# X / Twitter (optional — official X API; inject token through a secret provider)
KAIROS_X_BEARER_TOKEN=...
# $9 runtime allocation under the $10 provider cap; integer micro-USD
KAIROS_X_MONTHLY_BUDGET_MICROUSD=9000000
KAIROS_X_POST_READ_UNIT_COST_MICROUSD=5000
KAIROS_X_USER_READ_UNIT_COST_MICROUSD=10000
# Reddit (optional — official Reddit API, free; register an app at reddit.com/prefs/apps)
KAIROS_REDDIT_CLIENT_ID=...
KAIROS_REDDIT_CLIENT_SECRET=...
```

Paid DeepSeek calls are also fail-closed. In the durable runtime, Text Scouts
uses the shared PostgreSQL `kairos-llm-v1/deepseek` ledger and the `$4.50`
runtime ceiling before contacting the provider. An in-memory runtime has no
durable ledger and therefore denies paid calls, falling back locally.

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
