# kairos-text-scouts

**Layer 1B — Text Scouts.** A universal **event aggregator** over *official* APIs/feeds
(no self-hosted scrapers, no proxies, no 403/captcha juggling). It normalizes and
deduplicates events, drops the noise with a cheap **local filter**, then asks an LLM
(**low** reasoning effort → DeepSeek-V4-Flash, non-thinking) to turn the few relevant items
into a structured `SentimentSignal`. The expensive model only ever sees pre-filtered text.

## Pipeline
```
sources ──▶ normalize ──▶ dedup ──▶ relevance (+top-K) ──▶ LLM (effort=low) ──▶ SentimentSignal
```
Example output (matches the spec): `{"topic": "SEC ETF", "sentiment": 0.85, "impact": "bullish"}`.

## Sources
| source | provider | cost | enabled |
| --- | --- | --- | --- |
| **GDELT** DOC 2.0 | free official news API — already indexes Reuters, Bloomberg, CNBC, Coindesk… | $0 | always |
| **RSS** | Coindesk + Cointelegraph (crypto-native backstop) | $0 | always |
| **X / Twitter** | Bright Data **Web Scraper API** — live scrape of influencer handles (sync `/scrape`, async fallback) | metered | when token + dataset id set |
| **Reddit** | Bright Data **Web Scraper API** — live scrape of subreddits | metered | when token + dataset id set |

> The social sources use Bright Data's **Web Scraper API** (on-demand live scraping), **not** the
> static Dataset Marketplace — every poll fetches fresh posts. Calls prefer the synchronous
> real-time endpoint (≤20 URLs) and only fall back to the async snapshot flow for long jobs.

Each source is isolated: one provider failing (e.g. GDELT rate-limiting) never blinds the
layer. Reuters/Bloomberg no longer publish public RSS, so GDELT covers them. A real
transformer (BERT/FinBERT) can replace `LocalRelevanceFilter` behind its `select()` interface.
If DeepSeek-Flash is down the layer degrades to a deterministic local keyword sentiment.

## Configuration (env, `KAIROS_` prefix)
```bash
# News (free, on by default)
KAIROS_GDELT_QUERY='(bitcoin OR btc OR ethereum OR eth OR crypto OR etf OR sec OR cpi) sourcelang:english'
KAIROS_GDELT_TIMESPAN=15min
# Social (optional — set a token + dataset ids to enable)
KAIROS_BRIGHTDATA_API_TOKEN=...
KAIROS_BRIGHTDATA_X_DATASET_ID=...
KAIROS_BRIGHTDATA_REDDIT_DATASET_ID=...
```

## Run
```bash
pip install -e ../kairos-core -e ../kairos-llm && pip install -e ".[dev]"
make test
python -m kairos_text
```
Emits `kairos.sentiment.signal`. The LLM call goes through
[`kairos-llm`](https://github.com/Kairos-cryptoAI/kairos-llm).

---
Part of the [Kairos](https://github.com/Kairos-cryptoAI/kairos) system. MIT licensed.
