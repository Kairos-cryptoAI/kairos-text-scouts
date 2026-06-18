# kairos-text-scouts

**Layer 1B — Text Scouts.** Monitors news / RSS / X-influencers, drops ~95% of the noise
with a cheap **local filter**, then asks an LLM (**low** reasoning effort) to turn the few
relevant items into a structured `SentimentSignal`. The expensive model only ever sees
pre-filtered text.

## Pipeline
```
~100 items / 5 min ──▶ LocalRelevanceFilter (keeps ~5) ──▶ LLM (effort=low, batched) ──▶ SentimentSignal
```
Example output (matches the spec): `{"topic": "SEC ETF", "sentiment": 0.85, "impact": "bullish"}`.

The local filter is a transparent keyword/impact scorer exposing a `select()` interface, so
a real transformer (BERT/FinBERT) can be dropped in without touching the rest of the layer.
The LLM call goes through [`kairos-llm`](https://github.com/Kairos-cryptoAI/kairos-llm).

## Run
```bash
pip install -e ../kairos-core -e ../kairos-llm && pip install -e ".[dev]"
make test
python -m kairos_text
```
Emits `kairos.sentiment.signal`.

---
Part of the [Kairos](https://github.com/Kairos-cryptoAI/kairos) system. MIT licensed.
