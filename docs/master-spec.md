# Pine Foundry Master Specification

Pine Foundry is an original, local-first implementation of a real-time scanner and market/news event fabric.

## Core design

The system separates provider state from canonical scanner state:

~~~text
venue/provider
    |
    v
normalization
    |
    +--> market event
    +--> book event
    +--> news event
    +--> filing/disclosure event
    |
    v
SecurityState / CatalystEvent
    |
    +--> deterministic feature engine
    +--> scanner
    +--> evidence API
    +--> event journal
    +--> replay
~~~

## Functional requirements

- Equity issue types and session state.
- Explicit asset-class universes for equity, crypto and FX.
- Price, absolute change and 1m/5m/15m percentages.
- Day volume and rolling minute volume.
- Shares float, shares outstanding and market cap where supplied.
- Venue-aware crypto prices.
- Configurable primary crypto venue.
- Public trade streams for Binance, Kraken and Coinbase.
- Public order-book streams for Binance, Kraken and Coinbase.
- Order-book sequence validation and REST snapshot recovery.
- Spread, mid, microprice, depth, imbalance and liquidity features.
- Trade imbalance and CVD.
- Cross-venue price dislocation.
- Stream freshness/staleness health.
- Public Reddit RSS/JSON, Google News RSS and optional NewsAPI.
- News URL deduplication and deterministic semantic story clustering.
- Configurable issuer-specific RSS/Atom feeds.

- Deterministic catalyst classification.
- SEC EDGAR filing events.
- Canadian SEDAR+/TSX disclosure discovery.
- Unified CatalystEvent state.
- Bounded asynchronous JSONL event journal.
- Daily historical replay.
- Evidence API and browser evidence panel.
- No GitHub Actions.

## Scanner

Fields include original scanner values plus:

~~~text
SpreadBps
BookImbalance
LiquidityScore
TradeImbalance
Cvd
CrossVenueDislocationBps
NewsCount5m
NewsCount15m
NewsVelocity
NewsSources15m
StreamAgeMs
~~~

Every scan definition may constrain:
- issue types
- asset classes
- session
- min/max fields
- sort
- columns

## Order-book correctness

Incremental order books are never considered valid after a sequence gap.

Recovery sequence:

~~~text
stream update
    |
sequence check
    |
gap?
 +--+--+
 no    yes
 |      |
apply   mark invalid
 |      |
 +---   +--> REST snapshot
              |
              v
        replace local book
              |
              v
         resume updates
~~~

## News/catalyst pipeline

~~~text
ticker
  |
entity-aware query
  |
+-- Reddit RSS
+-- Reddit JSON
+-- Google News RSS
+-- NewsAPI
+-- SEC
+-- Canadian disclosure discovery
  |
normalize
  |
dedupe
  |
story cluster
  |
catalyst classifier
  |
CatalystEvent
~~~

An LLM is downstream of this deterministic evidence rather than in the market tick path.

## Replay

Raw normalized events are persisted to:

~~~text
data/events/YYYY-MM-DD.jsonl
~~~

Replay uses the same scanner/book/news/catalyst state engine used live.

## Operational limits

- NewsAPI is protected by local interval and daily guards.
- Reddit access is best effort and can be throttled.
- SEC access uses declared User-Agent and configured ticker polling.
- The journal queue is bounded and exposes dropped-record count.
- Provider failures are isolated.

## Research

The event fabric is designed for downstream research without changing scanner
state:

- JSONL remains the authoritative append-oriented journal.
- DuckDB/Parquet materialization is benchmarked outside the live hot path.
- Historical event studies can query the same normalized records in columnar form.
- Semantic story clusters can be used as research keys across providers.
- Issuer-specific RSS/Atom feeds enter the same NewsArticle/event-journal path.
- Historical relative-volume baselines.
- Order-flow studies.
- Champion/challenger strategy evaluation.
- Small-model evidence summarization.

## Security

Secrets such as NEWSAPI are read from environment variables and excluded from source control.
