# Architecture

## Runtime graph

~~~text
                         +-------------------------+
                         |     Public sources      |
                         +------------+------------+
                                      |
                     +----------------+----------------+
                     |                                 |
              market providers                   catalyst sources
                     |                                 |
          +----------+----------+            +---------+---------+
          |          |          |            |         |         |
        trades     quotes      books        news     SEC     Canada
          |          |          |            |         |         |
          +----------+----------+            +---------+---------+
                     |                                 |
                     +---------------+-----------------+
                                     v
                              normalized state
                                     |
                   +-----------------+------------------+
                   |                 |                  |
                scanner           evidence            journal
                   |                 |                  |
                   v                 v                  v
                 Web UI             API               replay
~~~

## State model

Each symbol has:
- asset class
- issue type
- session
- canonical last price
- per-venue quote/trade state
- per-venue book state
- rolling minute buckets
- trade-flow accumulators
- news velocity/source counters
- catalyst history

Crypto venue state is never directly merged before venue-aware comparison.

## Canonical price

For crypto, the configured primary venue controls canonical scanner price:

~~~text
PINE_FOUNDRY_CRYPTO_PRIMARY=binance
~~~

Other venues remain available for dislocation, spread and liquidity analysis.

## Order-book path

Every book update is sequence-checked where the source supplies sequences. An invalid incremental update marks the local book invalid and triggers a public REST snapshot recovery.

## News path

News retrieval is outside the market hot path. Source failures are isolated.

Normalized news is:
- deduplicated by canonical URL
- enriched with catalyst category
- grouped into story clusters
- retained in a bounded cache

## Catalyst path

News, SEC filings and Canadian disclosure discoveries update the symbol's CatalystEvent history.

The evidence endpoint joins:
- current market metrics
- venue state
- books
- stream age
- catalysts
- related news

## Journal

The journal is an asynchronous bounded channel feeding JSONL files:

~~~text
MarketEvent
Book
News
CanadianDisclosure
Filing
    |
    v
bounded queue
    |
    v
data/events/YYYY-MM-DD.jsonl
~~~

Dropped records are counted and exposed through /api/journal/health.

## Replay

Replay loads a day file and sends recorded events through the same state engine.

This is intentionally not a second implementation of the metric logic.

## Concurrency

- Tokio runtime.
- Independent WebSocket tasks per crypto venue.
- Async HTTP providers.
- RwLock market/scanner state.
- Broadcast channels for scanner WebSockets.
- Bounded journal channel.
- Bounded news/filing concurrency.

## Extension points

Next research-layer candidates:
- DuckDB/Parquet persistence after benchmark.
- Semantic story clustering.
- Issuer-specific Canadian feeds where documented public endpoints exist.
- Executable-size modeling.
- Historical relative-volume and volatility baselines.
- Catalyst reaction studies.
- Compact evidence packets for local AI.
