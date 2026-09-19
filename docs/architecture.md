# Architecture

## Runtime graph

~~~text
                         +-----------------------+
                         |    Public providers   |
                         +-----------+-----------+
                                     |
                    +----------------+----------------+
                    |                                 |
              REST snapshots                     WebSockets
                    |                                 |
                    +---------------+-----------------+
                                    v
                               MarketEvent
                                    |
                                    v
                           SecurityState
                         + rolling buckets
                                    |
                                    v
                                 Metrics
                                    |
                                    v
                             Filter engine
                                    |
                                    v
                               ScanRuntime
                              /          \
                             v            v
                       REST snapshot  WebSocket deltas
                                            |
                                            v
                                         Web UI

News providers
    |
    +--> Reddit RSS / JSON
    +--> Google News RSS
    +--> NewsAPI
            |
            v
       NewsArticle
            |
            v
      normalize/dedupe
            |
            v
      bounded news cache
            |
            +--> REST/API
            +--> UI
            +--> future research/AI
~~~

## Hot path

A market event touches one symbol. The engine updates that symbol's state, recalculates derived metrics and evaluates active scans.

WebSocket trade streams feed the same MarketEvent::Trade path used by the deterministic mock feed. The crypto providers run in independent Tokio tasks so one disconnect does not stall another venue.

## News path

News retrieval is deliberately outside the market-state hot path. Each search fans out concurrently to the configured public sources. Provider failures are isolated and recorded in news health state.

Results are normalized into NewsArticle, deduplicated by URL and sorted newest first before being placed in the bounded cache.

## Current concurrency

- Tokio runtime for timers, sockets and asynchronous filesystem access.
- Tokio WebSocket streams for crypto trade events.
- RwLock<HashMap<Symbol, SecurityState>> for market state.
- RwLock<HashMap<ScanId, ScanRuntime>> for active scans.
- RwLock news health/cache structures.
- Broadcast channels for per-scan WebSocket fanout.
- Futures concurrency limits for news fanout.

No blocking network or filesystem APIs are used in the asynchronous server path.

## Persistence

Only user-created presets are persisted today. Builtins are reconstructed at startup and immutable through the API.

News cache is intentionally volatile. Durable news history is a future research-storage layer.

## Scale path

The first optimization after real market-data integration should be dependency indexing by changed field:

~~~text
Field -> ScanId[]
~~~

For larger universes, replace full result sorting with an order-statistics index and introduce bounded delta batching for browser clients.

For news, add a persistent append-only article store only after measuring the memory/cache and downstream research requirements.

## Extension points

### Market providers

Normalize vendor-specific quotes, trades, reference data, session changes, halts and sequence IDs into MarketEvent values.

### News providers

Normalize feed-specific RSS/JSON/article schemas into NewsArticle.

### Research

Persist scanner and news observations and replay them together for event studies and historical scanner evaluation.

### AI

Consume interesting scanner transitions plus compact related-news evidence. Do not put an LLM in the tick path.

### Broker integration

A future symbol-selection bus can publish scanner selections to chart, market depth, order entry and watchlist consumers.
