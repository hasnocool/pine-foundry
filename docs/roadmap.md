# Pine Foundry Roadmap

## Event-fabric architecture

~~~text
                  public market + disclosure sources
                             |
          +------------------+------------------+
          |                                     |
      market events                         catalyst events
          |                                     |
  +-------+-------+                  +----------+----------+
  |       |       |                  |          |          |
 trades  quotes  books              news      filings  disclosures
  |       |       |                  |          |          |
  +-------+-------+                  +----------+----------+
          |                                     |
          v                                     v
                 normalized EventJournal
                             |
                             v
                    state + feature engine
                             |
            +----------------+----------------+
            |                |                |
         scanner         evidence         replay
            |                |                |
            v                v                v
          Web UI        API / AI        historical studies
~~~

## Implemented in this phase

### P0 — event-driven crypto

- Binance public WebSocket aggregate trades.
- Kraken public WebSocket trade channel.
- Coinbase Advanced Trade public market trades.
- Independent reconnect loops.
- Exponential reconnect backoff with jitter.
- Proactive connection renewal.
- Ping/pong handling.
- Provider health.
- Stream staleness.
- Sequence tracking and gap counters.
- Configurable primary venue.
- Venue-specific market state.

### P0 — order flow

- Normalized order-book state.
- Book snapshots and incremental updates.
- Top-of-book.
- Spread and spread bps.
- Mid and microprice.
- 5/10 level depth.
- Book imbalance.
- Liquidity score.
- CVD.
- Buy/sell trade imbalance.
- Cross-venue dislocation.

### P0 — evidence and replay

- Asynchronous JSONL event journal.
- Quote/trade/book/reference/news/filing records.
- Daily replay command.
- Same state/metric engine used for replay.
- Symbol evidence endpoint.
- Catalyst market-reaction evidence at now/5m/15m where replay data permits.

### P1 — news intelligence

- Reddit RSS.
- Reddit JSON.
- Google News RSS.
- NewsAPI.
- NewsAPI request guards.
- URL deduplication.
- Ticker-aware query expansion.
- Configurable stock/company aliases.
- Automatic .env loading for local NewsAPI/runtime configuration.
- Catalyst keyword classification.
- Story clustering.
- News velocity.
- Unique-source counts.
- Bounded news cache.
- Provider health.

### P1 — regulatory/corporate catalysts

- SEC EDGAR ticker discovery and submissions.
- SEC filing-type classification.
- Canadian disclosure discovery through public Google News indexing targeted at SEDAR+ and TSX pages.
- TMX/TSX public-news discovery through the same keyless search path.
- Filing/disclosure events journaled with market events.

The Canadian discovery adapter deliberately avoids depending on undocumented SEDAR+ internal JSON endpoints; SEDAR+ provides a public searchable filing interface, while TMX publishes current exchange news. This keeps the adapter on documented/public surfaces while remaining resilient to internal site changes.

### P1 — scanner evidence

Each scanner row can expose:

- venue prices and ages
- venue bid/ask
- book metrics
- catalyst classification
- recent related news
- stream freshness

### P2 — research

- Persistent raw event journal.
- Historical market/news/filing replay.
- Replay diagnostics.
- Catalyst-to-market reaction datasets.
- Event-study tooling.
- JSONL vs DuckDB/Parquet benchmark tooling via `scripts/benchmark_event_storage.py`.
- Optional materialization of the event journal into Parquet and a DuckDB research database.
- Deterministic hybrid semantic story clustering using token similarity plus SimHash, with configurable time/similarity thresholds.
- Configurable issuer-specific RSS/Atom feeds that enter the same normalized NewsArticle/event-journal path.

### P3 — AI

The deterministic evidence packet is the model boundary:

~~~text
ticker
  + market state
  + order flow
  + cross-venue state
  + catalyst events
  + source diversity
  + recent news
       |
       v
compact evidence packet
       |
       v
small local model
       |
       v
optional larger model
~~~

LLMs should not consume every market tick.

## Planned next iterations

1. Use benchmark results to decide whether/where DuckDB or Parquet should become the durable research layer rather than replacing JSONL blindly.
2. Evaluate local embedding models for a second-generation semantic clusterer without putting inference in the market tick path.
3. Add more issuer-specific official feeds only when their public RSS/Atom contract is documented and stable.
4. Canadian issuer-specific disclosure mapping where a documented public endpoint exists.
5. Historical relative-volume and volatility baselines.
6. More order-book depth and executable-size metrics.
7. Browser evidence/detail view for one-click catalyst inspection.
