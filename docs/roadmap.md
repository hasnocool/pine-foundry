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

### P1 — news intelligence

- Reddit RSS.
- Reddit JSON.
- Google News RSS.
- NewsAPI.
- NewsAPI request guards.
- URL deduplication.
- Ticker-aware query expansion.
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

1. Durable DuckDB/Parquet research storage after benchmarking JSONL.
2. Better semantic story clustering.
3. More official issuer feeds.
4. Canadian issuer-specific disclosure mapping where a documented public endpoint exists.
5. Historical relative-volume and volatility baselines.
6. More order-book depth and executable-size metrics.
7. Browser evidence/detail view for one-click catalyst inspection.
