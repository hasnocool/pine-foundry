# Pine Foundry Master Specification

Pine Foundry is an original, local-first implementation of a real-time market scanner with deterministic filtering, live market adapters, event-driven crypto streams and a normalized news side-channel.

## Functional model

- U.S. equity-style issue taxonomy with common stock first.
- Regular, pre-market, after-hours and closed session state.
- Price, absolute change, previous-close %, 1m/5m/15m %.
- Day volume and rolling 1m volume.
- Shares float, shares outstanding and market cap.
- Enabled/disabled numeric range filters.
- Issue-type universe restrictions.
- Stable multi-field tie-break sorting.
- Saved custom presets with immutable builtin presets.
- Concurrent scans with snapshot and delta WebSocket events.
- Deterministic synthetic feed for offline development.
- Public U.S., Canadian, FX and crypto adapters.
- Event-driven public crypto trade streams from Binance, Kraken and Coinbase.
- Public Reddit RSS/JSON, Google News RSS and optional NewsAPI aggregation.
- Bounded in-memory news cache and provider health reporting.
- Local-only checks; no GitHub Actions.

## Market data flow

~~~text
Provider
  |
  +--> REST snapshot / fallback
  |
  +--> WebSocket trade stream
  |
  v
MarketEvent
  |
  v
SecurityState
  |
  v
Metrics
  |
  v
filter evaluation
  |
  v
ScanRuntime
  |
  +--> REST snapshot
  |
  +--> scanner WebSocket deltas
~~~

## News data flow

~~~text
Ticker/query
  |
  +--> Reddit RSS
  +--> Reddit JSON
  +--> Google News RSS
  +--> NewsAPI
  |
  v
NewsArticle normalization
  |
  v
URL dedupe + newest-first ordering
  |
  v
bounded cache
  |
  +--> news search API
  +--> news UI
  +--> downstream research/AI
~~~

## Provider boundaries

Provider code stays outside the scanner truth model. Vendor-specific payloads are normalized before reaching the scanner.

The crypto WebSocket layer never blocks on network operations and reconnects each venue independently.

The news layer is deliberately outside the market tick path. A news outage cannot stop market-state ingestion.

## Advanced metrics

VWAP, relative volume, opening range, moving averages, ATR, RSI, halt/LULD state, sector strength, breadth and correlation can be added as new Field values without changing the filter execution model.

Crypto-specific metrics can be added without requiring equity-only universe assumptions.

## Research

Persist scanner entry/exit/update events and news observations for historical replay. Reuse the deterministic scanner engine for historical scan backtesting.

## AI

AI belongs downstream of deterministic market/news events:

~~~text
market/news event
   -> compact evidence
   -> deterministic enrichment
   -> small local model
   -> optional larger model
~~~

Never run an LLM on every market tick.

## Security

Secrets such as NEWSAPI are read from environment variables and excluded from source control. Never commit API credentials to the repository.

## Public reference boundary

The implementation is based on public documentation and observable concepts. It does not copy proprietary source code, branding, assets, private thresholds, private data-feed schemas or internal algorithms.
