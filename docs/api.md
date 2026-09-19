# API

Base URL: http://127.0.0.1:3000

## Health

GET /health

Returns scanner, market-provider, crypto-stream, news, SEC, Canadian-disclosure and journal-drop health.

Dedicated health endpoints:
- GET /api/providers/health
- GET /api/streams/health
- GET /api/journal/health
- GET /api/news/health
- GET /api/filings/health

## Presets

GET /api/presets

POST /api/presets

DELETE /api/presets/:id

Builtin scanner presets now include:
- equity gainers/fallers
- Gap & Go / Gap & Fade
- low float
- momentum breakout
- extended movers
- Crypto Momentum
- Crypto Order Flow
- FX Momentum

## Scans

GET /api/scans

POST /api/scans

PUT /api/scans/:id

DELETE /api/scans/:id

GET /api/scans/:id/snapshot

Scanner fields include the original equity fields plus:
- spread_bps
- best_bid
- best_ask
- mid_price
- microprice
- bid_depth_5 / ask_depth_5
- bid_depth_10 / ask_depth_10
- book_imbalance
- liquidity_score
- trade_imbalance
- cvd
- trade_count_1m
- trade_rate_1m
- buy_volume_1m
- sell_volume_1m
- vwap_15m
- cross_venue_dislocation_bps
- news_count_5m
- news_count_15m
- news_velocity
- news_sources_15m
- stream_age_ms

## Scanner WebSocket

GET /ws/scanner/:id

The first server message is a snapshot. Later messages are result_added, result_updated, result_removed or resync_required events.

## Symbol evidence

GET /api/evidence/:symbol

Returns:
- current scanner row
- current classified catalyst
- recent CatalystEvent records
- venue prices/bids/asks
- per-venue age
- book metrics
- recent related normalized news

GET /api/catalysts/:symbol

Returns the symbol's recent normalized CatalystEvent list.

## Market provider proxies

Provider discovery:
- GET /api/providers
- GET /api/providers/health

U.S. / Canada / FX:
- GET /api/providers/yahoo/:symbol
- GET /api/providers/yahoo/fx/:symbol
- GET /api/providers/nasdaq/:symbol
- GET /api/providers/nasdaq/:symbol/info
- GET /api/providers/tradingview/:exchange/:symbol
- GET /api/providers/tradingview/canada/:start/:end
- GET /api/providers/tradingview/forex/:start/:end
- GET /api/providers/frankfurter/:base/:quote
- GET /api/providers/frankfurter/:base/rates/:quotes
- GET /api/providers/bank-of-canada/:base/:quote
- GET /api/providers/bank-of-canada/series/:series

Crypto REST snapshots:
- GET /api/providers/binance/:symbol/ticker
- GET /api/providers/binance/:symbol/quote
- GET /api/providers/binance/:symbol/klines/:interval
- GET /api/providers/binance/:symbol/depth
- GET /api/providers/kraken/:symbol/ticker
- GET /api/providers/kraken/:symbol/ohlc/:interval
- GET /api/providers/kraken/:symbol/depth
- GET /api/providers/kraken/:symbol/trades
- GET /api/providers/coinbase/:symbol/ticker
- GET /api/providers/coinbase/:symbol/candles/:granularity
- GET /api/providers/coinbase/:symbol/book
- GET /api/providers/coinbase/:symbol/trades

## Regulatory and Canadian disclosures

SEC:
- GET /api/filings/:ticker
- Public SEC EDGAR submissions are normalized as FilingEvent records.

Canada:
- GET /api/canada/disclosures/:ticker
- Uses public Google News RSS discovery constrained to SEDAR+ and TSX public-news domains rather than undocumented SEDAR+ internal APIs.

## News

Unified search:
- GET /api/news/search?q={query}&ticker={ticker}&limit={n}

Ticker convenience:
- GET /api/news/ticker/:ticker

Reddit only:
- GET /api/news/reddit?q={query}&ticker={ticker}&limit={n}

Google News only:
- GET /api/news/google?q={query}&ticker={ticker}&limit={n}

NewsAPI only:
- GET /api/news/newsapi?q={query}&ticker={ticker}&limit={n}

Canadian disclosure discovery:
- GET /api/canada/disclosures/:ticker

Recent normalized cache:
- GET /api/news/cache

Story clusters:
- GET /api/news/clusters
- each cluster exposes id, canonical title, article/source/ticker counts, event type,
  semantic-hybrid method name and observed similarity floor.
- each normalized NewsArticle carries its cluster_id.

Research storage:
- scripts/benchmark_event_storage.py compares JSONL, DuckDB-over-JSONL,
  materialized DuckDB and Parquet paths without entering the scanner hot path.

News provider health:
- GET /api/news/health

## Replay

CLI:

~~~text
cargo run -- replay 2026-09-19
~~~

The replay reads data/events/YYYY-MM-DD.jsonl and replays:
- quotes
- trades
- books
- news
- Canadian disclosures
- SEC filings

through the same state/feature engine used by the live runtime.
