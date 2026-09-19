# Pine Foundry

Pine Foundry is a local-first, real-time market scanner and event-fabric foundation. It combines venue-aware market data, crypto WebSockets, order books, public news, filings, catalyst detection and replay around one deterministic state engine.

It is an original implementation inspired by the public workflow of desktop market scanners. It does not copy proprietary source code, branding, assets, private thresholds or undocumented vendor internals.

## Implemented

- Equity scanner with saved presets and custom scans.
- Explicit equity/crypto/FX asset-class universes.
- U.S., Canadian and FX public quote adapters.
- Binance, Kraken and Coinbase public crypto WebSocket trades.
- Binance, Kraken and Coinbase public order-book streams.
- Automatic REST order-book resync after sequence gaps.
- Venue-specific price/book state with configurable primary crypto venue.
- Cross-venue dislocation.
- Spread, microprice, depth, book imbalance and liquidity metrics.
- Trade imbalance and CVD.
- Stream freshness and independent provider health.
- Reddit RSS and JSON search.
- Google News RSS search.
- NewsAPI with local request-budget guards.
- News URL deduplication and semantic-hybrid story clustering.
- Configurable issuer-specific RSS/Atom feeds.
- Deterministic catalyst classification.
- SEC EDGAR filing ingestion.
- Canadian SEDAR+/TSX disclosure discovery through public Google News RSS restrictions.
- Unified CatalystEvent state.
- Asynchronous bounded JSONL event journal.
- Daily market/news/filing replay.
- Symbol evidence API and browser evidence panel.
- Crypto Momentum, Crypto Order Flow and FX Momentum presets.
- No GitHub Actions.

## Quick start

~~~text
cp .env.example .env
# Put NEWSAPI in .env only when using NewsAPI.
cargo fmt --all -- --check
cargo check
cargo test
cargo run -- serve
~~~

Server:

~~~text
http://127.0.0.1:3000
~~~

Replay a recorded day:

~~~text
cargo run -- replay 2026-09-19
~~~

Browser:

~~~text
cd web
npm install --no-audit --no-fund
npm run dev
~~~

## Environment

Core:

~~~text
PINE_FOUNDRY_ADDR=127.0.0.1:3000
PINE_FOUNDRY_DATA_DIR=data
PINE_FOUNDRY_FEED=auto
~~~

Equity/FX polling:

~~~text
PINE_FOUNDRY_POLL_SECS=5
PINE_FOUNDRY_TV_PAGE_SIZE=5000
PINE_FOUNDRY_TV_MAX_ROWS=20000
PINE_FOUNDRY_SESSION=regular
PINE_FOUNDRY_FX_PAIRS=EURUSD=X,USDCAD=X,GBPUSD=X,USDJPY=X
PINE_FOUNDRY_FX_POLL_SECS=15
~~~

Crypto streams:

~~~text
PINE_FOUNDRY_CRYPTO_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
PINE_FOUNDRY_CRYPTO_PRIMARY=binance
PINE_FOUNDRY_CRYPTO_WS_RECONNECT_SECS=3
PINE_FOUNDRY_STREAM_STALE_SECS=10
~~~

News:

~~~text
NEWSAPI=
PINE_FOUNDRY_NEWS_TICKERS=BTC,ETH,SOL
PINE_FOUNDRY_NEWS_ALIASES=AAPL=Apple|Apple Inc;MSFT=Microsoft|Microsoft Corporation
PINE_FOUNDRY_NEWS_QUERIES=
PINE_FOUNDRY_NEWS_POLL_SECS=60
PINE_FOUNDRY_NEWS_LIMIT=25
PINE_FOUNDRY_NEWS_LOOKBACK_HOURS=24
PINE_FOUNDRY_NEWS_CONCURRENCY=4
PINE_FOUNDRY_NEWS_CACHE_SIZE=500
PINE_FOUNDRY_NEWS_CLUSTER_THRESHOLD=0.58
PINE_FOUNDRY_NEWS_CLUSTER_WINDOW_MINS=360
PINE_FOUNDRY_ISSUER_FEEDS=
PINE_FOUNDRY_NEWSAPI_MIN_INTERVAL_SECS=1800
PINE_FOUNDRY_NEWSAPI_DAILY_LIMIT=90
~~~

Regulatory/corporate:

~~~text
PINE_FOUNDRY_SEC_TICKERS=
PINE_FOUNDRY_SEC_POLL_SECS=30
PINE_FOUNDRY_SEC_USER_AGENT=PineFoundry/0.4 research
PINE_FOUNDRY_CANADA_TICKERS=
PINE_FOUNDRY_CANADA_POLL_SECS=300
~~~

NEWSAPI is loaded from the local .env file or runtime environment and is sent with X-Api-Key. The credential is not committed to the repository.

## Event fabric

~~~text
                 public sources
                       |
         +-------------+-------------+
         |                           |
      market                      catalysts
         |                           |
 trades / quotes / books       news / filings
         |                           |
         +-------------+-------------+
                       |
                 normalized state
                       |
       +---------------+---------------+
       |               |               |
    scanner         evidence        journal
       |               |               |
       v               v               v
      Web UI           API           replay
~~~

Crypto venues stay separate:

~~~text
Binance ----\
Kraken -------> venue state ---> canonical state
Coinbase ----/                     |
                                   v
                         cross-venue features
~~~

## Crypto order flow

Pine Foundry computes:

~~~text
spread
spread_bps
mid
microprice
bid_depth_5
ask_depth_5
bid_depth_10
ask_depth_10
book_imbalance
liquidity_score
trade_imbalance
CVD
cross_venue_dislocation_bps
stream_age_ms
~~~

The primary venue is configurable with PINE_FOUNDRY_CRYPTO_PRIMARY.

## News and catalysts

Each ticker can be searched across:

~~~text
Reddit RSS
Reddit JSON
Google News RSS
NewsAPI
~~~

News is:
- normalized
- deduplicated
- clustered
- classified into catalyst types
- counted for velocity/source diversity

Catalyst types include earnings, guidance, M&A, offerings, buybacks, dividends, FDA/clinical, legal/regulatory, bankruptcy, management, contracts/partnerships, security incidents, ETFs and macro events.

## Regulatory/corporate feeds

U.S.:
- SEC EDGAR ticker discovery and submissions.

Canada:
- public Google News RSS discovery restricted to SEDAR+ and TSX pages.
- deliberately avoids undocumented SEDAR+ internal APIs.

## Research storage

The live event fabric remains asynchronous JSONL. Research storage is downstream
and can be benchmarked without changing the scanner hot path:

~~~text
python3 -m pip install duckdb
python3 scripts/benchmark_event_storage.py
~~~

The benchmark compares JSONL parsing, DuckDB-over-JSONL, materialized DuckDB,
and Parquet query paths. See docs/research-storage.md.

Issuer-specific public RSS/Atom sources can be added with
PINE_FOUNDRY_ISSUER_FEEDS. See docs/issuer-feeds.md.

## Event journal and replay

Normalized events are written asynchronously to:

~~~text
data/events/YYYY-MM-DD.jsonl
~~~

The queue is bounded and exposes dropped-record health at:

~~~text
/api/journal/health
~~~

Replay uses the same scanner engine:

~~~text
cargo run -- replay YYYY-MM-DD
~~~

## Evidence

GET:

~~~text
/api/evidence/SYMBOL
/api/catalysts/SYMBOL
~~~

The browser scanner lets you click a row to inspect:
- venue state
- bid/ask
- book metrics
- stream age
- catalyst history
- related news

## Key API surfaces

~~~text
GET /health
GET /api/providers
GET /api/streams/health
GET /api/news/search?q=bitcoin
GET /api/news/clusters
GET /api/filings/AAPL
GET /api/canada/disclosures/SHOP
GET /api/evidence/BTCUSDT
GET /api/catalysts/BTCUSDT
~~~

See docs/master-spec.md, docs/architecture.md, docs/providers.md, docs/news.md, docs/roadmap.md and docs/api.md.
