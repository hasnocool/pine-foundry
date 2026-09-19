# Pine Foundry

Pine Foundry is a local-first, real-time market scanner and research foundation built around a deterministic event-driven scanner engine.

It is an original implementation inspired by the public workflow of desktop equity scanners. It does not copy proprietary source code, branding, assets, private thresholds, or undocumented vendor internals.

## Implemented

- Event-driven market state instead of polling.
- Price, absolute change, previous-close %, 1m/5m/15m % metrics.
- Day volume and rolling 1m volume.
- Shares float, shares outstanding and market cap.
- Issue-type and session universe controls.
- Min/max filters with explicit enable/disable state.
- Saved builtin/custom presets with JSON persistence.
- Stable ranked result snapshots.
- Incremental add/remove/update scanner events.
- WebSocket snapshot + delta + resync protocol.
- REST API for scans and presets.
- Three-window-ready backend.
- Browser UI with filter editing, preset switching, column visibility and sorting.
- Deterministic synthetic feed for offline development.
- Public-provider fallback stack for U.S., Canadian, crypto and FX markets.
- Event-driven public crypto trade streams from Binance, Kraken and Coinbase.
- Unified news aggregation from Reddit RSS, Reddit JSON, Google News RSS and NewsAPI.
- Bounded local news cache and provider health metrics.
- Local tests and validation only; **no GitHub Actions**.

## Quick start

~~~
cp .env.example .env
# Set NEWSAPI in .env when using NewsAPI.
cargo fmt --all -- --check
cargo check
cargo test
cargo run -- serve
~~~

Server:

~~~text
http://127.0.0.1:3000
~~~

Health:

~~~
curl http://127.0.0.1:3000/health
~~~

List presets:

~~~
cargo run -- presets
~~~

List provider routes:

~~~
cargo run -- providers
~~~

Web client:

~~~
cd web
npm install
npm run dev
~~~

Then open the Vite URL shown by the dev server.

## Environment

Core:

~~~text
PINE_FOUNDRY_ADDR=127.0.0.1:3000
PINE_FOUNDRY_DATA_DIR=data
PINE_FOUNDRY_FEED=auto
~~~

Equity polling:

~~~text
PINE_FOUNDRY_POLL_SECS=5
PINE_FOUNDRY_TV_PAGE_SIZE=5000
PINE_FOUNDRY_TV_MAX_ROWS=20000
PINE_FOUNDRY_SESSION=regular
~~~

Crypto WebSockets:

~~~text
PINE_FOUNDRY_CRYPTO_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
PINE_FOUNDRY_CRYPTO_WS_RECONNECT_SECS=3
~~~

News:

~~~text
NEWSAPI=
PINE_FOUNDRY_NEWS_TICKERS=BTC,ETH,SOL
PINE_FOUNDRY_NEWS_QUERIES=
PINE_FOUNDRY_NEWS_POLL_SECS=60
PINE_FOUNDRY_NEWS_LIMIT=25
PINE_FOUNDRY_NEWS_LOOKBACK_HOURS=24
PINE_FOUNDRY_NEWS_CONCURRENCY=4
PINE_FOUNDRY_NEWS_CACHE_SIZE=500
~~~

NEWSAPI is read from the process environment and sent in the X-Api-Key header. The credential is intentionally not stored in the repository.

## Live crypto architecture

~~~text
Binance WS aggTrade ----\
Kraken WS trade ----------> normalized MarketEvent::Trade
Coinbase WS trades ------/            |
                                      v
                              SecurityState
                                      |
                                      v
                         rolling 1m/5m/15m metrics
                                      |
                                      v
                                ScanRuntime
                                      |
                         WebSocket scanner deltas
~~~

The three public crypto WebSockets run independently with reconnect loops. A provider disconnect does not stop the other streams or the equity/FX loops.

The stream layer is event-driven; the REST crypto endpoints remain available for snapshots, candles and order books.

## News architecture

~~~text
ticker/query
    |
    +--> Reddit search.rss
    +--> Reddit search.json
    +--> Google News RSS search
    +--> NewsAPI /v2/everything
                |
                v
        normalize + dedupe
                |
                v
          bounded cache
                |
                +--> REST search endpoint
                +--> ticker endpoint
                +--> health endpoint
~~~

The automatic news worker refreshes configured tickers/queries independently from market-data loops. A failed news provider does not stop the scanner.

See docs/providers.md, docs/news.md and docs/api.md for route coverage and limitations.

## Architecture

~~~text
market provider
   |
   v
MarketEvent
   |
   v
SecurityState + rolling minute history
   |
   v
Metrics
   |
   v
Filter evaluation
   |
   v
ScanRuntime membership
   |
   +--> REST snapshot
   |
   +--> WebSocket deltas
   |
   v
browser scanner
~~~

The production provider boundary should normalize quotes, trades, reference data, session changes, halt status and sequence information into the same event model used by the scanner.

See docs/master-spec.md and docs/development.md.
