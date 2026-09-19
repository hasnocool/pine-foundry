# Development

No GitHub Actions are used.

## Local checks

~~~text
cargo fmt --all -- --check
cargo check
cargo test

cd web
npm install --no-audit --no-fund
npm run build
~~~

scripts/check.sh runs the same local checks.

## Run

~~~text
cargo run -- serve
~~~

Server environment:

~~~text
PINE_FOUNDRY_ADDR=127.0.0.1:3000
PINE_FOUNDRY_DATA_DIR=data
PINE_FOUNDRY_FEED=auto
~~~

## Event-driven crypto

The runtime connects independently to Binance, Kraken and Coinbase public WebSocket streams.

~~~text
PINE_FOUNDRY_CRYPTO_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
PINE_FOUNDRY_CRYPTO_PRIMARY=binance
PINE_FOUNDRY_CRYPTO_WS_RECONNECT_SECS=3
PINE_FOUNDRY_STREAM_STALE_SECS=10
~~~

Trade and book events are venue-specific before they enter the canonical scanner state.

Book sequence problems trigger a REST snapshot recovery rather than leaving the incremental book marked as usable.

## News and catalysts

~~~text
NEWSAPI=
PINE_FOUNDRY_NEWS_TICKERS=BTC,ETH,SOL
PINE_FOUNDRY_NEWS_QUERIES=
PINE_FOUNDRY_NEWS_POLL_SECS=60
PINE_FOUNDRY_NEWS_LIMIT=25
PINE_FOUNDRY_NEWS_LOOKBACK_HOURS=24
PINE_FOUNDRY_NEWS_CONCURRENCY=4
PINE_FOUNDRY_NEWS_CACHE_SIZE=500
PINE_FOUNDRY_NEWSAPI_MIN_INTERVAL_SECS=1800
PINE_FOUNDRY_NEWSAPI_DAILY_LIMIT=90
~~~

News is classified deterministically into catalyst types before any AI processing. Stories are clustered across providers.

## Regulatory/corporate feeds

SEC:

~~~text
PINE_FOUNDRY_SEC_TICKERS=AAPL,MSFT
PINE_FOUNDRY_SEC_POLL_SECS=30
PINE_FOUNDRY_SEC_USER_AGENT=PineFoundry/0.4 research
~~~

Canada:

~~~text
PINE_FOUNDRY_CANADA_TICKERS=SHOP,RY,ABX
PINE_FOUNDRY_CANADA_POLL_SECS=300
~~~

Canadian discovery uses public Google News RSS queries constrained to SEDAR+ and TSX domains rather than undocumented SEDAR+ APIs.

## Event journal

Raw normalized market, book, news, Canadian disclosure and filing events are written asynchronously:

~~~text
data/events/YYYY-MM-DD.jsonl
~~~

The queue is bounded. Monitor:

~~~text
curl http://127.0.0.1:3000/api/journal/health
~~~

Dropped journal records should trigger investigation before using the data for historical research.

## Replay

~~~text
cargo run -- replay 2026-09-19
~~~

Replay uses the same market/book/news/catalyst state engine as live operation.

## API smoke test

~~~text
curl http://127.0.0.1:3000/health
curl http://127.0.0.1:3000/api/presets
curl http://127.0.0.1:3000/api/scans
curl 'http://127.0.0.1:3000/api/news/ticker/BTC'
curl 'http://127.0.0.1:3000/api/canada/disclosures/SHOP'
curl 'http://127.0.0.1:3000/api/filings/AAPL'
curl 'http://127.0.0.1:3000/api/evidence/BTCUSDT'
curl http://127.0.0.1:3000/api/streams/health
~~~

Keep asynchronous work non-blocking. The market, news, filings and journal loops use Tokio timers, WebSockets and asynchronous HTTP. Do not put blocking SDKs or synchronous HTTP clients inside the scanner hot path.

For performance work, benchmark before adding Redis, a database, custom indexes or more threads.
