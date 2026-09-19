# Development

No GitHub Actions are used.

## Local checks

~~~text
cargo fmt --all -- --check
cargo check
cargo test
~~~

## Run

~~~text
cargo run -- serve
~~~

Server environment:

- PINE_FOUNDRY_ADDR — default 127.0.0.1:3000
- PINE_FOUNDRY_DATA_DIR — default data
- PINE_FOUNDRY_FEED — auto or mock

## Event-driven crypto

The crypto runtime connects independently to Binance, Kraken and Coinbase public WebSocket trade streams.

~~~text
PINE_FOUNDRY_CRYPTO_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
PINE_FOUNDRY_CRYPTO_WS_RECONNECT_SECS=3
~~~

The WebSocket layer reconnects independently per venue. It feeds the same MarketEvent::Trade path used by the deterministic mock feed.

## News

Copy .env.example and configure NEWSAPI locally when NewsAPI search is desired.

~~~text
cp .env.example .env
~~~

News runtime settings:

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

The API key must remain in the local environment and never be committed.

## API smoke test

~~~text
curl http://127.0.0.1:3000/health
curl http://127.0.0.1:3000/api/presets
curl http://127.0.0.1:3000/api/scans
curl 'http://127.0.0.1:3000/api/news/ticker/BTC'
curl 'http://127.0.0.1:3000/api/news/search?q=bitcoin&limit=10'
~~~

Keep asynchronous work non-blocking. The market and news loops use Tokio timers, WebSockets and asynchronous HTTP. Do not put vendor SDKs or blocking HTTP clients inside the scanner hot path.

For performance work, benchmark the current implementation before adding Redis, a database, a custom sort index or more threads.
