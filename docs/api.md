# API

Base URL: http://127.0.0.1:3000

## Health

GET /health

Returns scanner, market-provider and news-provider health.

## Presets

GET /api/presets

POST /api/presets

DELETE /api/presets/:id

## Scans

GET /api/scans

POST /api/scans

PUT /api/scans/:id

DELETE /api/scans/:id

GET /api/scans/:id/snapshot

## Scanner WebSocket

GET /ws/scanner/:id

The first server message is a snapshot. Later messages are result_added, result_updated, result_removed or resync_required events.

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

Recent normalized cache:
- GET /api/news/cache

News provider health:
- GET /api/news/health

Example broad ticker search:

~~~text
GET /api/news/ticker/BTC
~~~

This expands BTC into a broader Bitcoin/Ripple-style name query where an alias is known, then queries Reddit RSS, Reddit JSON, Google News RSS and NewsAPI. Provider failures are tolerated as long as at least one provider returns results.
