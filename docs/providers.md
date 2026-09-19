# Public providers

Pine Foundry uses public market/data endpoints where practical and keeps every adapter behind a normalized event boundary. Provider failures are isolated and visible through health endpoints.

## Equities

### U.S.

TradingView America:
- POST https://scanner.tradingview.com/america/scan
- Broad anonymous scanner source.

Yahoo Finance:
- GET https://query1.finance.yahoo.com/v8/finance/chart/{symbol}
- GET https://query1.finance.yahoo.com/v7/finance/spark
- Per-symbol and batch fallback.

Nasdaq:
- GET https://api.nasdaq.com/api/quote/{symbol}/realtime?assetclass=stocks
- GET https://api.nasdaq.com/api/quote/{symbol}/info?assetclass=stocks
- Per-symbol fallback.

### Canada / TSX / TSXV

TradingView Canada:
- POST https://scanner.tradingview.com/canada/scan
- Bulk Canadian equity scanner source.

Yahoo Finance:
- Canadian symbols such as SHOP.TO and RY.TO through the public chart endpoint.

TMX:
- Pine Foundry does not depend on undocumented anonymous TMX market-data URLs. Official TMX material describes XML/JSON quote APIs as a service/entitlement integration.

## Crypto

### Binance

REST snapshots:
- /api/v3/ticker/24hr
- /api/v3/klines
- /api/v3/depth

WebSocket:
- Spot aggregate trade streams use wss://stream.binance.com:9443/stream with one or more lower-case SYMBOL@aggTrade streams.
- Pine Foundry maps each trade to MarketEvent::Trade immediately.

### Kraken

REST snapshots:
- /0/public/Ticker
- /0/public/OHLC
- /0/public/Depth
- /0/public/Trades

WebSocket:
- wss://ws.kraken.com/v2
- Public trade channel with multiple subscribed symbols.
- Pine Foundry consumes trade price, quantity and timestamp as MarketEvent::Trade.

### Coinbase

REST snapshots:
- /products/{product_id}/ticker
- /products/{product_id}/candles
- /products/{product_id}/book?level=2
- /products/{product_id}/trades

WebSocket:
- wss://advanced-trade-ws.coinbase.com
- Public market_trades subscription for configured products.
- Pine Foundry consumes trade price, size and timestamp as MarketEvent::Trade and also subscribes to heartbeats.

WebSocket streams run independently and reconnect after disconnects. REST crypto routes remain available for snapshots, candles and books.

Important:
- The streamed day volume starts from the first event Pine Foundry receives. It is not a historical exchange-day aggregate.
- No authenticated account or order routes are used.
- Public schemas and availability can change without notice.

## FX

TradingView Forex:
- POST https://scanner.tradingview.com/forex/scan

Yahoo Finance:
- Public chart symbols such as EURUSD=X and USDCAD=X.

Frankfurter:
- GET https://api.frankfurter.dev/v2/rate/{base}/{quote}
- GET https://api.frankfurter.dev/v2/rates

Bank of Canada:
- GET https://www.bankofcanada.ca/valet/observations/{series}/json
- Public keyless FX reference-data fallback.

## News

### Reddit

Global search RSS:
- GET https://www.reddit.com/search.rss?q={encoded_query}&sort=new&limit={n}

Global JSON:
- GET https://www.reddit.com/search.json?q={encoded_query}&sort=new&limit={n}

Pine Foundry attempts both. RSS is treated as the durable keyless path; JSON is an additional fast structured path when Reddit permits it from the client network.

### Google News

Search RSS:
- GET https://news.google.com/rss/search?q={encoded_query}&hl=en-CA&gl=CA&ceid=CA:en

The RSS response is normalized into the same NewsArticle model as Reddit and NewsAPI.

### NewsAPI

Everything:
- GET https://newsapi.org/v2/everything
- Query parameters include q, from, language, sortBy and pageSize.
- NEWSAPI or NEWSAPI_KEY is read from the process environment.
- The credential is sent with X-Api-Key and never stored in the repository.

Pine Foundry defaults NewsAPI searches to a configurable recent lookback window with:
- PINE_FOUNDRY_NEWS_LOOKBACK_HOURS
- PINE_FOUNDRY_NEWS_LIMIT

## Unified news model

Each result includes:
- provider
- query
- optional ticker
- title
- description
- URL
- source
- author
- subreddit when available
- publication timestamp

Results are deduplicated by canonical URL and sorted newest first. A bounded in-memory cache retains recent results for API consumers.

## Automatic news worker

Default:

~~~text
PINE_FOUNDRY_NEWS_TICKERS=BTC,ETH,SOL
PINE_FOUNDRY_NEWS_QUERIES=
PINE_FOUNDRY_NEWS_POLL_SECS=60
PINE_FOUNDRY_NEWS_LIMIT=25
PINE_FOUNDRY_NEWS_LOOKBACK_HOURS=24
PINE_FOUNDRY_NEWS_CONCURRENCY=4
PINE_FOUNDRY_NEWS_CACHE_SIZE=500
~~~

For a custom ticker list, populate PINE_FOUNDRY_NEWS_TICKERS with comma-separated symbols. PINE_FOUNDRY_NEWS_QUERIES accepts additional free-form broad searches.

## Health

Market provider health:
- GET /health
- GET /api/providers/health

News provider health:
- GET /api/news/health

A failing news source is isolated from the market-data runtime. Search aggregation still returns whatever sources remain available.
