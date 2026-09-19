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
- wss://stream.binance.com:9443/stream
- One or more lower-case SYMBOL@aggTrade streams.
- Pine Foundry additionally consumes @depth5@100ms book snapshots.
- REST /depth is used as the recovery snapshot if an incremental book sequence becomes invalid.

### Kraken

REST snapshots:
- /0/public/Ticker
- /0/public/OHLC
- /0/public/Depth
- /0/public/Trades

WebSocket:
- wss://ws.kraken.com/v2
- Public trade channel.
- Public book channel with configurable depth.
- Book state is rebuilt from a snapshot when sequence continuity becomes invalid.

### Coinbase

REST snapshots:
- /products/{product_id}/ticker
- /products/{product_id}/candles
- /products/{product_id}/book?level=2
- /products/{product_id}/trades

WebSocket:
- wss://advanced-trade-ws.coinbase.com
- Public market_trades.
- Public level2.
- Heartbeats are subscribed to alongside market data.
- REST book recovery is used after sequence problems.

WebSocket streams run independently and reconnect after disconnects. Each venue maintains its own price, freshness, sequence and book state. The configured crypto primary venue controls the canonical scanner last price.

Important:
- Streamed day volume starts from the first event Pine Foundry receives; it is not historical exchange-day volume.
- No authenticated account or order routes are used.
- Public schemas and availability can change.

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

## Regulatory filings

### SEC EDGAR

- https://www.sec.gov/files/company_tickers.json
- https://data.sec.gov/submissions/CIK##########.json
- Submission history is normalized into FilingEvent records.
- Filing types are classified into material event, reporting, registration, ownership and insider categories where possible.
- A declared User-Agent is configurable through PINE_FOUNDRY_SEC_USER_AGENT.
- The adapter polls only configured tickers through PINE_FOUNDRY_SEC_TICKERS.

Pine Foundry uses small concurrency rather than crawling the entire filing corpus.

### Canada / SEDAR+ and TSX disclosure discovery

SEDAR+ provides a public searchable filing interface, but Pine Foundry deliberately does not rely on undocumented internal SEDAR+ JSON endpoints.

Instead the Canadian disclosure adapter constructs public Google News RSS searches restricted to:
- site:sedarplus.ca
- site:sedarplus.ca/csa-party
- site:tsx.com/en/news

Configured tickers:

~~~text
PINE_FOUNDRY_CANADA_TICKERS=
PINE_FOUNDRY_CANADA_POLL_SECS=300
~~~

Returned articles are normalized into the same NewsArticle/CatalystEvent path and journaled as canada_disclosure.

## News

### Issuer-specific RSS/Atom

Issuer-specific feeds can be configured through:

~~~text
PINE_FOUNDRY_ISSUER_FEEDS=TICKER=https://issuer.example/feed.xml;TICKER2=https://issuer.example/atom.xml
~~~

Configured feeds use the existing RSS/Atom parser, URL deduplication, catalyst
classification, semantic story clustering and JSONL event journal. This is
intentionally configuration-driven so Pine Foundry does not depend on
undocumented issuer endpoints.

Documented source families include SEC EDGAR company-search RSS, issuer
Investor Relations RSS/Atom when published by the issuer, GlobeNewswire
direct-from-source press-release feeds, and Business Wire issuer-page RSS
links. See docs/issuer-feeds.md.

### Reddit

Global search RSS:
- GET https://www.reddit.com/search.rss?q={encoded_query}&sort=new&limit={n}

Global JSON:
- GET https://www.reddit.com/search.json?q={encoded_query}&sort=new&limit={n}

Pine Foundry attempts both. RSS is treated as the more durable keyless source; JSON is an additional structured source and may be unavailable or throttled.

### Google News

Search RSS:
- GET https://news.google.com/rss/search?q={encoded_query}&hl=en-CA&gl=CA&ceid=CA:en

Used for broad news searches and for public Canadian disclosure discovery.

### NewsAPI

Everything:
- GET https://newsapi.org/v2/everything
- Query parameters include q, from, language, sortBy and pageSize.
- NEWSAPI or NEWSAPI_KEY is read from the process environment.
- The credential is sent with X-Api-Key and never stored in the repository.

Local safety guards:
- PINE_FOUNDRY_NEWSAPI_MIN_INTERVAL_SECS
- PINE_FOUNDRY_NEWSAPI_DAILY_LIMIT

These apply an interval guard per normalized query plus one shared daily ceiling across all NewsAPI requests.

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
- catalyst event type
- story-cluster ID

Results are deduplicated by canonical URL where possible and sorted newest first. Cross-provider story clustering normalizes important headline tokens and retains source/ticker counts.

## Event and evidence layer

Market/news/filing events are persisted as JSONL records in:

~~~text
data/events/YYYY-MM-DD.jsonl
~~~

The event journal is bounded by an asynchronous channel. Queue drops are exposed through:
- GET /api/journal/health

Symbol evidence:
- GET /api/evidence/:symbol

This joins:
- venue state
- order-book metrics
- cross-venue dislocation
- stream age
- catalyst history
- related news

## Asset classes

Scanner universes can restrict asset_classes to:
- equity
- crypto
- fx

Built-in crypto and FX presets use these boundaries so they do not accidentally enter the equity scanner universe.
