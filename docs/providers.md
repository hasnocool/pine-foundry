# Public provider stack

Pine Foundry uses public, zero-auth market-data routes where the provider exposes them. Every adapter normalizes into the same PublicQuote structure and then into the scanner's existing market state/event pipeline.

## Coverage

### U.S. equities

**TradingView America**
- POST https://scanner.tradingview.com/america/scan
- Primary broad U.S. equity source.

**Yahoo Finance**
- GET https://query1.finance.yahoo.com/v8/finance/chart/{symbol}
- GET https://query1.finance.yahoo.com/v7/finance/spark
- Per-symbol and batch fallback.

**Nasdaq**
- GET https://api.nasdaq.com/api/quote/{symbol}/realtime?assetclass=stocks
- GET https://api.nasdaq.com/api/quote/{symbol}/info?assetclass=stocks
- Per-symbol U.S. fallback.

### Canada / TSX / TSXV

**TradingView Canada**
- POST https://scanner.tradingview.com/canada/scan
- Bulk Canadian screener/quote source.
- Keeps exchange-qualified symbols so TSX symbols do not collide with U.S. symbols.

**Yahoo Finance**
- Supports Canadian Yahoo symbols such as SHOP.TO and RY.TO through the public chart route.

TMX's official public material describes XML/JSON APIs as a service/entitlement offering rather than a freely exposed anonymous market-data API, so Pine Foundry does not rely on undocumented TMX endpoints.

### Crypto

**Binance**
- /api/v3/ticker/24hr
- /api/v3/klines
- /api/v3/depth

**Kraken**
- /0/public/Ticker
- /0/public/OHLC
- /0/public/Depth
- /0/public/Trades

**Coinbase Exchange**
- /products/{product_id}/ticker
- /products/{product_id}/candles
- /products/{product_id}/book?level=2
- /products/{product_id}/trades

Authenticated account/order routes are intentionally excluded.

### FX

**TradingView Forex**
- POST https://scanner.tradingview.com/forex/scan
- Broad FX screener snapshots.

**Yahoo Finance FX**
- Public chart symbols such as EURUSD=X, USDCAD=X, USDJPY=X.

**Frankfurter**
- GET https://api.frankfurter.dev/v2/rate/{base}/{quote}
- GET https://api.frankfurter.dev/v2/rates
- No-key reference-rate fallback.

## Live task model

NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN

The tasks are independent. A slow or unavailable crypto provider does not block equity updates.

## Default live configuration

NaN
NaN
NaN
NaN
NaN
NaN
NaN
NaN

Use PINE_FOUNDRY_FEED=mock for deterministic offline development.

## Provider health

- request count
- success count
- failure count
- last success timestamp
- last error
- current status

Routes:

NaN
NaN

## Normalization rules

Every public quote carries symbol, asset class, normalized issue type, venue, last price, previous close when available, percentage change when available, volume when available, reference fundamentals when available, event timestamp, session, and source provider.

Missing fields remain null; adapters never fabricate fundamentals.

## Reliability and limitations

These endpoints are public web/data interfaces, not exchange-entitled consolidated feeds. Availability, freshness, throttling and response schemas can change without notice. Pine Foundry keeps provider health and fallback routing explicit.

For commercial production use, replace or augment these adapters with contractually licensed feeds where required.