# Public provider stack

Pine Foundry now has a keyless public-provider layer. The live scanner uses an ordered fallback chain instead of depending on one source.

## Live feed order

1. TradingView America scanner — bulk U.S. equity scan.
2. Yahoo Finance Spark — batch watchlist fallback.
3. Nasdaq public realtime quote API — per-symbol fallback.

The feed defaults to PINE_FOUNDRY_FEED=auto. Use PINE_FOUNDRY_FEED=mock when working offline.

## Routes implemented

Provider | Public route | Authentication | Use
--- | --- | --- | ---
TradingView | POST scanner.tradingview.com/america/scan | none | bulk U.S. screener/quotes
TradingView | GET scanner.tradingview.com/symbol | none | per-symbol metrics/technical fields
Yahoo | GET query1.finance.yahoo.com/v8/finance/chart/{symbol} | none | intraday chart/price
Yahoo | GET query1.finance.yahoo.com/v7/finance/spark | none | batch watchlist quotes
Nasdaq | GET api.nasdaq.com/api/quote/{symbol}/realtime?assetclass=stocks | none | public U.S. quote
Nasdaq | GET api.nasdaq.com/api/quote/{symbol}/info?assetclass=stocks | none | security reference data
Binance | GET data-api.binance.vision/api/v3/ticker/24hr | none | crypto ticker
Binance | GET data-api.binance.vision/api/v3/klines | none | crypto candles
Binance | GET data-api.binance.vision/api/v3/depth | none | crypto order book

## Pine Foundry API proxies

- GET /api/providers — route catalog.
- GET /api/providers/health — runtime provider health.
- GET /api/providers/yahoo/:symbol — Yahoo chart snapshot.
- GET /api/providers/nasdaq/:symbol — Nasdaq quote.
- GET /api/providers/nasdaq/:symbol/info — Nasdaq reference response.
- GET /api/providers/tradingview/:exchange/:symbol — TradingView per-symbol metrics.
- GET /api/providers/binance/:symbol/ticker — Binance 24h ticker.
- GET /api/providers/binance/:symbol/klines/:interval — Binance candles.
- GET /api/providers/binance/:symbol/depth — Binance order book.

## Environment

    PINE_FOUNDRY_FEED=auto
    PINE_FOUNDRY_POLL_SECS=5
    PINE_FOUNDRY_TV_PAGE_SIZE=5000
    PINE_FOUNDRY_TV_MAX_ROWS=20000
    PINE_FOUNDRY_SESSION=regular

## Reliability policy

Public web endpoints are not treated as execution-grade market feeds. Pine Foundry records provider request/success/failure state and falls through to the next source when a source fails.

The adapters are isolated so a paid or authenticated provider can be introduced later without changing the scanner engine.

## Sources and limitations

TradingView's public scanner is useful for bulk screening but is an undocumented web endpoint rather than a contracted market-data feed.

Yahoo's chart/spark endpoints are convenient keyless web endpoints but can rate-limit automated callers.

Nasdaq's public endpoints may require browser-like headers and can change independently of the scanner.

Binance's documented market-data-only domain is suitable for crypto market data, not U.S. equity data.

Pine Foundry intentionally does not use Stooq or CNBC as primary live providers in the current stack because recent evidence shows anti-bot/access instability for automated retrieval.