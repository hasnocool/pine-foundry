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
- Working browser UI with filter editing, preset switching, column visibility and sorting.
- Deterministic synthetic feed so the system runs without a market-data API.
- CLI for listing builtin presets.
- Local tests and validation only; **no GitHub Actions**.

## Quick start

```bash
cargo fmt --all -- --check
cargo check
cargo test
cargo run -- serve
```

Server:

```
http://127.0.0.1:3000
```

Health:

```
curl http://127.0.0.1:3000/health
```

List presets:

```
cargo run -- presets
```

Web client:

```bash
cd web
npm install
npm run dev
```

Then open the Vite URL shown by the dev server.

## Configuration

`PINE_FOUNDRY_ADDR` defaults to `127.0.0.1:3000`.

`PINE_FOUNDRY_DATA_DIR` defaults to `data` and stores custom presets in `presets.json`.

## Keyless live market-data stack

The default server uses an asynchronous public-provider chain:

1. TradingView America bulk scanner.
2. Yahoo Finance Spark batch quote fallback.
3. Nasdaq public realtime quote fallback.

The repository also exposes Yahoo chart, Nasdaq reference data, TradingView symbol metrics, and Binance public ticker/candles/order-book routes. No API keys are required for these routes.

Configure live polling with PINE_FOUNDRY_FEED, PINE_FOUNDRY_POLL_SECS, PINE_FOUNDRY_TV_PAGE_SIZE, PINE_FOUNDRY_TV_MAX_ROWS, and PINE_FOUNDRY_SESSION.

See docs/providers.md for the complete route catalog and limitations.

## Expanded keyless live markets

The default live runtime now runs independent feeds for:

- U.S. equities: TradingView America, Yahoo Finance, Nasdaq.
- Canadian equities: TradingView Canada for TSX/TSXV plus Yahoo Canadian symbols.
- Crypto: Binance, Kraken, Coinbase.
- FX: Yahoo Finance FX, TradingView Forex, Frankfurter reference rates.

Default symbols:

    PINE_FOUNDRY_CRYPTO_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
    PINE_FOUNDRY_FX_PAIRS=EURUSD=X,USDCAD=X,GBPUSD=X,USDJPY=X

See docs/providers.md for route coverage, fallback behavior, normalization and provider limitations.
## Architecture

```
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
```

The production provider boundary should normalize quotes, trades, reference data, session changes, halt status and sequence information into the same event model used by the scanner.

See [docs/master-spec.md](docs/master-spec.md) and [docs/development.md](docs/development.md).
