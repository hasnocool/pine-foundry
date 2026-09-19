# API

Base URL: http://127.0.0.1:3000

## Health

GET /health

Returns:

~~~json
{"ok":true,"service":"pine-foundry","mock_feed":true}
~~~

## Presets

GET /api/presets

Returns builtins plus persisted custom presets.

POST /api/presets

Accepts a complete Preset. The server generates the identifier and marks it custom.

DELETE /api/presets/:id

Deletes only a custom preset. Builtins return 404.

## Scans

GET /api/scans

Lists active scanner definitions and current match counts.

POST /api/scans

Creates a scan from a ScanDefinition.

PUT /api/scans/:id

Replaces the definition, rebuilds membership and publishes resync_required.

DELETE /api/scans/:id

Deletes a scan runtime.

GET /api/scans/:id/snapshot

Returns the complete ranked result set.

## WebSocket

GET /ws/scanner/:id

The first server message is a snapshot. Subsequent messages are incremental events.

### Result added

~~~json
{"type":"result_added","scan_id":"...","row":{"symbol":"..."}}
~~~

### Result updated

Contains the full current row for the changed symbol.

### Result removed

~~~json
{"type":"result_removed","scan_id":"...","symbol":"..."}
~~~

### Resync required

Clients must request a fresh snapshot or reconnect.

Client text message resync requests a snapshot over the existing socket.

## Scan definition

Numeric filters use inclusive bounds:

min <= value <= max

Missing values fail enabled filters.

Universe issue types are applied before numeric filters. Session mismatches fail unless the scan uses closed as the wildcard-like session.


## Public market-data proxy routes

Provider discovery:

```
GET /api/providers
GET /api/providers/health
```

Canadian equities / FX:

```
GET /api/providers/tradingview/canada/:start/:end
GET /api/providers/tradingview/forex/:start/:end
GET /api/providers/yahoo/fx/:symbol
GET /api/providers/frankfurter/:base/:quote
GET /api/providers/frankfurter/:base/rates/:quotes
```

Crypto:

```
GET /api/providers/binance/:symbol/quote
GET /api/providers/binance/:symbol/ticker
GET /api/providers/binance/:symbol/klines/:interval
GET /api/providers/binance/:symbol/depth

GET /api/providers/kraken/:symbol/ticker
GET /api/providers/kraken/:symbol/ohlc/:interval
GET /api/providers/kraken/:symbol/depth
GET /api/providers/kraken/:symbol/trades

GET /api/providers/coinbase/:symbol/ticker
GET /api/providers/coinbase/:symbol/candles/:granularity
GET /api/providers/coinbase/:symbol/book
GET /api/providers/coinbase/:symbol/trades
```

U.S. and global equity routes remain:

```
GET /api/providers/yahoo/:symbol
GET /api/providers/nasdaq/:symbol
GET /api/providers/nasdaq/:symbol/info
GET /api/providers/tradingview/:exchange/:symbol
```

All public-provider routes are keyless in the current implementation. Provider health tracks successful/failed requests so a provider can be degraded without taking down the scanner.
