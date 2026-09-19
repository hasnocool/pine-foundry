# Pine Foundry Master Specification

Pine Foundry is an original, local-first implementation of a real-time equity scanner using the publicly observable workflow of modern trading scanners: configurable issue type, price, change, volume and fundamental filters; min/max bounds; saved presets; sortable/customizable result grids; concurrent scans; and a live snapshot+delta stream.

## Confirmed functional model implemented here

- U.S. equity-style issue taxonomy with common stock first.
- Regular, pre-market, after-hours and closed session state.
- Price, absolute change, previous-close %, 1m/5m/15m %, day volume and 1m volume.
- Shares float, shares outstanding and market cap.
- Enabled/disabled numeric range filters.
- Issue-type universe restrictions.
- Stable multi-field tie-break sorting.
- Saved custom presets with immutable builtin presets.
- Three-window-friendly backend (the backend is not artificially capped at three).
- WebSocket snapshot, add, update, remove and resync events.
- Symbol rows designed for chart/depth/watchlist linking.
- Deterministic synthetic feed for offline development.
- Historical metric state held as rolling minute buckets.
- Atomic JSON preset persistence.
- Local-only test/check workflow; no GitHub Actions.

## Data flow

```
Market provider
  -> normalized MarketEvent
  -> per-symbol SecurityState
  -> Metrics
  -> filter evaluation
  -> ScanRuntime membership
  -> ScannerEvent
  -> WebSocket
  -> scanner UI
```

## Engineering rules

1. Keep provider code outside the scanner truth model.
2. Never run an LLM on every market tick.
3. Avoid blocking file/network operations on async workers.
4. Batch UI deltas naturally through the WebSocket event stream.
5. Add dependency indexing when the number of simultaneous scans becomes large.
6. Benchmark before introducing Redis, a database, or a custom order-statistics structure.

## Future implementation layers

### Market connectivity
Provider adapters should normalize quotes, trades, reference data, session changes, halts, sequence IDs, and reconnects into MarketEvent values.

### Advanced metrics
VWAP, relative volume, opening range, moving averages, ATR, RSI, halt/LULD state, sector strength and market breadth can be added as new Field values without changing the filter execution model.

### Research
Record scanner enter/exit events and reuse the same deterministic engine for historical replay and scan backtesting.

### AI
AI belongs downstream of scanner events:
deterministic scan -> interesting event -> compact evidence -> small local model -> optional larger model.

## Public reference boundary

The implementation is based on public documentation and observable concepts. It does not copy proprietary source code, branding, assets, private thresholds, private data-feed schemas, or internal algorithms.
