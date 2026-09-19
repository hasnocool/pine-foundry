# Pine Foundry

A local-first, real-time market scanner and research foundation inspired by the public interaction model of modern equity scanners.

> This project implements an original scanner engine and UI architecture. It does not copy proprietary source code, branding, assets, or undocumented TradeZero internals.

## Goals

- Event-driven market-state engine instead of polling.
- Configurable issue-type, fundamental, price, change, and volume filters.
- 1m/5m/15m rolling metrics.
- Incremental result membership and ranking updates.
- WebSocket snapshot + delta protocol.
- Persistent, versioned scanner presets.
- Three independent scanner windows.
- Virtualized result-grid friendly API.
- Historical replay hooks for research.
- Clean boundary for future broker, alert, and AI integrations.

## Repository policy

No GitHub Actions are used. Development is driven by local tooling and explicit benchmark/test commands documented in `docs/development.md`.

## Layout

```
crates/
  scanner-core/     Pure scanner state, metrics, filters, ranking, presets.
  scanner-server/   Axum HTTP/WebSocket server and mock-feed runtime.
  scanner-cli/      Local CLI for loading/evaluating sample scans.
web/                TypeScript browser client.
docs/               Architecture, protocol, API, and development docs.
presets/            Default scanner definitions.
scripts/            Local verification/benchmark helpers.
```

## Quick start

```bash
cargo test --workspace
cargo run -p scanner-server
```

Then open `web/index.html` with a static HTTP server and point it at the scanner server.

The default server uses a deterministic synthetic market feed, so the project is usable without a market-data vendor. A provider adapter can be added without changing the scanner engine.

## License

MIT
