# Development

No GitHub Actions are used.

## Local checks

```bash
cargo fmt --all -- --check
cargo check
cargo test
```

## Run

```bash
cargo run -- serve
```

Server environment:

- `PINE_FOUNDRY_ADDR` — default `127.0.0.1:3000`
- `PINE_FOUNDRY_DATA_DIR` — default `data`

## Web UI

```bash
cd web
npm install
npm run dev
```

Use `VITE_API_BASE` to point the browser at a different server:

```bash
VITE_API_BASE=http://127.0.0.1:3000 npm run dev
```

## API smoke test

```bash
curl http://127.0.0.1:3000/health
curl http://127.0.0.1:3000/api/presets
curl http://127.0.0.1:3000/api/scans
```

## Engineering policy

Keep asynchronous work non-blocking. The market loop uses Tokio timers and synchronization primitives. File persistence uses Tokio filesystem APIs. Do not put vendor SDKs or blocking HTTP clients inside the scanner hot path.

For performance work, benchmark the current implementation before adding Redis, a database, a custom sort index, or more threads.
