# Architecture

## Runtime graph

~~~text
Provider adapter
      |
      v
  MarketEvent
      |
      v
SecurityState + 20 minute buckets
      |
      v
    Metrics
      |
      v
  Filter engine
      |
      v
 ScanRuntime membership
      |
      +----> REST snapshots
      |
      +----> WebSocket deltas
                    |
                    v
                 Web UI
~~~

## Hot path

A market event touches one symbol. The engine updates that symbol's state, recalculates derived metrics, and evaluates active scans. Each scan keeps a HashSet membership index, so add/remove transitions are O(1) membership operations. Full sorting happens for snapshots and is deliberately not performed for every update.

## Current concurrency

- Tokio runtime for timers, sockets and asynchronous filesystem access.
- RwLock<HashMap<Symbol, SecurityState>> for market state.
- RwLock<HashMap<ScanId, ScanRuntime>> for active scans.
- broadcast channels for per-scan WebSocket fanout.
- The mock feed emits normalized MarketEvent values, exactly as a future provider adapter should.

No blocking network or filesystem APIs are used in the asynchronous server path.

## Persistence

Only user-created presets are persisted today. Builtins are reconstructed at startup and are immutable through the API. Custom presets are written to a temporary file and atomically renamed.

## Scale path

The first optimization after real market-data integration should be dependency indexing by changed field:

~~~text
Field -> ScanId[]
~~~

For much larger universes, replace full result sorting with an order-statistics index and introduce bounded delta batching for browser clients.

## Extension points

### Market providers

Normalize vendor-specific data into MarketEvent.

### Research

Persist scanner entry/exit/update events and replay them into the same scanner engine.

### AI

Do not put an LLM in the tick path. Consume scanner events downstream and enrich only interesting transitions.

### Broker integration

A future symbol-selection bus can publish scanner selections to chart, market depth, order entry and watchlist consumers.
