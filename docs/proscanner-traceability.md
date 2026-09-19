# Public Feature Traceability

This document separates public behavior used as design input from implementation decisions made by Pine Foundry.

## Publicly documented concepts

The public ProScanner/ZeroPro material documents:

- configurable U.S. equity scanning
- price and percent-change criteria
- volume criteria
- fundamental criteria including float, shares outstanding and market cap
- issue type
- 1m/5m/15m change fields
- enabled filters with min/max-style controls
- saved presets
- customizable columns and sorting
- multiple scanner windows
- scanner linkage to chart/market-depth workflows
- live streaming behavior in current release notes

## Pine Foundry implementation

The code maps those concepts to:

- Field
- FilterSpec
- UniverseSpec
- ScanDefinition
- Preset
- ScanRuntime
- ScannerEvent

The implementation does not copy private source code, UI assets, branding, private feed protocols or undocumented threshold values.

## Thresholds

Builtin thresholds shipped here are original defaults chosen to make the demo useful. They are not assertions of proprietary vendor preset values.

## Exact internal behavior not publicly known

The following remain provider/implementation-specific:

- exact live market feed
- precise update coalescing
- proprietary preset thresholds
- internal ranking implementation
- private fundamental provider
- exact websocket wire schema
