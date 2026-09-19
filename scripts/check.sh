#!/usr/bin/env bash
# scripts/check.sh
set -euo pipefail

cargo fmt --all -- --check
cargo check
cargo test

if command -v npm >/dev/null 2>&1; then
  (cd web && npm install --no-audit --no-fund && npm run build)
fi

echo "Pine Foundry checks passed."
