#!/usr/bin/env bash
# scripts/check.sh
set -euo pipefail

cargo fmt --all -- --check
cargo check
cargo test

if grep -R --line-number --exclude-dir=target --exclude='*.lock' 'block_on' src; then
  echo "error: blocking async execution found in src/" >&2
  exit 1
fi

if [ -d .github/workflows ]; then
  echo "error: GitHub Actions are intentionally disabled for Pine Foundry" >&2
  exit 1
fi

if command -v npm >/dev/null 2>&1; then
  (cd web && npm install --no-audit --no-fund && npm run build)
fi

echo "Pine Foundry checks passed."
