#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python3 - <<'PY'
try:
    import yfinance  # noqa: F401
except Exception as exc:
    raise SystemExit(f"Python yfinance is required for live parity: {exc}")
PY

echo "== Small live parity smoke =="
python3 tools/parity_harness.py \
  --symbols AAPL,MSFT,VOO,BTC-USD \
  --period 1mo \
  --interval 1d

if [[ "${FULL_PARITY:-0}" == "1" ]]; then
  echo "== Full cross-market parity matrix =="
  python3 tools/parity_matrix.py
else
  echo
  echo "Full matrix skipped to avoid unnecessary Yahoo load."
  echo "Run FULL_PARITY=1 bash tools/live-parity-gate.sh when a broad live audit is intentional."
fi
