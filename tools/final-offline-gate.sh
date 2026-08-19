#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Prepare candidate source =="
python3 tools/prepare-hardening-candidate.py

echo "== Warnings-as-errors concurrency audit =="
bash tools/strict-concurrency-audit.sh

echo "== Final static hardening state =="
python3 tools/verify-hardening-source-state.py

echo
printf '%s\n' \
  "Offline hardening gate passed." \
  "Review the generated YFinanceClient/YFinanceResilience diff before committing." \
  "Next: commit the verified candidate, then run tools/live-parity-gate.sh."
