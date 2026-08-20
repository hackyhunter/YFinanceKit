#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Verify candidate source, build and tests =="
bash tools/verify.sh

echo "== Warnings-as-errors concurrency audit =="
bash tools/strict-concurrency-audit.sh

echo
printf '%s\n' \
  "Offline hardening gate passed." \
  "Review the YFinanceClient/YFinanceResilience diff before committing." \
  "Next: commit the verified candidate, then run tools/live-parity-gate.sh."
