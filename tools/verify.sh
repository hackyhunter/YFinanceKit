#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Swift toolchain =="
swift --version

echo "== Python hardening/parity script syntax =="
python3 -m py_compile \
  tools/parity_harness.py \
  tools/parity_matrix.py \
  tools/apply-core-rate-limit-hardening.py

echo "== Package manifest =="
swift package dump-package >/dev/null

echo "== Build =="
swift build

echo "== Offline/default tests =="
swift test

echo "YFinanceKit verification passed"
