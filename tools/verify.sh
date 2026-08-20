#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Swift toolchain =="
swift --version

echo "== Python hardening/parity script syntax =="
python3 -m py_compile tools/*.py

echo "== Final hardening source state =="
python3 tools/verify-final-hardening-source-state.py

echo "== Package manifest =="
swift package dump-package >/dev/null

echo "== Build =="
swift build

echo "== Offline/default tests =="
swift test

echo "YFinanceKit verification passed"
