#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Swift toolchain =="
swift --version

echo "== Build =="
swift build

echo "== Offline/default tests =="
swift test

echo "YFinanceKit verification passed"
