#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Strict concurrency build, warnings as errors =="
swift build \
  -Xswiftc -strict-concurrency=complete \
  -Xswiftc -warnings-as-errors

echo "== Strict concurrency tests, warnings as errors =="
swift test \
  -Xswiftc -strict-concurrency=complete \
  -Xswiftc -warnings-as-errors

echo "Strict concurrency warnings-as-errors audit passed"
