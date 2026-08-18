#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Swift strict concurrency audit =="
swift --version
swift test \
  -Xswiftc -warn-concurrency \
  -Xswiftc -strict-concurrency=complete
