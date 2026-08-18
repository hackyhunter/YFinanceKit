# YFinanceKit release checklist

Do not tag a stable YFinanceKit release or advance a production app pin by intuition. Use this checklist.

## 1. Choose the exact source commit

Record:

- YFinanceKit commit
- `YFinanceKitBuildInfo.packageVersion`
- upstream yfinance compatibility version
- upstream yfinance commit/date

Do not release from an unrecorded floating branch state.

## 2. Review upstream since the recorded baseline

Check current `ranaroussi/yfinance`:

- main/dev commits
- release notes
- merged PRs
- high-signal open PRs

Prioritize:

- cookie/crumb/session behavior
- rate limiting
- history/chart behavior
- price/dividend/split/currency repair
- fundamentals-timeseries
- quote/info/options/search/screener schemas
- calendar/timezone handling
- null/malformed Yahoo response handling

Update `UPSTREAM_BASELINE.md` with decisions. Open PRs are regression evidence, not automatically accepted implementation.

## 3. Local package verification

Run:

```sh
bash tools/verify.sh
bash tools/strict-concurrency.sh
```

Both must pass.

Do not re-enable automatic GitHub Actions to satisfy this gate.

## 4. High-value offline regression suites

Confirm coverage is green for at least:

- Yahoo cookie/crumb session
- blocked `fc.yahoo.com` fallback
- concurrent crumb callers
- invalid crumb bodies
- rate-limit classification/cooldown
- single-flight request coalescing
- fresh/stale cache behavior
- financial long-URL chunk fallback
- malformed/null JSON
- history integrity
- repair parity
- bounded interior 100x repair
- suspicious-large-repair safety valve
- history metadata fallback
- multi-info partial failure
- UTC/exchange date semantics

## 5. Parity harness

Run a representative symbol matrix, including when possible:

- US equities
- ETF
- mutual fund
- London GBp
- Johannesburg ZAc
- Tel Aviv ILA
- FX
- crypto
- index
- invalid/delisted symbol
- ticker with recent split/dividend
- thinly traded ticker

Example:

```sh
python3 tools/parity_harness.py --symbols AAPL,MSFT,VOO,BTC-USD
```

Inspect both warnings and failures. Do not blindly require byte-for-byte pandas parity where Swift intentionally differs.

## 6. Live Yahoo smoke

After offline tests pass, perform a small live smoke from a normal residential/device network:

- quote
- 5d daily history
- intraday history
- fundamentals statement
- options
- search
- metadata
- one international/subunit ticker

Avoid large live matrices that unnecessarily pressure Yahoo.

## 7. Version decision

Only after the previous gates pass:

- replace the development package version with the intended release semver
- update README/HARDENING/UPSTREAM_BASELINE if needed
- tag the exact verified commit

Do not change `__version__` unless the upstream yfinance compatibility target changed.

## 8. nommminal app-pin migration

Before changing `hackyhunter/nommminal`:

1. choose the exact verified YFinanceKit commit/tag
2. update every app/package/Xcode reference together
3. run ProviderParity tests
4. run JavaScript bridge/chart tests
5. build Debug simulator
6. build Release simulator
7. launch simulator smoke
8. test quote/history/revenue/earnings/options
9. test 429/error behavior
10. test cancellation and stale cache behavior
11. test app-local disk snapshot fallback if wired
12. run a real-device Yahoo/session smoke when practical

If signing/capabilities change, validate those separately in Xcode.

## 9. Rollback point

Before app migration, record the previous known-good YFinanceKit app pin so the integration can be reverted immediately if device behavior differs from simulator/offline tests.
