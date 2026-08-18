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

## 3. Apply the remaining surgical core rate-limit patch

The compatibility client is intentionally not whole-file rewritten through remote tooling. In a local checkout run:

```sh
python3 tools/apply-core-rate-limit-hardening.py
```

Review the resulting diff. It must remain narrowly scoped to:

- target-endpoint 429 bypasses crumb/session-strategy refresh
- HTTP `Retry-After` is parsed into structured `YFinanceError.retryAfter`
- `YFRequestCoordinator` honors provider `Retry-After` when opening cooldown

Do not accept unrelated giant-client churn in this step.

## 4. Local package verification

Run:

```sh
bash tools/verify.sh
bash tools/strict-concurrency.sh
```

Both must pass.

Do not re-enable automatic GitHub Actions to satisfy this gate.

## 5. High-value offline regression suites

Confirm coverage is green for at least:

- Yahoo cookie/crumb session
- blocked `fc.yahoo.com` fallback
- concurrent crumb callers
- invalid crumb bodies
- rate-limit classification/cooldown
- `Retry-After` parsing/metadata
- bounded and prioritized request scheduling
- queued request cancellation
- single-flight request coalescing
- fresh/stale cache behavior
- financial long-URL chunk fallback
- malformed/null/type-shifted Yahoo JSON
- history integrity
- repair parity
- bounded interior 100x repair
- suspicious-large-repair safety valve
- history metadata fallback
- multi-info partial failure
- UTC/exchange date semantics
- redacted diagnostics export and endpoint rollups
- focused quote/history/metadata/financial service façades

## 6. Parity harness

Start with a targeted run:

```sh
python3 tools/parity_harness.py --symbols AAPL,MSFT,VOO,BTC-USD
```

Then run the broader cross-market matrix:

```sh
python3 tools/parity_matrix.py
```

The matrix includes US equities/ETFs, intraday, UK subunit quotes, Europe, Asia-Pacific, Johannesburg/Tel Aviv, crypto/FX and indices.

Inspect warnings and failures. Do not blindly require byte-for-byte pandas parity where Swift intentionally differs. Generated reports are evidence for the exact tested commit only.

## 7. Live Yahoo smoke

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

## 8. Version decision

Only after the previous gates pass:

- replace the development package version with the intended release semver
- update README/HARDENING/UPSTREAM_BASELINE if needed
- tag the exact verified commit

Do not change `__version__` unless the upstream yfinance compatibility target changed.

## 9. nommminal app-pin migration

Use the `yfinance-hardening-integration` branch as the staged integration path.

Before changing `master`:

1. choose the exact verified YFinanceKit commit/tag
2. update every app/package/Xcode reference together
3. run `python3 scripts/apply-yfinance-hardening-integration.py`
4. run `python3 scripts/verify-yfinance-hardening.py`
5. run ProviderParity tests
6. run JavaScript bridge/chart tests
7. build Debug simulator
8. build Release simulator
9. launch simulator smoke
10. test quote/history/revenue/earnings/options
11. verify JS/native 429 makes one native attempt, then cools down
12. verify JS cancellation cancels the actual Swift/Yahoo task
13. test in-memory stale cache and app-local disk snapshot fallback
14. verify bridge-v2 metadata does not alter endpoint `data` keys
15. run a real-device Yahoo/session smoke when practical

If signing/capabilities change, validate those separately in Xcode. App/widget sharing still requires adding an App Group capability; the current widget does not contact Yahoo.

## 10. Rollback point

Before app migration, record the previous known-good YFinanceKit app pin so the integration can be reverted immediately if device behavior differs from simulator/offline tests.
