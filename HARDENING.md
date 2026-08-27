# YFinanceKit hardening

YFinanceKit is a native Swift Yahoo Finance implementation that tracks Python yfinance as a reverse-engineering oracle, but reliability takes precedence over mechanical Python parity.

## Identity

- Swift implementation version: `0.2.0-dev.4`
- yfinance compatibility target: `1.7.0`
- upstream baseline: `3d9d2f0cacb662bff689874cd6113bae3a30a885` (2026-08-26)

The Swift package version and the upstream compatibility version are intentionally independent.

## Architecture

### Compatibility monolith behind native façades

`YFinanceClient` still contains a large amount of compatibility endpoint and price-repair behavior. New app code should not depend on the monolith directly.

The native layering is:

1. `YFURLSessionTransport`
   - isolated URLSession send/HTTP normalization layer
   - Sendable response values: data, status, normalized headers
2. `YFCrumbStore`
   - Yahoo cookie/crumb strategy state
3. `YFinanceClient`
   - compatibility endpoints, parsing and legacy repair machinery
4. `YFRequestCoordinator`
   - global provider retry/backpressure policy
5. `YFResilientClient`
   - single-flight, cache and high-volume resilience façade
6. focused native service protocols/façades
   - quote
   - history
   - history metadata
   - financial statements
7. optional `YFRequestBudgetGate`
   - interactive / normal / background logical scheduling

The URLSession transport source is checked in now. Moving the giant client onto it is an exact local migration rather than a remote 174 KB whole-file replacement.

### Focused service surface

Narrow protocols keep feature code from accidentally growing into another all-endpoint client:

- `YFQuoteProviding`
- `YFCachedQuoteProviding`
- `YFHistoryProviding`
- `YFCachedHistoryProviding`
- `YFHistoryMetadataProviding`
- `YFFinancialStatementProviding`

Focused façades:

- `YFQuoteService`
- `YFHistoryService`
- `YFHistoryMetadataService`
- `YFFinancialStatementService`
- `YFMarketDataServices`

They can all share one `YFResilientClient`, so Yahoo cookie/crumb state remains unified.

## Yahoo session and request hardening

### Cookie/crumb lifecycle

Query1/query2 traffic uses paired Yahoo cookie/crumb state. Crumbs are not resurrected independently from an unrelated persisted cookie session.

Supported bootstrap paths:

- basic Yahoo cookie strategy
- CSRF/consent fallback

Regression coverage includes:

- blocked `fc.yahoo.com`
- consent fallback
- concurrent crumb callers sharing one bootstrap
- HTTP 200 HTML/garbage crumb bodies
- rate-limit behavior

### Retry policy

`YFRequestCoordinator` owns provider-level retry policy above the compatibility client:

- bounded concurrent Yahoo work
- retry transport/5xx-style transient failures only
- exponential retry delay
- jitter
- no coordinator retry for 429
- shared rate-limit cooldown
- cancellation checks
- bounded traces/diagnostics

A 429 is provider backpressure, not evidence that the cookie/crumb strategy is invalid.

### Retry-After

`YFRetryAfterParser` supports:

- delta-seconds
- RFC HTTP-date forms
- bounded maximum delay

`YFinanceError.rateLimited(retryAfter:)` and `YFinanceError.retryAfter` carry provider backpressure metadata without adding a new enum case that would source-break exhaustive downstream switches.

The final strict 429 behavior is applied locally as part of candidate preparation:

- target endpoint 429 bypasses crumb/session-strategy refresh
- `Retry-After` is captured
- the shared coordinator uses the provider delay as the cooldown floor

### Logical priority budget

`YFRequestBudgetGate` adds optional app-level logical scheduling above the transport coordinator:

- `interactive`
- `normal`
- `background`
- foreground work queues ahead of lower-priority work
- independent background concurrency cap
- cancellation-safe queued waiters
- active/queued diagnostics

This does not replace `YFRequestCoordinator`; it only decides which logical caller may enter that layer next.

## Single-flight and caching

`YFResilientClient` coalesces identical in-flight work for high-value operations such as quote/history/metadata/financial/info retrieval.

`YFStaleCache` distinguishes:

- fresh
- stale
- expired

Quote/history cached APIs can return last-known-good stale data rather than forcing a blank UI during temporary Yahoo failures.

## History/data hardening

### Structural validation

`YFHistorySeries.integrityReport()` detects:

- duplicate timestamps
- non-monotonic timestamps
- non-finite/non-positive prices
- negative volume
- high below low
- open/close outside low/high
- non-finite adjusted close
- classic unexplained 100x/0.01x jumps

`validateIntegrity()` rejects structural errors while leaving warning-level anomalies inspectable.

### Conservative 100x repair

The post-repair hardener can detect a **bounded interior** 100x/0.01x block using paired transitions. It deliberately does not guess at a lone edge transition.

`YFResilientClient.safelyRepairedHistory(...)` protects against the upstream #2927 class of bug: if the legacy repair engine marks an unusually large slice of a table, raw history is compared and a simpler bounded repair only replaces the legacy result when the raw data proves the interior-block case.

### OHLC/date/event hardening

- invalid high/low bounds can be normalized from finite OHLC values
- custom start/end actions can be trimmed to the exact half-open requested window
- Yahoo calendar epochs use explicit UTC semantics
- exchange-day calculations use explicit exchange timezones instead of the machine timezone

These capture the regression concerns in upstream #2947/#2948 without importing their Python-specific implementation.

## Fundamentals

Financial statements use Yahoo fundamentals-timeseries with typed output.

Behavior:

1. single-request fast path
2. detect empty/failing response
3. 60-key fallback chunks for long URL/proxy failures
4. merge into typed statement series
5. do **not** fan out chunk requests after auth or rate-limit failures

## Defensive schema handling

The package has deterministic malformed-response coverage for:

- null/empty chart results
- HTML instead of JSON
- truncated JSON
- timestamp type shifts
- numeric string type shifts
- null quote arrays/results
- null search collections
- Yahoo error objects with explicit provider reasons

`YFinanceMutationFuzzTests` adds a deterministic 96-seed mutation corpus over a valid chart response. Mutations delete fields, insert nulls, shift types, truncate arrays and corrupt nested shapes. Each case must either return best-effort output or a structured `YFinanceError`, never an unrelated exception/process trap.

## Bulk access

`YFTickers.infoResult(maxConcurrentRequests:)` provides bounded best-effort multi-symbol metadata retrieval. One symbol failing does not discard successful siblings.

## Observability

Diagnostics are operational, not payload logging.

Available data includes:

- logical requests
- network attempts
- retries
- rate limits
- success/failure/cancellation
- cache hits/misses
- coalesced requests
- active/queued requests
- cooldown deadline
- per-request duration/attempt count
- per-endpoint average/max latency
- per-endpoint failure-kind rollups

`YFDiagnosticsExport` is Codable and deliberately excludes:

- full URLs/query values
- headers
- cookies
- crumbs
- auth values
- request bodies
- response bodies

## Parity/regression matrix

`tools/parity_matrix.py` drives the canonical parity harness across:

- US equities
- US ETFs/fund
- US intraday
- London subunit quotes
- Europe
- Asia-Pacific
- Johannesburg/Tel Aviv
- crypto/FX
- indices

Generated parity reports are evidence for the exact tested commit only. Old generated “green” reports must not remain checked in as permanent badges.

## nommminal integration

The app has a dedicated staging branch:

`yfinance-hardening-integration`

That branch stages:

- one app-facing resilient market-data broker
- priority/background budgeting
- additive bridge schema v2
- backward-compatible JS bridge metadata
- real race-safe Swift task cancellation
- quote/chart/revenue broker migration without changing endpoint data contracts
- app-local last-known-good snapshot policy + fallback
- deterministic `.pbxproj` source/pin edits
- complete non-Xcode preparation + verification scripts

`nommminal/master` stays on the last Xcode-verified state until those gates pass.

There is no App Group entitlement today. The current widget does not contact Yahoo, so app/widget market-data sharing is optional and remains an Xcode signing/capability choice.

## Candidate preparation

Automatic CI and Dependabot remain disabled.

In a **local YFinanceKit checkout**, the authoritative candidate flow is:

```sh
python3 tools/prepare-hardening-candidate.py
```

That command:

1. applies `tools/apply-transport-extraction.py`
2. applies `tools/apply-core-rate-limit-hardening.py`
3. verifies the expected source state
4. runs `tools/verify.sh`
5. runs complete strict-concurrency checking

`tools/verify.sh` itself performs:

- Python migration/parity script syntax checks
- `tools/verify-hardening-source-state.py`
- Swift package manifest parse
- `swift build`
- `swift test`

Review the giant-client diff before committing. Then commit the verified result and use that exact SHA in nommminal:

```sh
python3 scripts/prepare-yfinance-hardening.py --revision <verified-yfinancekit-commit>
python3 scripts/verify-yfinance-hardening-all.py
```

Only after those non-Xcode gates pass should Xcode be opened for Debug/Release builds and runtime smoke.

## Remaining hard boundaries

These are not honestly solvable from the current remote-only environment:

- execute/review the exact giant-client transport + strict-429 migrations
- compile all Swift source and tests
- complete strict-concurrency audit
- run the live cross-market parity matrix
- small live Yahoo smoke from a normal network
- commit the verified final YFinanceKit candidate
- run nommminal's deterministic integration migration against that final commit
- Xcode Debug/Release simulator build
- simulator launch/function smoke
- real-device Yahoo/session/cancellation smoke
- signing/App Group work only if shared widget market snapshots are desired

Until those happen, this source tree is a hardened **pre-release candidate staging state**, not a claimed production-verified release.
