# YFinanceKit hardening

This document tracks reliability work that intentionally goes beyond API-shape parity with Python yfinance.

## Current baseline

- Swift package implementation: `0.2.0-dev.3`
- yfinance compatibility target: `1.6.0`
- upstream commit audited: `0af231f6a47eee5e773290830d228de0c20d5ee1` (2026-08-13)

The package implementation version and the Python compatibility target are intentionally independent.

## Architecture

### Compatibility client behind focused façades

`YFinanceClient` remains the broad Python-compatibility façade and still contains legacy endpoint/repair code. New high-volume app code should not grow that monolith further.

The preferred layers are:

1. `YFinanceClient` for Yahoo transport/session compatibility
2. `YFRequestCoordinator` for provider backpressure/retry/cooldown
3. `YFResilientClient` for coalescing and in-memory stale caches
4. narrow protocols and focused services for feature consumers

Narrow protocols include quote, cached quote, history, cached history, metadata and financial-statement providers. `YFQuoteService`, `YFHistoryService`, `YFHistoryMetadataService` and `YFFinancialStatementService` keep app features from depending on the compatibility monolith directly. `YFMarketDataServices` bundles those services around one shared resilient client/session.

This is the safe first half of splitting the giant client. Physically moving its private transport/session/repair internals remains a later local refactor with a real compiler/test loop.

## Network/session hardening

### Yahoo session state

Query1/query2 traffic uses a Yahoo cookie/crumb session. Crumbs are not resurrected from an unrelated persisted cookie session. The crumb store supports basic Yahoo cookie bootstrap and the CSRF/consent fallback strategy.

Regression coverage includes blocked `fc.yahoo.com`, consent fallback, concurrent crumb callers and HTML/garbage crumb bodies.

### Rate limits and retries

`YFRequestCoordinator` provides an app-facing request gate above `YFinanceClient`:

- bounded concurrent Yahoo work
- retry only for transport/5xx-style transient failures
- exponential retry delay with jitter
- no coordinator retry for 429
- shared exponential cooldown after rate limits
- cancellation checks
- bounded request trace diagnostics

`YFRetryAfterParser` understands both delta-seconds and HTTP-date `Retry-After` values. `YFinanceError.rateLimited(retryAfter:)` and `YFinanceError.retryAfter` carry that metadata without introducing a new enum case/source-breaking exhaustive switch.

The remaining giant-client core edit is checked in as an exact migration script:

```bash
python3 tools/apply-core-rate-limit-hardening.py
```

It is intentionally not a blind whole-file rewrite. It performs only three surgical changes:

- target-endpoint 429 bypasses cookie/crumb strategy refresh
- `Retry-After` is parsed from the HTTP response
- the coordinator uses provider `Retry-After` as the cooldown floor

Run/review this migration locally before a release. Until that patch is applied to the large compatibility client, a target 429 can still cause one session-strategy refresh before reaching the outer coordinator.

### Logical request priority

`YFRequestBudgetGate` is an opt-in scheduler above the transport coordinator for apps that have both foreground and background work:

- priorities: interactive, normal, background
- interactive work queues ahead of normal/background work
- independent background concurrency cap
- cancellation-safe queued waiters
- diagnostic snapshot of active/queued work

The transport coordinator remains the global Yahoo concurrency/backpressure authority. The priority gate only decides which logical caller gets to enter it next.

### Single-flight

`YFResilientClient` coalesces identical in-flight quote/history/metadata/financial/info operations. Callers waiting for the same work share one task instead of multiplying Yahoo requests.

### Stale-while-revalidate

`YFStaleCache` and `quoteCached` / `historyCached` distinguish fresh, stale, and expired entries. Stale data can be returned immediately while a refresh happens separately, which is preferable to blank UI during temporary Yahoo failures.

## Observability

Diagnostics are intentionally operational, not payload logging.

Available surfaces include:

- logical requests and network attempts
- retries and rate-limit count
- success/failure/cancellation outcomes
- cache hits/misses
- coalesced request count
- active/queued work and cooldown deadline
- bounded per-request endpoint/resource traces
- per-endpoint request count, attempts, average/max latency and failure-kind rollups
- Codable redacted export for app/debug tooling

The diagnostics export deliberately excludes URLs, query values, headers, cookies, crumbs, auth values, request bodies and response payloads.

## History/data hardening

### Structural integrity

`YFHistorySeries.integrityReport()` detects:

- duplicate timestamps
- non-monotonic timestamps
- non-finite/non-positive prices
- negative volume
- invalid high/low bounds
- open/close outside low/high
- non-finite adjusted close
- classic unexplained 100x/0.01x jumps

Structural errors can be rejected with `validateIntegrity()`.

### Conservative post-repair pass

`YFinanceHistoryHardening.swift` adds behavior inspired by the upstream 1.6 repair work and open PR #2927:

- paired 100x/0.01x transitions can identify a bounded interior bad-unit block
- only that interior block is scaled
- a lone edge transition is deliberately left alone rather than guessed at
- transitions near split events are excluded
- invalid high/low bounds can be reconstructed from finite OHLC without changing open/close
- start/end history corporate actions can be trimmed to the exact half-open requested window

`YFResilientClient.safelyRepairedHistory(...)` adds a safety valve around the large legacy repair engine: when that engine marks a suspiciously large fraction of the table, raw history is compared and a bounded interior-unit repair only overrides the legacy output when the raw evidence supports it.

### Date semantics

Yahoo calendar-event epochs are absolute UTC timestamps. `YFYahooDateSemantics.utcDateString(...)` makes that explicit. Exchange-day calculations use an explicit exchange timezone rather than the device local timezone.

This incorporates the regression concerns from upstream open PRs #2947 and #2948 without copying their Python-specific implementation.

## Fundamentals

Financial statements use Yahoo fundamentals-timeseries rather than relying solely on the old quoteSummary statement modules. The implementation:

1. tries the single-request fast path
2. detects empty/error/failing payloads
3. falls back to 60-key chunks
4. merges the results into typed statement series
5. does not fan out into chunk fallback for auth/rate-limit failures that cannot be fixed by shorter URLs

## Error and schema handling

`YFinanceErrorClassifier` maps errors into stable categories including unauthorized, forbidden, not found, rate limited, server unavailable, transport, decoding, missing data, and Yahoo API failures.

Yahoo-provided explanations are preserved rather than inventing a delisting explanation when Yahoo already told us why data is missing.

JSON numeric access rejects non-finite foundation numbers and returns nil for unsafe integer conversions instead of risking a trapping numeric conversion.

The offline schema-mutation suite exercises null/empty chart results, HTML instead of JSON, truncated JSON, timestamp/number type shifts, null quote arrays, null quote results and null search collections. The contract is graceful typed failure or best-effort output, never a process crash.

## Bulk access

`YFTickers.infoResult(maxConcurrentRequests:)` provides bounded best-effort metadata fan-out. Each symbol has an independent success/failure result, so one malformed or unavailable ticker does not discard all successful data.

## Parity and regression cage

The default package contains deterministic coverage for:

- Yahoo crumb/session handling and consent fallback
- rate-limit classification/cooldown and Retry-After parsing
- bounded concurrency and priority/background scheduling
- queued cancellation
- financial long-URL chunk fallback
- history metadata fallback
- history integrity
- GBp/ZAc/ILA repair restoration
- resilient retry/cooldown behavior
- fresh/stale cache semantics
- single-flight quote coalescing
- bounded interior 100x block repair
- suspicious-large-repair safety valve
- exact custom-window event trimming
- UTC/exchange date semantics
- malformed/non-finite/schema-shifted JSON values
- multi-info partial failure
- redacted diagnostics and per-endpoint rollups
- focused native service façades

`tools/parity_matrix.py` drives the existing Python-vs-Swift parity harness across US equities/ETFs, intraday, London subunit quotes, Europe, Asia-Pacific, Johannesburg/Tel Aviv, crypto/FX and indices.

Generated parity reports are evidence for the exact tested commit only. Stale generated reports should not remain checked in as permanent green badges.

## nommminal integration

The iOS app has a dedicated staging branch:

`yfinance-hardening-integration`

That branch prepares:

- one app-facing resilient market-data broker
- foreground/background request priority policy
- additive bridge schema v2
- JS bridge compatibility that preserves endpoint `data` keys
- app-sandbox last-known-good snapshot policy/coordinator
- real request-task cancellation with race-safe request reservations
- deterministic local migration for the large `.pbxproj`, provider and JS policy edits
- a pre-Xcode verification script

`nommminal/master` remains on the last Xcode-verified package revision until those local gates pass.

The current widget does not contact Yahoo. Sharing market snapshots with it remains optional and requires adding an App Group capability/signing configuration in Xcode.

## Manual verification

Automatic CI and Dependabot are intentionally disabled.

Before a release candidate:

```bash
python3 tools/apply-core-rate-limit-hardening.py
bash tools/verify.sh
bash tools/strict-concurrency.sh
python3 tools/parity_matrix.py
```

The core migration must be reviewed as a narrow diff before committing/tagging.

## Remaining hard boundaries

These require an actual local compiler/Xcode/device loop:

- execute and review the surgical core 429/Retry-After migration
- run package build/tests and complete strict-concurrency checking
- run the live cross-market parity matrix and a small Yahoo smoke
- physically reorganize private transport/session/repair internals out of the compatibility monolith, if still desirable after the façade split
- advance `nommminal` to the exact verified YFinanceKit revision
- run its deterministic integration migration and pre-Xcode gate
- Xcode Debug/Release simulator builds and launch smoke
- real-device Yahoo/session/cancellation smoke
- add App Group signing only if shared widget market snapshots are actually desired
