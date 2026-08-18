# YFinanceKit hardening

This document tracks reliability work that intentionally goes beyond API-shape parity with Python yfinance.

## Current baseline

- Swift package implementation: `0.2.0-dev.1`
- yfinance compatibility target: `1.6.0`
- upstream commit audited: `0af231f6a47eee5e773290830d228de0c20d5ee1` (2026-08-13)

## Network/session hardening

### Yahoo session state

Query1/query2 traffic uses a Yahoo cookie/crumb session. Crumbs are not resurrected from an unrelated persisted cookie session. The crumb store supports basic Yahoo cookie bootstrap and the CSRF/consent fallback strategy.

### Rate limits and retries

`YFRequestCoordinator` provides an app-facing request gate above `YFinanceClient`:

- bounded concurrent Yahoo work
- retry only for transport/5xx-style transient failures
- exponential retry delay with jitter
- no ordinary retry for 429
- shared exponential cooldown after rate limits
- optional `Retry-After` input for transports that can expose it
- cancellation checks
- bounded request trace diagnostics

The legacy core request layer still performs one Yahoo session-strategy refresh on query1/query2 request failure before surfacing the final error. This includes a target-endpoint 429 today. The resilience layer never adds another retry after the final 429. A future surgical core change can expose response headers and make this path even cleaner.

### Single-flight

`YFResilientClient` coalesces identical in-flight quote/history/metadata/financial/info operations. Callers waiting for the same work share one task instead of multiplying Yahoo requests.

### Stale-while-revalidate

`YFStaleCache` and `quoteCached` / `historyCached` distinguish fresh, stale, and expired entries. Stale data can be returned immediately while a refresh happens separately, which is preferable to blank UI during temporary Yahoo failures.

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

Use `YFResilientClient.hardenedHistory(...)` to combine coordinated fetching with this conservative post-repair pass.

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

## Error handling

`YFinanceErrorClassifier` maps errors into stable categories including unauthorized, forbidden, not found, rate limited, server unavailable, transport, decoding, missing data, and Yahoo API failures.

Yahoo-provided explanations are preserved rather than inventing a delisting explanation when Yahoo already told us why data is missing.

JSON numeric access now rejects non-finite foundation numbers and returns nil for unsafe integer conversions instead of risking a trapping numeric conversion.

## Bulk access

`YFTickers.infoResult(maxConcurrentRequests:)` provides bounded best-effort metadata fan-out. Each symbol has an independent success/failure result, so one malformed or unavailable ticker does not discard all successful data.

## Offline regression cage

The default package now contains deterministic coverage for:

- Yahoo crumb/session handling
- CSRF fallback
- 429 detection
- financial long-URL chunk fallback
- error classification
- history metadata fallback
- history integrity
- GBp/ZAc/ILA repair restoration
- resilient retry/cooldown behavior
- fresh/stale cache semantics
- single-flight quote coalescing
- bounded interior 100x block repair
- exact custom-window event trimming
- UTC/exchange date semantics
- malformed/non-finite JSON values
- multi-info partial failure

## Manual verification

Automatic CI is intentionally disabled.

```bash
bash tools/verify.sh
```

Complete concurrency audit:

```bash
bash tools/strict-concurrency.sh
```

## Remaining high-value work

These require either a surgical change to the large legacy client or an Xcode/local validation pass:

- expose HTTP response headers so `Retry-After` flows directly into the coordinator
- stop the legacy query1/query2 session refresh from retrying a target-endpoint 429
- run complete strict-concurrency checking and fix every warning before changing Swift language mode
- run the full parity harness against a broad symbol matrix
- expand checked-in price-repair fixtures for unusual exchanges/corporate actions
- update `nommminal` to the latest verified YFinanceKit revision and migrate its provider onto `YFResilientClient`
- add persistent App Group stale snapshots for app/widget sharing in `nommminal`
