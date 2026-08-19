# Upstream yfinance baseline

YFinanceKit is a native Swift implementation that tracks Python `ranaroussi/yfinance` for Yahoo Finance endpoint behavior and repair logic. It does not vendor or execute the Python package.

## Current baseline

- Swift package implementation: **0.2.0-dev.1**
- Upstream compatibility target: **yfinance 1.6.0**
- Upstream commit: `0af231f6a47eee5e773290830d228de0c20d5ee1`
- Upstream date: **2026-08-13**
- Review date: **2026-08-19**

## Ported from the 1.3-1.6 cycle

- Yahoo cookie/crumb session handling for query1/query2 traffic, including basic and CSRF/consent strategies.
- No independently persisted crumb reuse across unrelated cookie sessions.
- ETF screener support and updated equity screener fields/values.
- Auth/subscription-cookie support.
- Valuation measures via fundamentals-timeseries instead of HTML scraping.
- GBp/ZAc/ILA repair restores caller-visible quote units after internal major-unit repair.
- Full fundamentals-timeseries financial statement key sets, including `FixedMaturityInvestments` and `EquityInvestments`.
- Long fundamentals request fallback to 60-key chunks. Swift additionally tolerates individual empty chunks when other chunks contain valid data and refuses to fan out chunk requests on Yahoo rate-limit/auth backpressure.
- Structured failure classification for rate limits, authorization failures, server failures, transport failures, malformed data, and Yahoo API errors.
- Price-repair architecture covering reconstruction, 100x unit anomalies, unit switches, bad splits, corporate-action adjustment repair, OHLC normalization, finer-interval reconstruction, and repair-depth limiting.
- Read-only history-integrity diagnostics for malformed OHLC, duplicate/non-monotonic timestamps, negative volume, non-finite values, and classic 100x unit jumps that survive processing.
- Lazy history metadata parity from upstream PR #2922: recent typed chart/history decode seeds core symbol metadata; `5d/1h` `tradingPeriods` enrichment is explicit, cached, and non-fatal on failure.

## Swift hardening beyond direct parity

- `YFRequestCoordinator` centralizes bounded concurrency, transient retry with jitter, shared 429 cooldown, and diagnostics.
- `YFResilientClient` provides single-flight quote/history/metadata/financial/info requests plus stale-while-revalidate quote/history caches.
- `YFClock` and `YFJitterSource` make retry/cooldown behavior deterministic in offline tests.
- `YFHistorySeries.hardened()` adds conservative post-repair bounded-interior 100x correction and invalid high/low normalization.
- `safelyRepairedHistory(...)` provides a #2927-style safety valve: when the legacy engine marks a suspiciously large fraction of a table, Swift makes one raw comparison request and only replaces the result when the raw series proves a bounded interior 100x/0.01x block.
- Start/end resilient history trims corporate actions to the exact half-open request window.
- Explicit Yahoo UTC and exchange-timezone date helpers avoid device-local timezone interpretation.
- JSON non-finite values and unsafe integer conversions fail safely rather than reaching trapping conversions.
- `YFTickers.infoResult(...)` provides bounded best-effort multi-symbol metadata with isolated per-symbol failures.
- Session regression tests cover DNS-blocked `fc.yahoo.com`, concurrent crumb callers, and 200-OK HTML/garbage crumb bodies.

## August 2026 upstream PR review

### PR #2922: lazy `tradingPeriods` history metadata

**Ported.** Typed chart/history responses seed a short-lived core metadata snapshot, so `history()` followed by core metadata access does not trigger a second chart fetch. The `5d/1h` request is reserved for explicit `tradingPeriods` enrichment, cached after success, and enrichment failure leaves core metadata usable.

### PR #2927: genuine partial 100x blocks / unit-switch revert

**Relevant.** The Swift legacy unit-switch engine also models a single breakpoint and can scale an entire prefix/suffix. We did not copy the Python implementation into the giant client remotely. Instead:

- bounded interior-block detection was added with offline tests
- a heavily-repaired-result/raw-series comparison safety valve was added
- a future local/surgical core edit can move the invariant directly into the legacy engine after full Swift tests

### PR #2947: calendar event epoch local-timezone bug

**Behavior adopted, Python implementation not applicable.** Swift generally retains Yahoo JSON rather than calling Python's naive `datetime.fromtimestamp()`. Explicit `YFYahooDateSemantics.utcDateString(...)` now documents/tests that Yahoo calendar epochs are UTC instants.

### PR #2948: custom-period window timezone/event bounds

**Partially applicable.** Swift callers pass `Date` instants rather than Python custom-period strings, so the local-time parser bug does not map directly. Exact start/end event trimming was added to resilient history so actions outside the requested half-open interval cannot leak into the result.

### PR #2899: redundant timezone request for range history

**Already structurally avoided.** Swift range history requests the requested chart directly and receives timezone metadata in that response. It does not need Python's pre-flight timezone lookup merely to parse range strings.

### PR #2883: null/empty calendar and SEC filing results

**Already structurally safer.** Swift's JSON accessors use optional chaining/default empty values on these public paths. Null/missing nested objects do not produce Python-style `TypeError`/`KeyError` crashes. Adversarial malformed-data tests cover the general invariant.

## Intentional Swift differences

- Swift returns native value/table models instead of pandas DataFrames.
- The URLSession transport cannot reproduce `curl_cffi`'s browser TLS/HTTP fingerprint impersonation. Cookie/crumb correctness, bounded request volume, and conservative backpressure handling are therefore more important in Swift.
- Swift does not implement Python's arbitrary custom-period string parser, so several pandas/datetime-only bugs do not map directly.
- Calendar endpoint values are kept as Yahoo JSON until callers format them, avoiding Python's implicit local-time conversion path.
- Automatic GitHub Actions and Dependabot are intentionally disabled; verification is manual/local before app pin or release changes.

## Known remaining gaps / deliberate follow-ups

- The core query1/query2 request layer currently invalidates/refetches the Yahoo crumb once after a failed authenticated request. That recovery is useful for 401/403/session failures, but it can also cause one alternate-session retry after a target HTTP 429 before the rate limit reaches the caller. A future surgical core edit should skip session refresh for 429, expose `Retry-After`, and treat target 429 strictly as backpressure.
- The #2927 protection currently lives at the resilient/safety layer. Moving the bounded-block invariant directly into the private legacy repair function still requires a locally compiled surgical core patch.
- Python yfinance 1.6 rewrites some Yahoo error text when a user asks for 30m data but the internal fetch uses 15m. Swift still surfaces the Yahoo description from the internal request without that user-facing annotation.
- Full strict-concurrency and SwiftPM/Xcode verification cannot be run in this remote environment because it cannot resolve GitHub. Use `bash tools/verify.sh` and `bash tools/strict-concurrency.sh` locally before tagging a stable YFinanceKit release or advancing `nommminal`.

## Upstream watch

High-signal upstream changes to networking, Yahoo schemas, price repair, fundamentals, calendar/timezone handling, quotes/options, or screeners should be reviewed against this file and ported deliberately rather than by copying Python implementation details blindly.
