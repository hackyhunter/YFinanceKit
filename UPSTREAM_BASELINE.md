# Upstream yfinance baseline

YFinanceKit is a native Swift implementation that tracks Python `ranaroussi/yfinance` for Yahoo Finance endpoint behavior and repair logic. It does not vendor or execute the Python package.

## Current baseline

- Upstream compatibility target: **yfinance 1.6.0**
- Upstream commit: `0af231f6a47eee5e773290830d228de0c20d5ee1`
- Upstream date: **2026-08-13**
- Review date: **2026-08-18**

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
- Resilient history metadata: try intraday enrichment for `tradingPeriods`, then fall back to daily metadata for otherwise-valid tickers.

## Intentional Swift differences

- Swift returns native value/table models instead of pandas DataFrames.
- The URLSession transport cannot reproduce `curl_cffi`'s browser TLS/HTTP fingerprint impersonation. Cookie/crumb correctness and conservative request behavior are therefore more important in Swift.
- Swift does not implement Python's arbitrary custom-period string parser, so upstream custom-period timezone bugs do not map directly.
- Calendar endpoint values are kept as Yahoo JSON until callers format them, so Python's local-time `datetime.fromtimestamp()` calendar bug does not map directly.
- Automatic GitHub Actions and Dependabot are intentionally disabled for this repository; verification is expected to be run manually/local before releases.

## Known remaining gaps / deliberate follow-ups

- The core query1/query2 request layer currently invalidates/refetches the Yahoo crumb once after a failed authenticated request. That recovery is useful for 401/403/session failures, but it can also cause one alternate-session retry after a target HTTP 429 before the rate limit reaches the caller. A future surgical core edit should skip session refresh for 429 and treat it strictly as backpressure.
- Python yfinance 1.6 rewrites some Yahoo error text when a user asks for 30m data but the internal fetch uses 15m. Swift still surfaces the Yahoo description from the internal request without that user-facing annotation.
- Open upstream repair PRs are not copied blindly. In particular, the interior 100x-block work should be adopted only after it merges or after an equivalent Swift regression fixture proves a real gap.
- Full Xcode/SwiftPM verification is intentionally not delegated to automatic Actions. The package contains offline regression tests, but the definitive app integration pass should be run locally in Xcode before changing `nommminal` to consume diagnostic APIs added after its current pinned commit.

## Upstream watch

High-signal upstream changes to networking, Yahoo schemas, price repair, fundamentals, calendar/timezone handling, quotes/options, or screeners should be reviewed against this file and ported deliberately rather than by copying Python implementation details blindly.
