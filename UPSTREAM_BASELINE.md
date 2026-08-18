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
- Long fundamentals request fallback to 60-key chunks. Swift additionally tolerates individual empty chunks when other chunks contain valid data.
- Structured failure classification for rate limits, authorization failures, server failures, transport failures, malformed data, and Yahoo API errors.
- Price-repair architecture covering reconstruction, 100x unit anomalies, unit switches, bad splits, corporate-action adjustment repair, OHLC normalization, finer-interval reconstruction, and repair-depth limiting.

## Intentional Swift differences

- Swift returns native value/table models instead of pandas DataFrames.
- The URLSession transport cannot reproduce `curl_cffi`'s browser TLS/HTTP fingerprint impersonation. Cookie/crumb correctness and conservative request behavior are therefore more important in Swift.
- Swift does not implement Python's arbitrary custom-period string parser, so upstream custom-period timezone bugs do not map directly.
- Calendar endpoint values are kept as Yahoo JSON until callers format them, so Python's local-time `datetime.fromtimestamp()` calendar bug does not map directly.
- Automatic GitHub Actions and Dependabot are intentionally disabled for this repository; verification is expected to be run manually/local before releases.

## Upstream watch

High-signal upstream changes to networking, Yahoo schemas, price repair, fundamentals, calendar/timezone handling, quotes/options, or screeners should be reviewed against this file and ported deliberately rather than by copying Python implementation details blindly.
