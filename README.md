# YFinanceKit

Native Swift implementation of Yahoo Finance behavior for iOS/macOS, designed as a migration path from Python `yfinance` while remaining a Swift-native library rather than a mechanical Python translation.

## Build identity

YFinanceKit tracks two versions independently:

- Swift package implementation: `0.2.0-dev.1`
- upstream yfinance compatibility target: `1.6.0`
- audited upstream commit: `0af231f6a47eee5e773290830d228de0c20d5ee1`
- audited upstream date: `2026-08-13`

These are exposed through `YFinanceKitBuildInfo` plus compatibility constants such as `__version__`.

The current `0.2.0-dev.1` snapshot has substantial new resilience/hardening work and should be locally built/tested before being treated as a stable release or used to advance an app pin.

## What is implemented

- `Ticker` / `YFTicker`
  - `quote`, `history`, `historyMetadata`
  - optional MIC tuple construction for international symbols: `try Ticker(("OR", "XPAR"))` or `try Ticker(symbol: "OR", mic: "XPAR")`
  - history includes parsed events + optional `autoAdjust` / `backAdjust` / `repair` / `keepNa` / `rounding`
  - history supports both typed enums and Python-style string args such as `"1mo"` and `"1d"`
  - supports both `period` and `start/end` fetch style
  - Python-signature `history(...)` overload with snake_case labels (`auto_adjust`, `back_adjust`, `keepna`, `timeout`)
  - handles Yahoo `30m` fetch quirk via internal `15m` fetch + resample
  - table outputs for corporate actions: `dividendsTable`, `splitsTable`, `capitalGainsTable`, `actionsTable`
  - camelCase and snake_case alias methods (`getInfo` + `get_info`, etc.)
  - Python property-name callability wrappers (`major_holders`, `fast_info`, `earnings_dates`, `history_metadata`, etc.)
  - `YFHistorySeries` provides `barsTable()` and `eventsTable()`
  - `optionChain`, `options`
  - `YFOptionsChain.callsTable()` / `putsTable()` table helpers
  - option expiration-date validation
  - `info`, `fastInfo`
  - quote-summary access for recommendations, calendar, SEC filings, holders/insiders, sustainability, and analyst/earnings trend data
  - `incomeStmt`, `balanceSheet`, `cashFlow`, `financials`
  - typed frequency enums plus Python-style strings (`yearly`, `quarterly`, `trailing`, `ttm`)
  - statement table helpers
  - `news`, `earningsDates`, `fundsData`, `fundsDataRaw`, `isin`
  - `earningsDates` prefers Yahoo calendar scraping with visualization fallback
  - typed `YFFundsData`
  - Python financial aliases (`quarterly_*`, `ttm_*`, `incomestmt`/`balancesheet`/`cashflow`, plus `get_*` variants)
- `Tickers` / `YFTickers`
  - multi-symbol `quote`, `history`, `download`, `news`
  - Python-like defaults for `Tickers.history`/`download`
  - `tickers` map (`[String: YFTicker]`)
  - optional threaded parallel fetch
  - period and start/end history/download
  - Python-signature snake_case overloads
  - multi-symbol table outputs via `historyTable` / `downloadTable`
  - bounded best-effort `infoResult(maxConcurrentRequests:)` and `getInfo(...)`
- `download` equivalent
  - `yfDownload(...)` and `YF.download(...)`
  - Python-signature snake_case compatibility
- `Search`
  - `YFSearch` plus direct `client.search(...)`
  - Python-style init labels and raw-response accessors
- `Lookup`
  - `YFLookup` and typed lookup categories
  - snake_case getters and table outputs
- `Market`
  - market summary + market time/status endpoints
- `Sector` / `Industry`
  - domain endpoints and parsed table helpers
- `Screener`
  - predefined/custom queries
  - equity, fund, and ETF query validation
  - exported predefined constants
- `Calendars`
  - earnings / IPO / economic events / splits via visualization API
  - cached payloads with force refresh
- ISIN helpers
- Live stream
  - `WebSocket` + `AsyncWebSocket`
  - base64 protobuf payload decode into `YFPricingData`
- Config
  - proxy, retry, debug, cache-directory configuration
  - persistent timezone/ISIN cache support
- Table helpers
  - `YFTable` / `YFIndexedTable`
  - `head`, `tail`, `select`, `drop`, `sorted`, `filtered`, `index(by:)`, `transposed()`

## Yahoo session hardening

The core client uses a shared Yahoo cookie/crumb session for query1/query2 traffic, including chart/history.

The crumb/session layer includes:

- basic Yahoo cookie bootstrap
- CSRF/consent fallback strategy
- no independent reuse of a stale persisted crumb from an unrelated cookie session
- crumb HTTP-status validation
- rejection of HTML, JSON error bodies, rate-limit bodies, and other implausible crumbs
- explicit 429 classification
- fallback when `fc.yahoo.com` is DNS-blocked/unreachable
- actor serialization so concurrent crumb callers share one bootstrap

URLSession cannot reproduce `curl_cffi` browser TLS/HTTP fingerprint impersonation, so request volume and session correctness are treated conservatively.

## Resilient client

For high-volume/app-facing work, prefer `YFResilientClient` over independently layering retries around `YFinanceClient`.

```swift
import YFinanceKit

let rawClient = YFinanceClient()
let client = YFResilientClient(client: rawClient)

let quote = try await client.quoteCached(
    symbol: "AAPL",
    freshFor: 15,
    staleFor: 300
)

let history = try await client.hardenedHistoryCached(
    symbol: "AAPL",
    range: .oneMonth,
    interval: .oneDay,
    freshFor: 60,
    staleFor: 86_400,
    repair: true
)

let diagnostics = await client.diagnostics()
_ = (quote, history, diagnostics)
```

The resilience layer provides:

- bounded Yahoo concurrency
- single-flight/coalesced identical quote/history/metadata/financial/info work
- exponential retry + jitter for transport/5xx-style transient errors
- no resilience-layer retry for final 429 responses
- shared stateful rate-limit cooldown
- fresh/stale/expired in-memory quote/history caches
- request/cache/coalescing diagnostics
- injectable clock/jitter sources for deterministic tests

### Known core rate-limit limitation

The legacy query1/query2 request path may still perform one Yahoo cookie-strategy/crumb refresh after a target request fails, including a target 429, before the final 429 reaches `YFRequestCoordinator`.

The resilience layer never adds another retry after that final 429. A future local surgical core change should skip session refresh for target 429 responses and expose `Retry-After` response headers.

## Financial statements

For new code, prefer the typed fundamentals-timeseries implementation:

```swift
let statement = try await client.financialStatement(
    symbol: "AAPL",
    kind: .income,
    frequency: .yearly
)
```

The implementation:

1. attempts one fundamentals-timeseries request
2. validates empty/error payloads
3. reactively falls back to 60-key chunks when a long URL fails
4. merges chunks into typed statement data
5. does not fan out chunk requests for auth/rate-limit backpressure that shorter URLs cannot fix

## History integrity and repair safety

`YFHistorySeries` includes structural auditing:

```swift
let report = history.value.integrityReport()
if !report.isValid {
    // reject structurally corrupt chart data
}
```

Checks include duplicate/non-monotonic timestamps, invalid OHLC, negative volume, non-finite values, and classic unexplained 100x/0.01x jumps.

Additional conservative hardening APIs include:

- `hardened()`
- `repairingInteriorUnitScaleBlocks()`
- `normalizingInvalidOHLC()`
- `trimmingEvents(to:)`
- `safelyRepairedHistory(...)`

The bounded 100x repair only corrects a strongly evidenced interior block with paired inverse scale transitions. It deliberately does not guess at a lone edge unit switch.

`safelyRepairedHistory(...)` addresses the class of upstream issue described by yfinance PR #2927: if the legacy repair engine marks a suspiciously large fraction of a table, Swift makes one raw comparison request and only replaces the repaired result if raw history proves a bounded interior bad-unit block.

## Quick start

```swift
import YFinanceKit

let client = await YF.client()
let ticker = client.ticker("AAPL")

let quote = try await ticker.quote()
let history = try await ticker.history(period: .oneMonth, interval: .oneDay)
let info = try await ticker.info()

let screen = YFScreener(client: client)
let mostActives = try await screen.predefined(.mostActives, count: 25)

let custom = YFQueryBuilder.and([
    YFQueryBuilder.gt("percentchange", 3),
    YFQueryBuilder.eq("region", .string("us")),
])
let customResult = try await screen.run(query: custom, quoteType: .equity)
_ = (quote, history, info, mostActives, customResult)
```

## Python -> Swift examples

- `yf.Ticker("AAPL").history(period="1mo", interval="1d")`
  - `try await client.ticker("AAPL").history(period: "1mo", interval: "1d")`
- `yf.download(["AAPL", "MSFT"])`
  - `try await YF.download(["AAPL", "MSFT"], threads: true)`
- `yf.download(["AAPL", "MSFT"], start="2024-01-01", end="2024-12-31")`
  - `try await YF.download(["AAPL", "MSFT"], start: startDate, end: endDate, interval: "1d")`
- `yf.Search("apple")`
  - `let search = YF.searchObject("apple"); let quotes = try await search.quotes()`
- `yf.screen("most_actives")`
  - `try await YFScreener().predefined(.mostActives)`

## Parity and intentional differences

YFinanceKit has broad endpoint coverage, but it is not a byte-for-byte clone of Python `yfinance`.

Main intentional differences:

- no pandas DataFrame/Series runtime dependency
- native Swift models/tables instead of pandas objects
- Python implementation details that only exist because of pandas/datetime/packaging are not copied unless they expose a Yahoo behavior requirement
- Swift uses URLSession rather than `curl_cffi`
- Python open PRs are treated as regression evidence, not blindly copied implementation
- repair behavior is tested against Swift-specific invariants and can intentionally differ when that is safer

See:

- `PARITY_STATUS.md`
- `UPSTREAM_BASELINE.md`
- `HARDENING.md`
- `AGENTS.md`

## Parity harness

From a checkout of this standalone repository:

```bash
cd /path/to/YFinanceKit
python3 tools/parity_harness.py --symbols AAPL,MSFT,NVDA,TSLA,VOO,BTC-USD
```

Outputs:

- `artifacts/parity_report.json`
- `artifacts/parity_report.md`

The harness compares normalized Swift and Python yfinance output for selected quote/history/earnings/financial surfaces with tolerance-based checks.

## Verification

Automatic GitHub Actions and Dependabot are intentionally disabled.

Run locally:

```bash
bash tools/verify.sh
bash tools/strict-concurrency.sh
```

The second command runs the package tests with complete Swift strict-concurrency checking enabled.

Do not tag a stable package release or advance a production app pin until both commands pass on a real checkout.
