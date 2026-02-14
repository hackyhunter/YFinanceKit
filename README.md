# YFinanceKit (Swift/iOS port)

Native Swift package for Yahoo Finance endpoints, designed as a migration path from Python `yfinance` to iOS/macOS.

## What is implemented

- `Ticker` / `YFTicker`
  - `quote`, `history`, `historyMetadata`
  - optional MIC tuple construction for international symbols: `try Ticker(("OR", "XPAR"))` or `try Ticker(symbol: "OR", mic: "XPAR")`
  - history includes parsed events + optional `autoAdjust` / `backAdjust` / `repair` / `keepNa` / `rounding`
  - history supports both typed enums and Python-style string args (e.g. `"1mo"`, `"1d"`)
  - supports both `period` and `start/end` fetch style
  - Python-signature `history(...)` overload is available with snake_case labels (`auto_adjust`, `back_adjust`, `keepna`, `timeout`)
  - handles Yahoo `30m` fetch quirk via internal `15m` fetch + resample
  - table outputs for corporate actions: `dividendsTable`, `splitsTable`, `capitalGainsTable`, `actionsTable` (Python-style `Dividends` / `Stock Splits` / `Capital Gains` columns)
  - camelCase and snake_case alias methods (`getInfo` + `get_info`, etc.)
  - Python property-name callability wrappers are included (`major_holders`, `fast_info`, `earnings_dates`, `history_metadata`, etc.)
  - `YFHistorySeries` provides `barsTable()` and `eventsTable()`
  - `optionChain`, `options`
  - `YFOptionsChain` includes `callsTable()` / `putsTable()` table helpers (Python `_options2df` shape)
  - `optionChain(expirationDate:)` validates requested date against available expirations (Python-like error behavior)
  - `optionChain(date:tz:)` / `option_chain(..., tz:)` compatibility overloads
  - `info`, `fastInfo`
  - quote-summary module access (`recommendations`, `calendar`, `secFilings`, holder/insider methods, sustainability, analyst/earnings trend methods)
  - statement/history methods (`incomeStmt`, `balanceSheet`, `cashFlow`, `financials`)
  - statement/history methods accept both typed frequency enums and Python-style strings (`"yearly"`, `"quarterly"`, `"trailing"`, `"ttm"`)
  - statement table helpers (`earningsTable`, `incomeStmtTable`, `balanceSheetTable`, `cashFlowTable`, `financialsTable`)
  - `news`, `earningsDates`, `fundsData`, `fundsDataRaw`, `isin`
  - `earningsDates` now prefers Yahoo calendar scraping and falls back to visualization payloads when scrape data is unavailable
  - `fundsData` now returns typed `YFFundsData` (Python `FundsData` style sections + tables)
  - Python financial convenience aliases: `quarterly_*`, `ttm_*`, `incomestmt`/`balancesheet`/`cashflow`, plus `get_*` variants
- `Tickers` / `YFTickers`
  - multi-symbol `quote`, `history`, `download`, `news`
  - matches Python defaults for `Tickers.history`/`download` (`actions: true`)
  - exposes `tickers` map (`[String: YFTicker]`) like Python `Tickers.tickers`
  - threaded parallel fetch option (`threads: Bool`)
  - supports `period` and `start/end` for multi-symbol history/download
  - Python-signature `history`/`download` method overloads with snake_case labels (`group_by`, `auto_adjust`, `keepna`, `ignore_tz`, `multi_level_index`)
  - multi-symbol table outputs via `historyTable` / `downloadTable` with `groupBy`, `ignoreTZ`, and `multiLevelIndex` compatibility options
  - snake_case aliases (`get_history`, `get_download`, `get_history_table`, `get_download_table`, top-level `download_table`)
- `download` equivalent
  - `yfDownload(...)` and `YF.download(...)`
  - Python-signature compatibility overloads using snake_case labels (`group_by`, `auto_adjust`, `keepna`, `ignore_tz`, `multi_level_index`)
  - mixed compatibility signatures are supported (`period: YFinanceClient.Range` with `interval: "1d"` style strings)
- `Search`
  - `YFSearch` object plus direct `client.search(...)`
  - Python-style init labels (`max_results`, `include_cb`, `enable_fuzzy_query`, `recommended`, `raise_errors`) and `response`/`get_response` accessors
- `Lookup`
  - `YFLookup` and typed lookup categories
  - snake_case getter aliases (`get_all`, `get_stock`, `get_cryptocurrency`, etc.)
  - table outputs for lookup types (`allTable`, `stockTable`, ... + snake_case table aliases)
- `Market`
  - market summary + market time/status endpoints
  - snake_case getter aliases (`get_summary`, `get_status`)
- `Sector` / `Industry`
  - domain endpoints
  - snake_case aliases for domain getters (`top_etfs`, `top_growth_companies`, `get_top_*`, `sector_key`, etc.)
  - table helpers follow Python-style parsed columns for industries/top-companies summaries
- `Screener`
  - predefined and custom query execution
  - snake_case-compatible `screen(...)` overloads support `size`/`count` and `sortAsc` naming
  - exported `PREDEFINED_SCREENER_QUERIES` mapping, `PREDEFINED_SCREENER_QUERY_IDS` list, and `PREDEFINED_SCREENER_BODY_DEFAULTS`
  - query fields are validated when `quoteType` is provided (mirrors Python `EquityQuery`/`FundQuery` validation intent)
- `Calendars`
  - earnings / IPO / economic events / splits via visualization API
  - exported `PREDEFINED_CALENDARS` mapping and `PREDEFINED_CALENDAR_IDS` list constants
  - snake_case getter compatibility includes optional `force` argument (bypasses local cache, Python-like)
- Python `utils.py` ISIN helpers
  - `is_isin`, `get_all_by_isin`, `get_ticker_by_isin`, `get_info_by_isin`, `get_news_by_isin`
  - `try await Ticker(isin: "...")` convenience initializer
- Live stream
  - `WebSocket` + `AsyncWebSocket` wrappers around Yahoo stream endpoint
  - base64 protobuf payload is decoded into `YFPricingData`
  - semantic stream mappings for `quoteType` and `marketHours` cover Yahoo's currently published protobuf enum set
- Config surface
  - `YF.setConfig(...)`, `YF.enableDebugMode(...)`, `YF.setTZCacheLocation(...)`
  - Python-style aliases: `set_config(...)`, `set_tz_cache_location(...)`, `enable_debug_mode(...)`
  - proxy support: set via `set_config(proxy: ...)` and construct a configured client with `await YF.client()` / `await YFinanceClient.configured()`
  - persistent caches use `config.cacheDirectory` (set via `set_tz_cache_location(...)` / `set_cache_location(...)`) for timezone/ISIN/crumb storage
  - module constants/aliases include `version`, `__version__`, `__author__`, and `CalendarQuery`
- DataFrame-like table helpers
  - `YFTable` now supports `head`, `tail`, `select`, `drop`, `sorted`, `filtered`, `index(by:)`, `transposed()`

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
_ = customResult

let ws = YF.asyncWebSocket()
try await ws.subscribe("AAPL")
let stream = await ws.messages()
for try await message in stream {
    let livePrice = message.pricingData?.price
    _ = livePrice
}
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

## Important parity gaps

This package now mirrors most endpoint coverage, but it is **not yet a byte-for-byte behavioral clone** of Python `yfinance`.

Main differences:

- No pandas DataFrame/Series outputs; returns typed Swift models or raw JSON values.
- Price repair is largely ported: 100x unit mixups (sporadic + sudden switches), OHLC normalization, missing/wrong split adjustment repair, corporate-action adjusted-close repairs, Yahoo "live row split" merge fix, and a best-effort reconstruction step for missing/zero OHLC/volume via finer intervals (with Yahoo lookback limits). Full Python edge-case parity is still incomplete.
- `earnings_dates` now has scrape + visualization fallback, but Yahoo page/schema shifts can still affect exact output parity.
- DataFrame-specific formatting/index semantics are represented as `YFTable`/`YFIndexedTable` utilities, not pandas.
- Not all Python convenience aliases/properties are represented as Swift properties; most are methods.

For iOS app integration this is production-usable, but if you require exact pandas-level parity you should treat this as an ongoing migration layer.

## Parity Harness

Use the included harness to compare Swift output against Python `yfinance` for a symbol matrix:

```bash
cd /Users/mine/GitHub/yfinance-swift/swift/YFinanceKit
python3 tools/parity_harness.py --symbols AAPL,MSFT,NVDA,TSLA,VOO,BTC-USD
```

Outputs:

- `artifacts/parity_report.json`
- `artifacts/parity_report.md`

The harness runs `swift run YFParityCLI snapshot ...` for normalized Swift payloads and compares them against normalized Python payloads (quote/history/earnings_dates/income_stmt) with tolerance-based checks.
