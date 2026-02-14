# Parity Status (Python yfinance -> Swift YFinanceKit)

## Exposed surfaces

- `Ticker`: mostly covered by endpoint-level methods
- `Tickers` + `download`: covered
- `Search`, `Lookup`: covered
- `Market`, `Sector`, `Industry`: covered
- `Screener` and query objects: covered
- `screen(...)` has additional Python-signature compatibility overloads (`size`, `count`, `sortAsc`, snake-case quote type label)
- `screen(queryId:, offset: ...)` now mirrors Python behavior by switching from the predefined endpoint (which ignores `offset`) to the regular screener endpoint using the predefined query definition
- `Calendars`: covered
- Python-style top-level constants are exposed (`LOOKUP_TYPES`, `PREDEFINED_SCREENER_QUERIES`, `PREDEFINED_SCREENER_QUERY_IDS`, `PREDEFINED_SCREENER_BODY_DEFAULTS`, `PREDEFINED_CALENDARS`, `PREDEFINED_CALENDAR_IDS`, `QUOTE_SUMMARY_VALID_MODULES`)
- Module metadata aliases are exposed (`version`, `__version__`, `__author__`) alongside `CalendarQuery`/`EquityQuery`/`FundQuery` type aliases
- `WebSocket` + `AsyncWebSocket`: covered at transport level
- History adjustments/events: `autoAdjust`/`backAdjust` and corporate action parsing are included
- History options: `actions`, `keepNa`, `rounding`, string period/interval overloads, and threaded multi-ticker fetch
- `Ticker.history` includes Python-style snake_case signature compatibility (`auto_adjust`, `back_adjust`, `keepna`, `timeout`)
- `Ticker` supports MIC code tuple construction (`try Ticker(("OR", "XPAR"))`) similar to Python `(symbol, MIC)` support
- `download` parity includes both `period` and `start/end` signatures
- Python-style `download(...)` snake_case signature is available (`group_by`, `auto_adjust`, `back_adjust`, `keepna`, `ignore_tz`, `multi_level_index`)
- `download` now accepts mixed compatibility signatures where `period` is typed (`YFinanceClient.Range`) and `interval` is Python-style string (e.g. `"1d"`)
- `Tickers.history`/`Tickers.download` also include Python-style snake_case signature overloads returning table-shaped output
- `Tickers.history`/`Tickers.download` now default `actions=true` like Python (`yf.download` remains `actions=false`)
- `Tickers.tickers` mapping is exposed (`[String: YFTicker]`)
- Yahoo `30m` quirk handling is included (`15m` fetch + resample)
- Multi-symbol table shaping is available (`historyTable`/`downloadTable`) with compatibility controls for `group_by`, `ignore_tz`, and `multi_level_index`
- `download_table(...)` now includes snake_case `multi_level_index` compatibility overloads for string-interval start/end forms
- Table helpers: `YFTable`/`YFIndexedTable` with dataframe-like operations (`head`, `tail`, `select`, `drop`, `sorted`, `index`, `transposed`)
- Corporate action tables are available (`dividendsTable`, `splitsTable`, `capitalGainsTable`, `actionsTable`) and now derive from daily history (Python-like) with non-zero filtering; `Capital Gains` is only included when expected (mutual funds/ETFs)
- History repair tables include Python's `Repaired?` column when `repair=true` (and resampling propagates it via `any` semantics)
- Price reconstruction fetches finer-grained data with `repair=true` and limits reconstruction depth to 2 (Python-like)
- Financial statement table helpers are available (`earningsTable`, `incomeStmtTable`, `balanceSheetTable`, `cashFlowTable`, `financialsTable`)
- Option-chain table helpers are available (`YFOptionsChain.callsTable()` / `putsTable()`) with Python `_options2df` column layout
- `FundsData` parity model is exposed as typed `YFFundsData` via `Ticker.fundsData()` (`fundsDataRaw()` retained for raw JSON)
- Additional Python financial alias coverage is included (`quarterly_*`, `ttm_*`, `incomestmt`/`balancesheet`/`cashflow`, plus `get_*` variants with `as_dict`/`pretty` argument compatibility)
- Financial frequencies support both typed enums and Python-style string values (`yearly`, `quarterly`, `trailing`, `ttm`)
- Option-chain APIs include Python-style `date`/`tz` call compatibility and invalid-date validation against available expirations
- `earnings_dates` retrieval now attempts Yahoo calendar scraping first and falls back to visualization payloads
- Stream semantics: `quoteType` and `marketHours` enum mappings now include Yahoo's currently published protobuf enum values (unknown codes are preserved as `unknown(...)`)
- `live(...)` APIs now support optional handlers (matching Python `message_handler=None` behavior)
- `Ticker.live(...)` / `Tickers.live(...)` convenience methods are included for Python migration ergonomics (they return a `WebSocket` instance)
- Python-style config aliases: `set_config`, `set_tz_cache_location`, `enable_debug_mode`
- Python-style snake_case method aliases for `Ticker`/`Tickers` are included for migration ergonomics (including table-oriented `get_history_table`/`get_download_table` and top-level `download_table`)
- Python property-name callability wrappers on `Ticker` are available (`major_holders`, `fast_info`, `earnings_dates`, `history_metadata`, etc.)
- `Ticker.historyMetadata()` now requests `5d`/`1h` so Yahoo includes `tradingPeriods` in the returned metadata (Python-like)
- `Search` includes Python-style constructor labels (`max_results`, `include_cb`, `enable_fuzzy_query`, `recommended`, `raise_errors`) and raw-response accessors (`response`, `get_response`)
- `Search`/`Lookup` now honor `timeout` (URLRequest timeout) and `raise_errors`/`raiseErrors` (suppresses errors and returns empty results when `false`)
- `Lookup`/`Market`/`Sector`/`Industry`/`Calendars` now expose additional snake_case aliases for Python migration ergonomics
- `Lookup` also exposes typed table helpers per lookup type (`allTable`, `stockTable`, ... and snake_case table aliases)
- `Sector`/`Industry` table helpers now mirror Python parsed column conventions for industry/company summaries
- Calendars now cache visualization payloads per calendar type and request body; `force=true` bypasses the local cache (Python-like)
- Python `utils.py` ISIN helpers are available (`is_isin`, `get_all_by_isin`, `get_ticker_by_isin`, `get_info_by_isin`, `get_news_by_isin`)
- Persistent caches are supported under `set_tz_cache_location(...)` / `set_cache_location(...)` (timezone cache, ISIN->ticker cache, Yahoo crumb cache)
- Proxy config is supported via `set_config(proxy: ...)` when you construct clients with `await YF.client()` / `await YFinanceClient.configured()`

## Remaining non-parity items

- DataFrame/Series formatting and transformations from pandas are not identical.
- Some legacy/deprecated Python methods are represented as raw endpoint calls rather than identical table outputs.
- Price-repair support is improved (100x unit mixups incl. sudden switches + OHLC normalization + missing/wrong split adjustment repair + corporate-action adjusted-close repairs + live-row merge + best-effort reconstruction of missing/zero OHLC/volume via finer intervals, including split-event-driven reconstruction triggers). Major repair stages are covered, including interday per-column split handling with suspension-aware range splitting.
- `earnings_dates` behavior can still vary with Yahoo HTML/schema changes across regions; scrape fallback is best-effort.

## Practical interpretation

Endpoint coverage is broad and app-usable, but exact output shape parity with Python pandas objects is still incomplete.

## Validation workflow

- Use `tools/parity_harness.py` to run repeatable Swift-vs-Python comparisons and produce:
  - `artifacts/parity_report.json`
  - `artifacts/parity_report.md`
- The harness compares normalized `quote`, `history`, `earnings_dates`, and `income_stmt` payloads for a symbol matrix and reports pass/warn/fail per symbol.
