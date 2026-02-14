# YFinanceKit Parity Report

- Generated: `2026-02-13T20:32:09.353592+00:00`
- Config: period=`1mo` interval=`1d` history_limit=30 earnings_limit=4 income_limit=4 income_freq=`yearly`
- Summary: pass=4 warn=0 fail=0 skip=2 score=100.0

## Symbol Status

| Symbol | Status | Quote | History | Earnings | Income |
|---|---:|---:|---:|---:|---:|
| AAPL | pass | pass | pass | pass | pass |
| MSFT | pass | pass | pass | pass | pass |
| NVDA | pass | pass | pass | pass | pass |
| TSLA | pass | pass | pass | pass | pass |
| VOO | skip | pass | pass | skip | skip |
| BTC-USD | skip | pass | pass | skip | skip |

## Details

### AAPL (`pass`)
- `quote`: **pass** - 0 fail-level, 0 warn-level differences
- `history`: **pass** - overlap=23 avg_close_rel_diff=0.0000
- `earnings_dates`: **pass** - overlap=4 avg_eps_rel_diff=0.0000
- `income_stmt`: **pass** - overlap=4 avg_stmt_rel_diff=0.0000

### MSFT (`pass`)
- `quote`: **pass** - 0 fail-level, 0 warn-level differences
- `history`: **pass** - overlap=23 avg_close_rel_diff=0.0000
- `earnings_dates`: **pass** - overlap=4 avg_eps_rel_diff=0.0000
- `income_stmt`: **pass** - overlap=4 avg_stmt_rel_diff=0.0000

### NVDA (`pass`)
- `quote`: **pass** - 0 fail-level, 0 warn-level differences
- `history`: **pass** - overlap=23 avg_close_rel_diff=0.0000
- `earnings_dates`: **pass** - overlap=4 avg_eps_rel_diff=0.0000
- `income_stmt`: **pass** - overlap=4 avg_stmt_rel_diff=0.0000

### TSLA (`pass`)
- `quote`: **pass** - 0 fail-level, 0 warn-level differences
- `history`: **pass** - overlap=23 avg_close_rel_diff=0.0000
- `earnings_dates`: **pass** - overlap=4 avg_eps_rel_diff=0.0000
- `income_stmt`: **pass** - overlap=4 avg_stmt_rel_diff=0.0010

### VOO (`skip`)
- Swift errors:
  - `{'error': 'Yahoo API error [Unauthorized]: Invalid Crumb', 'operation': 'earnings-dates'}`
  - `{'error': 'HTTP error 404', 'operation': 'income-stmt'}`
- `quote`: **pass** - 0 fail-level, 0 warn-level differences
- `history`: **pass** - overlap=23 avg_close_rel_diff=0.0000
- `earnings_dates`: **skip** - No earnings rows from either side
- `income_stmt`: **skip** - No income rows from either side

### BTC-USD (`skip`)
- Swift errors:
  - `{'error': 'Yahoo API error [Unauthorized]: Invalid Crumb', 'operation': 'earnings-dates'}`
  - `{'error': 'HTTP error 404', 'operation': 'income-stmt'}`
- `quote`: **pass** - 0 fail-level, 0 warn-level differences
- `history`: **pass** - overlap=30 avg_close_rel_diff=0.0000
- `earnings_dates`: **skip** - No earnings rows from either side
- `income_stmt`: **skip** - No income rows from either side
