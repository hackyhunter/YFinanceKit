# Parity artifacts

The `*.sample.*` files in this directory are example fixture/output shapes for the parity harness.

Generated `parity_report.json` / `parity_report.md` files are intentionally **not** kept here unless they were produced from the current locally verified YFinanceKit commit and the report records that exact commit/baseline.

The previous generated report was removed because it dated from 2026-02-13, predating the Yahoo session fix and the 1.6 hardening work.

To generate a fresh report from a real checkout:

```sh
python3 tools/parity_harness.py --symbols AAPL,MSFT,NVDA,TSLA,VOO,BTC-USD
```

Before committing generated reports, record or verify:

- YFinanceKit source commit
- YFinanceKit package version
- yfinance compatibility version/commit
- generation timestamp
- symbol/config matrix

A stale green parity report is worse than no report.
