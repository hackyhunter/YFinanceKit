# AGENTS.md

## Mission

`YFinanceKit` is the canonical Swift implementation of Yahoo Finance behavior for the `nommminal` app and other Swift consumers. Python `ranaroussi/yfinance` is an upstream behavioral oracle, not a source tree to mechanically translate.

The package currently targets upstream yfinance **1.6.0** at commit:

`0af231f6a47eee5e773290830d228de0c20d5ee1`

Keep Swift package versioning separate from upstream compatibility. See `YFinanceKitBuildInfo`.

## Hard rules

1. **Do not re-enable automatic GitHub Actions or Dependabot.** Workflows are intentionally manual-only.
2. **Do not replace or substantially rewrite `YFinanceClient.swift` wholesale.** It contains a large, delicate repair engine. Prefer additive services/extensions and surgical changes with focused regression tests.
3. **Do not blindly port Python implementation details.** Port behavior that is relevant to Swift/Yahoo semantics. Skip pandas, packaging, and Python-only machinery.
4. **New reliability fixes need offline regression tests.** Yahoo/network tests are useful for smoke testing but are not a substitute for deterministic fixtures/mocks.
5. **429 is not a normal retry.** Do not aggressively retry a Yahoo rate limit. Use the shared cooldown/request coordinator.
6. **Never persist/reuse a crumb independently of its Yahoo cookie session.** Cookie and crumb state are coupled.
7. **Do not log cookies, crumbs, Yahoo login cookies, authorization data, or unredacted sensitive query parameters.**
8. **Malformed Yahoo data must fail safely.** Return structured errors, nil/empty best-effort values where appropriate, or integrity failures. Do not crash on null/missing/type-shifted fields.
9. **Preserve the caller-visible quotation unit after repair.** GBp/ZAc/ILA repair may use major currency internally but must restore original units before returning data.
10. **Keep `nommminal` pins deliberate.** Do not make the app float on `main`. Update its YFinanceKit revision only after a local Swift/Xcode verification pass.

## Architecture

### Stable facade

`YFinanceClient` remains the compatibility/public facade for existing callers.

### Session/auth

- `YFinanceCrumbStore.swift`: Yahoo basic + CSRF/consent cookie strategies.
- `YFinanceAuth.swift`: Yahoo login/subscription support.

### Resilience layer

Prefer `YFResilientClient` for high-volume app code.

It provides:

- bounded concurrency
- single-flight request coalescing
- exponential transient retry with jitter
- shared stateful 429 cooldown
- stale-while-revalidate quote/history caches
- request/cache/coalescing diagnostics
- injectable clock and jitter sources for deterministic tests

Use `hardenedHistory(...)` / `hardenedHistoryCached(...)` when price repair is desired.

### Data hardening

- `YFinanceHistoryIntegrity.swift`: audit structural chart problems.
- `YFinanceHistoryHardening.swift`: conservative bounded 100x block repair, OHLC normalization, exact custom-window event trimming, deterministic UTC/exchange date semantics.
- `YFinanceFinancials.swift`: fundamentals-timeseries statements with reactive 60-key chunk fallback.
- `YFinanceErrorClassification.swift`: structured failure categories.

### Bulk access

`YFTickers.infoResult(...)` uses bounded concurrency and isolates per-symbol failures. One bad symbol must not discard good results.

## Upstream changes worth evaluating first

When yfinance changes, prioritize:

1. Yahoo cookie/crumb/session behavior
2. rate-limit/network behavior
3. chart/history semantics
4. price/dividend/split/currency repair
5. fundamentals-timeseries
6. quote/info/schema null handling
7. calendar/timezone semantics
8. options/search/screener schema changes

Do not spend time porting Python packaging, pandas implementation choices, CI, or dependency churn unless they reveal a Yahoo behavior change.

For open upstream PRs, port the regression case before borrowing the implementation. An open PR is evidence, not truth.

## Testing

From the repo root:

```bash
bash tools/verify.sh
```

For the stricter concurrency audit:

```bash
bash tools/strict-concurrency.sh
```

High-value offline suites include session/crumb, financial chunk fallback, error classification, history integrity, metadata fallback, repair parity, resilience coordination, single-flight/cache behavior, multi-info partial failure, and adversarial malformed-data cases.

## App integration

`hackyhunter/nommminal` is the primary real consumer. Its native bridge has an established JSON contract. Preserve that contract unless the JS/native schema version is deliberately bumped on both sides.

The app should progressively move networking behavior into YFinanceKit rather than maintaining a second retry/rate-limit engine. App-only concerns such as presentation formatting, bridge payload shape, App Group persistence, widget timelines, and UI cancellation remain in `nommminal`.
