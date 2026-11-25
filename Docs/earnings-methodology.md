# EPS methodology selection

Port of ranaroussi/yfinance#2999.

```swift
let ticker = YFTicker("GOOGL")
let gaap = try await ticker.earningsEstimate(methodology: "gaap")
let normalized = try await ticker.earningsHistory(methodology: "normalized")
let yahooDefault = try await ticker.epsTrend(methodology: nil)
let revisions = try await ticker.get_eps_revisions(methodology: "GAAP")
```

All four getters accept `gaap`, `normalized`, or explicit `nil`. Matching is case-insensitive. Unsupported values fail before networking. The requested module is respectively `earningsTrend[Gaap|NonGaap]` or `earningsHistory[Gaap|NonGaap]`; an explicit selection takes precedence over Yahoo's sometimes misleading `defaultMethodology` label.

New overloads annotate nonempty rows with `methodology` when known. Periodic rows retain the existing raw-value unwrapping and four-period limit, and expose Yahoo's currency when supplied. History rows retain Yahoo's raw field wrappers, consistent with YFinanceKit's existing history getter.

Existing no-argument methods remain unchanged, including their returned schema. Pass `methodology: nil` to opt into the new default-methodology annotation. Table, camel-case getter, and Python-style JSON getter overloads are included. This port does not add the Python implementation's analysis-result cache or change the exported quote-summary module enum; the new getters use explicit module names through the existing string-based endpoint.

## Verification

Run `swift test --filter YFinanceEarningsMethodologyTests` and the repository's complete documented hardening-candidate verification on macOS before merging. The offline tests exercise module selection, different methodology values, label precedence, invalid input, and missing data. No live Yahoo traffic is necessary.
