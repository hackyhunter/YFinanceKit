# AGENTS.md

## Mission

`YFinanceKit` is the canonical Swift implementation of Yahoo Finance behavior for `nommminal` and other Swift consumers. Python `ranaroussi/yfinance` is an upstream behavioral oracle, not a source tree to mechanically translate.

The compatibility target is yfinance **1.7.0** at upstream commit:

`3d9d2f0cacb662bff689874cd6113bae3a30a885`

Keep the Swift package version separate from the upstream compatibility version. See `YFinanceKitBuildInfo`.

## Hard rules

1. **Do not re-enable automatic GitHub Actions or Dependabot.** Workflows are intentionally manual-only.
2. **Do not wholesale-rewrite `YFinanceClient.swift`.** It contains a large repair/compatibility engine. Prefer additive files and exact surgical migrations.
3. **Do not blindly port Python implementation details.** Port Yahoo behavior relevant to Swift.
4. **New reliability fixes need deterministic offline regression tests.**
5. **429 is provider backpressure, not a normal retry.** Never stack aggressive retries around it.
6. **Cookie + crumb state are coupled.** Never persist/reuse a crumb independently of its Yahoo cookie session.
7. **Never log cookies, crumbs, auth values, sensitive headers, response bodies, or unredacted sensitive query parameters.**
8. **Malformed Yahoo data must fail safely.** Null/missing/type-shifted data may yield structured errors or conservative best-effort output, never a crash.
9. **Preserve caller-visible quotation units after repair.** GBp/ZAc/ILA may be repaired in major units internally, then must be restored.
10. **Do not make `nommminal` float on `main`.** The app only advances to an exact locally verified YFinanceKit commit.

## Architecture direction

`YFinanceClient` remains the broad compatibility facade. New app-facing code should prefer the smaller layers around it:

- `YFinanceCrumbStore.swift`: Yahoo cookie/crumb strategies.
- `YFinanceTransport.swift`: isolated URLSession transport boundary.
- `YFRequestCoordinator`: global Yahoo concurrency, transient retry and cooldown.
- `YFRequestBudgetGate`: interactive / normal / background logical priority.
- `YFResilientClient`: single-flight, in-memory stale-while-revalidate, diagnostics.
- `YFinanceResilienceProtocols.swift` and `YFinanceNativeServices.swift`: narrow feature-facing protocols/services.
- `YFinanceHistoryIntegrity.swift` / `YFinanceHistoryHardening.swift`: structural audit and conservative repair.
- `YFinanceFinancials.swift`: fundamentals-timeseries statements with chunk fallback.
- `YFinanceErrorClassification.swift`: stable structured failure categories.

Avoid growing the giant client when a focused facade/extension can carry the behavior.

## Upstream changes worth evaluating first

Prioritize cookie/crumb/session changes, rate limiting, chart/history semantics, price/dividend/split/currency repair, fundamentals-timeseries, quote/info null/schema handling, timezone/calendar behavior, options/search/screener changes.

For open upstream PRs, port the regression case before borrowing implementation. Open PRs are evidence, not truth.

## Local pickup: remote hardening work is staged, not compile-verified

This is the important handoff point.

The remote work intentionally stopped short of applying the large exact migrations inside `YFinanceClient.swift` / `YFinanceResilience.swift`, because those edits should be followed immediately by a real Swift compile/test loop. Do **not** interpret current `main` as the final verified app candidate merely because the source and migration tooling are present.

Remote staging now includes:

- isolated `YFURLSessionTransport` + tests
- shared request coordinator, single-flight and SWR caches
- foreground/background request budgeting
- redacted diagnostics + per-endpoint rollups
- `Retry-After` parsing and structured metadata
- cross-market parity tooling
- adversarial schema tests plus deterministic 96-seed Yahoo JSON mutation fuzzing
- focused quote/history/metadata/financial service facades
- exact migration scripts for the remaining giant-client changes
- static source-state gates
- warnings-as-errors complete-concurrency gate
- small live-parity gate

Static review also found two subtle coordinator races that are deliberately handled by the final local migration chain:

1. a task cancelled while queued for the global Yahoo permit could otherwise resume later and still start provider work;
2. a request could pass the cooldown check, queue for a permit, then start after another request opened a 429 cooldown unless cooldown is rechecked after permit acquisition.

The final local chain also:

- extracts direct URLSession request I/O out of `YFinanceClient`;
- prevents a target-endpoint 429 from triggering cookie/crumb strategy refresh and a second Yahoo request;
- parses HTTP `Retry-After` and makes the coordinator honor it as the cooldown floor.

### Authoritative local YFinanceKit command

From a local `YFinanceKit` checkout, run:

```bash
python3 tools/prepare-hardening-candidate-final.py
```

That command is the authoritative entry point. It applies, in order:

1. `tools/apply-transport-extraction.py`
2. `tools/apply-coordinator-cancellation-hardening.py`
3. `tools/apply-post-permit-cooldown-recheck.py`
4. `tools/apply-core-rate-limit-hardening.py`
5. final static source-state verification
6. `bash tools/verify.sh`
7. `bash tools/strict-concurrency-audit.sh`

Do not manually re-create those edits unless a migration fails because source drifted. If a script fails, inspect the source and update the surgical migration deliberately rather than doing a broad rewrite.

After the command passes:

1. Review the diff carefully, especially `YFinanceClient.swift` and `YFinanceResilience.swift`.
2. Fix any real compiler/test/concurrency findings rather than weakening the gates.
3. Commit the exact verified candidate locally.
4. Record that commit SHA. That SHA, not a moving `main`, becomes the app candidate.
5. Run the small live Yahoo gate:

```bash
bash tools/live-parity-gate.sh
```

Use the broad matrix only deliberately if needed. Avoid hammering Yahoo just to manufacture confidence.

### Then hand the exact verified SHA to nommminal

On `hackyhunter/nommminal`, use branch:

`yfinance-hardening-integration`

Then run:

```bash
python3 scripts/prepare-yfinance-hardening.py --revision <verified-yfinancekit-commit>
python3 scripts/verify-yfinance-hardening-all.py
```

Only after those pass should Xcode be opened for the final app gate.

See `FINAL_HARDENING_FLOW.md` and `RELEASE_CHECKLIST.md` for the same flow in runbook form.

## What still requires a real local/Xcode environment

Do not claim these are done until they actually run:

- final Swift compilation after the exact giant-client migrations
- all offline tests after those migrations
- warnings-as-errors complete strict-concurrency audit
- live Yahoo parity/smoke on the committed candidate
- `nommminal` ProviderParity + Node bridge/client tests against that exact candidate SHA
- Xcode Debug simulator build
- Xcode Release simulator build
- simulator launch smoke
- real cancellation / rapid-symbol-switch behavior
- stale disk fallback after a successful fetch followed by network failure
- 429 / Retry-After behavior end to end
- chart 1d/5d/1mo + intraday behavior
- revenue, earnings, options and background quote refresh
- preferably one real-device Yahoo cookie/crumb/session smoke

Do not tag a stable YFinanceKit release or advance `nommminal/master` before these gates pass.
