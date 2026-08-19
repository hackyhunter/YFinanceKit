# Final hardening flow

This is the authoritative path from the remotely staged YFinanceKit source to a verified commit suitable for `nommminal`.

## 1. Prepare and verify the final offline candidate

In a normal local YFinanceKit checkout:

```sh
python3 tools/prepare-hardening-candidate-final.py
```

This applies the exact large-file migrations in order:

1. extract raw URLSession I/O into `YFURLSessionTransport`
2. make the global request permit queue cancellation-safe
3. recheck Yahoo cooldown after queued permit acquisition
4. make target 429 bypass crumb/session recovery
5. capture HTTP `Retry-After`
6. make the shared request coordinator honor that provider delay

Then it runs:

- final static prepared-source gate
- package/build/offline tests
- complete Swift concurrency checking with warnings as errors

The command does **not** commit anything.

## 2. Review the diff

Pay particular attention to:

- `Sources/YFinanceKit/YFinanceClient.swift`
- `Sources/YFinanceKit/YFinanceResilience.swift`

The intended diff is surgical. Do not accept unrelated endpoint/repair churn from the migration scripts.

## 3. Commit the verified candidate

Commit the exact state that passed the offline gate. Record the full SHA.

Do not tag a stable release yet.

## 4. Small live parity smoke

```sh
bash tools/live-parity-gate.sh
```

This intentionally uses a small symbol set. A full cross-market matrix is opt-in:

```sh
FULL_PARITY=1 bash tools/live-parity-gate.sh
```

Run the full matrix deliberately, not on every edit, to avoid unnecessary Yahoo pressure.

## 5. Move nommminal to the exact verified commit

In the `nommminal` repository on branch `yfinance-hardening-integration`:

```sh
python3 scripts/prepare-yfinance-hardening.py --revision <verified-yfinancekit-commit>
python3 scripts/verify-yfinance-hardening-all.py
```

The first command synchronizes every app pin and applies the staged source/project migrations. The second runs every non-Xcode gate available to the app repo.

## 6. Xcode boundary

Only after the two non-Xcode repositories are green:

- Debug simulator build
- Release simulator build
- launch smoke
- quote + duplicate quote/coalescing
- daily + intraday charts
- rapid symbol/range switching and real cancellation
- offline/stale in-memory + disk fallback
- 429 / Retry-After bridge behavior
- revenue / earnings / options
- background refresh
- real-device Yahoo/session smoke when practical
- privacy/archive report

The current widget does not contact Yahoo. App Group/signing work is optional and only needed if shared live market snapshots are later added to the widget.

## Automation policy

Do not turn automatic GitHub Actions or Dependabot back on as part of this flow. Verification is intentionally local/manual.
