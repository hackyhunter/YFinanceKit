#!/usr/bin/env python3
"""Authoritative local preparation for the final pre-release hardening candidate.

This supersedes the earlier incremental candidate helper. It applies every exact
large-file migration currently staged by the remote hardening work, then runs
static, build/test and warnings-as-errors strict-concurrency gates.

It does not commit, tag, or run live Yahoo parity. Review the diff before commit.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


class PreparationError(RuntimeError):
    pass


def run(command: list[str], label: str) -> None:
    print(f"\n== {label} ==")
    print("$ " + " ".join(command))
    proc = subprocess.run(command, cwd=ROOT, check=False)
    if proc.returncode != 0:
        raise PreparationError(f"{label} failed with exit code {proc.returncode}")


def main() -> int:
    run([sys.executable, "tools/apply-transport-extraction.py"], "Extract URLSession transport")
    run(
        [sys.executable, "tools/apply-coordinator-cancellation-hardening.py"],
        "Make global permit waiting cancellation-safe",
    )
    run(
        [sys.executable, "tools/apply-post-permit-cooldown-recheck.py"],
        "Recheck cooldown after queued permit acquisition",
    )
    run(
        [sys.executable, "tools/apply-core-rate-limit-hardening.py"],
        "Apply strict 429 and Retry-After behavior",
    )

    run(
        [sys.executable, "tools/verify-final-hardening-source-state.py"],
        "Verify prepared source invariants",
    )
    run(["bash", "tools/verify.sh"], "Build and offline regression tests")
    run(
        ["bash", "tools/strict-concurrency-audit.sh"],
        "Complete strict concurrency, warnings as errors",
    )

    print("\nFinal offline YFinanceKit hardening candidate is prepared.")
    print("Review the diff before committing, especially YFinanceClient.swift and YFinanceResilience.swift.")
    print("After commit:")
    print("  bash tools/live-parity-gate.sh")
    print("  cd ../nommminal")
    print("  python3 scripts/prepare-yfinance-hardening.py --revision <verified-yfinancekit-commit>")
    print("  python3 scripts/verify-yfinance-hardening-all.py")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PreparationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
