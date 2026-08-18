#!/usr/bin/env python3
"""Prepare the final local YFinanceKit hardening candidate.

This command applies only exact-source migrations to the giant compatibility
client, then runs the package/strict-concurrency gates. It does not commit or tag
anything. Review the resulting diff before committing the candidate.
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


def verify_source_state() -> None:
    client = (ROOT / "Sources/YFinanceKit/YFinanceClient.swift").read_text(encoding="utf-8")
    resilience = (ROOT / "Sources/YFinanceKit/YFinanceResilience.swift").read_text(encoding="utf-8")

    required_client = (
        "private let transport: any YFHTTPTransporting",
        "self.transport = YFURLSessionTransport(session: session)",
        "let response = try await transport.send(request)",
        "retryAfterHeader: response.header(\"Retry-After\")",
        "YFinanceErrorClassifier.kind(of: error) == .rateLimited",
        "YFinanceError.rateLimited(retryAfter: retryAfter)",
    )
    for fragment in required_client:
        if fragment not in client:
            raise PreparationError(f"Prepared client missing expected fragment: {fragment}")

    if "session.data(for: request)" in client:
        raise PreparationError("Prepared YFinanceClient still contains direct URLSession request I/O")

    if "(error as? YFinanceError)?.retryAfter" not in resilience:
        raise PreparationError("Prepared request coordinator does not honor Retry-After")


def main() -> int:
    run([sys.executable, "tools/apply-transport-extraction.py"], "Extract URLSession transport")
    run([sys.executable, "tools/apply-core-rate-limit-hardening.py"], "Apply strict 429/Retry-After behavior")
    verify_source_state()
    run(["bash", "tools/verify.sh"], "Build + offline tests")
    run(["bash", "tools/strict-concurrency.sh"], "Complete strict-concurrency audit")

    print("\nYFinanceKit hardening candidate prepared and verified locally.")
    print("Review the diff, commit it, then use that exact commit in nommminal:")
    print("  python3 scripts/prepare-yfinance-hardening.py --revision <new-yfinancekit-commit>")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PreparationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
