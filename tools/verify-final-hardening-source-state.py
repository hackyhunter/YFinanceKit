#!/usr/bin/env python3
"""Static checks specific to the fully prepared YFinanceKit candidate."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


class GateFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise GateFailure(message)


def run_base_gate() -> None:
    proc = subprocess.run(
        [sys.executable, "tools/verify-hardening-source-state.py"],
        cwd=ROOT,
        check=False,
    )
    if proc.returncode != 0:
        raise GateFailure("Base hardening source-state gate failed")


def main() -> int:
    run_base_gate()

    client = (ROOT / "Sources/YFinanceKit/YFinanceClient.swift").read_text(encoding="utf-8")
    resilience = (ROOT / "Sources/YFinanceKit/YFinanceResilience.swift").read_text(encoding="utf-8")

    client_required = (
        "private let transport: any YFHTTPTransporting",
        "self.transport = YFURLSessionTransport(session: session)",
        "let response = try await transport.send(request)",
        'retryAfterHeader: response.header("Retry-After")',
        "YFinanceErrorClassifier.kind(of: error) == .rateLimited",
        "YFinanceError.rateLimited(retryAfter: retryAfter)",
    )
    for fragment in client_required:
        require(fragment in client, f"Prepared client missing: {fragment}")
    require("session.data(for: request)" not in client, "Prepared client still performs direct URLSession request I/O")

    coordinator_required = (
        "private struct PermitWaiter",
        "preparingPermitWaiterIDs",
        "cancelledBeforePermitEnqueue",
        "try await acquirePermit()",
        "private func cancelPermitWaiter(id: UUID)",
        "Cooldown may have opened while this request was queued",
        "(error as? YFinanceError)?.retryAfter",
    )
    for fragment in coordinator_required:
        require(fragment in resilience, f"Prepared coordinator missing: {fragment}")

    require(
        resilience.count("try await waitForCooldown()") >= 2,
        "Prepared coordinator must recheck cooldown after permit acquisition",
    )
    require(
        "[CheckedContinuation<Void, Never>]" not in resilience,
        "Legacy non-cancellable coordinator permit queue remains",
    )

    print("Final prepared-candidate source-state gate passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateFailure as exc:
        print(f"FINAL HARDENING SOURCE-STATE GATE FAILED: {exc}", file=sys.stderr)
        raise SystemExit(2)
