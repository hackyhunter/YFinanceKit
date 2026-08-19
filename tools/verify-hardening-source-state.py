#!/usr/bin/env python3
"""Static hardening gate that does not require Swift compilation or Yahoo.

This is intentionally narrower than `verify.sh`: it checks that the repository
contains the expected source/tooling invariants and that automation remains off.
It can run before the local giant-client migrations to validate staging, or
after them to validate the prepared candidate state.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]


class GateFailure(RuntimeError):
    pass


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise GateFailure(message)


def require_file(relative: str) -> None:
    require((ROOT / relative).exists(), f"Missing hardening file: {relative}")


def verify_build_info() -> None:
    aliases = read("Sources/YFinanceKit/YFinanceAliases.swift")
    package = re.search(r'packageVersion = "([^"]+)"', aliases)
    upstream = re.search(r'upstreamVersion = "([^"]+)"', aliases)
    commit = re.search(r'upstreamCommit = "([0-9a-f]{40})"', aliases)
    require(package is not None, "Missing YFinanceKit package version metadata")
    require(upstream is not None, "Missing upstream yfinance version metadata")
    require(commit is not None, "Missing full upstream yfinance commit metadata")
    require(package.group(1) != upstream.group(1), "Package version must remain separate from upstream compatibility version")


def verify_architecture_sources() -> None:
    required = (
        "Sources/YFinanceKit/YFinanceResilience.swift",
        "Sources/YFinanceKit/YFinanceResilienceProtocols.swift",
        "Sources/YFinanceKit/YFinanceNativeServices.swift",
        "Sources/YFinanceKit/YFinanceRequestBudget.swift",
        "Sources/YFinanceKit/YFinanceDiagnosticsExport.swift",
        "Sources/YFinanceKit/YFinanceDiagnosticsAnalysis.swift",
        "Sources/YFinanceKit/YFinanceRetryAfter.swift",
        "Sources/YFinanceKit/YFinanceTransport.swift",
        "Sources/YFinanceKit/YFinanceHistoryHardening.swift",
        "Sources/YFinanceKit/YFinanceHistoryIntegrity.swift",
        "Sources/YFinanceKit/YFinanceFinancials.swift",
    )
    for relative in required:
        require_file(relative)

    resilience = read("Sources/YFinanceKit/YFinanceResilience.swift")
    require("actor YFRequestCoordinator" in resilience, "Missing shared request coordinator")
    require("actor YFResilientClient" in resilience, "Missing resilient client façade")
    require("quoteFlights" in resilience and "historyFlights" in resilience, "Missing single-flight maps")

    budget = read("Sources/YFinanceKit/YFinanceRequestBudget.swift")
    for fragment in (
        "case interactive",
        "case normal",
        "case background",
        "maxBackgroundRequests",
        "withTaskCancellationHandler",
    ):
        require(fragment in budget, f"Request budget is missing: {fragment}")

    transport = read("Sources/YFinanceKit/YFinanceTransport.swift")
    for fragment in (
        "protocol YFHTTPTransporting: Sendable",
        "final class YFURLSessionTransport",
        "statusCode: Int",
        "func header(_ name: String)",
    ):
        require(fragment in transport, f"Transport boundary is missing: {fragment}")


def verify_regression_cage() -> None:
    required_tests = (
        "Tests/YFinanceKitTests/YFinanceSessionTests.swift",
        "Tests/YFinanceKitTests/YFinanceCrumbHardeningTests.swift",
        "Tests/YFinanceKitTests/YFinanceResilienceTests.swift",
        "Tests/YFinanceKitTests/YFinanceResilientClientTests.swift",
        "Tests/YFinanceKitTests/YFinanceRequestConcurrencyTests.swift",
        "Tests/YFinanceKitTests/YFinanceRequestBudgetTests.swift",
        "Tests/YFinanceKitTests/YFinanceRetryAfterTests.swift",
        "Tests/YFinanceKitTests/YFinanceTransportTests.swift",
        "Tests/YFinanceKitTests/YFinanceSchemaMutationTests.swift",
        "Tests/YFinanceKitTests/YFinanceMutationFuzzTests.swift",
        "Tests/YFinanceKitTests/YFinanceRepairParityTests.swift",
        "Tests/YFinanceKitTests/YFinanceDiagnosticsExportTests.swift",
        "Tests/YFinanceKitTests/YFinanceDiagnosticsAnalysisTests.swift",
        "Tests/YFinanceKitTests/YFinanceNativeServicesTests.swift",
    )
    for relative in required_tests:
        require_file(relative)

    fuzz = read("Tests/YFinanceKitTests/YFinanceMutationFuzzTests.swift")
    require("for seed in 1...96" in fuzz, "Mutation fuzz corpus is smaller/different than documented")

    matrix = read("tools/parity_matrix.py")
    for symbol in ("VOD.L", "NPN.JO", "TEVA.TA", "BTC-USD", "^GSPC"):
        require(symbol in matrix, f"Parity matrix is missing cross-market symbol {symbol}")


def verify_migration_tooling() -> None:
    required = (
        "tools/apply-transport-extraction.py",
        "tools/apply-core-rate-limit-hardening.py",
        "tools/prepare-hardening-candidate.py",
        "tools/verify.sh",
        "tools/strict-concurrency.sh",
        "tools/parity_harness.py",
        "tools/parity_matrix.py",
    )
    for relative in required:
        require_file(relative)

    candidate = read("tools/prepare-hardening-candidate.py")
    require("apply-transport-extraction.py" in candidate, "Candidate prep must extract transport")
    require("apply-core-rate-limit-hardening.py" in candidate, "Candidate prep must apply strict 429 hardening")
    require("tools/verify.sh" in candidate, "Candidate prep must run build/tests")
    require("tools/strict-concurrency.sh" in candidate, "Candidate prep must run strict concurrency")


def verify_automation_policy() -> None:
    require(not (ROOT / ".github/dependabot.yml").exists(), "Dependabot config must remain disabled")

    workflows = ROOT / ".github/workflows"
    if not workflows.exists():
        return
    forbidden = ("push:", "pull_request:", "schedule:", "release:")
    for path in workflows.glob("*.y*ml"):
        source = path.read_text(encoding="utf-8")
        for token in forbidden:
            require(token not in source, f"Automatic workflow trigger {token!r} found in {path.relative_to(ROOT)}")


def verify_no_stale_generated_green_report() -> None:
    require(not (ROOT / "artifacts/parity_report.json").exists(), "Stale generated parity_report.json should not be checked in")
    require(not (ROOT / "artifacts/parity_report.md").exists(), "Stale generated parity_report.md should not be checked in")


def main() -> int:
    verify_build_info()
    verify_architecture_sources()
    verify_regression_cage()
    verify_migration_tooling()
    verify_automation_policy()
    verify_no_stale_generated_green_report()
    print("YFinanceKit static hardening source-state gate passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateFailure as exc:
        print(f"HARDENING SOURCE-STATE GATE FAILED: {exc}", file=sys.stderr)
        raise SystemExit(2)
