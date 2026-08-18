#!/usr/bin/env python3
"""Apply the remaining surgical 429/Retry-After core edits.

This script is composable with `apply-transport-extraction.py`. It refuses to
guess if source has drifted. When transport extraction already moved response
headers into Sendable values, the 429 parser step is recognized as complete and
only the crumb-recovery/coordinator edits are applied.
"""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "Sources" / "YFinanceKit" / "YFinanceClient.swift"
RESILIENCE = ROOT / "Sources" / "YFinanceKit" / "YFinanceResilience.swift"


class MigrationError(RuntimeError):
    pass


def replace_exact(path: Path, old: str, new: str) -> bool:
    text = path.read_text(encoding="utf-8")
    if new in text and old not in text:
        return False
    if old not in text:
        raise MigrationError(f"Expected source pattern not found in {path.relative_to(ROOT)}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    return True


def patch_client() -> bool:
    changed = False

    old_recovery = """        } catch {\n            guard shouldUseCrumb else {\n                throw error\n            }\n\n            await crumbStore.invalidate()\n            let refreshedCrumb = try await crumbStore.currentCrumb(forceRefresh: true)\n            return try await executeRequest(\n                baseURL: baseURL,\n                path: path,\n                queryItems: withCrumb(queryItems, crumb: refreshedCrumb),\n                method: method,\n                body: body,\n                headers: headers,\n                timeout: effectiveTimeout\n            )\n        }\n"""
    new_recovery = """        } catch {\n            // A target-endpoint 429 is provider backpressure, not evidence that\n            // the Yahoo cookie/crumb strategy is invalid. Do not generate a\n            // second request by switching/refreshing the session.\n            if YFinanceErrorClassifier.kind(of: error) == .rateLimited {\n                throw error\n            }\n\n            guard shouldUseCrumb else {\n                throw error\n            }\n\n            await crumbStore.invalidate()\n            let refreshedCrumb = try await crumbStore.currentCrumb(forceRefresh: true)\n            return try await executeRequest(\n                baseURL: baseURL,\n                path: path,\n                queryItems: withCrumb(queryItems, crumb: refreshedCrumb),\n                method: method,\n                body: body,\n                headers: headers,\n                timeout: effectiveTimeout\n            )\n        }\n"""
    changed |= replace_exact(CLIENT, old_recovery, new_recovery)

    client_text = CLIENT.read_text(encoding="utf-8")
    retry_after_already_extracted = (
        "retryAfterHeader: String?" in client_text
        and "YFRetryAfterParser.parse(retryAfterHeader)" in client_text
        and "YFinanceError.rateLimited(retryAfter: retryAfter)" in client_text
    )

    if not retry_after_already_extracted:
        old_429 = """        if response.statusCode == 429 {\n            throw YFinanceError.httpStatus(429)\n        }\n"""
        new_429 = """        if response.statusCode == 429 {\n            let retryAfter = YFRetryAfterParser.parse(\n                response.value(forHTTPHeaderField: \"Retry-After\")\n            )\n            throw YFinanceError.rateLimited(retryAfter: retryAfter)\n        }\n"""
        changed |= replace_exact(CLIENT, old_429, new_429)

    final = CLIENT.read_text(encoding="utf-8")
    if "YFinanceErrorClassifier.kind(of: error) == .rateLimited" not in final:
        raise MigrationError("Client patch is missing strict 429 crumb-recovery bypass")
    if "YFinanceError.rateLimited(retryAfter: retryAfter)" not in final:
        raise MigrationError("Client patch is missing Retry-After-aware 429 error")
    if not (
        "response.value(forHTTPHeaderField: \"Retry-After\")" in final
        or "retryAfterHeader: String?" in final
    ):
        raise MigrationError("Client patch is missing a Retry-After header path")
    return changed


def patch_coordinator() -> bool:
    old = """                if kind == .rateLimited {\n                    rateLimits += 1\n                    failures += 1\n                    rateLimitStreak += 1\n                    await openRateLimitCooldown()\n                    let endedAt = await clock.now()\n"""
    new = """                if kind == .rateLimited {\n                    rateLimits += 1\n                    failures += 1\n                    rateLimitStreak += 1\n                    if let retryAfter = (error as? YFinanceError)?.retryAfter, retryAfter > 0 {\n                        let now = await clock.now()\n                        let proposed = now.addingTimeInterval(\n                            min(retryAfter, policy.maxRateLimitCooldown)\n                        )\n                        if cooldownUntil == nil || proposed > cooldownUntil! {\n                            cooldownUntil = proposed\n                        }\n                    } else {\n                        await openRateLimitCooldown()\n                    }\n                    let endedAt = await clock.now()\n"""
    changed = replace_exact(RESILIENCE, old, new)

    old_comment = """    /// Allows a transport that can see `Retry-After` to feed a stronger cooldown\n    /// signal into the shared gate. Current `YFinanceClient` does not expose response\n    /// headers, so resilient wrappers otherwise use exponential cooldowns.\n"""
    new_comment = """    /// Allows callers/transports to feed an explicit `Retry-After` signal into\n    /// the shared gate. Core HTTP 429 errors also carry this metadata when the\n    /// provider sends the header.\n"""
    text = RESILIENCE.read_text(encoding="utf-8")
    if old_comment in text:
        RESILIENCE.write_text(text.replace(old_comment, new_comment, 1), encoding="utf-8")
        changed = True

    final = RESILIENCE.read_text(encoding="utf-8")
    if "(error as? YFinanceError)?.retryAfter" not in final:
        raise MigrationError("Coordinator patch did not preserve Retry-After metadata")
    return changed


def main() -> int:
    changed = []
    if patch_client():
        changed.append(CLIENT)
    if patch_coordinator():
        changed.append(RESILIENCE)

    if changed:
        print("Updated:")
        for path in changed:
            print(f"  - {path.relative_to(ROOT)}")
    else:
        print("Core rate-limit hardening already applied.")

    print("Next: bash tools/verify.sh && bash tools/strict-concurrency.sh")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MigrationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
