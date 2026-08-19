#!/usr/bin/env python3
"""Recheck Yahoo cooldown after queued requests acquire a permit.

A request can pass the first cooldown check, wait in the global concurrency
queue, and observe a new 429 while queued. Without a second check, permit resume
could leak that request through the newly-opened cooldown. This exact migration
runs after coordinator cancellation hardening.
"""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "Sources" / "YFinanceKit" / "YFinanceResilience.swift"


class MigrationError(RuntimeError):
    pass


def main() -> int:
    text = TARGET.read_text(encoding="utf-8")
    new_fragment = """            do {
                // Cooldown may have opened while this request was queued for a
                // global permit. Recheck before the provider operation starts.
                try await waitForCooldown()
                try Task.checkCancellation()
                let value = try await operation()
"""
    old_fragment = """            do {
                try Task.checkCancellation()
                let value = try await operation()
"""

    if new_fragment in text:
        changed = False
    elif old_fragment in text:
        text = text.replace(old_fragment, new_fragment, 1)
        changed = True
    else:
        raise MigrationError("Expected post-permit operation block not found")

    if text.count("try await waitForCooldown()") < 2:
        raise MigrationError("Coordinator must check cooldown both before and after permit acquisition")
    if "try await acquirePermit()" not in text:
        raise MigrationError("Coordinator cancellation/permit migration must run before cooldown recheck")

    if changed:
        TARGET.write_text(text, encoding="utf-8")
        print("Added post-permit Yahoo cooldown recheck.")
    else:
        print("Post-permit Yahoo cooldown recheck already applied.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MigrationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
