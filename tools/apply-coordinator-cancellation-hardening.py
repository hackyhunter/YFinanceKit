#!/usr/bin/env python3
"""Make YFRequestCoordinator's global permit queue cancellation-safe.

This is an exact local migration so we do not remote-replace the whole
resilience source. It closes the case where a task is cancelled while waiting
for the Yahoo concurrency permit but later resumes and still starts provider
work.
"""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "Sources" / "YFinanceKit" / "YFinanceResilience.swift"


class MigrationError(RuntimeError):
    pass


def replace_once(text: str, old: str, new: str, identity: str) -> tuple[str, bool]:
    if identity in text:
        return text, False
    if old not in text:
        raise MigrationError(f"Expected coordinator source pattern not found for {identity}")
    return text.replace(old, new, 1), True


def main() -> int:
    text = TARGET.read_text(encoding="utf-8")
    changed = False

    old_state = """    private var activeRequests = 0
    private var permitWaiters: [CheckedContinuation<Void, Never>] = []
    private var cooldownUntil: Date?
"""
    new_state = """    private struct PermitWaiter {
        let id: UUID
        let continuation: CheckedContinuation<Void, Error>
    }

    private var activeRequests = 0
    private var permitWaiters: [PermitWaiter] = []
    private var preparingPermitWaiterIDs: Set<UUID> = []
    private var cancelledBeforePermitEnqueue: Set<UUID> = []
    private var cooldownUntil: Date?
"""
    text, did = replace_once(
        text,
        old_state,
        new_state,
        "private struct PermitWaiter",
    )
    changed |= did

    old_acquire_call = """            try await waitForCooldown()
            await acquirePermit()
            attempts += 1
            attempt += 1

            do {
                let value = try await operation()
"""
    new_acquire_call = """            do {
                try await waitForCooldown()
                try await acquirePermit()
            } catch is CancellationError {
                let endedAt = await clock.now()
                appendTrace(
                    YFRequestTrace(
                        endpoint: endpoint,
                        resource: resource,
                        startedAt: requestStartedAt,
                        duration: endedAt.timeIntervalSince(requestStartedAt),
                        attempts: attempt,
                        outcome: .cancelled,
                        failureKind: nil
                    )
                )
                throw CancellationError()
            }

            attempts += 1
            attempt += 1

            do {
                try Task.checkCancellation()
                let value = try await operation()
"""
    text, did = replace_once(
        text,
        old_acquire_call,
        new_acquire_call,
        "try await acquirePermit()",
    )
    changed |= did

    old_catch = """            } catch {
                releasePermit()
                let kind = YFinanceErrorClassifier.kind(of: error)

                if kind == .rateLimited {
"""
    new_catch = """            } catch is CancellationError {
                releasePermit()
                let endedAt = await clock.now()
                appendTrace(
                    YFRequestTrace(
                        endpoint: endpoint,
                        resource: resource,
                        startedAt: requestStartedAt,
                        duration: endedAt.timeIntervalSince(requestStartedAt),
                        attempts: attempt,
                        outcome: .cancelled,
                        failureKind: nil
                    )
                )
                throw CancellationError()
            } catch {
                releasePermit()
                let kind = YFinanceErrorClassifier.kind(of: error)

                if kind == .rateLimited {
"""
    text, did = replace_once(
        text,
        old_catch,
        new_catch,
        "} catch is CancellationError {\n                releasePermit()",
    )
    changed |= did

    old_acquire = """    private func acquirePermit() async {
        if activeRequests < policy.maxConcurrentRequests {
            activeRequests += 1
            return
        }

        await withCheckedContinuation { continuation in
            permitWaiters.append(continuation)
        }
    }

    private func releasePermit() {
        if !permitWaiters.isEmpty {
            let waiter = permitWaiters.removeFirst()
            waiter.resume()
            return
        }
        activeRequests = max(0, activeRequests - 1)
    }
"""
    new_acquire = """    private func acquirePermit() async throws {
        try Task.checkCancellation()
        if activeRequests < policy.maxConcurrentRequests {
            activeRequests += 1
            return
        }

        let id = UUID()
        preparingPermitWaiterIDs.insert(id)
        try await withTaskCancellationHandler(
            operation: {
                try await withCheckedThrowingContinuation { continuation in
                    preparingPermitWaiterIDs.remove(id)
                    if cancelledBeforePermitEnqueue.remove(id) != nil || Task.isCancelled {
                        continuation.resume(throwing: CancellationError())
                        return
                    }
                    permitWaiters.append(
                        PermitWaiter(id: id, continuation: continuation)
                    )
                }
            },
            onCancel: {
                Task { await self.cancelPermitWaiter(id: id) }
            }
        )
    }

    private func cancelPermitWaiter(id: UUID) {
        if let index = permitWaiters.firstIndex(where: { $0.id == id }) {
            let waiter = permitWaiters.remove(at: index)
            waiter.continuation.resume(throwing: CancellationError())
            return
        }
        if preparingPermitWaiterIDs.contains(id) {
            cancelledBeforePermitEnqueue.insert(id)
        }
    }

    private func releasePermit() {
        while !permitWaiters.isEmpty {
            let waiter = permitWaiters.removeFirst()
            waiter.continuation.resume()
            return
        }
        activeRequests = max(0, activeRequests - 1)
    }
"""
    text, did = replace_once(
        text,
        old_acquire,
        new_acquire,
        "private func cancelPermitWaiter(id: UUID)",
    )
    changed |= did

    old_queued = """            queuedRequests: permitWaiters.count,
"""
    new_queued = """            queuedRequests: permitWaiters.count + preparingPermitWaiterIDs.count,
"""
    text, did = replace_once(
        text,
        old_queued,
        new_queued,
        "queuedRequests: permitWaiters.count + preparingPermitWaiterIDs.count",
    )
    changed |= did

    required = (
        "private struct PermitWaiter",
        "try await acquirePermit()",
        "try Task.checkCancellation()",
        "private func cancelPermitWaiter(id: UUID)",
        "withCheckedThrowingContinuation",
        "outcome: .cancelled",
        "queuedRequests: permitWaiters.count + preparingPermitWaiterIDs.count",
    )
    for fragment in required:
        if fragment not in text:
            raise MigrationError(f"Coordinator cancellation patch missing expected fragment: {fragment}")

    if "[CheckedContinuation<Void, Never>]" in text:
        raise MigrationError("Legacy non-cancellable permit continuation is still present")

    if changed:
        TARGET.write_text(text, encoding="utf-8")
        print("Made YFRequestCoordinator permit waiting cancellation-safe.")
    else:
        print("Coordinator permit cancellation hardening already applied.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MigrationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
