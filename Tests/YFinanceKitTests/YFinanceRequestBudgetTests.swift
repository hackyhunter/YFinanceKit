import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceRequestBudgetTests: XCTestCase {
    func testInteractiveWorkQueuesAheadOfNormalWork() async throws {
        let gate = YFRequestBudgetGate(
            policy: YFRequestBudgetPolicy(maxConcurrentRequests: 1, maxBackgroundRequests: 1)
        )
        let probe = RequestBudgetProbe()

        let background = Task {
            try await gate.withPermit(priority: .background) {
                await probe.record("background")
                await probe.waitForRelease()
                return 0
            }
        }
        await probe.waitUntilRecorded("background")

        let normal = Task {
            try await gate.withPermit(priority: .normal) {
                await probe.record("normal")
                return 1
            }
        }
        let interactive = Task {
            try await gate.withPermit(priority: .interactive) {
                await probe.record("interactive")
                return 2
            }
        }

        await waitUntil {
            let snapshot = await gate.snapshot()
            return snapshot.queuedNormal == 1 && snapshot.queuedInteractive == 1
        }

        await probe.release()
        _ = try await background.value
        _ = try await interactive.value
        _ = try await normal.value

        let values = await probe.values()
        XCTAssertEqual(values, ["background", "interactive", "normal"])
    }

    func testBackgroundConcurrencyHasIndependentCap() async throws {
        let gate = YFRequestBudgetGate(
            policy: YFRequestBudgetPolicy(maxConcurrentRequests: 3, maxBackgroundRequests: 1)
        )
        let probe = RequestBudgetProbe()

        let tasks = (0..<3).map { index in
            Task {
                try await gate.withPermit(priority: .background) {
                    await probe.record("b\(index)")
                    await probe.waitForRelease()
                    return index
                }
            }
        }

        await waitUntil {
            let snapshot = await gate.snapshot()
            return snapshot.activeBackgroundRequests == 1 && snapshot.queuedBackground == 2
        }
        let snapshot = await gate.snapshot()
        XCTAssertEqual(snapshot.activeRequests, 1)
        XCTAssertEqual(snapshot.activeBackgroundRequests, 1)

        await probe.release()
        for task in tasks {
            _ = try await task.value
        }
    }

    func testQueuedCancellationRemovesWaiter() async throws {
        let gate = YFRequestBudgetGate(
            policy: YFRequestBudgetPolicy(maxConcurrentRequests: 1, maxBackgroundRequests: 1)
        )
        let probe = RequestBudgetProbe()

        let holder = Task {
            try await gate.withPermit(priority: .normal) {
                await probe.record("holder")
                await probe.waitForRelease()
                return 0
            }
        }
        await probe.waitUntilRecorded("holder")

        let queued = Task {
            try await gate.withPermit(priority: .normal) {
                XCTFail("Cancelled waiter must not execute")
                return 1
            }
        }

        await waitUntil {
            let snapshot = await gate.snapshot()
            return snapshot.queuedNormal == 1
        }
        queued.cancel()

        do {
            _ = try await queued.value
            XCTFail("Expected cancellation")
        } catch is CancellationError {
            // Expected.
        }

        await waitUntil {
            let snapshot = await gate.snapshot()
            return snapshot.queuedNormal == 0
        }
        await probe.release()
        _ = try await holder.value
    }

    private func waitUntil(
        timeoutIterations: Int = 1_000,
        condition: @escaping () async -> Bool
    ) async {
        for _ in 0..<timeoutIterations {
            if await condition() { return }
            await Task.yield()
        }
        XCTFail("Timed out waiting for async condition")
    }
}

private actor RequestBudgetProbe {
    private var recorded: [String] = []
    private var released = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var recordWaiters: [(String, CheckedContinuation<Void, Never>)] = []

    func record(_ value: String) {
        recorded.append(value)
        var remaining: [(String, CheckedContinuation<Void, Never>)] = []
        for waiter in recordWaiters {
            if waiter.0 == value {
                waiter.1.resume()
            } else {
                remaining.append(waiter)
            }
        }
        recordWaiters = remaining
    }

    func waitUntilRecorded(_ value: String) async {
        if recorded.contains(value) { return }
        await withCheckedContinuation { continuation in
            recordWaiters.append((value, continuation))
        }
    }

    func waitForRelease() async {
        if released { return }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func release() {
        released = true
        let pending = waiters
        waiters.removeAll()
        pending.forEach { $0.resume() }
    }

    func values() -> [String] {
        recorded
    }
}
