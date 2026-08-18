import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceRequestConcurrencyTests: XCTestCase {
    func testCoordinatorNeverExceedsConfiguredConcurrency() async throws {
        let probe = ConcurrencyProbe(targetActive: 2)
        let coordinator = YFRequestCoordinator(
            policy: YFRequestPolicy(
                maxConcurrentRequests: 2,
                maxAttempts: 1,
                baseRetryDelay: 0,
                maxRetryDelay: 0,
                retryJitterFraction: 0,
                baseRateLimitCooldown: 1,
                maxRateLimitCooldown: 5,
                traceCapacity: 20
            ),
            jitter: YFZeroJitterSource()
        )

        let tasks = (0..<6).map { index in
            Task {
                try await coordinator.execute(endpoint: "history", resource: "S\(index)") {
                    await probe.enter()
                    await probe.waitForRelease()
                    await probe.leave()
                    return index
                }
            }
        }

        await probe.waitUntilTargetActive()
        let beforeRelease = await probe.snapshot()
        XCTAssertEqual(beforeRelease.active, 2)
        XCTAssertEqual(beforeRelease.maximum, 2)

        await probe.releaseAll()
        let values = try await tasks.asyncValues()
        XCTAssertEqual(values.sorted(), Array(0..<6))

        let final = await probe.snapshot()
        XCTAssertEqual(final.active, 0)
        XCTAssertEqual(final.maximum, 2)

        let diagnostics = await coordinator.snapshot()
        XCTAssertEqual(diagnostics.successes, 6)
        XCTAssertEqual(diagnostics.attempts, 6)
    }
}

private actor ConcurrencyProbe {
    private let targetActive: Int
    private var active = 0
    private var maximum = 0
    private var released = false
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []
    private var targetWaiters: [CheckedContinuation<Void, Never>] = []

    init(targetActive: Int) {
        self.targetActive = targetActive
    }

    func enter() {
        active += 1
        maximum = max(maximum, active)
        if active >= targetActive {
            let waiters = targetWaiters
            targetWaiters.removeAll()
            waiters.forEach { $0.resume() }
        }
    }

    func leave() {
        active = max(0, active - 1)
    }

    func waitUntilTargetActive() async {
        if active >= targetActive { return }
        await withCheckedContinuation { continuation in
            targetWaiters.append(continuation)
        }
    }

    func waitForRelease() async {
        if released { return }
        await withCheckedContinuation { continuation in
            releaseWaiters.append(continuation)
        }
    }

    func releaseAll() {
        released = true
        let waiters = releaseWaiters
        releaseWaiters.removeAll()
        waiters.forEach { $0.resume() }
    }

    func snapshot() -> (active: Int, maximum: Int) {
        (active, maximum)
    }
}

private extension Array where Element == Task<Int, Error> {
    func asyncValues() async throws -> [Int] {
        var output: [Int] = []
        output.reserveCapacity(count)
        for task in self {
            output.append(try await task.value)
        }
        return output
    }
}
