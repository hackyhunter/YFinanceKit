import Foundation

public enum YFRequestPriority: Int, CaseIterable, Sendable, Comparable {
    case interactive = 0
    case normal = 1
    case background = 2

    public static func < (lhs: YFRequestPriority, rhs: YFRequestPriority) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

public struct YFRequestBudgetPolicy: Sendable, Equatable {
    public var maxConcurrentRequests: Int
    public var maxBackgroundRequests: Int

    public init(
        maxConcurrentRequests: Int = 4,
        maxBackgroundRequests: Int = 1
    ) {
        self.maxConcurrentRequests = max(1, maxConcurrentRequests)
        self.maxBackgroundRequests = min(
            self.maxConcurrentRequests,
            max(0, maxBackgroundRequests)
        )
    }

    public static let `default` = YFRequestBudgetPolicy()
}

public struct YFRequestBudgetSnapshot: Sendable, Equatable {
    public let activeRequests: Int
    public let activeBackgroundRequests: Int
    public let queuedInteractive: Int
    public let queuedNormal: Int
    public let queuedBackground: Int

    public init(
        activeRequests: Int,
        activeBackgroundRequests: Int,
        queuedInteractive: Int,
        queuedNormal: Int,
        queuedBackground: Int
    ) {
        self.activeRequests = activeRequests
        self.activeBackgroundRequests = activeBackgroundRequests
        self.queuedInteractive = queuedInteractive
        self.queuedNormal = queuedNormal
        self.queuedBackground = queuedBackground
    }
}

/// Optional scheduling layer for consumers that have both interactive and
/// background Yahoo work. `YFRequestCoordinator` remains the transport-level
/// concurrency/backpressure authority; this gate only decides which logical
/// caller is allowed to enter that layer next.
public actor YFRequestBudgetGate {
    private struct Waiter {
        let id: UUID
        let priority: YFRequestPriority
        let continuation: CheckedContinuation<Void, Error>
    }

    private let policy: YFRequestBudgetPolicy
    private var activeRequests = 0
    private var activeBackgroundRequests = 0
    private var waiters: [Waiter] = []
    private var cancelledBeforeEnqueue: Set<UUID> = []

    public init(policy: YFRequestBudgetPolicy = .default) {
        self.policy = policy
    }

    public func withPermit<T: Sendable>(
        priority: YFRequestPriority = .normal,
        operation: @Sendable @escaping () async throws -> T
    ) async throws -> T {
        try await acquire(priority: priority)
        defer { release(priority: priority) }
        try Task.checkCancellation()
        return try await operation()
    }

    public func snapshot() -> YFRequestBudgetSnapshot {
        YFRequestBudgetSnapshot(
            activeRequests: activeRequests,
            activeBackgroundRequests: activeBackgroundRequests,
            queuedInteractive: waiters.filter { $0.priority == .interactive }.count,
            queuedNormal: waiters.filter { $0.priority == .normal }.count,
            queuedBackground: waiters.filter { $0.priority == .background }.count
        )
    }

    private func acquire(priority: YFRequestPriority) async throws {
        try Task.checkCancellation()
        if canStart(priority: priority) {
            markStarted(priority: priority)
            return
        }

        let id = UUID()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                if cancelledBeforeEnqueue.remove(id) != nil || Task.isCancelled {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                waiters.append(
                    Waiter(id: id, priority: priority, continuation: continuation)
                )
                drainWaiters()
            }
        } onCancel: {
            Task { await self.cancelWaiter(id: id) }
        }
    }

    private func release(priority: YFRequestPriority) {
        activeRequests = max(0, activeRequests - 1)
        if priority == .background {
            activeBackgroundRequests = max(0, activeBackgroundRequests - 1)
        }
        drainWaiters()
    }

    private func canStart(priority: YFRequestPriority) -> Bool {
        guard activeRequests < policy.maxConcurrentRequests else { return false }
        if priority == .background {
            return activeBackgroundRequests < policy.maxBackgroundRequests
        }
        return true
    }

    private func markStarted(priority: YFRequestPriority) {
        activeRequests += 1
        if priority == .background {
            activeBackgroundRequests += 1
        }
    }

    private func cancelWaiter(id: UUID) {
        if let index = waiters.firstIndex(where: { $0.id == id }) {
            let waiter = waiters.remove(at: index)
            waiter.continuation.resume(throwing: CancellationError())
            return
        }
        // Handles the narrow race where cancellation fires immediately before
        // the continuation is enqueued.
        cancelledBeforeEnqueue.insert(id)
    }

    private func drainWaiters() {
        while activeRequests < policy.maxConcurrentRequests {
            guard let index = nextRunnableWaiterIndex() else { return }
            let waiter = waiters.remove(at: index)
            markStarted(priority: waiter.priority)
            waiter.continuation.resume()
        }
    }

    private func nextRunnableWaiterIndex() -> Int? {
        for priority in YFRequestPriority.allCases.sorted() {
            if priority == .background,
               activeBackgroundRequests >= policy.maxBackgroundRequests {
                continue
            }
            if let index = waiters.firstIndex(where: { $0.priority == priority }) {
                return index
            }
        }
        return nil
    }
}
