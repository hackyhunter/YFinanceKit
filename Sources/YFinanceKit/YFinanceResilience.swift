import Foundation

public protocol YFClock: Sendable {
    func now() async -> Date
    func sleep(for seconds: TimeInterval) async throws
}

public struct YFSystemClock: YFClock {
    public init() {}

    public func now() async -> Date {
        Date()
    }

    public func sleep(for seconds: TimeInterval) async throws {
        guard seconds > 0 else { return }
        let nanoseconds = UInt64(min(seconds, 86_400) * 1_000_000_000)
        try await Task.sleep(nanoseconds: nanoseconds)
    }
}

public protocol YFJitterSource: Sendable {
    func random(in range: ClosedRange<Double>) -> Double
}

public struct YFSystemJitterSource: YFJitterSource {
    public init() {}

    public func random(in range: ClosedRange<Double>) -> Double {
        Double.random(in: range)
    }
}

public struct YFZeroJitterSource: YFJitterSource {
    public init() {}

    public func random(in range: ClosedRange<Double>) -> Double {
        range.lowerBound
    }
}

public struct YFRequestPolicy: Sendable, Equatable {
    public var maxConcurrentRequests: Int
    public var maxAttempts: Int
    public var baseRetryDelay: TimeInterval
    public var maxRetryDelay: TimeInterval
    public var retryJitterFraction: Double
    public var baseRateLimitCooldown: TimeInterval
    public var maxRateLimitCooldown: TimeInterval
    public var traceCapacity: Int

    public init(
        maxConcurrentRequests: Int = 4,
        maxAttempts: Int = 3,
        baseRetryDelay: TimeInterval = 0.25,
        maxRetryDelay: TimeInterval = 2.0,
        retryJitterFraction: Double = 0.25,
        baseRateLimitCooldown: TimeInterval = 2.5,
        maxRateLimitCooldown: TimeInterval = 60,
        traceCapacity: Int = 100
    ) {
        self.maxConcurrentRequests = max(1, maxConcurrentRequests)
        self.maxAttempts = max(1, maxAttempts)
        self.baseRetryDelay = max(0, baseRetryDelay)
        self.maxRetryDelay = max(self.baseRetryDelay, maxRetryDelay)
        self.retryJitterFraction = min(max(0, retryJitterFraction), 1)
        self.baseRateLimitCooldown = max(0, baseRateLimitCooldown)
        self.maxRateLimitCooldown = max(self.baseRateLimitCooldown, maxRateLimitCooldown)
        self.traceCapacity = max(0, traceCapacity)
    }

    public static let `default` = YFRequestPolicy()
}

public enum YFRequestTraceOutcome: String, Sendable {
    case success
    case failure
    case rateLimited
    case cancelled
}

public struct YFRequestTrace: Sendable {
    public let endpoint: String
    public let resource: String
    public let startedAt: Date
    public let duration: TimeInterval
    public let attempts: Int
    public let outcome: YFRequestTraceOutcome
    public let failureKind: YFinanceFailureKind?

    public init(
        endpoint: String,
        resource: String,
        startedAt: Date,
        duration: TimeInterval,
        attempts: Int,
        outcome: YFRequestTraceOutcome,
        failureKind: YFinanceFailureKind? = nil
    ) {
        self.endpoint = endpoint
        self.resource = resource
        self.startedAt = startedAt
        self.duration = duration
        self.attempts = attempts
        self.outcome = outcome
        self.failureKind = failureKind
    }
}

public struct YFRequestDiagnosticsSnapshot: Sendable {
    public let logicalRequests: Int
    public let attempts: Int
    public let successes: Int
    public let failures: Int
    public let retries: Int
    public let rateLimits: Int
    public let coalescedRequests: Int
    public let cacheHits: Int
    public let cacheMisses: Int
    public let activeRequests: Int
    public let queuedRequests: Int
    public let cooldownUntil: Date?
    public let recentTraces: [YFRequestTrace]

    public init(
        logicalRequests: Int,
        attempts: Int,
        successes: Int,
        failures: Int,
        retries: Int,
        rateLimits: Int,
        coalescedRequests: Int,
        cacheHits: Int,
        cacheMisses: Int,
        activeRequests: Int,
        queuedRequests: Int,
        cooldownUntil: Date?,
        recentTraces: [YFRequestTrace]
    ) {
        self.logicalRequests = logicalRequests
        self.attempts = attempts
        self.successes = successes
        self.failures = failures
        self.retries = retries
        self.rateLimits = rateLimits
        self.coalescedRequests = coalescedRequests
        self.cacheHits = cacheHits
        self.cacheMisses = cacheMisses
        self.activeRequests = activeRequests
        self.queuedRequests = queuedRequests
        self.cooldownUntil = cooldownUntil
        self.recentTraces = recentTraces
    }
}

/// Coordinates Yahoo request volume above `YFinanceClient` without changing its
/// existing public API. It deliberately does not retry 429 responses. Instead,
/// a rate limit opens a shared cooldown gate that future requests honor.
public actor YFRequestCoordinator {
    private let policy: YFRequestPolicy
    private let clock: any YFClock
    private let jitter: any YFJitterSource

    private struct PermitWaiter {
        let id: UUID
        let continuation: CheckedContinuation<Void, Error>
    }

    private var activeRequests = 0
    private var permitWaiters: [PermitWaiter] = []
    private var preparingPermitWaiterIDs: Set<UUID> = []
    private var cancelledBeforePermitEnqueue: Set<UUID> = []
    private var cooldownUntil: Date?
    private var rateLimitStreak = 0

    private var logicalRequests = 0
    private var attempts = 0
    private var successes = 0
    private var failures = 0
    private var retries = 0
    private var rateLimits = 0
    private var coalescedRequests = 0
    private var cacheHits = 0
    private var cacheMisses = 0
    private var recentTraces: [YFRequestTrace] = []

    public init(
        policy: YFRequestPolicy = .default,
        clock: any YFClock = YFSystemClock(),
        jitter: any YFJitterSource = YFSystemJitterSource()
    ) {
        self.policy = policy
        self.clock = clock
        self.jitter = jitter
    }

    public func execute<T: Sendable>(
        endpoint: String,
        resource: String,
        operation: @Sendable @escaping () async throws -> T
    ) async throws -> T {
        logicalRequests += 1
        let requestStartedAt = await clock.now()
        var attempt = 0

        while true {
            do {
                try Task.checkCancellation()
            } catch {
                let endedAt = await clock.now()
                appendTrace(
                    YFRequestTrace(
                        endpoint: endpoint,
                        resource: resource,
                        startedAt: requestStartedAt,
                        duration: endedAt.timeIntervalSince(requestStartedAt),
                        attempts: attempt,
                        outcome: .cancelled,
                        failureKind: .transport
                    )
                )
                throw error
            }

            do {
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
                // Cooldown may have opened while this request was queued for a
                // global permit. Recheck before the provider operation starts.
                try await waitForCooldown()
                try Task.checkCancellation()
                let value = try await operation()
                releasePermit()
                successes += 1
                rateLimitStreak = 0
                let endedAt = await clock.now()
                appendTrace(
                    YFRequestTrace(
                        endpoint: endpoint,
                        resource: resource,
                        startedAt: requestStartedAt,
                        duration: endedAt.timeIntervalSince(requestStartedAt),
                        attempts: attempt,
                        outcome: .success
                    )
                )
                return value
            } catch is CancellationError {
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
                    rateLimits += 1
                    failures += 1
                    rateLimitStreak += 1
                    if let retryAfter = (error as? YFinanceError)?.retryAfter, retryAfter > 0 {
                        let now = await clock.now()
                        let proposed = now.addingTimeInterval(
                            min(retryAfter, policy.maxRateLimitCooldown)
                        )
                        if cooldownUntil == nil || proposed > cooldownUntil! {
                            cooldownUntil = proposed
                        }
                    } else {
                        await openRateLimitCooldown()
                    }
                    let endedAt = await clock.now()
                    appendTrace(
                        YFRequestTrace(
                            endpoint: endpoint,
                            resource: resource,
                            startedAt: requestStartedAt,
                            duration: endedAt.timeIntervalSince(requestStartedAt),
                            attempts: attempt,
                            outcome: .rateLimited,
                            failureKind: kind
                        )
                    )
                    throw error
                }

                let canRetry = YFinanceErrorClassifier.isTransient(error)
                    && kind != .rateLimited
                    && attempt < policy.maxAttempts

                if canRetry {
                    retries += 1
                    try await sleepForRetry(attempt: attempt)
                    continue
                }

                failures += 1
                let endedAt = await clock.now()
                appendTrace(
                    YFRequestTrace(
                        endpoint: endpoint,
                        resource: resource,
                        startedAt: requestStartedAt,
                        duration: endedAt.timeIntervalSince(requestStartedAt),
                        attempts: attempt,
                        outcome: .failure,
                        failureKind: kind
                    )
                )
                throw error
            }
        }
    }

    /// Allows callers/transports to feed an explicit `Retry-After` signal into
    /// the shared gate. Core HTTP 429 errors also carry this metadata when the
    /// provider sends the header.
    public func noteRateLimit(retryAfter: TimeInterval? = nil) async {
        rateLimits += 1
        rateLimitStreak += 1
        if let retryAfter, retryAfter > 0 {
            let now = await clock.now()
            let proposed = now.addingTimeInterval(min(retryAfter, policy.maxRateLimitCooldown))
            if cooldownUntil == nil || proposed > cooldownUntil! {
                cooldownUntil = proposed
            }
        } else {
            await openRateLimitCooldown()
        }
    }

    public func noteCoalescedRequest() {
        coalescedRequests += 1
    }

    public func noteCacheHit() {
        cacheHits += 1
    }

    public func noteCacheMiss() {
        cacheMisses += 1
    }

    public func clearCooldown() {
        cooldownUntil = nil
        rateLimitStreak = 0
    }

    public func snapshot() -> YFRequestDiagnosticsSnapshot {
        YFRequestDiagnosticsSnapshot(
            logicalRequests: logicalRequests,
            attempts: attempts,
            successes: successes,
            failures: failures,
            retries: retries,
            rateLimits: rateLimits,
            coalescedRequests: coalescedRequests,
            cacheHits: cacheHits,
            cacheMisses: cacheMisses,
            activeRequests: activeRequests,
            queuedRequests: permitWaiters.count + preparingPermitWaiterIDs.count,
            cooldownUntil: cooldownUntil,
            recentTraces: recentTraces
        )
    }

    private func acquirePermit() async throws {
        try Task.checkCancellation()
        if activeRequests < policy.maxConcurrentRequests {
            activeRequests += 1
            return
        }

        let id = UUID()
        preparingPermitWaiterIDs.insert(id)
        let _: Void = try await withTaskCancellationHandler(
            operation: {
                try await withCheckedThrowingContinuation {
                    (continuation: CheckedContinuation<Void, Error>) in
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

    private func waitForCooldown() async throws {
        while let deadline = cooldownUntil {
            let now = await clock.now()
            if now >= deadline {
                cooldownUntil = nil
                return
            }
            try Task.checkCancellation()
            try await clock.sleep(for: deadline.timeIntervalSince(now))
        }
    }

    private func sleepForRetry(attempt: Int) async throws {
        let exponent = max(0, attempt - 1)
        let base = min(
            policy.maxRetryDelay,
            policy.baseRetryDelay * pow(2, Double(exponent))
        )
        let jitterWidth = base * policy.retryJitterFraction
        let extra = jitterWidth > 0 ? jitter.random(in: 0...jitterWidth) : 0
        try await clock.sleep(for: base + extra)
    }

    private func openRateLimitCooldown() async {
        let now = await clock.now()
        let exponent = max(0, min(rateLimitStreak - 1, 8))
        let base = min(
            policy.maxRateLimitCooldown,
            policy.baseRateLimitCooldown * pow(2, Double(exponent))
        )
        let jitterWidth = base * policy.retryJitterFraction
        let extra = jitterWidth > 0 ? jitter.random(in: 0...jitterWidth) : 0
        let proposed = now.addingTimeInterval(min(policy.maxRateLimitCooldown, base + extra))
        if cooldownUntil == nil || proposed > cooldownUntil! {
            cooldownUntil = proposed
        }
    }

    private func appendTrace(_ trace: YFRequestTrace) {
        guard policy.traceCapacity > 0 else { return }
        recentTraces.append(trace)
        if recentTraces.count > policy.traceCapacity {
            recentTraces.removeFirst(recentTraces.count - policy.traceCapacity)
        }
    }
}

public enum YFCacheFreshness: String, Sendable {
    case fresh
    case stale
}

public struct YFCachedResult<Value: Sendable>: Sendable {
    public let value: Value
    public let freshness: YFCacheFreshness
    public let storedAt: Date

    public init(value: Value, freshness: YFCacheFreshness, storedAt: Date) {
        self.value = value
        self.freshness = freshness
        self.storedAt = storedAt
    }
}

public enum YFCacheLookup<Value: Sendable>: Sendable {
    case fresh(value: Value, storedAt: Date)
    case stale(value: Value, storedAt: Date)
    case miss
}

/// Tiny actor cache intended for stale-while-revalidate call sites. Expiration is
/// decided at lookup time so callers can choose different freshness budgets.
public actor YFStaleCache<Key: Hashable & Sendable, Value: Sendable> {
    private struct Entry: Sendable {
        let value: Value
        let storedAt: Date
    }

    private var entries: [Key: Entry] = [:]
    private let capacity: Int

    public init(capacity: Int = 256) {
        self.capacity = max(1, capacity)
    }

    public func lookup(
        _ key: Key,
        freshFor: TimeInterval,
        staleFor: TimeInterval,
        now: Date = Date()
    ) -> YFCacheLookup<Value> {
        guard let entry = entries[key] else { return .miss }
        let age = max(0, now.timeIntervalSince(entry.storedAt))
        let freshWindow = max(0, freshFor)
        let staleWindow = max(freshWindow, staleFor)

        if age <= freshWindow {
            return .fresh(value: entry.value, storedAt: entry.storedAt)
        }
        if age <= staleWindow {
            return .stale(value: entry.value, storedAt: entry.storedAt)
        }

        entries.removeValue(forKey: key)
        return .miss
    }

    public func store(_ value: Value, for key: Key, at date: Date = Date()) {
        if entries.count >= capacity, entries[key] == nil,
           let oldest = entries.min(by: { $0.value.storedAt < $1.value.storedAt })?.key {
            entries.removeValue(forKey: oldest)
        }
        entries[key] = Entry(value: value, storedAt: date)
    }

    public func removeValue(for key: Key) {
        entries.removeValue(forKey: key)
    }

    public func removeAll() {
        entries.removeAll(keepingCapacity: true)
    }

    public var count: Int { entries.count }
}

/// A hardened façade for the highest-volume Yahoo operations. The underlying
/// `YFinanceClient` remains available and unchanged, while this layer centralizes
/// request budgeting, coalescing, cooldowns, retries and SWR caching.
public actor YFResilientClient {
    private let client: YFinanceClient
    private let coordinator: YFRequestCoordinator
    private let clock: any YFClock

    private let quoteCache = YFStaleCache<String, YFQuote?>(capacity: 512)
    private let historyCache = YFStaleCache<String, YFHistorySeries>(capacity: 128)

    private var quoteFlights: [String: Task<YFQuote?, Error>] = [:]
    private var historyFlights: [String: Task<YFHistorySeries, Error>] = [:]
    private var metadataFlights: [String: Task<YFHistoryMetadataResult, Error>] = [:]
    private var financialFlights: [String: Task<YFFinancialStatementSeries, Error>] = [:]
    private var infoFlights: [String: Task<YFJSONValue, Error>] = [:]

    public init(
        client: YFinanceClient = YFinanceClient(),
        policy: YFRequestPolicy = .default,
        clock: any YFClock = YFSystemClock(),
        jitter: any YFJitterSource = YFSystemJitterSource()
    ) {
        self.client = client
        self.clock = clock
        self.coordinator = YFRequestCoordinator(policy: policy, clock: clock, jitter: jitter)
    }

    public func rawClient() -> YFinanceClient {
        client
    }

    /// Runs a lower-volume compatibility operation through the same global
    /// Yahoo coordinator used by quote, history, metadata and financials.
    /// This is intentionally not a second retry layer: callers supply the raw
    /// operation while this actor supplies the one shared concurrency gate,
    /// transient retry policy, diagnostics and 429 cooldown.
    public func perform<T: Sendable>(
        endpoint: String,
        resource: String,
        operation: @Sendable @escaping (YFinanceClient) async throws -> T
    ) async throws -> T {
        let client = self.client
        let coordinator = self.coordinator
        return try await coordinator.execute(endpoint: endpoint, resource: resource) {
            try await operation(client)
        }
    }

    public func quote(symbol: String) async throws -> YFQuote? {
        let symbol = Self.cleanSymbol(symbol)
        if let existing = quoteFlights[symbol] {
            await coordinator.noteCoalescedRequest()
            return try await existing.value
        }

        let client = self.client
        let coordinator = self.coordinator
        let task = Task<YFQuote?, Error> {
            try await coordinator.execute(endpoint: "quote", resource: symbol) {
                try await client.quote(symbol: symbol)
            }
        }
        quoteFlights[symbol] = task
        defer { quoteFlights[symbol] = nil }
        return try await task.value
    }

    public func quoteCached(
        symbol: String,
        freshFor: TimeInterval = 15,
        staleFor: TimeInterval = 5 * 60
    ) async throws -> YFCachedResult<YFQuote?> {
        let symbol = Self.cleanSymbol(symbol)
        let now = await clock.now()
        switch await quoteCache.lookup(symbol, freshFor: freshFor, staleFor: staleFor, now: now) {
        case .fresh(let value, let storedAt):
            await coordinator.noteCacheHit()
            return YFCachedResult(value: value, freshness: .fresh, storedAt: storedAt)
        case .stale(let value, let storedAt):
            await coordinator.noteCacheHit()
            Task { [symbol] in
                _ = try? await self.refreshQuoteCache(symbol: symbol)
            }
            return YFCachedResult(value: value, freshness: .stale, storedAt: storedAt)
        case .miss:
            await coordinator.noteCacheMiss()
            let value = try await refreshQuoteCache(symbol: symbol)
            let storedAt = await clock.now()
            return YFCachedResult(value: value, freshness: .fresh, storedAt: storedAt)
        }
    }

    public func history(
        symbol: String,
        range: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFHistorySeries {
        let symbol = Self.cleanSymbol(symbol)
        let key = Self.historyKey(
            symbol: symbol,
            range: range.rawValue,
            interval: interval.rawValue,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
        )

        if let existing = historyFlights[key] {
            await coordinator.noteCoalescedRequest()
            return try await existing.value
        }

        let client = self.client
        let coordinator = self.coordinator
        let task = Task<YFHistorySeries, Error> {
            try await coordinator.execute(endpoint: "history", resource: "\(symbol):\(range.rawValue):\(interval.rawValue)") {
                try await client.history(
                    symbol: symbol,
                    range: range,
                    interval: interval,
                    includePrePost: includePrePost,
                    events: events,
                    autoAdjust: autoAdjust,
                    backAdjust: backAdjust,
                    repair: repair,
                    keepNa: keepNa,
                    rounding: rounding,
                    timeout: timeout
                )
            }
        }
        historyFlights[key] = task
        defer { historyFlights[key] = nil }
        return try await task.value
    }

    public func history(
        symbol: String,
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFHistorySeries {
        let symbol = Self.cleanSymbol(symbol)
        let key = Self.historyKey(
            symbol: symbol,
            range: "\(Int(start.timeIntervalSince1970))-\(Int(end.timeIntervalSince1970))",
            interval: interval.rawValue,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
        )

        if let existing = historyFlights[key] {
            await coordinator.noteCoalescedRequest()
            return try await existing.value
        }

        let client = self.client
        let coordinator = self.coordinator
        let task = Task<YFHistorySeries, Error> {
            let series = try await coordinator.execute(endpoint: "history", resource: "\(symbol):custom:\(interval.rawValue)") {
                try await client.history(
                    symbol: symbol,
                    start: start,
                    end: end,
                    interval: interval,
                    includePrePost: includePrePost,
                    events: events,
                    autoAdjust: autoAdjust,
                    backAdjust: backAdjust,
                    repair: repair,
                    keepNa: keepNa,
                    rounding: rounding,
                    timeout: timeout
                )
            }
            return series.trimmingEvents(to: start..<end)
        }
        historyFlights[key] = task
        defer { historyFlights[key] = nil }
        return try await task.value
    }

    public func historyCached(
        symbol: String,
        range: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        freshFor: TimeInterval = 60,
        staleFor: TimeInterval = 24 * 60 * 60,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFCachedResult<YFHistorySeries> {
        let symbol = Self.cleanSymbol(symbol)
        let key = Self.historyKey(
            symbol: symbol,
            range: range.rawValue,
            interval: interval.rawValue,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
        )
        let now = await clock.now()

        switch await historyCache.lookup(key, freshFor: freshFor, staleFor: staleFor, now: now) {
        case .fresh(let value, let storedAt):
            await coordinator.noteCacheHit()
            return YFCachedResult(value: value, freshness: .fresh, storedAt: storedAt)
        case .stale(let value, let storedAt):
            await coordinator.noteCacheHit()
            Task {
                _ = try? await self.refreshHistoryCache(
                    cacheKey: key,
                    symbol: symbol,
                    range: range,
                    interval: interval,
                    includePrePost: includePrePost,
                    events: events,
                    autoAdjust: autoAdjust,
                    backAdjust: backAdjust,
                    repair: repair,
                    keepNa: keepNa,
                    rounding: rounding,
                    timeout: timeout
                )
            }
            return YFCachedResult(value: value, freshness: .stale, storedAt: storedAt)
        case .miss:
            await coordinator.noteCacheMiss()
            let value = try await refreshHistoryCache(
                cacheKey: key,
                symbol: symbol,
                range: range,
                interval: interval,
                includePrePost: includePrePost,
                events: events,
                autoAdjust: autoAdjust,
                backAdjust: backAdjust,
                repair: repair,
                keepNa: keepNa,
                rounding: rounding,
                timeout: timeout
            )
            let storedAt = await clock.now()
            return YFCachedResult(value: value, freshness: .fresh, storedAt: storedAt)
        }
    }

    public func historyMetadata(
        symbol: String,
        timeout: TimeInterval? = 10
    ) async throws -> YFHistoryMetadataResult {
        try await historyMetadata(
            symbol: symbol,
            includeTradingPeriods: false,
            timeout: timeout
        )
    }

    public func historyMetadata(
        symbol: String,
        includeTradingPeriods: Bool,
        timeout: TimeInterval? = 10
    ) async throws -> YFHistoryMetadataResult {
        let symbol = Self.cleanSymbol(symbol)
        let flightKey = "\(symbol)|\(includeTradingPeriods ? "trading-periods" : "core")"
        if let existing = metadataFlights[flightKey] {
            await coordinator.noteCoalescedRequest()
            return try await existing.value
        }

        let client = self.client
        let coordinator = self.coordinator
        let task = Task<YFHistoryMetadataResult, Error> {
            try await coordinator.execute(endpoint: "history-metadata", resource: flightKey) {
                try await client.robustHistoryMetadata(
                    symbol: symbol,
                    includeTradingPeriods: includeTradingPeriods,
                    timeout: timeout
                )
            }
        }
        metadataFlights[flightKey] = task
        defer { metadataFlights[flightKey] = nil }
        return try await task.value
    }

    public func financialStatement(
        symbol: String,
        kind: YFFinancialStatementKind,
        frequency: YFFinancialFrequency = .yearly,
        timeout: TimeInterval? = 15
    ) async throws -> YFFinancialStatementSeries {
        let symbol = Self.cleanSymbol(symbol)
        let key = "\(symbol)|\(kind.rawValue)|\(String(describing: frequency))"
        if let existing = financialFlights[key] {
            await coordinator.noteCoalescedRequest()
            return try await existing.value
        }

        let client = self.client
        let coordinator = self.coordinator
        let task = Task<YFFinancialStatementSeries, Error> {
            try await coordinator.execute(endpoint: "financials", resource: "\(symbol):\(kind.rawValue)") {
                try await client.financialStatement(
                    symbol: symbol,
                    kind: kind,
                    frequency: frequency,
                    timeout: timeout
                )
            }
        }
        financialFlights[key] = task
        defer { financialFlights[key] = nil }
        return try await task.value
    }

    public func info(symbol: String) async throws -> YFJSONValue {
        let symbol = Self.cleanSymbol(symbol)
        if let existing = infoFlights[symbol] {
            await coordinator.noteCoalescedRequest()
            return try await existing.value
        }

        let client = self.client
        let coordinator = self.coordinator
        let task = Task<YFJSONValue, Error> {
            try await coordinator.execute(endpoint: "info", resource: symbol) {
                try await client.ticker(symbol).info()
            }
        }
        infoFlights[symbol] = task
        defer { infoFlights[symbol] = nil }
        return try await task.value
    }

    public func diagnostics() async -> YFRequestDiagnosticsSnapshot {
        await coordinator.snapshot()
    }

    public func clearCaches() async {
        await quoteCache.removeAll()
        await historyCache.removeAll()
    }

    public func clearRateLimitCooldown() async {
        await coordinator.clearCooldown()
    }

    private func refreshQuoteCache(symbol: String) async throws -> YFQuote? {
        let value = try await quote(symbol: symbol)
        await quoteCache.store(value, for: symbol, at: await clock.now())
        return value
    }

    private func refreshHistoryCache(
        cacheKey: String,
        symbol: String,
        range: YFinanceClient.Range,
        interval: YFinanceClient.Interval,
        includePrePost: Bool,
        events: Set<YFinanceClient.HistoryEvent>,
        autoAdjust: Bool,
        backAdjust: Bool,
        repair: Bool,
        keepNa: Bool,
        rounding: Bool,
        timeout: TimeInterval?
    ) async throws -> YFHistorySeries {
        let value = try await history(
            symbol: symbol,
            range: range,
            interval: interval,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            timeout: timeout
        )
        await historyCache.store(value, for: cacheKey, at: await clock.now())
        return value
    }

    private static func cleanSymbol(_ symbol: String) -> String {
        symbol.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
    }

    private static func historyKey(
        symbol: String,
        range: String,
        interval: String,
        includePrePost: Bool,
        events: Set<YFinanceClient.HistoryEvent>,
        autoAdjust: Bool,
        backAdjust: Bool,
        repair: Bool,
        keepNa: Bool,
        rounding: Bool
    ) -> String {
        let eventKey = events.map(\.rawValue).sorted().joined(separator: ",")
        return [
            symbol,
            range,
            interval,
            includePrePost ? "prepost" : "regular",
            eventKey,
            autoAdjust ? "auto" : "raw",
            backAdjust ? "back" : "front",
            repair ? "repair" : "norepair",
            keepNa ? "keepna" : "dropna",
            rounding ? "round" : "noround",
        ].joined(separator: "|")
    }
}
