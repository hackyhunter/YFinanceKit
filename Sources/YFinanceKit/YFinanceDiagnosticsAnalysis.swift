import Foundation

public struct YFEndpointDiagnosticsSummary: Codable, Sendable, Equatable {
    public let endpoint: String
    public let requestCount: Int
    public let successes: Int
    public let failures: Int
    public let rateLimited: Int
    public let cancelled: Int
    public let totalAttempts: Int
    public let maxAttempts: Int
    public let averageDurationMilliseconds: Int
    public let maxDurationMilliseconds: Int
    public let failureKinds: [String: Int]

    public init(
        endpoint: String,
        requestCount: Int,
        successes: Int,
        failures: Int,
        rateLimited: Int,
        cancelled: Int,
        totalAttempts: Int,
        maxAttempts: Int,
        averageDurationMilliseconds: Int,
        maxDurationMilliseconds: Int,
        failureKinds: [String: Int]
    ) {
        self.endpoint = endpoint
        self.requestCount = requestCount
        self.successes = successes
        self.failures = failures
        self.rateLimited = rateLimited
        self.cancelled = cancelled
        self.totalAttempts = totalAttempts
        self.maxAttempts = maxAttempts
        self.averageDurationMilliseconds = averageDurationMilliseconds
        self.maxDurationMilliseconds = maxDurationMilliseconds
        self.failureKinds = failureKinds
    }
}

public extension YFRequestDiagnosticsSnapshot {
    func endpointSummaries() -> [YFEndpointDiagnosticsSummary] {
        let groups = Dictionary(grouping: recentTraces, by: \.endpoint)
        return groups.keys.sorted().compactMap { endpoint in
            guard let traces = groups[endpoint], !traces.isEmpty else { return nil }
            var successes = 0
            var failures = 0
            var rateLimited = 0
            var cancelled = 0
            var totalAttempts = 0
            var maxAttempts = 0
            var totalDurationMs = 0
            var maxDurationMs = 0
            var failureKinds: [String: Int] = [:]

            for trace in traces {
                switch trace.outcome {
                case .success: successes += 1
                case .failure: failures += 1
                case .rateLimited: rateLimited += 1
                case .cancelled: cancelled += 1
                }
                totalAttempts += trace.attempts
                maxAttempts = max(maxAttempts, trace.attempts)
                let durationMs = max(0, Int((trace.duration * 1_000).rounded()))
                totalDurationMs += durationMs
                maxDurationMs = max(maxDurationMs, durationMs)
                if let failureKind = trace.failureKind?.rawValue {
                    failureKinds[failureKind, default: 0] += 1
                }
            }

            return YFEndpointDiagnosticsSummary(
                endpoint: endpoint,
                requestCount: traces.count,
                successes: successes,
                failures: failures,
                rateLimited: rateLimited,
                cancelled: cancelled,
                totalAttempts: totalAttempts,
                maxAttempts: maxAttempts,
                averageDurationMilliseconds: totalDurationMs / traces.count,
                maxDurationMilliseconds: maxDurationMs,
                failureKinds: failureKinds
            )
        }
    }
}
