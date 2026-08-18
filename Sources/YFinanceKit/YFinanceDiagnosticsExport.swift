import Foundation

/// Codable diagnostics intended for local debugging/telemetry surfaces.
/// Deliberately excludes URLs, headers, cookies, crumbs, request bodies and
/// response payloads.
public struct YFDiagnosticsExport: Codable, Sendable {
    public let generatedAt: Date
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
    public let traces: [YFDiagnosticsTraceExport]

    public init(
        snapshot: YFRequestDiagnosticsSnapshot,
        generatedAt: Date = Date()
    ) {
        self.generatedAt = generatedAt
        self.logicalRequests = snapshot.logicalRequests
        self.attempts = snapshot.attempts
        self.successes = snapshot.successes
        self.failures = snapshot.failures
        self.retries = snapshot.retries
        self.rateLimits = snapshot.rateLimits
        self.coalescedRequests = snapshot.coalescedRequests
        self.cacheHits = snapshot.cacheHits
        self.cacheMisses = snapshot.cacheMisses
        self.activeRequests = snapshot.activeRequests
        self.queuedRequests = snapshot.queuedRequests
        self.cooldownUntil = snapshot.cooldownUntil
        self.traces = snapshot.recentTraces.map(YFDiagnosticsTraceExport.init)
    }
}

public struct YFDiagnosticsTraceExport: Codable, Sendable {
    public let endpoint: String
    public let resource: String
    public let startedAt: Date
    public let durationMilliseconds: Int
    public let attempts: Int
    public let outcome: String
    public let failureKind: String?

    public init(_ trace: YFRequestTrace) {
        self.endpoint = trace.endpoint
        self.resource = trace.resource
        self.startedAt = trace.startedAt
        self.durationMilliseconds = max(0, Int((trace.duration * 1_000).rounded()))
        self.attempts = trace.attempts
        self.outcome = trace.outcome.rawValue
        self.failureKind = trace.failureKind?.rawValue
    }
}

public extension YFRequestDiagnosticsSnapshot {
    func redactedExport(generatedAt: Date = Date()) -> YFDiagnosticsExport {
        YFDiagnosticsExport(snapshot: self, generatedAt: generatedAt)
    }
}

public extension YFResilientClient {
    func diagnosticsExport(generatedAt: Date = Date()) async -> YFDiagnosticsExport {
        let snapshot = await diagnostics()
        return snapshot.redactedExport(generatedAt: generatedAt)
    }
}
