import Foundation

/// Focused quote façade over the resilient provider. Consumers that only need
/// quotes do not need to depend on the entire Yahoo compatibility client.
public struct YFQuoteService: Sendable {
    private let provider: any YFCachedQuoteProviding

    public init(provider: any YFCachedQuoteProviding) {
        self.provider = provider
    }

    public func fetch(_ symbol: String) async throws -> YFQuote? {
        try await provider.quote(symbol: symbol)
    }

    public func cached(
        _ symbol: String,
        freshFor: TimeInterval = 15,
        staleFor: TimeInterval = 5 * 60
    ) async throws -> YFCachedResult<YFQuote?> {
        try await provider.quoteCached(
            symbol: symbol,
            freshFor: freshFor,
            staleFor: staleFor
        )
    }
}

/// Focused chart/history façade. Repair, integrity and app presentation policy
/// can evolve here without growing the compatibility client further.
public struct YFHistoryService: Sendable {
    private let provider: any YFCachedHistoryProviding

    public init(provider: any YFCachedHistoryProviding) {
        self.provider = provider
    }

    public func fetch(
        _ symbol: String,
        range: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = 12
    ) async throws -> YFHistorySeries {
        try await provider.history(
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

    public func cached(
        _ symbol: String,
        range: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        freshFor: TimeInterval = 60,
        staleFor: TimeInterval = 24 * 60 * 60,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = 12
    ) async throws -> YFCachedResult<YFHistorySeries> {
        try await provider.historyCached(
            symbol: symbol,
            range: range,
            interval: interval,
            freshFor: freshFor,
            staleFor: staleFor,
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

public struct YFHistoryMetadataService: Sendable {
    private let provider: any YFHistoryMetadataProviding

    public init(provider: any YFHistoryMetadataProviding) {
        self.provider = provider
    }

    public func fetch(
        _ symbol: String,
        timeout: TimeInterval? = 10
    ) async throws -> YFHistoryMetadataResult {
        try await provider.historyMetadata(symbol: symbol, timeout: timeout)
    }
}

public struct YFFinancialStatementService: Sendable {
    private let provider: any YFFinancialStatementProviding

    public init(provider: any YFFinancialStatementProviding) {
        self.provider = provider
    }

    public func fetch(
        _ symbol: String,
        kind: YFFinancialStatementKind,
        frequency: YFFinancialFrequency = .yearly,
        timeout: TimeInterval? = 15
    ) async throws -> YFFinancialStatementSeries {
        try await provider.financialStatement(
            symbol: symbol,
            kind: kind,
            frequency: frequency,
            timeout: timeout
        )
    }
}

/// Convenient bundle for apps that want one shared resilient session but
/// feature-specific dependencies internally.
public struct YFMarketDataServices: Sendable {
    public let quotes: YFQuoteService
    public let history: YFHistoryService
    public let metadata: YFHistoryMetadataService
    public let financials: YFFinancialStatementService

    public init(client: YFResilientClient) {
        self.quotes = YFQuoteService(provider: client)
        self.history = YFHistoryService(provider: client)
        self.metadata = YFHistoryMetadataService(provider: client)
        self.financials = YFFinancialStatementService(provider: client)
    }
}
