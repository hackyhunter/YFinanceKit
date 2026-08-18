import Foundation

/// Narrow dependency-injection surface for high-volume Yahoo operations.
///
/// App code should depend on this protocol rather than the concrete
/// `YFinanceClient` actor. That keeps the compatibility client behind a façade
/// and lets endpoint mappers/tests substitute deterministic providers.
public protocol YFResilientMarketDataProviding: Sendable {
    func quote(symbol: String) async throws -> YFQuote?

    func quoteCached(
        symbol: String,
        freshFor: TimeInterval,
        staleFor: TimeInterval
    ) async throws -> YFCachedResult<YFQuote?>

    func history(
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
    ) async throws -> YFHistorySeries

    func historyCached(
        symbol: String,
        range: YFinanceClient.Range,
        interval: YFinanceClient.Interval,
        freshFor: TimeInterval,
        staleFor: TimeInterval,
        includePrePost: Bool,
        events: Set<YFinanceClient.HistoryEvent>,
        autoAdjust: Bool,
        backAdjust: Bool,
        repair: Bool,
        keepNa: Bool,
        rounding: Bool,
        timeout: TimeInterval?
    ) async throws -> YFCachedResult<YFHistorySeries>

    func historyMetadata(
        symbol: String,
        timeout: TimeInterval?
    ) async throws -> YFHistoryMetadataResult

    func financialStatement(
        symbol: String,
        kind: YFFinancialStatementKind,
        frequency: YFFinancialFrequency,
        timeout: TimeInterval?
    ) async throws -> YFFinancialStatementSeries

    func info(symbol: String) async throws -> YFJSONValue
    func diagnostics() async -> YFRequestDiagnosticsSnapshot
    func clearCaches() async
    func clearRateLimitCooldown() async
}

extension YFResilientClient: YFResilientMarketDataProviding {}

/// Smaller read-only protocols are useful for focused consumers that should not
/// accidentally grow into an all-endpoint Yahoo client.
public protocol YFQuoteProviding: Sendable {
    func quote(symbol: String) async throws -> YFQuote?
}

public protocol YFCachedQuoteProviding: YFQuoteProviding {
    func quoteCached(
        symbol: String,
        freshFor: TimeInterval,
        staleFor: TimeInterval
    ) async throws -> YFCachedResult<YFQuote?>
}

public protocol YFHistoryProviding: Sendable {
    func history(
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
    ) async throws -> YFHistorySeries
}

public protocol YFCachedHistoryProviding: YFHistoryProviding {
    func historyCached(
        symbol: String,
        range: YFinanceClient.Range,
        interval: YFinanceClient.Interval,
        freshFor: TimeInterval,
        staleFor: TimeInterval,
        includePrePost: Bool,
        events: Set<YFinanceClient.HistoryEvent>,
        autoAdjust: Bool,
        backAdjust: Bool,
        repair: Bool,
        keepNa: Bool,
        rounding: Bool,
        timeout: TimeInterval?
    ) async throws -> YFCachedResult<YFHistorySeries>
}

public protocol YFHistoryMetadataProviding: Sendable {
    func historyMetadata(
        symbol: String,
        timeout: TimeInterval?
    ) async throws -> YFHistoryMetadataResult
}

public protocol YFFinancialStatementProviding: Sendable {
    func financialStatement(
        symbol: String,
        kind: YFFinancialStatementKind,
        frequency: YFFinancialFrequency,
        timeout: TimeInterval?
    ) async throws -> YFFinancialStatementSeries
}

extension YFResilientClient:
    YFQuoteProviding,
    YFCachedQuoteProviding,
    YFHistoryProviding,
    YFCachedHistoryProviding,
    YFHistoryMetadataProviding,
    YFFinancialStatementProviding
{}
