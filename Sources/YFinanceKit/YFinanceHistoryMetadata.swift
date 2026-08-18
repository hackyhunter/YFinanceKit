import Foundation

public struct YFHistoryMetadataResult: Sendable {
    public let metadata: YFJSONValue
    public let intervalUsed: YFinanceClient.Interval
    public let hasTradingPeriods: Bool

    public init(
        metadata: YFJSONValue,
        intervalUsed: YFinanceClient.Interval,
        hasTradingPeriods: Bool
    ) {
        self.metadata = metadata
        self.intervalUsed = intervalUsed
        self.hasTradingPeriods = hasTradingPeriods
    }
}

public extension YFinanceClient {
    /// Fetches history metadata without making optional intraday enrichment a
    /// single point of failure.
    ///
    /// Python yfinance has had regressions where `get_history_metadata()` failed
    /// for valid tickers whose 1h history was unavailable. Swift first asks for 1h
    /// so Yahoo can include `tradingPeriods`, then falls back to daily history for
    /// core metadata such as currency, exchange and timezone.
    func robustHistoryMetadata(
        symbol: String,
        timeout: TimeInterval? = 10
    ) async throws -> YFHistoryMetadataResult {
        let intervals: [Interval] = [.oneHour, .oneDay]
        var lastError: Error?
        var lastYahooReason: String?

        for interval in intervals {
            do {
                let raw = try await historyRaw(
                    symbol: symbol,
                    range: .fiveDays,
                    interval: interval,
                    includePrePost: true,
                    events: [],
                    timeout: timeout
                )

                if let reason = raw["chart"]?["error"]?["description"]?.stringValue,
                   !reason.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    lastYahooReason = reason
                    continue
                }

                guard let result = raw["chart"]?["result"]?[0],
                      let meta = result["meta"],
                      meta.objectValue != nil else {
                    continue
                }

                let hasTradingPeriods: Bool = {
                    guard let tradingPeriods = meta["tradingPeriods"] else { return false }
                    switch tradingPeriods {
                    case .null:
                        return false
                    case .array(let values):
                        return !values.isEmpty
                    case .object(let values):
                        return !values.isEmpty
                    default:
                        return true
                    }
                }()

                return YFHistoryMetadataResult(
                    metadata: meta,
                    intervalUsed: interval,
                    hasTradingPeriods: hasTradingPeriods
                )
            } catch {
                lastError = error
            }
        }

        if let lastError { throw lastError }
        if let lastYahooReason {
            throw YFinanceError.missingData(lastYahooReason)
        }
        throw YFinanceError.missingData("No history metadata returned for \(symbol.uppercased())")
    }
}

public extension YF {
    static func historyMetadata(
        _ symbol: String,
        timeout: TimeInterval? = 10,
        client: YFinanceClient = YFinanceClient()
    ) async throws -> YFHistoryMetadataResult {
        try await client.robustHistoryMetadata(symbol: symbol, timeout: timeout)
    }
}
