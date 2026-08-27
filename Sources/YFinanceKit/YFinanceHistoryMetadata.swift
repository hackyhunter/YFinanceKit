import Foundation

public struct YFHistoryMetadataResult: Sendable {
    public let metadata: YFJSONValue
    /// Interval used for the core metadata fetch. Since yfinance 1.7.0 parity,
    /// this is normally `.oneDay`; optional trading-period enrichment may make
    /// an additional 1h request without replacing the daily metadata.
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
    /// Fetches core history metadata from daily chart data and only performs the
    /// intraday request needed for Yahoo `tradingPeriods` when explicitly asked.
    ///
    /// This mirrors yfinance 1.7.0 / PR #2922. Python can lazy-load a dictionary
    /// key synchronously; Swift cannot hide async network I/O behind a JSON
    /// subscript, so callers opt into that enrichment with `includeTradingPeriods`.
    func robustHistoryMetadata(
        symbol: String,
        includeTradingPeriods: Bool = false,
        timeout: TimeInterval? = 10
    ) async throws -> YFHistoryMetadataResult {
        let cleanedSymbol = symbol.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        let raw = try await historyRaw(
            symbol: cleanedSymbol,
            range: .fiveDays,
            interval: .oneDay,
            includePrePost: false,
            events: [],
            timeout: timeout
        )

        if let reason = raw["chart"]?["error"]?["description"]?.stringValue,
           !reason.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            throw YFinanceError.missingData(reason)
        }

        guard let result = raw["chart"]?["result"]?[0],
              let baseMeta = result["meta"],
              var metadata = baseMeta.objectValue else {
            throw YFinanceError.missingData("No history metadata returned for \(cleanedSymbol)")
        }

        func hasUsableTradingPeriods(_ value: YFJSONValue?) -> Bool {
            guard let value else { return false }
            switch value {
            case .null:
                return false
            case .array(let values):
                return !values.isEmpty
            case .object(let values):
                return !values.isEmpty
            default:
                return true
            }
        }

        var hasTradingPeriods = hasUsableTradingPeriods(metadata["tradingPeriods"])

        if includeTradingPeriods, !hasTradingPeriods {
            do {
                let intraday = try await historyRaw(
                    symbol: cleanedSymbol,
                    range: .fiveDays,
                    interval: .oneHour,
                    includePrePost: true,
                    events: [],
                    timeout: timeout
                )

                if let intradayMeta = intraday["chart"]?["result"]?[0]?["meta"],
                   let tradingPeriods = intradayMeta["tradingPeriods"],
                   hasUsableTradingPeriods(tradingPeriods) {
                    // Preserve daily/core metadata. The intraday response exists only
                    // to supply the one field Yahoo withholds from daily chart data.
                    metadata["tradingPeriods"] = tradingPeriods
                    hasTradingPeriods = true
                }
            } catch {
                // Optional enrichment must not make otherwise-valid metadata fail.
                // Do not swallow structured task cancellation.
                try Task.checkCancellation()
            }
        }

        return YFHistoryMetadataResult(
            metadata: .object(metadata),
            intervalUsed: .oneDay,
            hasTradingPeriods: hasTradingPeriods
        )
    }
}

public extension YF {
    static func historyMetadata(
        _ symbol: String,
        includeTradingPeriods: Bool = false,
        timeout: TimeInterval? = 10,
        client: YFinanceClient = YFinanceClient()
    ) async throws -> YFHistoryMetadataResult {
        try await client.robustHistoryMetadata(
            symbol: symbol,
            includeTradingPeriods: includeTradingPeriods,
            timeout: timeout
        )
    }
}
