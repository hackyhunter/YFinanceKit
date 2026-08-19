import Foundation

struct YFStoredHistoryMetadata: Sendable {
    let metadata: YFJSONValue
    let intervalUsed: YFinanceClient.Interval
}

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

private func yfMetadataHasTradingPeriods(_ metadata: YFJSONValue) -> Bool {
    guard let tradingPeriods = metadata["tradingPeriods"] else { return false }
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
}

extension YFinanceClient {
    /// Records the exact raw Yahoo metadata from a successful typed chart
    /// response without issuing another request.
    func recordHistoryMetadataSnapshotIfPresent(
        data: Data,
        path: String,
        queryItems: [URLQueryItem]
    ) {
        let prefix = "/v8/finance/chart/"
        guard path.hasPrefix(prefix) else { return }

        let rawSymbol = String(path.dropFirst(prefix.count))
        let symbol = (rawSymbol.removingPercentEncoding ?? rawSymbol)
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .uppercased()
        guard !symbol.isEmpty,
              let raw = try? YFJSONValue.decode(data: data),
              let incomingMetadata = raw["chart"]?["result"]?[0]?["meta"],
              var incomingObject = incomingMetadata.objectValue else {
            return
        }

        // Keep a previously enriched trading-period payload when a later normal
        // history request refreshes the core metadata without that optional key.
        if incomingObject["tradingPeriods"] == nil,
           let cachedTradingPeriods = historyMetadataSnapshots[symbol]?
               .metadata["tradingPeriods"] {
            incomingObject["tradingPeriods"] = cachedTradingPeriods
        }

        let interval = queryItems
            .first(where: { $0.name == "interval" })?
            .value
            .flatMap { YFinanceClient.Interval(pythonValue: $0) }
            ?? .oneDay

        historyMetadataSnapshots[symbol] = YFStoredHistoryMetadata(
            metadata: .object(incomingObject),
            intervalUsed: interval
        )
    }
}

public extension YFinanceClient {
    /// Returns core history metadata without paying for intraday enrichment.
    /// If a normal chart request already ran on this client, its Yahoo `meta`
    /// object is reused and this method performs no chart request.
    func robustHistoryMetadata(
        symbol: String,
        timeout: TimeInterval? = 10
    ) async throws -> YFHistoryMetadataResult {
        try await robustHistoryMetadata(
            symbol: symbol,
            includeTradingPeriods: false,
            timeout: timeout
        )
    }

    /// Returns history metadata, optionally enriching `tradingPeriods` with the
    /// `5d/1h` request Yahoo requires for that field. Intraday enrichment is
    /// best-effort and never invalidates otherwise-valid core metadata.
    func robustHistoryMetadata(
        symbol: String,
        includeTradingPeriods: Bool,
        timeout: TimeInterval? = 10
    ) async throws -> YFHistoryMetadataResult {
        let symbol = symbol
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .uppercased()
        guard !symbol.isEmpty else {
            throw YFinanceError.invalidRequest("symbol cannot be empty")
        }

        if let cached = historyMetadataSnapshots[symbol] {
            return await historyMetadataResult(
                symbol: symbol,
                base: cached,
                includeTradingPeriods: includeTradingPeriods,
                timeout: timeout
            )
        }

        let raw = try await historyRaw(
            symbol: symbol,
            range: .fiveDays,
            interval: .oneDay,
            includePrePost: true,
            events: [],
            timeout: timeout
        )

        if let reason = raw["chart"]?["error"]?["description"]?.stringValue,
           !reason.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            throw YFinanceError.missingData(reason)
        }

        guard let metadata = raw["chart"]?["result"]?[0]?["meta"],
              metadata.objectValue != nil else {
            throw YFinanceError.missingData(
                "No history metadata returned for \(symbol)"
            )
        }

        let base = YFStoredHistoryMetadata(
            metadata: metadata,
            intervalUsed: .oneDay
        )
        historyMetadataSnapshots[symbol] = base

        return await historyMetadataResult(
            symbol: symbol,
            base: base,
            includeTradingPeriods: includeTradingPeriods,
            timeout: timeout
        )
    }

    private func historyMetadataResult(
        symbol: String,
        base: YFStoredHistoryMetadata,
        includeTradingPeriods: Bool,
        timeout: TimeInterval?
    ) async -> YFHistoryMetadataResult {
        let baseHasTradingPeriods = yfMetadataHasTradingPeriods(base.metadata)
        if !includeTradingPeriods || baseHasTradingPeriods {
            return YFHistoryMetadataResult(
                metadata: base.metadata,
                intervalUsed: base.intervalUsed,
                hasTradingPeriods: baseHasTradingPeriods
            )
        }

        do {
            let raw = try await historyRaw(
                symbol: symbol,
                range: .fiveDays,
                interval: .oneHour,
                includePrePost: true,
                events: [],
                timeout: timeout
            )

            guard let intradayMetadata = raw["chart"]?["result"]?[0]?["meta"],
                  let tradingPeriods = intradayMetadata["tradingPeriods"],
                  var merged = base.metadata.objectValue else {
                return YFHistoryMetadataResult(
                    metadata: base.metadata,
                    intervalUsed: base.intervalUsed,
                    hasTradingPeriods: false
                )
            }

            merged["tradingPeriods"] = tradingPeriods
            let enrichedMetadata = YFJSONValue.object(merged)
            guard yfMetadataHasTradingPeriods(enrichedMetadata) else {
                return YFHistoryMetadataResult(
                    metadata: base.metadata,
                    intervalUsed: base.intervalUsed,
                    hasTradingPeriods: false
                )
            }

            let enriched = YFStoredHistoryMetadata(
                metadata: enrichedMetadata,
                intervalUsed: .oneHour
            )
            historyMetadataSnapshots[symbol] = enriched
            return YFHistoryMetadataResult(
                metadata: enriched.metadata,
                intervalUsed: enriched.intervalUsed,
                hasTradingPeriods: true
            )
        } catch {
            return YFHistoryMetadataResult(
                metadata: base.metadata,
                intervalUsed: base.intervalUsed,
                hasTradingPeriods: false
            )
        }
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

    static func historyMetadata(
        _ symbol: String,
        includeTradingPeriods: Bool,
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
