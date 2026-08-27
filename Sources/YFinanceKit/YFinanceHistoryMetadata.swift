import Foundation

struct YFStoredHistoryMetadata: Sendable {
    let metadata: YFJSONValue
    let intervalUsed: YFinanceClient.Interval
}

public struct YFHistoryMetadataResult: Sendable {
    public let metadata: YFJSONValue
    /// Interval used for the core metadata source. Since yfinance 1.7.0 parity,
    /// this is normally `.oneDay`; optional trading-period enrichment may make
    /// an additional 1h request without replacing the core metadata.
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

final class YFDecodedHistoryMetadataStore: @unchecked Sendable {
    static let shared = YFDecodedHistoryMetadataStore()

    private struct Entry {
        let snapshot: YFStoredHistoryMetadata
        let recordedAt: Date
    }

    private let lock = NSLock()
    private var entries: [String: Entry] = [:]
    private let ttl: TimeInterval = 5 * 60

    private init() {}

    func record(_ meta: YFHistoryMeta) {
        guard let symbol = Self.cleanSymbol(meta.symbol) else { return }

        var object: [String: YFJSONValue] = [:]
        Self.set(meta.currency, key: "currency", in: &object)
        Self.set(meta.symbol, key: "symbol", in: &object)
        Self.set(meta.exchangeName, key: "exchangeName", in: &object)
        Self.set(meta.instrumentType, key: "instrumentType", in: &object)
        Self.set(meta.timezone, key: "timezone", in: &object)
        Self.set(meta.exchangeTimezoneName, key: "exchangeTimezoneName", in: &object)
        Self.set(meta.regularMarketPrice, key: "regularMarketPrice", in: &object)
        Self.set(meta.chartPreviousClose, key: "chartPreviousClose", in: &object)
        Self.set(meta.previousClose, key: "previousClose", in: &object)
        Self.set(meta.gmtoffset, key: "gmtoffset", in: &object)
        Self.set(meta.dataGranularity, key: "dataGranularity", in: &object)
        Self.set(meta.priceHint, key: "priceHint", in: &object)
        Self.set(meta.range, key: "range", in: &object)

        if let validRanges = meta.validRanges {
            object["validRanges"] = .array(validRanges.map { .string($0) })
        }
        if let lastTrade = meta.lastTrade {
            var value: [String: YFJSONValue] = [:]
            Self.set(lastTrade.price, key: "price", in: &value)
            Self.set(lastTrade.time, key: "time", in: &value)
            object["lastTrade"] = .object(value)
        }

        let interval = meta.dataGranularity
            .flatMap { YFinanceClient.Interval(pythonValue: $0) }
            ?? .oneDay
        record(
            metadata: .object(object),
            intervalUsed: interval,
            symbol: symbol
        )
    }

    @discardableResult
    func record(
        metadata: YFJSONValue,
        intervalUsed: YFinanceClient.Interval,
        symbol: String
    ) -> YFStoredHistoryMetadata {
        let cleaned = Self.cleanSymbol(symbol) ?? symbol
        var storedMetadata = metadata

        lock.lock()
        defer { lock.unlock() }

        if var object = metadata.objectValue,
           object["tradingPeriods"] == nil,
           let cachedTradingPeriods = entries[cleaned]?.snapshot
               .metadata["tradingPeriods"] {
            object["tradingPeriods"] = cachedTradingPeriods
            storedMetadata = .object(object)
        }

        let snapshot = YFStoredHistoryMetadata(
            metadata: storedMetadata,
            intervalUsed: intervalUsed
        )
        entries[cleaned] = Entry(snapshot: snapshot, recordedAt: Date())
        return snapshot
    }

    func lookup(_ symbol: String) -> YFStoredHistoryMetadata? {
        guard let cleaned = Self.cleanSymbol(symbol) else { return nil }
        let now = Date()

        lock.lock()
        defer { lock.unlock() }

        guard let entry = entries[cleaned] else { return nil }
        guard now.timeIntervalSince(entry.recordedAt) <= ttl else {
            entries.removeValue(forKey: cleaned)
            return nil
        }
        return entry.snapshot
    }

    private static func cleanSymbol(_ symbol: String?) -> String? {
        guard let symbol else { return nil }
        let cleaned = symbol
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .uppercased()
        return cleaned.isEmpty ? nil : cleaned
    }

    private static func set(
        _ value: String?,
        key: String,
        in object: inout [String: YFJSONValue]
    ) {
        if let value { object[key] = .string(value) }
    }

    private static func set(
        _ value: Double?,
        key: String,
        in object: inout [String: YFJSONValue]
    ) {
        if let value, value.isFinite { object[key] = .number(value) }
    }

    private static func set(
        _ value: Int?,
        key: String,
        in object: inout [String: YFJSONValue]
    ) {
        if let value { object[key] = .number(Double(value)) }
    }
}

extension YFHistoryMeta {
    public init(from decoder: Decoder) throws {
        enum CodingKeys: String, CodingKey {
            case currency
            case symbol
            case exchangeName
            case instrumentType
            case timezone
            case exchangeTimezoneName
            case regularMarketPrice
            case chartPreviousClose
            case previousClose
            case gmtoffset
            case dataGranularity
            case priceHint
            case range
            case validRanges
            case lastTrade
            case tradingPeriods
        }

        let container = try decoder.container(keyedBy: CodingKeys.self)
        currency = try container.decodeIfPresent(String.self, forKey: .currency)
        symbol = try container.decodeIfPresent(String.self, forKey: .symbol)
        exchangeName = try container.decodeIfPresent(String.self, forKey: .exchangeName)
        instrumentType = try container.decodeIfPresent(String.self, forKey: .instrumentType)
        timezone = try container.decodeIfPresent(String.self, forKey: .timezone)
        exchangeTimezoneName = try container.decodeIfPresent(String.self, forKey: .exchangeTimezoneName)
        regularMarketPrice = try container.decodeIfPresent(Double.self, forKey: .regularMarketPrice)
        chartPreviousClose = try container.decodeIfPresent(Double.self, forKey: .chartPreviousClose)
        previousClose = try container.decodeIfPresent(Double.self, forKey: .previousClose)
        gmtoffset = try container.decodeIfPresent(Int.self, forKey: .gmtoffset)
        dataGranularity = try container.decodeIfPresent(String.self, forKey: .dataGranularity)
        priceHint = try container.decodeIfPresent(Int.self, forKey: .priceHint)
        range = try container.decodeIfPresent(String.self, forKey: .range)
        validRanges = try container.decodeIfPresent([String].self, forKey: .validRanges)
        lastTrade = try container.decodeIfPresent(YFHistoryLastTrade.self, forKey: .lastTrade)
        tradingPeriods = try container.decodeIfPresent(YFTradingPeriods.self, forKey: .tradingPeriods)

        YFDecodedHistoryMetadataStore.shared.record(self)
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

public extension YFinanceClient {
    /// Returns core history metadata without paying for intraday enrichment.
    /// A recent typed chart/history response for this symbol is reused when
    /// available; otherwise core metadata comes from a `5d/1d` chart request.
    ///
    /// This mirrors yfinance 1.7.0 / PR #2922. Swift callers explicitly opt into
    /// the asynchronous `tradingPeriods` enrichment request.
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

        if let cached = YFDecodedHistoryMetadataStore.shared.lookup(symbol) {
            return try await historyMetadataResult(
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
            includePrePost: false,
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

        let base = YFDecodedHistoryMetadataStore.shared.record(
            metadata: metadata,
            intervalUsed: .oneDay,
            symbol: symbol
        )

        return try await historyMetadataResult(
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
    ) async throws -> YFHistoryMetadataResult {
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

            let enriched = YFDecodedHistoryMetadataStore.shared.record(
                metadata: enrichedMetadata,
                intervalUsed: base.intervalUsed,
                symbol: symbol
            )
            return YFHistoryMetadataResult(
                metadata: enriched.metadata,
                intervalUsed: enriched.intervalUsed,
                hasTradingPeriods: true
            )
        } catch {
            // Optional enrichment must not invalidate valid core metadata, but
            // structured task cancellation must still escape.
            try Task.checkCancellation()
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
