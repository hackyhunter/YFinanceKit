import Foundation

extension YFTicker {
    /// Select "gaap", "normalized" (non-GAAP), or nil for Yahoo's default.
    /// The explicit overload preserves the existing no-argument API and schema.
    public func earningsEstimate(methodology: String?) async throws -> YFJSONValue {
        try await earningsAnalysis(key: "earningsEstimate", methodology: methodology)
    }

    public func epsTrend(methodology: String?) async throws -> YFJSONValue {
        try await earningsAnalysis(key: "epsTrend", methodology: methodology)
    }

    public func epsRevisions(methodology: String?) async throws -> YFJSONValue {
        try await earningsAnalysis(key: "epsRevisions", methodology: methodology)
    }

    public func earningsHistory(methodology: String?) async throws -> YFJSONValue {
        try await earningsAnalysis(key: "history", methodology: methodology)
    }

    public func earningsEstimateTable(methodology: String?) async throws -> YFTable {
        try await earningsEstimate(methodology: methodology).toTable()
    }
    public func epsTrendTable(methodology: String?) async throws -> YFTable {
        try await epsTrend(methodology: methodology).toTable()
    }
    public func epsRevisionsTable(methodology: String?) async throws -> YFTable {
        try await epsRevisions(methodology: methodology).toTable()
    }
    public func earningsHistoryTable(methodology: String?) async throws -> YFTable {
        try await earningsHistory(methodology: methodology).toTable()
    }

    public func getEarningsEstimate(asDict _: Bool = false, methodology: String?) async throws -> YFJSONValue {
        try await earningsEstimate(methodology: methodology)
    }
    public func getEarningsHistory(asDict _: Bool = false, methodology: String?) async throws -> YFJSONValue {
        try await earningsHistory(methodology: methodology)
    }
    public func getEpsTrend(asDict _: Bool = false, methodology: String?) async throws -> YFJSONValue {
        try await epsTrend(methodology: methodology)
    }
    public func getEpsRevisions(asDict _: Bool = false, methodology: String?) async throws -> YFJSONValue {
        try await epsRevisions(methodology: methodology)
    }
    public func get_earnings_estimate(as_dict _: Bool = false, methodology: String?) async throws -> YFJSONValue {
        try await earningsEstimate(methodology: methodology)
    }
    public func get_earnings_history(as_dict _: Bool = false, methodology: String?) async throws -> YFJSONValue {
        try await earningsHistory(methodology: methodology)
    }
    public func get_eps_trend(as_dict _: Bool = false, methodology: String?) async throws -> YFJSONValue {
        try await epsTrend(methodology: methodology)
    }
    public func get_eps_revisions(as_dict _: Bool = false, methodology: String?) async throws -> YFJSONValue {
        try await epsRevisions(methodology: methodology)
    }
    public func earnings_estimate(methodology: String?) async throws -> YFJSONValue {
        try await earningsEstimate(methodology: methodology)
    }
    public func earnings_history(methodology: String?) async throws -> YFJSONValue {
        try await earningsHistory(methodology: methodology)
    }
    public func eps_trend(methodology: String?) async throws -> YFJSONValue {
        try await epsTrend(methodology: methodology)
    }
    public func eps_revisions(methodology: String?) async throws -> YFJSONValue {
        try await epsRevisions(methodology: methodology)
    }

    private func earningsAnalysis(key: String, methodology: String?) async throws -> YFJSONValue {
        try await YFEarningsMethodologyAnalysis.fetch(key: key, methodology: methodology) { modules in
            try await self.quoteSummaryRaw(modules: modules)
        }
    }
}

/// No shared result cache: separate module requests cannot contaminate another
/// methodology, client, or symbol. Existing no-argument getters are unchanged.
enum YFEarningsMethodologyAnalysis {
    static func normalized(_ value: String?) throws -> String? {
        guard let value else { return nil }
        let result = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard result == "gaap" || result == "normalized" else {
            throw YFinanceError.invalidRequest("methodology must be nil, 'gaap' or 'normalized'")
        }
        return result
    }

    static func moduleName(key: String, methodology: String?) throws -> String {
        let methodology = try normalized(methodology)
        let suffix = methodology == "gaap" ? "Gaap" : methodology == "normalized" ? "NonGaap" : ""
        return (key == "history" ? "earningsHistory" : "earningsTrend") + suffix
    }

    static func fetch(
        key: String,
        methodology: String?,
        load: @Sendable ([String]) async throws -> YFJSONValue
    ) async throws -> YFJSONValue {
        let selection = try normalized(methodology)
        let module = try moduleName(key: key, methodology: selection)
        let response = try await load([module])
        let data = response["quoteSummary"]?["result"]?[0]?[module]
        return rows(from: data, key: key, methodology: selection)
    }

    static func rows(from data: YFJSONValue?, key: String, methodology: String?) -> YFJSONValue {
        let defaultLabel = data?["defaultMethodology"]?.stringValue
        let label = methodology ?? (defaultLabel?.lowercased() == "nongaap" ? "normalized" : defaultLabel)
        if key == "history" {
            let rows = (data?["history"]?.arrayValue ?? []).compactMap { row -> YFJSONValue? in
                guard var object = row.objectValue, !object.isEmpty else { return nil }
                // Preserve the existing history API's raw Yahoo field wrappers.
                if let label { object["methodology"] = .string(label) }
                return .object(object)
            }
            return .array(rows)
        }

        let currencyKey: String
        switch key {
        case "earningsEstimate": currencyKey = "earningsCurrency"
        case "epsTrend": currencyKey = "epsTrendCurrency"
        default: currencyKey = "epsRevisionsCurrency"
        }
        let trend = Array((data?["trend"]?.arrayValue ?? []).prefix(4))
        let currency = trend.compactMap { $0[currencyKey]?.stringValue }.first
        return .array(trend.compactMap { row in
            guard let detail = row[key]?.objectValue, !detail.isEmpty else { return nil }
            var object = detail.mapValues(unwrap)
            object["period"] = row["period"] ?? .null
            if let currency { object["currency"] = .string(currency) }
            if let label { object["methodology"] = .string(label) }
            return .object(object)
        })
    }

    private static func unwrap(_ value: YFJSONValue) -> YFJSONValue {
        if let object = value.objectValue {
            if let raw = object["raw"] { return raw }
            if let formatted = object["fmt"] { return formatted }
            return .object(object.mapValues(unwrap))
        }
        if let array = value.arrayValue { return .array(array.map(unwrap)) }
        return value
    }
}
