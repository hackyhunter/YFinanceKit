import Foundation

public enum YFValuationFrequency: String, CaseIterable, Sendable {
    case quarterly
    case monthly
    case yearly
    case trailing

    fileprivate var yahooPrefix: String {
        switch self {
        case .quarterly: return "quarterly"
        case .monthly: return "monthly"
        case .yearly: return "annual"
        case .trailing: return "trailing"
        }
    }

    public init?(pythonValue: String) {
        switch pythonValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "quarterly", "quarter", "q": self = .quarterly
        case "monthly", "month", "m": self = .monthly
        case "yearly", "annual", "year", "y": self = .yearly
        case "trailing", "ttm": self = .trailing
        default: return nil
        }
    }
}

private let yfValuationMeasureLabels: [(key: String, label: String)] = [
    ("MarketCap", "Market Cap"),
    ("EnterpriseValue", "Enterprise Value"),
    ("PeRatio", "Trailing P/E"),
    ("ForwardPeRatio", "Forward P/E"),
    ("PegRatio", "PEG Ratio (5yr expected)"),
    ("PsRatio", "Price/Sales"),
    ("PbRatio", "Price/Book"),
    ("EnterprisesValueRevenueRatio", "Enterprise Value/Revenue"),
    ("EnterprisesValueEBITDARatio", "Enterprise Value/EBITDA"),
]

public extension YFinanceClient {
    /// Fetches Yahoo's valuation-measure time series, matching yfinance 1.6's
    /// `get_valuation_measures(freq:, periods:)` data source and table shape.
    func valuationMeasures(
        symbol: String,
        frequency: YFValuationFrequency = .quarterly,
        periods: Int? = 5,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        if let periods, periods < 0 {
            throw YFinanceError.invalidRequest("periods must be >= 0 or nil")
        }

        let cleanedSymbol = symbol.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        guard !cleanedSymbol.isEmpty else {
            throw YFinanceError.invalidRequest("symbol cannot be empty")
        }

        let requestedPrefix = frequency.yahooPrefix
        let prefixes = Set([requestedPrefix, "trailing"])
        let types = yfValuationMeasureLabels.flatMap { measure in
            prefixes.map { "\($0)\(measure.key)" }
        }.sorted()

        // Upstream yfinance requests history back to 2016-12-31 and always fetches
        // trailing values to populate the Current column.
        let raw = try await rawGet(
            host: .query2,
            path: "/ws/fundamentals-timeseries/v1/finance/timeseries/\(cleanedSymbol)",
            queryItems: [
                URLQueryItem(name: "symbol", value: cleanedSymbol),
                URLQueryItem(name: "type", value: types.joined(separator: ",")),
                URLQueryItem(name: "period1", value: "1483142400"),
                URLQueryItem(name: "period2", value: String(Int(Date().timeIntervalSince1970) + 86_400)),
            ],
            requiresCrumb: true,
            timeout: timeout
        )

        if let error = raw["timeseries"]?["error"], error != .null {
            let code = error["code"]?.stringValue ?? "timeseries_error"
            let description = error["description"]?.stringValue ?? "Yahoo valuation timeseries error"
            throw YFinanceError.serverError(code: code, description: description)
        }

        let result = raw["timeseries"]?["result"]?.arrayValue ?? []

        // label -> ISO date -> raw value
        var requested: [String: [String: Double]] = [:]
        var trailing: [String: [String: Double]] = [:]
        let labelsByKey = Dictionary(uniqueKeysWithValues: yfValuationMeasureLabels.map { ($0.key, $0.label) })

        for item in result {
            guard let object = item.objectValue else { continue }
            for (typeName, value) in object {
                if typeName == "meta" || typeName == "timestamp" { continue }
                guard let points = value.arrayValue else { continue }

                let baseKey: String
                let targetIsTrailing: Bool
                if typeName.hasPrefix("trailing") {
                    baseKey = String(typeName.dropFirst("trailing".count))
                    targetIsTrailing = true
                } else if typeName.hasPrefix(requestedPrefix) {
                    baseKey = String(typeName.dropFirst(requestedPrefix.count))
                    targetIsTrailing = false
                } else {
                    continue
                }

                guard let label = labelsByKey[baseKey] else { continue }
                for point in points {
                    guard let date = point["asOfDate"]?.stringValue,
                          let number = point["reportedValue"]?["raw"]?.doubleValue else {
                        continue
                    }
                    if targetIsTrailing {
                        trailing[label, default: [:]][date] = number
                    } else {
                        requested[label, default: [:]][date] = number
                    }
                }
            }
        }

        if frequency == .trailing {
            requested = trailing
        }

        var dates = Set(requested.values.flatMap { $0.keys }).sorted(by: >)
        if let periods {
            dates = Array(dates.prefix(periods))
        }

        let dateColumns = dates.map(Self.valuationDisplayDate)
        let columns = ["Measure", "Current"] + dateColumns

        let rows: [[String: YFJSONValue]] = yfValuationMeasureLabels.map { measure in
            var row: [String: YFJSONValue] = ["Measure": .string(measure.label)]

            if let latest = trailing[measure.label]?.max(by: { $0.key < $1.key })?.value {
                row["Current"] = .number(latest)
            } else {
                row["Current"] = .null
            }

            for (date, column) in zip(dates, dateColumns) {
                if let value = requested[measure.label]?[date] {
                    row[column] = .number(value)
                } else {
                    row[column] = .null
                }
            }
            return row
        }

        return YFTable(columns: columns, rows: rows)
    }

    func valuationMeasures(
        symbol: String,
        freq: String,
        periods: Int? = 5,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        guard let frequency = YFValuationFrequency(pythonValue: freq) else {
            throw YFinanceError.invalidRequest("freq must be quarterly, monthly, yearly, or trailing")
        }
        return try await valuationMeasures(
            symbol: symbol,
            frequency: frequency,
            periods: periods,
            timeout: timeout
        )
    }

    private static func valuationDisplayDate(_ isoDate: String) -> String {
        let pieces = isoDate.split(separator: "-")
        guard pieces.count == 3,
              let year = Int(pieces[0]),
              let month = Int(pieces[1]),
              let day = Int(pieces[2]) else {
            return isoDate
        }
        return "\(month)/\(day)/\(year)"
    }
}

public extension YF {
    static func valuationMeasures(
        _ symbol: String,
        frequency: YFValuationFrequency = .quarterly,
        periods: Int? = 5,
        timeout: TimeInterval? = nil,
        client: YFinanceClient = YFinanceClient()
    ) async throws -> YFTable {
        try await client.valuationMeasures(
            symbol: symbol,
            frequency: frequency,
            periods: periods,
            timeout: timeout
        )
    }
}
