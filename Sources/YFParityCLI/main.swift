import Foundation
import YFinanceKit

enum ParityCLIError: LocalizedError {
    case usage(String)
    case missingOption(String)
    case invalidOption(String)

    var errorDescription: String? {
        switch self {
        case .usage(let message):
            return message
        case .missingOption(let option):
            return "Missing required option: \(option)"
        case .invalidOption(let message):
            return message
        }
    }
}

struct Args {
    let command: String
    let options: [String: String]
}

@main
struct YFParityCLI {
    static func main() async {
        do {
            let args = try parseArgs(Array(CommandLine.arguments.dropFirst()))
            let payload = try await run(args: args)
            try printJSON(payload)
        } catch {
            let message = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
            let payload: [String: Any] = [
                "ok": false,
                "error": message
            ]
            do {
                try printJSON(payload)
            } catch {
                fputs("{\"ok\":false,\"error\":\"\(message)\"}\n", stderr)
            }
            exit(1)
        }
    }

    static func parseArgs(_ raw: [String]) throws -> Args {
        guard let command = raw.first else {
            throw ParityCLIError.usage(usageText)
        }

        var options: [String: String] = [:]
        var index = 1
        while index < raw.count {
            let token = raw[index]
            guard token.hasPrefix("--") else {
                throw ParityCLIError.invalidOption("Unexpected token: \(token)")
            }
            let key = String(token.dropFirst(2))
            let next = index + 1
            if next < raw.count, !raw[next].hasPrefix("--") {
                options[key] = raw[next]
                index += 2
            } else {
                options[key] = "true"
                index += 1
            }
        }

        return Args(command: command.lowercased(), options: options)
    }

    static var usageText: String {
        """
        Usage:
          YFParityCLI snapshot --symbol AAPL [--period 1mo] [--interval 1d] [--history-limit 30] [--earnings-limit 4] [--income-limit 4] [--freq yearly]
          YFParityCLI quote --symbol AAPL
          YFParityCLI history --symbol AAPL [--period 1mo] [--interval 1d] [--limit 30]
          YFParityCLI earnings-dates --symbol AAPL [--limit 4]
          YFParityCLI income-stmt --symbol AAPL [--freq yearly|quarterly] [--limit 4]
        """
    }

    static func run(args: Args) async throws -> [String: Any] {
        switch args.command {
        case "snapshot":
            let symbol = try requiredOption("symbol", in: args.options)
            let period = args.options["period"] ?? "1mo"
            let interval = args.options["interval"] ?? "1d"
            let historyLimit = intOption("history-limit", in: args.options, defaultValue: 30)
            let earningsLimit = intOption("earnings-limit", in: args.options, defaultValue: 4)
            let incomeLimit = intOption("income-limit", in: args.options, defaultValue: 4)
            let freq = args.options["freq"] ?? "yearly"
            return try await snapshotPayload(
                symbol: symbol,
                period: period,
                interval: interval,
                historyLimit: historyLimit,
                earningsLimit: earningsLimit,
                incomeLimit: incomeLimit,
                incomeFrequency: freq
            )
        case "quote":
            let symbol = try requiredOption("symbol", in: args.options)
            return try await quotePayload(symbol: symbol)
        case "history":
            let symbol = try requiredOption("symbol", in: args.options)
            let period = args.options["period"] ?? "1mo"
            let interval = args.options["interval"] ?? "1d"
            let limit = intOption("limit", in: args.options, defaultValue: 30)
            return try await historyPayload(symbol: symbol, period: period, interval: interval, limit: limit)
        case "earnings-dates":
            let symbol = try requiredOption("symbol", in: args.options)
            let limit = intOption("limit", in: args.options, defaultValue: 4)
            return try await earningsPayload(symbol: symbol, limit: limit)
        case "income-stmt":
            let symbol = try requiredOption("symbol", in: args.options)
            let limit = intOption("limit", in: args.options, defaultValue: 4)
            let freq = args.options["freq"] ?? "yearly"
            return try await incomePayload(symbol: symbol, frequency: freq, limit: limit)
        default:
            throw ParityCLIError.usage(usageText)
        }
    }

    static func requiredOption(_ name: String, in options: [String: String]) throws -> String {
        guard let value = options[name]?.trimmingCharacters(in: .whitespacesAndNewlines), !value.isEmpty else {
            throw ParityCLIError.missingOption("--\(name)")
        }
        return value
    }

    static func intOption(_ name: String, in options: [String: String], defaultValue: Int) -> Int {
        guard let raw = options[name], let value = Int(raw) else {
            return defaultValue
        }
        return max(1, value)
    }

    static func quotePayload(symbol: String) async throws -> [String: Any] {
        let ticker = YFTicker(symbol)
        let quote = try await ticker.quote()
        let normalized = normalizeQuote(symbol: symbol, quote: quote)
        return [
            "ok": true,
            "operation": "quote",
            "symbol": symbol.uppercased(),
            "data": normalized
        ]
    }

    static func historyPayload(symbol: String, period: String, interval: String, limit: Int) async throws -> [String: Any] {
        let ticker = YFTicker(symbol)
        let series = try await ticker.history(
            period: period,
            interval: interval,
            prepost: false,
            actions: true,
            autoAdjust: true,
            backAdjust: false,
            repair: false,
            keepNa: false,
            rounding: false
        )

        let isIntraday = interval.lowercased().contains("m") || interval.lowercased().contains("h")
        let sorted = series.bars.sorted { $0.date < $1.date }
        let sliced = Array(sorted.suffix(max(1, limit)))
        let rows: [[String: Any]] = sliced.map { bar in
            [
                "date": formatDate(bar.date, includeTime: isIntraday),
                "open": numberOrNull(bar.open),
                "high": numberOrNull(bar.high),
                "low": numberOrNull(bar.low),
                "close": numberOrNull(bar.close),
                "adjustedClose": numberOrNull(bar.adjustedClose),
                "volume": intOrNull(bar.volume),
                "repaired": bar.repaired
            ]
        }

        return [
            "ok": true,
            "operation": "history",
            "symbol": symbol.uppercased(),
            "data": [
                "period": period,
                "interval": interval,
                "barCount": rows.count,
                "bars": rows
            ]
        ]
    }

    static func earningsPayload(symbol: String, limit: Int) async throws -> [String: Any] {
        let ticker = YFTicker(symbol)
        let fetchLimit = min(max(limit * 4, 12), 100)
        let table = try await ticker.earningsDatesTable(limit: fetchLimit, offset: 0)
        let normalized = normalizeEarningsRows(table.rows, limit: limit)

        return [
            "ok": true,
            "operation": "earnings-dates",
            "symbol": symbol.uppercased(),
            "data": [
                "rowCount": normalized.count,
                "rows": normalized
            ]
        ]
    }

    static func incomePayload(symbol: String, frequency: String, limit: Int) async throws -> [String: Any] {
        guard let freq = YFFinancialFrequency(pythonValue: frequency) else {
            throw ParityCLIError.invalidOption("Unsupported frequency: \(frequency)")
        }

        let ticker = YFTicker(symbol)
        let table = try await ticker.incomeStmtTable(freq: freq)
        let rows = normalizeIncomeRows(table.rows, limit: limit)

        return [
            "ok": true,
            "operation": "income-stmt",
            "symbol": symbol.uppercased(),
            "data": [
                "frequency": freq.rawValue,
                "rowCount": rows.count,
                "rows": rows
            ]
        ]
    }

    static func snapshotPayload(
        symbol: String,
        period: String,
        interval: String,
        historyLimit: Int,
        earningsLimit: Int,
        incomeLimit: Int,
        incomeFrequency: String
    ) async throws -> [String: Any] {
        var errors: [[String: Any]] = []
        var quote: Any = NSNull()
        var history: Any = NSNull()
        var earnings: Any = NSNull()
        var income: Any = NSNull()

        do {
            let result = try await quotePayload(symbol: symbol)
            quote = result["data"] ?? NSNull()
        } catch {
            errors.append(["operation": "quote", "error": error.localizedDescription])
        }

        do {
            let result = try await historyPayload(symbol: symbol, period: period, interval: interval, limit: historyLimit)
            history = result["data"] ?? NSNull()
        } catch {
            errors.append(["operation": "history", "error": error.localizedDescription])
        }

        do {
            let result = try await earningsPayload(symbol: symbol, limit: earningsLimit)
            earnings = result["data"] ?? NSNull()
        } catch {
            errors.append(["operation": "earnings-dates", "error": error.localizedDescription])
        }

        do {
            let result = try await incomePayload(symbol: symbol, frequency: incomeFrequency, limit: incomeLimit)
            income = result["data"] ?? NSNull()
        } catch {
            errors.append(["operation": "income-stmt", "error": error.localizedDescription])
        }

        return [
            "ok": errors.isEmpty,
            "operation": "snapshot",
            "symbol": symbol.uppercased(),
            "quote": quote,
            "history": history,
            "earnings_dates": earnings,
            "income_stmt": income,
            "errors": errors
        ]
    }

    static func normalizeQuote(symbol: String, quote: YFQuote?) -> [String: Any] {
        [
            "symbol": quote?.symbol ?? symbol.uppercased(),
            "name": stringOrNull(firstNonEmpty(quote?.longName, quote?.shortName)),
            "currency": stringOrNull(quote?.currency),
            "exchange": stringOrNull(quote?.exchange),
            "quoteType": stringOrNull(quote?.quoteType),
            "regularMarketPrice": numberOrNull(quote?.regularMarketPrice),
            "regularMarketChange": numberOrNull(quote?.regularMarketChange),
            "regularMarketChangePercent": numberOrNull(quote?.regularMarketChangePercent),
            "regularMarketVolume": intOrNull(quote?.regularMarketVolume),
            "marketCap": numberOrNull(quote?.marketCap),
            "trailingPE": numberOrNull(quote?.trailingPE),
            "forwardPE": numberOrNull(quote?.forwardPE)
        ]
    }

    static func normalizeEarningsRows(_ rows: [[String: YFJSONValue]], limit: Int) -> [[String: Any]] {
        var mapped: [[String: Any]] = []
        var seen = Set<String>()

        for row in rows {
            let date = normalizeDate(
                value(in: row, keys: ["Earnings Date", "Event Start Date", "startdatetime", "date", "quarter", "period"])
            )
            guard let date else { continue }
            if seen.contains(date) { continue }
            seen.insert(date)

            let estimate = number(
                value(in: row, keys: ["EPS Estimate", "epsestimate", "epsEstimate", "estimate"])
            )
            let actual = number(
                value(in: row, keys: ["Reported EPS", "epsactual", "epsActual", "actual"])
            )
            let surprise = number(
                value(in: row, keys: ["Surprise(%)", "Surprise (%)", "epssurprisepct", "surprisePercent"])
            )

            mapped.append([
                "date": date,
                "epsEstimate": numberOrNull(estimate),
                "epsActual": numberOrNull(actual),
                "surprisePercent": numberOrNull(surprise)
            ])
        }

        mapped.sort {
            String(describing: $0["date"] ?? "") > String(describing: $1["date"] ?? "")
        }
        if mapped.count > limit {
            mapped = Array(mapped.prefix(limit))
        }
        return mapped
    }

    static func normalizeIncomeRows(_ rows: [[String: YFJSONValue]], limit: Int) -> [[String: Any]] {
        var byYear: [String: (revenue: Double?, income: Double?)] = [:]

        for row in rows {
            guard let year = extractYear(from: row) else { continue }

            let revenue = number(
                value(in: row, keys: ["totalRevenue", "revenue", "operatingRevenue", "totalOperatingRevenue"])
            )
            let income = number(
                value(in: row, keys: [
                    "netIncome",
                    "netIncomeCommonStockholders",
                    "netIncomeContinuingOperations",
                    "netIncomeFromContinuingOperations",
                    "netIncomeFromContinuingOperationNetOfMinorityInterest"
                ])
            )

            if revenue == nil && income == nil { continue }

            let existing = byYear[year]
            byYear[year] = (
                revenue: existing?.revenue ?? revenue,
                income: existing?.income ?? income
            )
        }

        let sortedYears = byYear.keys.sorted(by: >)
        var out: [[String: Any]] = []
        for year in sortedYears {
            guard let row = byYear[year] else { continue }
            out.append([
                "year": year,
                "totalRevenue": numberOrNull(row.revenue),
                "netIncome": numberOrNull(row.income)
            ])
            if out.count >= limit { break }
        }
        return out
    }

    static func value(in row: [String: YFJSONValue], keys: [String]) -> YFJSONValue? {
        for key in keys {
            if let value = row[key], value != .null {
                return value
            }
        }
        let lowercased = Dictionary(uniqueKeysWithValues: row.map { ($0.key.lowercased(), $0.value) })
        for key in keys {
            let lookup = key.lowercased()
            if let value = lowercased[lookup], value != .null {
                return value
            }
        }
        return nil
    }

    static func number(_ value: YFJSONValue?) -> Double? {
        guard let value else { return nil }
        switch value {
        case .number(let n):
            return n.isFinite ? n : nil
        case .string(let text):
            let cleaned = text
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .replacingOccurrences(of: ",", with: "")
                .replacingOccurrences(of: "%", with: "")
            return Double(cleaned)
        case .object(let object):
            if let raw = object["raw"] {
                return number(raw)
            }
            if let fmt = object["fmt"] {
                return number(fmt)
            }
            return nil
        default:
            return nil
        }
    }

    static func normalizeDate(_ value: YFJSONValue?) -> String? {
        guard let value else { return nil }

        if let raw = number(value), let date = dateFromEpoch(raw) {
            return formatDate(date, includeTime: false)
        }

        switch value {
        case .string(let text):
            let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty { return nil }
            if trimmed.count >= 10,
               trimmed.range(of: #"^\d{4}-\d{2}-\d{2}"#, options: .regularExpression) != nil {
                return String(trimmed.prefix(10))
            }
            if let match = trimmed.range(of: #"(19|20)\d{2}-\d{2}-\d{2}"#, options: .regularExpression) {
                return String(trimmed[match])
            }
            if let parsed = parseDate(trimmed) {
                return formatDate(parsed, includeTime: false)
            }
            return trimmed
        case .object(let object):
            return normalizeDate(object["raw"] ?? object["fmt"])
        default:
            return nil
        }
    }

    static func extractYear(from row: [String: YFJSONValue]) -> String? {
        let keys = ["endDate", "asOfDate", "fiscalDateEnding", "date", "period"]
        for key in keys {
            if let value = value(in: row, keys: [key]),
               let year = year(from: value) {
                return year
            }
        }
        return nil
    }

    static func year(from value: YFJSONValue) -> String? {
        if let raw = number(value), let date = dateFromEpoch(raw) {
            let year = Calendar(identifier: .gregorian).component(.year, from: date)
            if (1900...2100).contains(year) {
                return String(year)
            }
        }

        switch value {
        case .string(let text):
            let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.count >= 4,
               let year = Int(trimmed.prefix(4)),
               (1900...2100).contains(year) {
                return String(year)
            }
            if let match = trimmed.range(of: #"(19|20)\d{2}"#, options: .regularExpression),
               let year = Int(trimmed[match]),
               (1900...2100).contains(year) {
                return String(year)
            }
            return nil
        case .object(let object):
            if let raw = object["raw"], let year = year(from: raw) { return year }
            if let fmt = object["fmt"], let year = year(from: fmt) { return year }
            return nil
        default:
            return nil
        }
    }

    static func dateFromEpoch(_ raw: Double) -> Date? {
        guard raw.isFinite else { return nil }
        var seconds = raw
        if abs(seconds) > 9_999_999_999 {
            seconds /= 1000
        }
        guard abs(seconds) > 1_000_000 else {
            return nil
        }
        return Date(timeIntervalSince1970: seconds)
    }

    static func parseDate(_ text: String) -> Date? {
        let formats = [
            "yyyy-MM-dd'T'HH:mm:ssXXXXX",
            "yyyy-MM-dd HH:mm:ssXXXXX",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            "MMMM d, yyyy 'at' h a zzz",
            "MMMM d, yyyy 'at' h:mm a zzz",
            "MMM d, yyyy, h a zzz",
            "MMM d, yyyy, h:mm a zzz",
            "MMMM d, yyyy 'at' h a",
            "MMMM d, yyyy 'at' h:mm a",
            "MMM d, yyyy, h a",
            "MMM d, yyyy, h:mm a"
        ]
        for format in formats {
            let formatter = DateFormatter()
            formatter.locale = Locale(identifier: "en_US_POSIX")
            formatter.timeZone = TimeZone(secondsFromGMT: 0)
            formatter.dateFormat = format
            if let date = formatter.date(from: text) {
                return date
            }
        }

        let strippedTimeZone = text.replacingOccurrences(
            of: #"(\s+[A-Z]{2,5})$"#,
            with: "",
            options: .regularExpression
        )
        if strippedTimeZone != text {
            for format in ["MMMM d, yyyy 'at' h a", "MMMM d, yyyy 'at' h:mm a", "MMM d, yyyy, h a", "MMM d, yyyy, h:mm a"] {
                let formatter = DateFormatter()
                formatter.locale = Locale(identifier: "en_US_POSIX")
                formatter.timeZone = TimeZone(secondsFromGMT: 0)
                formatter.dateFormat = format
                if let date = formatter.date(from: strippedTimeZone) {
                    return date
                }
            }
        }
        return nil
    }

    static func formatDate(_ date: Date, includeTime: Bool) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = includeTime ? "yyyy-MM-dd'T'HH:mm:ss'Z'" : "yyyy-MM-dd"
        return formatter.string(from: date)
    }

    static func firstNonEmpty(_ values: String?...) -> String? {
        for value in values {
            if let trimmed = value?.trimmingCharacters(in: .whitespacesAndNewlines), !trimmed.isEmpty {
                return trimmed
            }
        }
        return nil
    }

    static func stringOrNull(_ value: String?) -> Any {
        if let value {
            return value
        }
        return NSNull()
    }

    static func numberOrNull(_ value: Double?) -> Any {
        if let value, value.isFinite {
            return value
        }
        return NSNull()
    }

    static func intOrNull(_ value: Int?) -> Any {
        if let value {
            return value
        }
        return NSNull()
    }

    static func printJSON(_ payload: [String: Any]) throws {
        let data = try JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys])
        if let text = String(data: data, encoding: .utf8) {
            print(text)
        } else {
            print("{\"ok\":false,\"error\":\"encoding_failed\"}")
        }
    }
}
