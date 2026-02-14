import Foundation

public enum YFinanceError: LocalizedError {
    case invalidURL(String)
    case invalidRequest(String)
    case transport(Error)
    case httpStatus(Int)
    case serverError(code: String, description: String)
    case decoding(Error)
    case missingData(String)

    public var errorDescription: String? {
        switch self {
        case .invalidURL(let message):
            return "Invalid URL: \(message)"
        case .invalidRequest(let message):
            return "Invalid request: \(message)"
        case .transport(let error):
            return "Network error: \(error.localizedDescription)"
        case .httpStatus(let status):
            return "HTTP error \(status)"
        case .serverError(let code, let description):
            return "Yahoo API error [\(code)]: \(description)"
        case .decoding(let error):
            return "Failed to decode response: \(error.localizedDescription)"
        case .missingData(let message):
            return "Missing data: \(message)"
        }
    }
}

public struct YFQuote: Decodable, Sendable {
    public let symbol: String
    public let shortName: String?
    public let longName: String?
    public let currency: String?
    public let exchange: String?
    public let quoteType: String?
    public let regularMarketPrice: Double?
    public let regularMarketChange: Double?
    public let regularMarketChangePercent: Double?
    public let regularMarketPreviousClose: Double?
    public let regularMarketOpen: Double?
    public let regularMarketDayHigh: Double?
    public let regularMarketDayLow: Double?
    public let regularMarketVolume: Int?
    public let marketCap: Double?
    public let trailingPE: Double?
    public let forwardPE: Double?
    public let fiftyTwoWeekLow: Double?
    public let fiftyTwoWeekHigh: Double?

    enum CodingKeys: String, CodingKey {
        case symbol
        case shortName
        case longName
        case currency
        case exchange
        case quoteType
        case regularMarketPrice
        case regularMarketChange
        case regularMarketChangePercent
        case regularMarketPreviousClose
        case regularMarketOpen
        case regularMarketDayHigh
        case regularMarketDayLow
        case regularMarketVolume
        case marketCap
        case trailingPE
        case forwardPE
        case fiftyTwoWeekLow
        case fiftyTwoWeekHigh
    }
}

public struct YFSearchResult: Decodable, Sendable {
    public let count: Int?
    public let quotes: [YFSearchQuote]
    public let news: [YFSearchNews]
    public let lists: [YFSearchList]
    public let researchReports: [YFSearchResearchReport]
    public let nav: [YFSearchNavLink]

    public init(
        count: Int? = nil,
        quotes: [YFSearchQuote] = [],
        news: [YFSearchNews] = [],
        lists: [YFSearchList] = [],
        researchReports: [YFSearchResearchReport] = [],
        nav: [YFSearchNavLink] = []
    ) {
        self.count = count
        self.quotes = quotes
        self.news = news
        self.lists = lists
        self.researchReports = researchReports
        self.nav = nav
    }

    public static let empty = YFSearchResult()

    enum CodingKeys: String, CodingKey {
        case count
        case quotes
        case news
        case lists
        case researchReports
        case nav
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        count = try container.decodeIfPresent(Int.self, forKey: .count)
        let decodedQuotes = try container.decodeIfPresent([YFSearchQuote].self, forKey: .quotes) ?? []
        quotes = decodedQuotes.filter { quote in
            if let symbol = quote.symbol?.trimmingCharacters(in: .whitespacesAndNewlines), !symbol.isEmpty {
                return true
            }
            return false
        }
        news = try container.decodeIfPresent([YFSearchNews].self, forKey: .news) ?? []
        lists = try container.decodeIfPresent([YFSearchList].self, forKey: .lists) ?? []
        researchReports = try container.decodeIfPresent([YFSearchResearchReport].self, forKey: .researchReports) ?? []
        nav = try container.decodeIfPresent([YFSearchNavLink].self, forKey: .nav) ?? []
    }
}

public struct YFSearchQuote: Decodable, Sendable {
    public let symbol: String?
    public let shortName: String?
    public let longName: String?
    public let exchange: String?
    public let exchangeDisplay: String?
    public let quoteType: String?
    public let typeDisplay: String?
    public let score: Double?

    enum CodingKeys: String, CodingKey {
        case symbol
        case shortName = "shortname"
        case longName = "longname"
        case exchange
        case exchangeDisplay = "exchDisp"
        case quoteType
        case typeDisplay = "typeDisp"
        case score
    }
}

public struct YFSearchNews: Decodable, Sendable {
    public let uuid: String?
    public let title: String?
    public let publisher: String?
    public let link: String?
    public let providerPublishTime: Int?
    public let type: String?
    public let relatedTickers: [String]?
}

public struct YFSearchList: Decodable, Sendable {
    public let name: String?
    public let score: Double?
}

public struct YFSearchResearchReport: Decodable, Sendable {
    public let reportId: String?
    public let title: String?
    public let provider: String?
}

public struct YFSearchNavLink: Decodable, Sendable {
    public let navName: String?
    public let navUrl: String?
}

public struct YFTable: Sendable {
    public let columns: [String]
    public let rows: [[String: YFJSONValue]]

    public init(columns: [String], rows: [[String: YFJSONValue]]) {
        self.columns = columns
        self.rows = rows
    }

    public var isEmpty: Bool { rows.isEmpty }
    public var rowCount: Int { rows.count }
    public var columnCount: Int { columns.count }

    public subscript(row row: Int, column column: String) -> YFJSONValue? {
        guard rows.indices.contains(row) else {
            return nil
        }
        return rows[row][column]
    }

    public func column(_ name: String) -> [YFJSONValue] {
        rows.map { $0[name] ?? .null }
    }

    public func head(_ count: Int = 5) -> YFTable {
        let limit = max(0, count)
        return YFTable(columns: columns, rows: Array(rows.prefix(limit)))
    }

    public func tail(_ count: Int = 5) -> YFTable {
        let limit = max(0, count)
        return YFTable(columns: columns, rows: Array(rows.suffix(limit)))
    }

    public func select(columns selected: [String]) -> YFTable {
        let keep = selected.filter { columns.contains($0) }
        let mappedRows = rows.map { row in
            var mapped: [String: YFJSONValue] = [:]
            for column in keep {
                mapped[column] = row[column] ?? .null
            }
            return mapped
        }
        return YFTable(columns: keep, rows: mappedRows)
    }

    public func drop(columns dropped: [String]) -> YFTable {
        let droppedSet = Set(dropped)
        let keep = columns.filter { !droppedSet.contains($0) }
        return select(columns: keep)
    }

    public func renamed(columns mapping: [String: String]) -> YFTable {
        let newColumns = columns.map { mapping[$0] ?? $0 }
        let newRows = rows.map { row in
            var mapped: [String: YFJSONValue] = [:]
            for (key, value) in row {
                mapped[mapping[key] ?? key] = value
            }
            return mapped
        }
        return YFTable(columns: newColumns, rows: newRows)
    }

    public func sorted(
        by column: String,
        ascending: Bool = true,
        nilsLast: Bool = true
    ) -> YFTable {
        guard columns.contains(column) else {
            return self
        }

        let sortedRows = rows.sorted { lhs, rhs in
            let lhsValue = lhs[column] ?? .null
            let rhsValue = rhs[column] ?? .null
            switch Self.compare(lhsValue, rhsValue, nilsLast: nilsLast) {
            case .orderedAscending:
                return ascending
            case .orderedDescending:
                return !ascending
            case .orderedSame:
                return false
            }
        }
        return YFTable(columns: columns, rows: sortedRows)
    }

    public func filtered(_ predicate: ([String: YFJSONValue]) -> Bool) -> YFTable {
        YFTable(columns: columns, rows: rows.filter(predicate))
    }

    public func withRowNumber(column name: String = "index") -> YFTable {
        var newColumns = columns
        if !newColumns.contains(name) {
            newColumns.insert(name, at: 0)
        }
        let newRows = rows.enumerated().map { index, row in
            var output = row
            output[name] = .number(Double(index))
            return output
        }
        return YFTable(columns: newColumns, rows: newRows)
    }

    public func index(by column: String) -> YFIndexedTable {
        YFIndexedTable(indexColumn: column, table: self)
    }

    public func transposed(
        indexColumn: String = "column",
        valuePrefix: String = "row_"
    ) -> YFTable {
        let valueColumns = (0..<rows.count).map { "\(valuePrefix)\($0)" }
        let transposedRows: [[String: YFJSONValue]] = columns.map { column in
            var row: [String: YFJSONValue] = [indexColumn: .string(column)]
            for (index, sourceRow) in rows.enumerated() {
                row["\(valuePrefix)\(index)"] = sourceRow[column] ?? .null
            }
            return row
        }
        return YFTable(columns: [indexColumn] + valueColumns, rows: transposedRows)
    }

    public static func fromObjects(_ values: [YFJSONValue]) -> YFTable {
        let objects = values.compactMap(\.objectValue)
        var allColumns: [String] = []
        var columnSet: Set<String> = []
        for object in objects {
            for key in object.keys {
                if !columnSet.contains(key) {
                    columnSet.insert(key)
                    allColumns.append(key)
                }
            }
        }
        return YFTable(columns: allColumns, rows: objects)
    }

    private static func compare(_ lhs: YFJSONValue, _ rhs: YFJSONValue, nilsLast: Bool) -> ComparisonResult {
        let lhsNil = isNilLike(lhs)
        let rhsNil = isNilLike(rhs)
        if lhsNil || rhsNil {
            if lhsNil && rhsNil {
                return .orderedSame
            }
            if lhsNil {
                return nilsLast ? .orderedDescending : .orderedAscending
            }
            return nilsLast ? .orderedAscending : .orderedDescending
        }

        switch (lhs, rhs) {
        case let (.number(lv), .number(rv)):
            if lv < rv { return .orderedAscending }
            if lv > rv { return .orderedDescending }
            return .orderedSame
        case let (.string(lv), .string(rv)):
            return lv.localizedCaseInsensitiveCompare(rv)
        case let (.bool(lv), .bool(rv)):
            if lv == rv { return .orderedSame }
            return lv ? .orderedDescending : .orderedAscending
        default:
            let l = stringForComparison(lhs)
            let r = stringForComparison(rhs)
            return l.localizedCaseInsensitiveCompare(r)
        }
    }

    private static func isNilLike(_ value: YFJSONValue) -> Bool {
        if case .null = value {
            return true
        }
        return false
    }

    private static func stringForComparison(_ value: YFJSONValue) -> String {
        switch value {
        case .null:
            return ""
        case .bool(let v):
            return v ? "true" : "false"
        case .number(let v):
            return String(v)
        case .string(let v):
            return v
        case .array(let values):
            return values.map(stringForComparison).joined(separator: ",")
        case .object(let object):
            let keys = object.keys.sorted()
            return keys.map { key in
                "\(key)=\(stringForComparison(object[key] ?? .null))"
            }.joined(separator: ",")
        }
    }
}

public struct YFIndexedTable: Sendable {
    public let indexColumn: String
    public let columns: [String]
    public let order: [String]
    private let rowsByKey: [String: [String: YFJSONValue]]

    init(indexColumn: String, table: YFTable) {
        self.indexColumn = indexColumn
        self.columns = table.columns

        var order: [String] = []
        var rows: [String: [String: YFJSONValue]] = [:]

        for row in table.rows {
            guard let rawKey = row[indexColumn], let key = Self.keyString(rawKey) else {
                continue
            }
            if rows[key] == nil {
                order.append(key)
            }
            rows[key] = row
        }

        self.order = order
        self.rowsByKey = rows
    }

    public var keys: [String] { order }
    public var isEmpty: Bool { rowsByKey.isEmpty }

    public subscript(_ key: String) -> [String: YFJSONValue]? {
        rowsByKey[key]
    }

    private static func keyString(_ value: YFJSONValue) -> String? {
        switch value {
        case .null:
            return nil
        case .string(let text):
            return text
        case .number(let number):
            return String(number)
        case .bool(let flag):
            return flag ? "true" : "false"
        case .array, .object:
            return nil
        }
    }
}

public enum YFHistoryEventKind: String, Sendable {
    case dividend
    case split
    case capitalGain
}

public struct YFHistoryEvent: Sendable {
    public let kind: YFHistoryEventKind
    public let date: Date
    public let value: Double?
    public let ratio: Double?
    public let raw: YFJSONValue
}

public struct YFHistorySeries: Sendable {
    public let symbol: String
    public let meta: YFHistoryMeta
    public let interval: YFinanceClient.Interval
    public let bars: [YFHistoryBar]
    public let events: [YFHistoryEvent]
    /// Mirrors Python yfinance behavior: when `repair=true`, the returned table includes a `Repaired?` column.
    public let repairEnabled: Bool

    public init(
        symbol: String,
        meta: YFHistoryMeta,
        interval: YFinanceClient.Interval,
        bars: [YFHistoryBar],
        events: [YFHistoryEvent] = [],
        repairEnabled: Bool = false
    ) {
        self.symbol = symbol
        self.meta = meta
        self.interval = interval
        self.bars = bars
        self.events = events
        self.repairEnabled = repairEnabled
    }

    public init(symbol: String, meta: YFHistoryMeta, bars: [YFHistoryBar], events: [YFHistoryEvent] = []) {
        self.init(
            symbol: symbol,
            meta: meta,
            interval: YFinanceClient.Interval(pythonValue: meta.dataGranularity ?? "") ?? .oneDay,
            bars: bars,
            events: events,
            repairEnabled: false
        )
    }

    public func barsTable() -> YFTable {
        let rows: [[String: YFJSONValue]] = bars.map { bar in
            var row: [String: YFJSONValue] = [
                "date": .number(bar.date.timeIntervalSince1970),
            ]
            row["open"] = bar.open.map { .number($0) } ?? .null
            row["high"] = bar.high.map { .number($0) } ?? .null
            row["low"] = bar.low.map { .number($0) } ?? .null
            row["close"] = bar.close.map { .number($0) } ?? .null
            row["adjustedClose"] = bar.adjustedClose.map { .number($0) } ?? .null
            row["volume"] = bar.volume.map { .number(Double($0)) } ?? .null
            return row
        }
        return YFTable(
            columns: ["date", "open", "high", "low", "close", "adjustedClose", "volume"],
            rows: rows
        )
    }

    public func eventsTable() -> YFTable {
        let rows: [[String: YFJSONValue]] = events.map { event in
            var row: [String: YFJSONValue] = [
                "kind": .string(event.kind.rawValue),
                "date": .number(event.date.timeIntervalSince1970),
                "raw": event.raw,
            ]
            row["value"] = event.value.map { .number($0) } ?? .null
            row["ratio"] = event.ratio.map { .number($0) } ?? .null
            return row
        }
        return YFTable(columns: ["kind", "date", "value", "ratio", "raw"], rows: rows)
    }

    /// Returns a DataFrame-like table matching Python `yfinance.Ticker.history(...)` column conventions.
    /// - The `date` column is represented as either local-time strings (when `ignoreTZ=true`) or epoch seconds (when `ignoreTZ=false`).
    public func historyTable(includeActions: Bool = true, ignoreTZ: Bool? = nil) -> YFTable {
        let timezone = Self.resolveTimeZone(from: meta.exchangeTimezoneName)
        let ignoreTZ = ignoreTZ ?? Self.defaultIgnoreTZ(interval: interval)
        let intraday = Self.isIntraday(interval: interval)
        let orderedBars = bars.sorted { $0.date < $1.date }
        var exchangeCalendar = Calendar(identifier: .gregorian)
        exchangeCalendar.timeZone = timezone

        let includeAdjClose = orderedBars.contains { $0.adjustedClose != nil }
        let instrumentType = meta.instrumentType?.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        let expectCapitalGains = instrumentType == "MUTUALFUND" || instrumentType == "ETF"
        let includeCapitalGains = includeActions && (expectCapitalGains || events.contains(where: { $0.kind == .capitalGain }))
        let includeRepaired = repairEnabled

        var columns: [String] = ["date", "Open", "High", "Low", "Close"]
        if includeAdjClose {
            columns.append("Adj Close")
        }
        columns.append("Volume")

        if includeActions {
            columns.append("Dividends")
            columns.append("Stock Splits")
            if includeCapitalGains {
                columns.append("Capital Gains")
            }
        }
        if includeRepaired {
            columns.append("Repaired?")
        }

        if orderedBars.isEmpty, (!includeActions || events.isEmpty) {
            return YFTable(columns: columns, rows: [])
        }

        var rowsByKey: [String: [String: YFJSONValue]] = [:]
        var indexPairs: [(date: Date, rowKey: String)] = []
        indexPairs.reserveCapacity(orderedBars.count)

        var barRowKeys: [String] = []
        var barDayKeys: [Int] = []

        barRowKeys.reserveCapacity(orderedBars.count)
        if intraday {
            barDayKeys.reserveCapacity(orderedBars.count)
        }
        for bar in orderedBars {
            let rowKey = Self.tableRowKey(date: bar.date, timezone: timezone, ignoreTZ: ignoreTZ)

            barRowKeys.append(rowKey)
            indexPairs.append((date: bar.date, rowKey: rowKey))
            if intraday {
                barDayKeys.append(Self.dayKey(for: bar.date, calendar: exchangeCalendar))
            }

            var row = rowsByKey[rowKey] ?? [
                "date": Self.tableDateValue(date: bar.date, timezone: timezone, ignoreTZ: ignoreTZ),
            ]

            row["Open"] = bar.open.map { .number($0) } ?? .null
            row["High"] = bar.high.map { .number($0) } ?? .null
            row["Low"] = bar.low.map { .number($0) } ?? .null
            row["Close"] = bar.close.map { .number($0) } ?? .null
            if includeAdjClose {
                row["Adj Close"] = bar.adjustedClose.map { .number($0) } ?? .null
            }
            row["Volume"] = .number(Double(bar.volume ?? 0))
            if includeRepaired {
                let prior = row["Repaired?"]?.boolValue ?? false
                row["Repaired?"] = .bool(prior || bar.repaired)
            }

            if includeActions {
                row["Dividends"] = row["Dividends"] ?? .number(0)
                row["Stock Splits"] = row["Stock Splits"] ?? .number(0)
                if includeCapitalGains {
                    row["Capital Gains"] = row["Capital Gains"] ?? .number(0)
                }
            }

            rowsByKey[rowKey] = row
        }

        if includeActions, !events.isEmpty, !indexPairs.isEmpty, !intraday {
            // Mimic Python safe_merge_dfs(...) behavior for event rows beyond price range.
            // For 1d, add all out-of-range event dates; for other interday intervals, only add
            // out-of-range events that occur in the interval immediately after the last price row.
            let sortedPairs = indexPairs.sorted { $0.date < $1.date }
            let firstDate = sortedPairs.first!.date
            let lastDate = sortedPairs.last!.date

            if let lastPlusTd = Self.addInterval(lastDate, interval: interval, calendar: exchangeCalendar) {
                var insertionDates: [Date] = []
                insertionDates.reserveCapacity(events.count)

                if interval == .oneDay {
                    for event in events where event.date < firstDate || event.date >= lastPlusTd {
                        insertionDates.append(event.date)
                    }
                } else if let nextIntervalEnd = Self.addInterval(lastPlusTd, interval: interval, calendar: exchangeCalendar) {
                    for event in events where event.date >= lastPlusTd && event.date < nextIntervalEnd {
                        insertionDates.append(event.date)
                    }
                }

                if !insertionDates.isEmpty {
                    // Deduplicate by row key to avoid redundant rows for same timestamp.
                    var seenRowKeys: Set<String> = []
                    seenRowKeys.reserveCapacity(insertionDates.count)
                    for date in insertionDates {
                        let rowKey = Self.tableRowKey(date: date, timezone: timezone, ignoreTZ: ignoreTZ)
                        if seenRowKeys.contains(rowKey) {
                            continue
                        }
                        seenRowKeys.insert(rowKey)

                        if rowsByKey[rowKey] == nil {
                            var row: [String: YFJSONValue] = [
                                "date": Self.tableDateValue(date: date, timezone: timezone, ignoreTZ: ignoreTZ),
                                "Open": .null,
                                "High": .null,
                                "Low": .null,
                                "Close": .null,
                                "Volume": .number(0),
                            ]
                            if includeAdjClose {
                                row["Adj Close"] = .null
                            }
                            row["Dividends"] = .number(0)
                            row["Stock Splits"] = .number(0)
                            if includeCapitalGains {
                                row["Capital Gains"] = .number(0)
                            }
                            if includeRepaired {
                                row["Repaired?"] = .bool(false)
                            }
                            rowsByKey[rowKey] = row
                            indexPairs.append((date: date, rowKey: rowKey))
                        }
                    }
                }
            }
        }

        if includeActions, !events.isEmpty {
            // Rebuild index after any inserted event-only rows.
            let sortedIndexPairs = indexPairs.sorted { $0.date < $1.date }
            let indexDates = sortedIndexPairs.map(\.date)
            let indexRowKeys = sortedIndexPairs.map(\.rowKey)

            let firstDate = indexDates.first
            let lastDate = indexDates.last
            let endDateExclusive: Date? = {
                guard let lastDate else { return nil }
                return Self.addInterval(lastDate, interval: interval, calendar: exchangeCalendar)
            }()

            for event in events {
                let rowKey: String
                if intraday {
                    guard let firstDay = barDayKeys.first,
                          let lastDay = barDayKeys.last else {
                        continue
                    }
                    let eventDay = Self.dayKey(for: event.date, calendar: exchangeCalendar)
                    guard eventDay >= firstDay && eventDay <= lastDay else {
                        continue
                    }
                    let insertion = Self.lowerBound(barDayKeys, target: eventDay)
                    guard barRowKeys.indices.contains(insertion) else {
                        continue
                    }
                    rowKey = barRowKeys[insertion]
                } else {
                    guard let firstDate,
                          let endDateExclusive else {
                        continue
                    }
                    // Match safe_merge_dfs out-of-range handling: discard events beyond [firstDate, lastDate+td).
                    if event.date < firstDate || event.date >= endDateExclusive {
                        continue
                    }

                    let insertion = Self.upperBound(indexDates, target: event.date)
                    let index = insertion - 1
                    guard indexRowKeys.indices.contains(index) else {
                        continue
                    }
                    rowKey = indexRowKeys[index]
                }

                guard var row = rowsByKey[rowKey] else {
                    continue
                }

                switch event.kind {
                case .dividend:
                    let value = event.value ?? 0
                    let prior = row["Dividends"]?.doubleValue ?? 0
                    row["Dividends"] = .number(prior + value)
                case .split:
                    let ratio = event.ratio ?? event.value ?? 0
                    let prior = row["Stock Splits"]?.doubleValue ?? 0
                    if prior == 0 {
                        row["Stock Splits"] = .number(ratio)
                    } else {
                        row["Stock Splits"] = .number(prior * ratio)
                    }
                case .capitalGain:
                    guard includeCapitalGains else { break }
                    let value = event.value ?? 0
                    let prior = row["Capital Gains"]?.doubleValue ?? 0
                    row["Capital Gains"] = .number(prior + value)
                }

                rowsByKey[rowKey] = row
            }
        }

        let sortedRows = rowsByKey.values.sorted { lhs, rhs in
            switch (lhs["date"], rhs["date"]) {
            case let (.some(.number(left)), .some(.number(right))):
                return left < right
            case let (.some(.string(left)), .some(.string(right))):
                return left < right
            case let (.some(.number(left)), .some(.string(right))):
                return String(left) < right
            case let (.some(.string(left)), .some(.number(right))):
                return left < String(right)
            default:
                return false
            }
        }

        let completedRows = sortedRows.map { row in
            var output = row
            for column in columns where output[column] == nil {
                output[column] = .null
            }
            return output
        }

        return YFTable(columns: columns, rows: completedRows)
    }

    private static func defaultIgnoreTZ(interval: YFinanceClient.Interval) -> Bool {
        // Python yfinance default: ignore_tz defaults to False for intraday, True otherwise.
        return !isIntraday(interval: interval)
    }

    private static func resolveTimeZone(from exchangeTimezoneName: String?) -> TimeZone {
        if let exchangeTimezoneName,
           let timezone = TimeZone(identifier: exchangeTimezoneName) {
            return timezone
        }
        return TimeZone(secondsFromGMT: 0) ?? .current
    }

    private static func tableRowKey(date: Date, timezone: TimeZone, ignoreTZ: Bool) -> String {
        if ignoreTZ {
            return localDateFormatter(timezone: timezone).string(from: date)
        }
        return String(Int(date.timeIntervalSince1970))
    }

    private static func tableDateValue(date: Date, timezone: TimeZone, ignoreTZ: Bool) -> YFJSONValue {
        if ignoreTZ {
            return .string(localDateFormatter(timezone: timezone).string(from: date))
        }
        return .number(Double(Int(date.timeIntervalSince1970)))
    }

    private static func localDateFormatter(timezone: TimeZone) -> DateFormatter {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = timezone
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter
    }

    private static func isIntraday(interval: YFinanceClient.Interval) -> Bool {
        switch interval {
        case .oneDay, .fiveDays, .oneWeek, .oneMonth, .threeMonths:
            return false
        default:
            return true
        }
    }

    private static func upperBound(_ dates: [Date], target: Date) -> Int {
        var low = 0
        var high = dates.count
        while low < high {
            let mid = (low + high) / 2
            if dates[mid] <= target {
                low = mid + 1
            } else {
                high = mid
            }
        }
        return low
    }

    private static func lowerBound(_ values: [Int], target: Int) -> Int {
        var low = 0
        var high = values.count
        while low < high {
            let mid = (low + high) / 2
            if values[mid] < target {
                low = mid + 1
            } else {
                high = mid
            }
        }
        return low
    }

    private static func dayKey(for date: Date, calendar: Calendar) -> Int {
        let comps = calendar.dateComponents([.year, .month, .day], from: date)
        let year = comps.year ?? 0
        let month = comps.month ?? 0
        let day = comps.day ?? 0
        return year * 10_000 + month * 100 + day
    }

    private static func addInterval(_ date: Date, interval: YFinanceClient.Interval, calendar: Calendar) -> Date? {
        switch interval {
        case .oneDay:
            return calendar.date(byAdding: .day, value: 1, to: date)
        case .fiveDays:
            return calendar.date(byAdding: .day, value: 5, to: date)
        case .oneWeek:
            return calendar.date(byAdding: .weekOfYear, value: 1, to: date)
        case .oneMonth:
            return calendar.date(byAdding: .month, value: 1, to: date)
        case .threeMonths:
            return calendar.date(byAdding: .month, value: 3, to: date)
        default:
            return nil
        }
    }
}

public struct YFHistoryLastTrade: Decodable, Sendable {
    public let price: Double?
    public let time: Int?
}

public struct YFTradingPeriod: Decodable, Sendable {
    public let start: Int?
    public let end: Int?
    public let timezone: String?
    public let gmtoffset: Int?
}

public struct YFTradingPeriods: Decodable, Sendable {
    public let regular: [YFTradingPeriod]
    public let pre: [YFTradingPeriod]
    public let post: [YFTradingPeriod]

    public init(
        regular: [YFTradingPeriod] = [],
        pre: [YFTradingPeriod] = [],
        post: [YFTradingPeriod] = []
    ) {
        self.regular = regular
        self.pre = pre
        self.post = post
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()

        // Shape 1: list of lists => only regular sessions.
        if let nested = try? container.decode([[YFTradingPeriod]].self) {
            self.init(regular: nested.flatMap { $0 })
            return
        }

        // Shape 2: dict with pre/regular/post arrays-of-arrays.
        if let dict = try? container.decode([String: [[YFTradingPeriod]]].self) {
            self.init(
                regular: (dict["regular"] ?? []).flatMap { $0 },
                pre: (dict["pre"] ?? []).flatMap { $0 },
                post: (dict["post"] ?? []).flatMap { $0 }
            )
            return
        }

        // Fallback: unknown/empty shape.
        self.init()
    }
}

public struct YFHistoryMeta: Decodable, Sendable {
    public let currency: String?
    public let symbol: String?
    public let exchangeName: String?
    public let instrumentType: String?
    public let timezone: String?
    public let exchangeTimezoneName: String?
    public let regularMarketPrice: Double?
    public let chartPreviousClose: Double?
    public let previousClose: Double?
    public let gmtoffset: Int?
    public let dataGranularity: String?
    public let priceHint: Int?
    public let range: String?
    public let validRanges: [String]?
    public let lastTrade: YFHistoryLastTrade?
    public let tradingPeriods: YFTradingPeriods?
}

public struct YFHistoryBar: Sendable {
    public let date: Date
    public let open: Double?
    public let high: Double?
    public let low: Double?
    public let close: Double?
    public let adjustedClose: Double?
    public let volume: Int?
    public let repaired: Bool

    public init(
        date: Date,
        open: Double?,
        high: Double?,
        low: Double?,
        close: Double?,
        adjustedClose: Double?,
        volume: Int?,
        repaired: Bool = false
    ) {
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.adjustedClose = adjustedClose
        self.volume = volume
        self.repaired = repaired
    }
}

struct YFFinanceErrorEnvelope: Decodable {
    let finance: YFFinancePayload?
}

struct YFFinancePayload: Decodable {
    let error: YFYahooError?
}

struct YFYahooError: Decodable {
    let code: String?
    let description: String?
}

struct YFQuoteResponse: Decodable {
    let quoteResponse: YFQuoteResponseBody
}

struct YFQuoteResponseBody: Decodable {
    let result: [YFQuote]
    let error: YFYahooError?
}

struct YFChartResponse: Decodable {
    let chart: YFChartPayload
}

struct YFChartPayload: Decodable {
    let result: [YFChartResult]?
    let error: YFYahooError?
}

struct YFChartResult: Decodable {
    let meta: YFHistoryMeta
    let timestamp: [Int]?
    let indicators: YFChartIndicators
    let events: [String: [String: YFChartEventData]]?
}

struct YFChartIndicators: Decodable {
    let quote: [YFChartQuote]
    let adjclose: [YFChartAdjClose]?
}

struct YFChartQuote: Decodable {
    let open: [Double?]?
    let high: [Double?]?
    let low: [Double?]?
    let close: [Double?]?
    let volume: [Double?]?
}

struct YFChartAdjClose: Decodable {
    let adjclose: [Double?]?
}

struct YFChartEventData: Decodable {
    let amount: Double?
    let date: Int?
    let currency: String?
    let numerator: Double?
    let denominator: Double?
    let splitRatio: String?
}
