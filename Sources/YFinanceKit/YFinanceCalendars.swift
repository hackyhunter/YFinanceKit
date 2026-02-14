import Foundation

public struct YFCalendarQuery: Sendable {
    public let `operator`: String
    public var operands: [YFCalendarOperand]

    public init(_ operator: String, operands: [YFCalendarOperand]) {
        self.operator = `operator`.uppercased()
        self.operands = operands
    }

    mutating public func append(_ operand: YFCalendarOperand) {
        operands.append(operand)
    }

    mutating public func append(_ operand: String) {
        append(.string(operand))
    }

    mutating public func append(_ operand: Int) {
        append(.int(operand))
    }

    mutating public func append(_ operand: Double) {
        append(.double(operand))
    }

    mutating public func append(_ operand: Bool) {
        append(.bool(operand))
    }

    mutating public func append(_ operand: YFCalendarQuery) {
        append(.query(operand))
    }

    public var isEmpty: Bool {
        operands.isEmpty
    }

    public var is_empty: Bool {
        isEmpty
    }

    public func toJSONValue() -> YFJSONValue {
        .object([
            "operator": .string(`operator`),
            "operands": .array(operands.map { $0.toJSONValue() }),
        ])
    }

    public func toDict() -> YFJSONValue {
        toJSONValue()
    }

    public func to_dict() -> YFJSONValue {
        toDict()
    }
}

public indirect enum YFCalendarOperand: Sendable {
    case query(YFCalendarQuery)
    case string(String)
    case int(Int)
    case double(Double)
    case bool(Bool)

    func toJSONValue() -> YFJSONValue {
        switch self {
        case .query(let query):
            return query.toJSONValue()
        case .string(let value):
            return .string(value)
        case .int(let value):
            return .number(Double(value))
        case .double(let value):
            return .number(value)
        case .bool(let value):
            return .bool(value)
        }
    }
}

public actor YFCalendars {
    private let client: YFinanceClient
    private let defaultStart: String
    private let defaultEnd: String
    private var cachedRequestBodies: [YFCalendarType: YFJSONValue] = [:]
    private var cachedCalendars: [YFCalendarType: YFJSONValue] = [:]
    private var mostActiveQuery: YFCalendarQuery = YFCalendarQuery("or", operands: [])

    public init(
        start: Date? = nil,
        end: Date? = nil,
        client: YFinanceClient = YFinanceClient()
    ) {
        self.client = client

        let now = start ?? Date()
        let startString = Self.formatDate(now)
        let endDate = end ?? Calendar.current.date(byAdding: .day, value: 7, to: now) ?? now
        let endString = Self.formatDate(endDate)

        self.defaultStart = startString
        self.defaultEnd = endString
    }

    public func getEarningsCalendar(
        marketCap: Double? = nil,
        filterMostActive: Bool = true,
        start: Date? = nil,
        end: Date? = nil,
        limit: Int = 12,
        offset: Int = 0,
        force: Bool = false
    ) async throws -> YFJSONValue {
        let startDate = dateString(start) ?? defaultStart
        let endDate = dateString(end) ?? defaultEnd

        var query = YFCalendarQuery("and", operands: [
            .query(YFCalendarQuery("eq", operands: [.string("region"), .string("us")])),
            .query(YFCalendarQuery("or", operands: [
                .query(YFCalendarQuery("eq", operands: [.string("eventtype"), .string("EAD")])),
                .query(YFCalendarQuery("eq", operands: [.string("eventtype"), .string("ERA")])),
            ])),
            .query(YFCalendarQuery("gte", operands: [.string("startdatetime"), .string(startDate)])),
            .query(YFCalendarQuery("lte", operands: [.string("startdatetime"), .string(endDate)])),
        ])

        if let marketCap {
            query.append(.query(YFCalendarQuery("gte", operands: [.string("intradaymarketcap"), .double(marketCap)])))
        }

        if filterMostActive && offset == 0 {
            if let mostActiveQuery = await mostActiveOperand(marketCap: marketCap) {
                query.append(.query(mostActiveQuery))
            }
        }

        return try await getCalendar(
            type: .spEarnings,
            query: query,
            limit: limit,
            offset: offset,
            force: force
        )
    }

    public func getIpoInfoCalendar(
        start: Date? = nil,
        end: Date? = nil,
        limit: Int = 12,
        offset: Int = 0,
        force: Bool = false
    ) async throws -> YFJSONValue {
        let startDate = dateString(start) ?? defaultStart
        let endDate = dateString(end) ?? defaultEnd

        let query = YFCalendarQuery("or", operands: [
            .query(YFCalendarQuery("gtelt", operands: [.string("startdatetime"), .string(startDate), .string(endDate)])),
            .query(YFCalendarQuery("gtelt", operands: [.string("filingdate"), .string(startDate), .string(endDate)])),
            .query(YFCalendarQuery("gtelt", operands: [.string("amendeddate"), .string(startDate), .string(endDate)])),
        ])

        return try await getCalendar(type: .ipoInfo, query: query, limit: limit, offset: offset, force: force)
    }

    public func getEconomicEventsCalendar(
        start: Date? = nil,
        end: Date? = nil,
        limit: Int = 12,
        offset: Int = 0,
        force: Bool = false
    ) async throws -> YFJSONValue {
        let query = startDateTimeQuery(start: start, end: end)
        return try await getCalendar(type: .economicEvent, query: query, limit: limit, offset: offset, force: force)
    }

    public func getSplitsCalendar(
        start: Date? = nil,
        end: Date? = nil,
        limit: Int = 12,
        offset: Int = 0,
        force: Bool = false
    ) async throws -> YFJSONValue {
        let query = startDateTimeQuery(start: start, end: end)
        return try await getCalendar(type: .splits, query: query, limit: limit, offset: offset, force: force)
    }

    public func earningsCalendar() async throws -> YFJSONValue {
        try await getEarningsCalendar()
    }

    public func earningsCalendarTable() async throws -> YFTable {
        let raw = try await earningsCalendar()
        return cleanedCalendarTable(type: .spEarnings, raw: raw)
    }

    public func ipoInfoCalendar() async throws -> YFJSONValue {
        try await getIpoInfoCalendar()
    }

    public func ipoInfoCalendarTable() async throws -> YFTable {
        let raw = try await ipoInfoCalendar()
        return cleanedCalendarTable(type: .ipoInfo, raw: raw)
    }

    public func economicEventsCalendar() async throws -> YFJSONValue {
        try await getEconomicEventsCalendar()
    }

    public func economicEventsCalendarTable() async throws -> YFTable {
        let raw = try await economicEventsCalendar()
        return cleanedCalendarTable(type: .economicEvent, raw: raw)
    }

    public func splitsCalendar() async throws -> YFJSONValue {
        try await getSplitsCalendar()
    }

    public func splitsCalendarTable() async throws -> YFTable {
        let raw = try await splitsCalendar()
        return cleanedCalendarTable(type: .splits, raw: raw)
    }

    // Python-style snake_case aliases.
    public func get_earnings_calendar(
        market_cap: Double? = nil,
        filter_most_active: Bool = true,
        start: Date? = nil,
        end: Date? = nil,
        limit: Int = 12,
        offset: Int = 0,
        force: Bool = false
    ) async throws -> YFJSONValue {
        try await getEarningsCalendar(
            marketCap: market_cap,
            filterMostActive: filter_most_active,
            start: start,
            end: end,
            limit: limit,
            offset: offset,
            force: force
        )
    }

    public func get_ipo_info_calendar(
        start: Date? = nil,
        end: Date? = nil,
        limit: Int = 12,
        offset: Int = 0,
        force: Bool = false
    ) async throws -> YFJSONValue {
        try await getIpoInfoCalendar(start: start, end: end, limit: limit, offset: offset, force: force)
    }

    public func get_economic_events_calendar(
        start: Date? = nil,
        end: Date? = nil,
        limit: Int = 12,
        offset: Int = 0,
        force: Bool = false
    ) async throws -> YFJSONValue {
        try await getEconomicEventsCalendar(start: start, end: end, limit: limit, offset: offset, force: force)
    }

    public func get_splits_calendar(
        start: Date? = nil,
        end: Date? = nil,
        limit: Int = 12,
        offset: Int = 0,
        force: Bool = false
    ) async throws -> YFJSONValue {
        try await getSplitsCalendar(start: start, end: end, limit: limit, offset: offset, force: force)
    }

    public func earnings_calendar() async throws -> YFJSONValue { try await earningsCalendar() }
    public func ipo_info_calendar() async throws -> YFJSONValue { try await ipoInfoCalendar() }
    public func economic_events_calendar() async throws -> YFJSONValue { try await economicEventsCalendar() }
    public func splits_calendar() async throws -> YFJSONValue { try await splitsCalendar() }
    public func earnings_calendar_table() async throws -> YFTable { try await earningsCalendarTable() }
    public func ipo_info_calendar_table() async throws -> YFTable { try await ipoInfoCalendarTable() }
    public func economic_events_calendar_table() async throws -> YFTable { try await economicEventsCalendarTable() }
    public func splits_calendar_table() async throws -> YFTable { try await splitsCalendarTable() }

    private func getCalendar(
        type: YFCalendarType,
        query: YFCalendarQuery,
        limit: Int,
        offset: Int,
        force: Bool
    ) async throws -> YFJSONValue {
        let preset = Self.presets[type]!
        let body: YFJSONValue = .object([
            "sortType": .string("DESC"),
            "entityIdType": .string(type.rawValue),
            "sortField": .string(preset.sortField),
            "includeFields": .array(preset.includeFields.map { .string($0) }),
            "size": .number(Double(min(max(limit, 1), 100))),
            "offset": .number(Double(max(offset, 0))),
            "query": query.toJSONValue(),
        ])

        if !force,
           let cachedBody = cachedRequestBodies[type],
           let cached = cachedCalendars[type],
           cachedBody == body {
            return cached
        }

        cachedRequestBodies[type] = body

        let raw = try await client.visualization(body: body)
        let flattened = Self.flattenVisualization(raw)
        cachedCalendars[type] = flattened
        return flattened
    }

    private func startDateTimeQuery(start: Date?, end: Date?) -> YFCalendarQuery {
        let startDate = dateString(start) ?? defaultStart
        let endDate = dateString(end) ?? defaultEnd

        return YFCalendarQuery("and", operands: [
            .query(YFCalendarQuery("gte", operands: [.string("startdatetime"), .string(startDate)])),
            .query(YFCalendarQuery("lte", operands: [.string("startdatetime"), .string(endDate)])),
        ])
    }

    private func mostActiveOperand(marketCap: Double?) async -> YFCalendarQuery? {
        // Cache semantics mirror Python yfinance: only query once per Calendars instance.
        if !mostActiveQuery.isEmpty {
            return mostActiveQuery
        }

        do {
            let raw = try await client.screenerPredefined(id: "most_actives", count: 200)
            let quotes = raw["finance"]?["result"]?[0]?["quotes"]?.arrayValue ?? []
            if quotes.isEmpty {
                return nil
            }

            var operands: [YFCalendarOperand] = []
            for quote in quotes {
                guard let ticker = quote["symbol"]?.stringValue, !ticker.isEmpty else {
                    continue
                }
                if let marketCap {
                    let quoteMarketCap = quote["marketCap"]?.doubleValue ?? 0
                    if quoteMarketCap < marketCap {
                        continue
                    }
                }
                operands.append(.query(YFCalendarQuery("eq", operands: [.string("ticker"), .string(ticker)])))
            }

            if operands.isEmpty {
                return nil
            }

            let query = YFCalendarQuery("or", operands: operands)
            mostActiveQuery = query
            return query
        } catch {
            // Best-effort: if fetching most-actives fails, skip the filter.
            return mostActiveQuery.isEmpty ? nil : mostActiveQuery
        }
    }

    private func dateString(_ date: Date?) -> String? {
        guard let date else {
            return nil
        }
        return Self.formatDate(date)
    }

    private func cleanedCalendarTable(type: YFCalendarType, raw: YFJSONValue) -> YFTable {
        var table = raw.toTable()

        guard let preset = PREDEFINED_CALENDARS[type.rawValue] else {
            return table
        }

        // Apply nan_cols before renames (mirrors Python ordering).
        let nanCols: [String] = preset["nan_cols"]?.arrayValue?.compactMap { $0.stringValue } ?? []
        if !nanCols.isEmpty {
            let updatedRows: [[String: YFJSONValue]] = table.rows.map { row in
                var updated = row
                for col in nanCols {
                    if updated[col]?.doubleValue == 0 {
                        updated[col] = .null
                    }
                }
                return updated
            }
            table = YFTable(columns: table.columns, rows: updatedRows)
        }

        let renamesObject = preset["renames"]?.objectValue ?? [:]
        var renames: [String: String] = [:]
        renames.reserveCapacity(renamesObject.count)
        for (from, toValue) in renamesObject {
            if let to = toValue.stringValue, !to.isEmpty {
                renames[from] = to
            }
        }

        if !renames.isEmpty {
            let updatedColumns = table.columns.map { renames[$0] ?? $0 }
            let updatedRows: [[String: YFJSONValue]] = table.rows.map { row in
                var updated = row
                for (from, to) in renames {
                    if let value = updated[from] {
                        updated[to] = value
                        updated.removeValue(forKey: from)
                    }
                }
                return updated
            }
            table = YFTable(columns: updatedColumns, rows: updatedRows)
        }

        return table
    }

    private static func flattenVisualization(_ raw: YFJSONValue) -> YFJSONValue {
        guard
            let document = raw["finance"]?["result"]?[0]?["documents"]?[0],
            let columnItems = document["columns"]?.arrayValue,
            let rows = document["rows"]?.arrayValue
        else {
            return raw
        }

        let labels = columnItems.map { $0["label"]?.stringValue ?? "" }
        let flattened: [YFJSONValue] = rows.map { row in
            guard let rowValues = row.arrayValue else {
                return row
            }
            var object: [String: YFJSONValue] = [:]
            for (index, label) in labels.enumerated() where !label.isEmpty {
                if rowValues.indices.contains(index) {
                    object[label] = rowValues[index]
                }
            }
            return .object(object)
        }
        return .array(flattened)
    }

    private static func formatDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.string(from: date)
    }

    private static let presets: [YFCalendarType: (sortField: String, includeFields: [String])] = [
        .spEarnings: (
            "intradaymarketcap",
            [
                "ticker",
                "companyshortname",
                "intradaymarketcap",
                "eventname",
                "startdatetime",
                "startdatetimetype",
                "epsestimate",
                "epsactual",
                "epssurprisepct",
            ]
        ),
        .ipoInfo: (
            "startdatetime",
            [
                "ticker",
                "companyshortname",
                "exchange_short_name",
                "filingdate",
                "startdatetime",
                "amendeddate",
                "pricefrom",
                "priceto",
                "offerprice",
                "currencyname",
                "shares",
                "dealtype",
            ]
        ),
        .economicEvent: (
            "startdatetime",
            [
                "econ_release",
                "country_code",
                "startdatetime",
                "period",
                "after_release_actual",
                "consensus_estimate",
                "prior_release_actual",
                "originally_reported_actual",
            ]
        ),
        .splits: (
            "startdatetime",
            [
                "ticker",
                "companyshortname",
                "startdatetime",
                "optionable",
                "old_share_worth",
                "share_worth",
            ]
        ),
    ]
}
