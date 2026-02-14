import Foundation

public struct YFTickers: Sendable, CustomStringConvertible {
    public let symbols: [String]
    private let client: YFinanceClient

    public init(_ symbols: [String], client: YFinanceClient = YFinanceClient()) {
        self.init(symbols: symbols, client: client)
    }

    public init(_ tickers: [(String, String)], client: YFinanceClient = YFinanceClient()) throws {
        let resolved = try tickers.map { try yahooTicker(baseSymbol: $0.0, mic: $0.1) }
        self.init(symbols: resolved, client: client)
    }

    public init(symbols: [String], client: YFinanceClient = YFinanceClient()) {
        self.symbols = symbols
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).uppercased() }
            .filter { !$0.isEmpty }
        self.client = client
    }

    public init(_ tickers: String, client: YFinanceClient = YFinanceClient()) {
        let parsed = tickers
            .replacingOccurrences(of: ",", with: " ")
            .split(separator: " ")
            .map(String.init)
        self.init(symbols: parsed, client: client)
    }

    public var description: String {
        "yfinance.Tickers object <\(symbols.joined(separator: ","))>"
    }

    public var tickers: [String: YFTicker] {
        var map: [String: YFTicker] = [:]
        map.reserveCapacity(symbols.count)
        for symbol in symbols {
            map[symbol] = YFTicker(symbol: symbol, client: client)
        }
        return map
    }

    public func ticker(_ symbol: String) -> YFTicker {
        YFTicker(symbol: symbol, client: client)
    }

    public func live(
        messageHandler: (@Sendable (YFStreamingMessage) -> Void)?,
        verbose: Bool = true
    ) -> YFWebSocket {
        let ws = YFWebSocket(verbose: verbose)
        ws.subscribe(symbols)
        ws.listen(messageHandler)
        return ws
    }

    public func live(
        message_handler: (@Sendable (YFStreamingMessage) -> Void)? = nil,
        verbose: Bool = true
    ) -> YFWebSocket {
        live(messageHandler: message_handler, verbose: verbose)
    }

    public func quote() async throws -> [String: YFQuote] {
        let quotes = try await client.quote(symbols: symbols)
        var map: [String: YFQuote] = [:]
        map.reserveCapacity(quotes.count)
        for quote in quotes {
            map[quote.symbol] = quote
        }
        return map
    }

    public func history(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> [String: YFHistorySeries] {
        let requestedEvents: Set<YFinanceClient.HistoryEvent> = actions ? [.dividends, .splits, .capitalGains] : []
        if !threads {
            var map: [String: YFHistorySeries] = [:]
            map.reserveCapacity(symbols.count)
            for symbol in symbols {
                map[symbol] = try await client.history(
                    symbol: symbol,
                    range: period,
                    interval: interval,
                    includePrePost: prepost,
                    events: requestedEvents,
                    autoAdjust: autoAdjust,
                    backAdjust: backAdjust,
                    repair: repair,
                    keepNa: keepNa,
                    rounding: rounding,
                    timeout: timeout
                )
            }
            return map
        }

        return try await withThrowingTaskGroup(of: (String, YFHistorySeries).self) { group in
            for symbol in symbols {
                group.addTask { [client] in
                    let series = try await client.history(
                        symbol: symbol,
                        range: period,
                        interval: interval,
                        includePrePost: prepost,
                        events: requestedEvents,
                        autoAdjust: autoAdjust,
                        backAdjust: backAdjust,
                        repair: repair,
                        keepNa: keepNa,
                        rounding: rounding,
                        timeout: timeout
                    )
                    return (symbol, series)
                }
            }

            var map: [String: YFHistorySeries] = [:]
            map.reserveCapacity(symbols.count)
            for try await (symbol, series) in group {
                map[symbol] = series
            }
            return map
        }
    }

    public func history(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> [String: YFHistorySeries] {
        let requestedEvents: Set<YFinanceClient.HistoryEvent> = actions ? [.dividends, .splits, .capitalGains] : []
        if !threads {
            var map: [String: YFHistorySeries] = [:]
            map.reserveCapacity(symbols.count)
            for symbol in symbols {
                map[symbol] = try await client.history(
                    symbol: symbol,
                    period: period,
                    interval: interval,
                    includePrePost: prepost,
                    events: requestedEvents,
                    autoAdjust: autoAdjust,
                    backAdjust: backAdjust,
                    repair: repair,
                    keepNa: keepNa,
                    rounding: rounding,
                    timeout: timeout
                )
            }
            return map
        }

        return try await withThrowingTaskGroup(of: (String, YFHistorySeries).self) { group in
            for symbol in symbols {
                group.addTask { [client] in
                    let series = try await client.history(
                        symbol: symbol,
                        period: period,
                        interval: interval,
                        includePrePost: prepost,
                        events: requestedEvents,
                        autoAdjust: autoAdjust,
                        backAdjust: backAdjust,
                        repair: repair,
                        keepNa: keepNa,
                        rounding: rounding,
                        timeout: timeout
                    )
                    return (symbol, series)
                }
            }

            var map: [String: YFHistorySeries] = [:]
            map.reserveCapacity(symbols.count)
            for try await (symbol, series) in group {
                map[symbol] = series
            }
            return map
        }
    }

    public func history(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> [String: YFHistorySeries] {
        let requestedEvents: Set<YFinanceClient.HistoryEvent> = actions ? [.dividends, .splits, .capitalGains] : []
        if !threads {
            var map: [String: YFHistorySeries] = [:]
            map.reserveCapacity(symbols.count)
            for symbol in symbols {
                map[symbol] = try await client.history(
                    symbol: symbol,
                    start: start,
                    end: end,
                    interval: interval,
                    includePrePost: prepost,
                    events: requestedEvents,
                    autoAdjust: autoAdjust,
                    backAdjust: backAdjust,
                    repair: repair,
                    keepNa: keepNa,
                    rounding: rounding,
                    timeout: timeout
                )
            }
            return map
        }

        return try await withThrowingTaskGroup(of: (String, YFHistorySeries).self) { group in
            for symbol in symbols {
                group.addTask { [client] in
                    let series = try await client.history(
                        symbol: symbol,
                        start: start,
                        end: end,
                        interval: interval,
                        includePrePost: prepost,
                        events: requestedEvents,
                        autoAdjust: autoAdjust,
                        backAdjust: backAdjust,
                        repair: repair,
                        keepNa: keepNa,
                        rounding: rounding,
                        timeout: timeout
                    )
                    return (symbol, series)
                }
            }

            var map: [String: YFHistorySeries] = [:]
            map.reserveCapacity(symbols.count)
            for try await (symbol, series) in group {
                map[symbol] = series
            }
            return map
        }
    }

    public func history(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> [String: YFHistorySeries] {
        let requestedEvents: Set<YFinanceClient.HistoryEvent> = actions ? [.dividends, .splits, .capitalGains] : []
        if !threads {
            var map: [String: YFHistorySeries] = [:]
            map.reserveCapacity(symbols.count)
            for symbol in symbols {
                map[symbol] = try await client.history(
                    symbol: symbol,
                    start: start,
                    end: end,
                    interval: interval,
                    includePrePost: prepost,
                    events: requestedEvents,
                    autoAdjust: autoAdjust,
                    backAdjust: backAdjust,
                    repair: repair,
                    keepNa: keepNa,
                    rounding: rounding,
                    timeout: timeout
                )
            }
            return map
        }

        return try await withThrowingTaskGroup(of: (String, YFHistorySeries).self) { group in
            for symbol in symbols {
                group.addTask { [client] in
                    let series = try await client.history(
                        symbol: symbol,
                        start: start,
                        end: end,
                        interval: interval,
                        includePrePost: prepost,
                        events: requestedEvents,
                        autoAdjust: autoAdjust,
                        backAdjust: backAdjust,
                        repair: repair,
                        keepNa: keepNa,
                        rounding: rounding,
                        timeout: timeout
                    )
                    return (symbol, series)
                }
            }

            var map: [String: YFHistorySeries] = [:]
            map.reserveCapacity(symbols.count)
            for try await (symbol, series) in group {
                map[symbol] = series
            }
            return map
        }
    }

    public func download(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func download(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func download(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await history(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func download(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await history(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func history(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await historyTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func history(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await historyTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func history(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await historyTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func history(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await historyTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func download(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await downloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func download(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await downloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func download(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await downloadTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func download(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await downloadTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func news(
        count: Int = 10,
        tab: YFNewsTab = .news,
        threads: Bool = true
    ) async throws -> [String: [YFJSONValue]] {
        if !threads {
            var output: [String: [YFJSONValue]] = [:]
            output.reserveCapacity(symbols.count)
            for symbol in symbols {
                let ticker = YFTicker(symbol: symbol, client: client)
                output[symbol] = try await ticker.news(count: count, tab: tab).arrayValue ?? []
            }
            return output
        }

        return try await withThrowingTaskGroup(of: (String, [YFJSONValue]).self) { group in
            for symbol in symbols {
                group.addTask { [client] in
                    let ticker = YFTicker(symbol: symbol, client: client)
                    let items = try await ticker.news(count: count, tab: tab).arrayValue ?? []
                    return (symbol, items)
                }
            }

            var output: [String: [YFJSONValue]] = [:]
            output.reserveCapacity(symbols.count)
            for try await (symbol, items) in group {
                output[symbol] = items
            }
            return output
        }
    }

    public func historyTable(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        let seriesBySymbol = try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            timeout: timeout
        )

        let resolvedIgnoreTZ = ignoreTZ ?? Self.defaultIgnoreTZ(interval: interval.rawValue)
        return Self.combineHistoryTable(
            seriesBySymbol: seriesBySymbol,
            symbolOrderHint: symbols,
            groupBy: groupBy,
            ignoreTZ: resolvedIgnoreTZ,
            includeActions: actions,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func historyTable(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        let seriesBySymbol = try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            timeout: timeout
        )

        let resolvedIgnoreTZ = ignoreTZ ?? Self.defaultIgnoreTZ(interval: interval)
        return Self.combineHistoryTable(
            seriesBySymbol: seriesBySymbol,
            symbolOrderHint: symbols,
            groupBy: groupBy,
            ignoreTZ: resolvedIgnoreTZ,
            includeActions: actions,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func historyTable(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        let seriesBySymbol = try await history(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            timeout: timeout
        )

        let resolvedIgnoreTZ = ignoreTZ ?? Self.defaultIgnoreTZ(interval: interval.rawValue)
        return Self.combineHistoryTable(
            seriesBySymbol: seriesBySymbol,
            symbolOrderHint: symbols,
            groupBy: groupBy,
            ignoreTZ: resolvedIgnoreTZ,
            includeActions: actions,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func historyTable(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        let seriesBySymbol = try await history(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            timeout: timeout
        )

        let resolvedIgnoreTZ = ignoreTZ ?? Self.defaultIgnoreTZ(interval: interval)
        return Self.combineHistoryTable(
            seriesBySymbol: seriesBySymbol,
            symbolOrderHint: symbols,
            groupBy: groupBy,
            ignoreTZ: resolvedIgnoreTZ,
            includeActions: actions,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func historyTable(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: String,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        guard let parsedGroupBy = YFGroupBy(pythonValue: groupBy) else {
            throw YFinanceError.invalidRequest("groupBy must be 'column' or 'ticker'")
        }
        return try await historyTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: parsedGroupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
    }

    public func historyTable(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: String,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        guard let parsedGroupBy = YFGroupBy(pythonValue: groupBy) else {
            throw YFinanceError.invalidRequest("groupBy must be 'column' or 'ticker'")
        }
        return try await historyTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: parsedGroupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
    }

    public func downloadTable(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        try await historyTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
    }

    public func downloadTable(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        try await historyTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
    }

    public func downloadTable(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        try await historyTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
    }

    public func downloadTable(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        try await historyTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
    }

    public func downloadTable(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: String,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        guard let parsedGroupBy = YFGroupBy(pythonValue: groupBy) else {
            throw YFinanceError.invalidRequest("groupBy must be 'column' or 'ticker'")
        }
        return try await downloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: parsedGroupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
    }

    public func downloadTable(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: String,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true,
        timeout: TimeInterval? = nil
    ) async throws -> YFTable {
        guard let parsedGroupBy = YFGroupBy(pythonValue: groupBy) else {
            throw YFinanceError.invalidRequest("groupBy must be 'column' or 'ticker'")
        }
        return try await downloadTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: parsedGroupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
    }

    public func getHistoryTable(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await historyTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func getHistoryTable(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await historyTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func getDownloadTable(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await downloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func getDownloadTable(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await downloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func get_history_table(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await getHistoryTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func get_history_table(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await getHistoryTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func get_download_table(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await getDownloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    public func get_download_table(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        groupBy: YFGroupBy = .column,
        ignoreTZ: Bool? = nil,
        multiLevelIndex: Bool = true
    ) async throws -> YFTable {
        try await getDownloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex
        )
    }

    private static func defaultIgnoreTZ(interval: String) -> Bool {
        let normalized = interval.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if normalized.hasSuffix("m") || normalized.hasSuffix("h") {
            return false
        }
        return true
    }

    private static func combineHistoryTable(
        seriesBySymbol: [String: YFHistorySeries],
        symbolOrderHint: [String],
        groupBy: YFGroupBy,
        ignoreTZ: Bool,
        includeActions: Bool,
        multiLevelIndex: Bool
    ) -> YFTable {
        if seriesBySymbol.isEmpty {
            return YFTable(columns: ["date"], rows: [])
        }

        var symbolOrder: [String] = symbolOrderHint.filter { seriesBySymbol[$0] != nil }
        for symbol in seriesBySymbol.keys where !symbolOrder.contains(symbol) {
            symbolOrder.append(symbol)
        }

        let flattenSingleLevel = !multiLevelIndex && symbolOrder.count == 1

        let includeAdjClose = seriesBySymbol.values.contains { series in
            series.bars.contains { $0.adjustedClose != nil }
        }

        let includeCapitalGains = includeActions && seriesBySymbol.values.contains { series in
            if series.events.contains(where: { $0.kind == .capitalGain }) {
                return true
            }
            let instrumentType = series.meta.instrumentType?
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .uppercased()
            return instrumentType == "MUTUALFUND" || instrumentType == "ETF"
        }

        let fields = historyFieldNames(
            includeActions: includeActions,
            includeAdjClose: includeAdjClose,
            includeCapitalGains: includeCapitalGains
        )
        let orderedFields = (groupBy == .column) ? fields.sorted() : fields
        let columns = historyColumns(
            symbolOrder: symbolOrder,
            fields: orderedFields,
            groupBy: groupBy,
            flattenSingleLevel: flattenSingleLevel
        )

        var rowsByKey: [String: [String: YFJSONValue]] = [:]

        for symbol in symbolOrder {
            guard let series = seriesBySymbol[symbol] else {
                continue
            }

            let timezone = resolveTimeZone(from: series.meta.exchangeTimezoneName)
            let intraday = isIntraday(granularity: series.meta.dataGranularity)
            var rowKeyByDay: [String: String] = [:]
            let orderedBars = series.bars.sorted { $0.date < $1.date }
            var barDates: [Date] = []
            var barRowKeys: [String] = []
            barDates.reserveCapacity(orderedBars.count)
            barRowKeys.reserveCapacity(orderedBars.count)

            for bar in orderedBars {
                let rowKey = tableRowKey(date: bar.date, timezone: timezone, ignoreTZ: ignoreTZ)
                let dayKey = tableDayKey(date: bar.date, timezone: timezone, ignoreTZ: ignoreTZ)

                barDates.append(bar.date)
                barRowKeys.append(rowKey)

                if intraday, rowKeyByDay[dayKey] == nil {
                    rowKeyByDay[dayKey] = rowKey
                }

                var row = rowsByKey[rowKey] ?? [
                    "date": tableDateValue(date: bar.date, timezone: timezone, ignoreTZ: ignoreTZ),
                ]

                row[historyColumnName(
                    symbol: symbol,
                    field: "Open",
                    groupBy: groupBy,
                    flattenSingleLevel: flattenSingleLevel
                )] = bar.open.map { .number($0) } ?? .null
                row[historyColumnName(
                    symbol: symbol,
                    field: "High",
                    groupBy: groupBy,
                    flattenSingleLevel: flattenSingleLevel
                )] = bar.high.map { .number($0) } ?? .null
                row[historyColumnName(
                    symbol: symbol,
                    field: "Low",
                    groupBy: groupBy,
                    flattenSingleLevel: flattenSingleLevel
                )] = bar.low.map { .number($0) } ?? .null
                row[historyColumnName(
                    symbol: symbol,
                    field: "Close",
                    groupBy: groupBy,
                    flattenSingleLevel: flattenSingleLevel
                )] = bar.close.map { .number($0) } ?? .null
                if includeAdjClose {
                    row[historyColumnName(
                        symbol: symbol,
                        field: "Adj Close",
                        groupBy: groupBy,
                        flattenSingleLevel: flattenSingleLevel
                    )] = bar.adjustedClose.map { .number($0) } ?? .null
                }
                row[historyColumnName(
                    symbol: symbol,
                    field: "Volume",
                    groupBy: groupBy,
                    flattenSingleLevel: flattenSingleLevel
                )] = bar.volume.map { .number(Double($0)) } ?? .null

                if includeActions {
                    row[historyColumnName(
                        symbol: symbol,
                        field: "Dividends",
                        groupBy: groupBy,
                        flattenSingleLevel: flattenSingleLevel
                    )] = row[historyColumnName(
                        symbol: symbol,
                        field: "Dividends",
                        groupBy: groupBy,
                        flattenSingleLevel: flattenSingleLevel
                    )] ?? .number(0)
                    row[historyColumnName(
                        symbol: symbol,
                        field: "Stock Splits",
                        groupBy: groupBy,
                        flattenSingleLevel: flattenSingleLevel
                    )] = row[historyColumnName(
                        symbol: symbol,
                        field: "Stock Splits",
                        groupBy: groupBy,
                        flattenSingleLevel: flattenSingleLevel
                    )] ?? .number(0)
                    row[historyColumnName(
                        symbol: symbol,
                        field: "Capital Gains",
                        groupBy: groupBy,
                        flattenSingleLevel: flattenSingleLevel
                    )] = includeCapitalGains ? (row[historyColumnName(
                        symbol: symbol,
                        field: "Capital Gains",
                        groupBy: groupBy,
                        flattenSingleLevel: flattenSingleLevel
                    )] ?? .number(0)) : nil
                }

                rowsByKey[rowKey] = row
            }

            if includeActions {
                for event in series.events {
                    let rowKey: String
                    if intraday {
                        let dayKey = tableDayKey(date: event.date, timezone: timezone, ignoreTZ: ignoreTZ)
                        rowKey = rowKeyByDay[dayKey] ?? tableRowKey(date: event.date, timezone: timezone, ignoreTZ: ignoreTZ)
                    } else {
                        let insertion = upperBound(barDates, target: event.date)
                        let index = insertion - 1
                        guard barRowKeys.indices.contains(index) else {
                            continue
                        }
                        rowKey = barRowKeys[index]
                    }
                    var row = rowsByKey[rowKey] ?? [
                        "date": tableDateValue(date: event.date, timezone: timezone, ignoreTZ: ignoreTZ),
                    ]

                    switch event.kind {
                    case .dividend:
                        let value = event.value ?? 0
                        let column = historyColumnName(
                            symbol: symbol,
                            field: "Dividends",
                            groupBy: groupBy,
                            flattenSingleLevel: flattenSingleLevel
                        )
                        let prior = row[column]?.doubleValue ?? 0
                        row[column] = .number(prior + value)
                    case .split:
                        let ratio = event.ratio ?? event.value ?? 0
                        let column = historyColumnName(
                            symbol: symbol,
                            field: "Stock Splits",
                            groupBy: groupBy,
                            flattenSingleLevel: flattenSingleLevel
                        )
                        let prior = row[column]?.doubleValue ?? 0
                        if prior == 0 {
                            row[column] = .number(ratio)
                        } else {
                            row[column] = .number(prior * ratio)
                        }
                    case .capitalGain:
                        guard includeCapitalGains else { break }
                        let value = event.value ?? 0
                        let column = historyColumnName(
                            symbol: symbol,
                            field: "Capital Gains",
                            groupBy: groupBy,
                            flattenSingleLevel: flattenSingleLevel
                        )
                        let prior = row[column]?.doubleValue ?? 0
                        row[column] = .number(prior + value)
                    }

                    rowsByKey[rowKey] = row
                }
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

    private static func historyFieldNames(includeActions: Bool) -> [String] {
        historyFieldNames(includeActions: includeActions, includeAdjClose: true, includeCapitalGains: includeActions)
    }

    private static func historyFieldNames(
        includeActions: Bool,
        includeAdjClose: Bool,
        includeCapitalGains: Bool
    ) -> [String] {
        var fields: [String] = ["Open", "High", "Low", "Close"]
        if includeAdjClose {
            fields.append("Adj Close")
        }
        fields.append("Volume")

        if includeActions {
            fields.append("Dividends")
            fields.append("Stock Splits")
            if includeCapitalGains {
                fields.append("Capital Gains")
            }
        }

        return fields
    }

    private static func historyColumns(
        symbolOrder: [String],
        fields: [String],
        groupBy: YFGroupBy,
        flattenSingleLevel: Bool
    ) -> [String] {
        var columns: [String] = ["date"]
        if flattenSingleLevel {
            columns.append(contentsOf: fields)
            return columns
        }

        switch groupBy {
        case .ticker:
            for symbol in symbolOrder {
                for field in fields {
                    columns.append("\(symbol).\(field)")
                }
            }
        case .column:
            for field in fields {
                for symbol in symbolOrder {
                    columns.append("\(field).\(symbol)")
                }
            }
        }
        return columns
    }

    private static func historyColumnName(
        symbol: String,
        field: String,
        groupBy: YFGroupBy,
        flattenSingleLevel: Bool
    ) -> String {
        if flattenSingleLevel {
            return field
        }
        switch groupBy {
        case .ticker:
            return "\(symbol).\(field)"
        case .column:
            return "\(field).\(symbol)"
        }
    }

    private static func resolveTimeZone(from exchangeTimezoneName: String?) -> TimeZone {
        if let exchangeTimezoneName,
           let timezone = TimeZone(identifier: exchangeTimezoneName) {
            return timezone
        }
        return TimeZone(secondsFromGMT: 0) ?? .current
    }

    private static func isIntraday(granularity: String?) -> Bool {
        let normalized = (granularity ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        return normalized.hasSuffix("m") || normalized.hasSuffix("h")
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

    private static func tableRowKey(date: Date, timezone: TimeZone, ignoreTZ: Bool) -> String {
        if ignoreTZ {
            return localDateFormatter(timezone: timezone).string(from: date)
        }
        return String(Int(date.timeIntervalSince1970))
    }

    private static func tableDayKey(date: Date, timezone: TimeZone, ignoreTZ: Bool) -> String {
        if ignoreTZ {
            return localDayFormatter(timezone: timezone).string(from: date)
        }
        let key = Int(floor(date.timeIntervalSince1970 / 86_400))
        return String(key)
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

    private static func localDayFormatter(timezone: TimeZone) -> DateFormatter {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = timezone
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }

    public func liveSocket() -> YFWebSocket {
        let socket = YFWebSocket()
        socket.subscribe(symbols)
        return socket
    }

    public func live(
        _ handler: (@Sendable (YFStreamingMessage) -> Void)?,
        verbose: Bool = true
    ) -> YFWebSocket {
        let socket = YFWebSocket(verbose: verbose)
        socket.subscribe(symbols)
        socket.listen(handler)
        return socket
    }

    public func history(
        start: Date? = nil,
        end: Date? = nil,
        prepost: Bool = false,
        actions: Bool = true,
        auto_adjust: Bool = true,
        back_adjust: Bool = false,
        repair: Bool = false,
        keepna: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        group_by: String = "column",
        ignore_tz: Bool? = nil,
        progress _: Bool = true,
        period: String? = nil,
        interval: String = "1d",
        timeout: TimeInterval = 10,
        multi_level_index: Bool = true
    ) async throws -> YFTable {
        guard let parsedGroupBy = YFGroupBy(pythonValue: group_by) else {
            throw YFinanceError.invalidRequest("group_by must be 'column' or 'ticker'")
        }

        if let start {
            return try await historyTable(
                start: start,
                end: end ?? Date(),
                interval: interval,
                prepost: prepost,
                actions: actions,
                autoAdjust: auto_adjust,
                backAdjust: back_adjust,
                repair: repair,
                keepNa: keepna,
                rounding: rounding,
                threads: threads,
                groupBy: parsedGroupBy,
                ignoreTZ: ignore_tz,
                multiLevelIndex: multi_level_index,
                timeout: timeout
            )
        }

        return try await historyTable(
            period: period ?? "1mo",
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: auto_adjust,
            backAdjust: back_adjust,
            repair: repair,
            keepNa: keepna,
            rounding: rounding,
            threads: threads,
            groupBy: parsedGroupBy,
            ignoreTZ: ignore_tz,
            multiLevelIndex: multi_level_index,
            timeout: timeout
        )
    }

    public func download(
        start: Date? = nil,
        end: Date? = nil,
        prepost: Bool = false,
        actions: Bool = true,
        auto_adjust: Bool = true,
        back_adjust: Bool = false,
        repair: Bool = false,
        keepna: Bool = false,
        rounding: Bool = false,
        threads: Bool = true,
        group_by: String = "column",
        ignore_tz: Bool? = nil,
        progress: Bool = true,
        period: String? = nil,
        interval: String = "1d",
        timeout: TimeInterval = 10,
        multi_level_index: Bool = true
    ) async throws -> YFTable {
        try await history(
            start: start,
            end: end,
            prepost: prepost,
            actions: actions,
            auto_adjust: auto_adjust,
            back_adjust: back_adjust,
            repair: repair,
            keepna: keepna,
            rounding: rounding,
            threads: threads,
            group_by: group_by,
            ignore_tz: ignore_tz,
            progress: progress,
            period: period,
            interval: interval,
            timeout: timeout,
            multi_level_index: multi_level_index
        )
    }

    // Python-style aliases.
    public func getHistory(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func getDownload(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await download(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func getHistory(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func getDownload(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await download(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func getHistory(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await history(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func getHistory(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await history(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func getDownload(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await download(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func getDownload(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await download(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    // Python-style snake_case aliases.
    public func get_history(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await getHistory(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func get_history(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await getHistory(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func get_download(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await getDownload(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func get_download(
        period: String,
        interval: String = "1d",
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await getDownload(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func get_history(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await getHistory(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func get_history(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await getHistory(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func get_download(
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await getDownload(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

    public func get_download(
        start: Date,
        end: Date = Date(),
        interval: String,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        threads: Bool = true
    ) async throws -> [String: YFHistorySeries] {
        try await getDownload(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
    }

}

public func yfDownload(
    _ symbols: [String],
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await client
        .tickers(symbols)
        .download(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
}

public func yfDownload(
    _ symbols: [String],
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await client
        .tickers(symbols)
        .download(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
}

public func yfDownload(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await client
        .tickers(symbols)
        .download(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
}

public func yfDownload(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await client
        .tickers(symbols)
        .download(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads
        )
}

public func download(
    _ symbols: [String],
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
    ) async throws -> [String: YFHistorySeries] {
    try await yfDownload(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

public func download(
    _ symbols: String,
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await download(
        parseTickerSymbols(symbols),
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

public func download(
    _ symbols: [String],
    period: YFinanceClient.Range = .oneMonth,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await download(
        symbols,
        period: period.rawValue,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

public func download(
    _ symbols: String,
    period: YFinanceClient.Range = .oneMonth,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await download(
        parseTickerSymbols(symbols),
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

public func download(
    _ symbols: [String],
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
    ) async throws -> [String: YFHistorySeries] {
    try await yfDownload(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

public func download(
    _ symbols: String,
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await download(
        parseTickerSymbols(symbols),
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

public func download(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
    ) async throws -> [String: YFHistorySeries] {
    try await yfDownload(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

public func download(
    _ symbols: String,
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await download(
        parseTickerSymbols(symbols),
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

public func download(
    _ symbols: String,
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await download(
        parseTickerSymbols(symbols),
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}

private func parseTickerSymbols(_ tickers: String) -> [String] {
    tickers
        .replacingOccurrences(of: ",", with: " ")
        .split(separator: " ")
        .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).uppercased() }
        .filter { !$0.isEmpty }
}

public func download(
    _ symbols: [String],
    start: Date? = nil,
    end: Date? = nil,
    actions: Bool = false,
    threads: Bool = true,
    ignore_tz: Bool? = nil,
    group_by: String = "column",
    auto_adjust: Bool = true,
    back_adjust: Bool = false,
    repair: Bool = false,
    keepna: Bool = false,
    progress _: Bool = true,
    period: String? = nil,
    interval: String = "1d",
    prepost: Bool = false,
    rounding: Bool = false,
    timeout: TimeInterval = 10,
    multi_level_index: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    guard let parsedGroupBy = YFGroupBy(pythonValue: group_by) else {
        throw YFinanceError.invalidRequest("group_by must be 'column' or 'ticker'")
    }

    if let start {
        return try await downloadTable(
            symbols,
            start: start,
            end: end ?? Date(),
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: auto_adjust,
            backAdjust: back_adjust,
            repair: repair,
            keepNa: keepna,
            rounding: rounding,
            threads: threads,
            groupBy: parsedGroupBy,
            ignoreTZ: ignore_tz,
            multiLevelIndex: multi_level_index,
            timeout: timeout,
            client: client
        )
    }

    return try await downloadTable(
        symbols,
        period: period ?? "1mo",
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: auto_adjust,
        backAdjust: back_adjust,
        repair: repair,
        keepNa: keepna,
        rounding: rounding,
        threads: threads,
        groupBy: parsedGroupBy,
        ignoreTZ: ignore_tz,
        multiLevelIndex: multi_level_index,
        timeout: timeout,
        client: client
    )
}

public func download(
    _ symbols: String,
    start: Date? = nil,
    end: Date? = nil,
    actions: Bool = false,
    threads: Bool = true,
    ignore_tz: Bool? = nil,
    group_by: String = "column",
    auto_adjust: Bool = true,
    back_adjust: Bool = false,
    repair: Bool = false,
    keepna: Bool = false,
    progress: Bool = true,
    period: String? = nil,
    interval: String = "1d",
    prepost: Bool = false,
    rounding: Bool = false,
    timeout: TimeInterval = 10,
    multi_level_index: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await download(
        parseTickerSymbols(symbols),
        start: start,
        end: end,
        actions: actions,
        threads: threads,
        ignore_tz: ignore_tz,
        group_by: group_by,
        auto_adjust: auto_adjust,
        back_adjust: back_adjust,
        repair: repair,
        keepna: keepna,
        progress: progress,
        period: period,
        interval: interval,
        prepost: prepost,
        rounding: rounding,
        timeout: timeout,
        multi_level_index: multi_level_index,
        client: client
    )
}

public func download(
    _ symbols: [String],
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download(
    _ symbols: [String],
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download(
    _ symbols: [String],
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: String,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    guard let parsedGroupBy = YFGroupBy(pythonValue: groupBy) else {
        throw YFinanceError.invalidRequest("groupBy must be 'column' or 'ticker'")
    }
    return try await download(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: parsedGroupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download(
    _ symbols: [String],
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: String,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    guard let parsedGroupBy = YFGroupBy(pythonValue: groupBy) else {
        throw YFinanceError.invalidRequest("groupBy must be 'column' or 'ticker'")
    }
    return try await download(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: parsedGroupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: String,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    guard let parsedGroupBy = YFGroupBy(pythonValue: groupBy) else {
        throw YFinanceError.invalidRequest("groupBy must be 'column' or 'ticker'")
    }
    return try await download(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: parsedGroupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: String,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    guard let parsedGroupBy = YFGroupBy(pythonValue: groupBy) else {
        throw YFinanceError.invalidRequest("groupBy must be 'column' or 'ticker'")
    }
    return try await download(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: parsedGroupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: String,
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: [String],
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
    ) async throws -> YFTable {
    try await downloadTable(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: String,
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: [String],
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
    ) async throws -> YFTable {
    try await downloadTable(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: String,
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
    ) async throws -> YFTable {
    try await downloadTable(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: String,
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await downloadTable(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        client: client
    )
}

public func download_table(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multi_level_index: Bool,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await download_table(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multi_level_index,
        client: client
    )
}

public func download_table(
    _ symbols: String,
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multi_level_index: Bool,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await download_table(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multi_level_index,
        client: client
    )
}

public func yfDownloadTable(
    _ symbols: [String],
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await client
        .tickers(symbols)
        .downloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
}

public func yfDownloadTable(
    _ symbols: [String],
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await client
        .tickers(symbols)
        .downloadTable(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
}

public func yfDownloadTable(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await client
        .tickers(symbols)
        .downloadTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
}

public func yfDownloadTable(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await client
        .tickers(symbols)
        .downloadTable(
            start: start,
            end: end,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            threads: threads,
            groupBy: groupBy,
            ignoreTZ: ignoreTZ,
            multiLevelIndex: multiLevelIndex,
            timeout: timeout
        )
}

public func downloadTable(
    _ symbols: String,
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await yfDownloadTable(
        parseTickerSymbols(symbols),
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        timeout: timeout,
        client: client
    )
}

public func downloadTable(
    _ symbols: String,
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await yfDownloadTable(
        parseTickerSymbols(symbols),
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        timeout: timeout,
        client: client
    )
}

public func downloadTable(
    _ symbols: String,
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await yfDownloadTable(
        parseTickerSymbols(symbols),
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        timeout: timeout,
        client: client
    )
}

public func downloadTable(
    _ symbols: String,
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await yfDownloadTable(
        parseTickerSymbols(symbols),
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        timeout: timeout,
        client: client
    )
}

public func downloadTable(
    _ symbols: [String],
    period: YFinanceClient.Range = .oneMonth,
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await yfDownloadTable(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        timeout: timeout,
        client: client
    )
}

public func downloadTable(
    _ symbols: [String],
    period: String,
    interval: String = "1d",
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await yfDownloadTable(
        symbols,
        period: period,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        timeout: timeout,
        client: client
    )
}

public func downloadTable(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: YFinanceClient.Interval = .oneDay,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await yfDownloadTable(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        timeout: timeout,
        client: client
    )
}

public func downloadTable(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    groupBy: YFGroupBy = .column,
    ignoreTZ: Bool? = nil,
    multiLevelIndex: Bool = true,
    timeout: TimeInterval? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFTable {
    try await yfDownloadTable(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        groupBy: groupBy,
        ignoreTZ: ignoreTZ,
        multiLevelIndex: multiLevelIndex,
        timeout: timeout,
        client: client
    )
}

public func download(
    _ symbols: [String],
    start: Date,
    end: Date = Date(),
    interval: String,
    prepost: Bool = false,
    actions: Bool = false,
    autoAdjust: Bool = true,
    backAdjust: Bool = false,
    repair: Bool = false,
    keepNa: Bool = false,
    rounding: Bool = false,
    threads: Bool = true,
    client: YFinanceClient = YFinanceClient()
) async throws -> [String: YFHistorySeries] {
    try await yfDownload(
        symbols,
        start: start,
        end: end,
        interval: interval,
        prepost: prepost,
        actions: actions,
        autoAdjust: autoAdjust,
        backAdjust: backAdjust,
        repair: repair,
        keepNa: keepNa,
        rounding: rounding,
        threads: threads,
        client: client
    )
}
