import Foundation

public struct YFTicker: Sendable, CustomStringConvertible {
    public let symbol: String
    private let client: YFinanceClient

    public init(_ symbol: String, client: YFinanceClient = YFinanceClient()) {
        self.init(symbol: symbol, client: client)
    }

    public init(symbol: String, client: YFinanceClient = YFinanceClient()) {
        self.symbol = symbol.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        self.client = client
    }

    public init(_ ticker: (String, String), client: YFinanceClient = YFinanceClient()) throws {
        self.init(symbol: try yahooTicker(baseSymbol: ticker.0, mic: ticker.1), client: client)
    }

    public init(symbol: String, mic: String, client: YFinanceClient = YFinanceClient()) throws {
        self.init(symbol: try yahooTicker(baseSymbol: symbol, mic: mic), client: client)
    }

    public init(isin: String, client: YFinanceClient = YFinanceClient()) async throws {
        let resolved = try await get_ticker_by_isin(isin, client: client)
        let cleaned = resolved.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !cleaned.isEmpty else {
            throw YFinanceError.invalidRequest("Invalid ISIN number: \(isin)")
        }
        self.symbol = cleaned.uppercased()
        self.client = client
    }

    public var description: String {
        "yfinance.Ticker object <\(symbol)>"
    }

    public func quote() async throws -> YFQuote? {
        try await client.quote(symbol: symbol)
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
        let requestedEvents: Set<YFinanceClient.HistoryEvent> = actions ? [.dividends, .splits, .capitalGains] : []
        return try await client.history(
            symbol: symbol,
            range: period,
            interval: interval,
            includePrePost: prepost,
            events: requestedEvents,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
        let requestedEvents: Set<YFinanceClient.HistoryEvent> = actions ? [.dividends, .splits, .capitalGains] : []
        return try await client.history(
            symbol: symbol,
            period: period,
            interval: interval,
            includePrePost: prepost,
            events: requestedEvents,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
        let requestedEvents: Set<YFinanceClient.HistoryEvent> = actions ? [.dividends, .splits, .capitalGains] : []
        return try await client.history(
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
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
        guard let parsedInterval = YFinanceClient.Interval(pythonValue: interval) else {
            throw YFinanceError.invalidRequest("Invalid interval '\(interval)'")
        }
        return try await history(
            start: start,
            end: end,
            interval: parsedInterval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
        )
    }

    public func history(
        period: String? = nil,
        interval: String = "1d",
        start: Date? = nil,
        end: Date? = nil,
        prepost: Bool = false,
        actions: Bool = true,
        auto_adjust: Bool = true,
        back_adjust: Bool = false,
        repair: Bool = false,
        keepna: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval = 10
    ) async throws -> YFHistorySeries {
        let requestedEvents: Set<YFinanceClient.HistoryEvent> = actions ? [.dividends, .splits, .capitalGains] : []

        if let start {
            return try await client.history(
                symbol: symbol,
                start: start,
                end: end ?? Date(),
                interval: interval,
                includePrePost: prepost,
                events: requestedEvents,
                autoAdjust: auto_adjust,
                backAdjust: back_adjust,
                repair: repair,
                keepNa: keepna,
                rounding: rounding,
                timeout: timeout
            )
        }

        return try await client.history(
            symbol: symbol,
            period: period ?? "1mo",
            interval: interval,
            includePrePost: prepost,
            events: requestedEvents,
            autoAdjust: auto_adjust,
            backAdjust: back_adjust,
            repair: repair,
            keepNa: keepna,
            rounding: rounding,
            timeout: timeout
        )
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
        ignoreTZ: Bool? = nil
    ) async throws -> YFTable {
        let series = try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
        )
        return series.historyTable(includeActions: actions, ignoreTZ: ignoreTZ)
    }

    public func historyMetadata() async throws -> YFJSONValue {
        // Python yfinance requests intraday history so Yahoo returns tradingPeriods in metadata.
        let raw = try await client.historyRaw(symbol: symbol, range: .fiveDays, interval: .oneHour, includePrePost: true)
        return raw["chart"]?["result"]?[0]?["meta"] ?? .object([:])
    }

    public func tickerTimeZone() async throws -> String? {
        try await client.tickerTimeZone(symbol: symbol)
    }

    public func ticker_time_zone() async throws -> String? {
        try await tickerTimeZone()
    }

    public func live(
        messageHandler: (@Sendable (YFStreamingMessage) -> Void)?,
        verbose: Bool = true
    ) -> YFWebSocket {
        let ws = YFWebSocket(verbose: verbose)
        ws.subscribe(symbol)
        ws.listen(messageHandler)
        return ws
    }

    public func live(
        message_handler: (@Sendable (YFStreamingMessage) -> Void)? = nil,
        verbose: Bool = true
    ) -> YFWebSocket {
        live(messageHandler: message_handler, verbose: verbose)
    }

    public func options() async throws -> [String] {
        let chain = try await optionChain(expirationDate: nil, tz: nil)
        return chain.expirationDates.map { Self.formatExpirationDate($0) }
    }

    public func isin() async throws -> String? {
        if symbol.contains("-") || symbol.contains("^") {
            return "-"
        }

        let quote = try await self.quote()
        let lookupQuery = quote?.shortName ?? symbol
        guard let encoded = lookupQuery.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed),
              let url = URL(string: "https://markets.businessinsider.com/ajax/SearchController_Suggest?max_results=25&query=\(encoded)") else {
            return nil
        }

        let text = try await client.rawText(url: url)

        var searchToken = "\"\(symbol)|"
        if !text.contains(searchToken) {
            if text.lowercased().contains(lookupQuery.lowercased()) {
                searchToken = "\"|"
                if !text.contains(searchToken) {
                    return "-"
                }
            } else {
                return "-"
            }
        }

        guard let after = text.components(separatedBy: searchToken).dropFirst().first else {
            return nil
        }
        let first = after.components(separatedBy: "\"").first ?? ""
        let isin = first.components(separatedBy: "|").first ?? ""
        return isin.isEmpty ? nil : isin
    }

    public func optionChain(expirationDate: String? = nil, tz _: TimeZone? = nil) async throws -> YFOptionsChain {
        if let expirationDate {
            guard Self.parseExpirationDate(expirationDate) != nil else {
                throw YFinanceError.invalidRequest("Expiration must be formatted YYYY-MM-DD")
            }

            let baseRaw = try await client.options(symbol: symbol)
            let expirationsByDate = Self.expirationDateMap(from: baseRaw)
            guard let expirationEpoch = expirationsByDate[expirationDate] else {
                let available = expirationsByDate.keys.sorted().joined(separator: ", ")
                throw YFinanceError.invalidRequest(
                    "Expiration '\(expirationDate)' cannot be found. Available expirations are: [\(available)]"
                )
            }

            let raw = try await client.options(symbol: symbol, expirationEpoch: expirationEpoch)
            return Self.parseOptions(raw: raw)
        }

        let raw = try await client.options(symbol: symbol)
        return Self.parseOptions(raw: raw)
    }

    public func optionChain(expirationEpoch: Int) async throws -> YFOptionsChain {
        let raw = try await client.options(symbol: symbol, expirationEpoch: expirationEpoch)
        return Self.parseOptions(raw: raw)
    }

    public func optionChain(date: String?, tz: TimeZone? = nil) async throws -> YFOptionsChain {
        try await optionChain(expirationDate: date, tz: tz)
    }

    public func info() async throws -> YFJSONValue {
        let summary = try await quoteSummaryRaw(modules: [
            "financialData",
            "quoteType",
            "defaultKeyStatistics",
            "assetProfile",
            "summaryDetail",
        ])
        let additional = try await client.rawGet(
            host: .query1,
            path: "/v7/finance/quote",
            queryItems: [
                URLQueryItem(name: "symbols", value: symbol),
                URLQueryItem(name: "formatted", value: "false"),
            ],
            requiresCrumb: true
        )

        let merged = Self.mergeInfo(summary: summary, additionalQuote: additional, symbol: symbol)
        return .object(merged)
    }

    public func fastInfo() async throws -> YFJSONValue {
        let quote = try await self.quote()
        let metadata = try await self.historyMetadata()
        var object: [String: YFJSONValue] = [:]

        if let quote {
            object["currency"] = quote.currency.map { .string($0) } ?? .null
            object["quoteType"] = quote.quoteType.map { .string($0) } ?? .null
            object["exchange"] = quote.exchange.map { .string($0) } ?? .null
            object["lastPrice"] = quote.regularMarketPrice.map { .number($0) } ?? .null
            object["previousClose"] = quote.regularMarketPreviousClose.map { .number($0) } ?? .null
            object["open"] = quote.regularMarketOpen.map { .number($0) } ?? .null
            object["dayHigh"] = quote.regularMarketDayHigh.map { .number($0) } ?? .null
            object["dayLow"] = quote.regularMarketDayLow.map { .number($0) } ?? .null
            object["lastVolume"] = quote.regularMarketVolume.map { .number(Double($0)) } ?? .null
            object["marketCap"] = quote.marketCap.map { .number($0) } ?? .null
            object["yearHigh"] = quote.fiftyTwoWeekHigh.map { .number($0) } ?? .null
            object["yearLow"] = quote.fiftyTwoWeekLow.map { .number($0) } ?? .null
        }

        if let timezone = metadata["exchangeTimezoneName"]?.stringValue {
            object["timezone"] = .string(timezone)
        }

        return .object(object)
    }

    public func quoteSummary(modules: [YFQuoteSummaryModule]) async throws -> YFJSONValue {
        try await client.quoteSummary(symbol: symbol, modules: modules.map(\.rawValue))
    }

    public func profileFundamentals() async throws -> YFProfileFundamentals {
        let result = try await summaryResult(modules: [
            "summaryProfile",
            "summaryDetail",
            "financialData",
            "defaultKeyStatistics",
            "price",
        ])
        return Self.parseProfileFundamentals(from: result)
    }

    public func profile_fundamentals() async throws -> YFProfileFundamentals {
        try await profileFundamentals()
    }

    public func recommendations() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["recommendationTrend"])
        return result["recommendationTrend"]?["trend"] ?? .array([])
    }

    public func recommendationsSummary() async throws -> YFJSONValue {
        try await recommendations()
    }

    public func upgradesDowngrades() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["upgradeDowngradeHistory"])
        return result["upgradeDowngradeHistory"]?["history"] ?? .array([])
    }

    public func calendar() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["calendarEvents"])
        return result["calendarEvents"] ?? .object([:])
    }

    public func secFilings() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["secFilings"])
        return result["secFilings"]?["filings"] ?? .array([])
    }

    public func majorHolders() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["majorHoldersBreakdown"])
        return result["majorHoldersBreakdown"] ?? .object([:])
    }

    public func institutionalHolders() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["institutionOwnership"])
        return result["institutionOwnership"]?["ownershipList"] ?? .array([])
    }

    public func mutualfundHolders() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["fundOwnership"])
        return result["fundOwnership"]?["ownershipList"] ?? .array([])
    }

    public func insiderTransactions() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["insiderTransactions"])
        return result["insiderTransactions"]?["transactions"] ?? .array([])
    }

    public func insiderPurchases() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["netSharePurchaseActivity"])
        return result["netSharePurchaseActivity"] ?? .object([:])
    }

    public func insiderRosterHolders() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["insiderHolders"])
        return result["insiderHolders"]?["holders"] ?? .array([])
    }

    public func sustainability() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["esgScores"])
        return result["esgScores"] ?? .object([:])
    }

    public func analystPriceTargets() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["financialData"])
        guard let financialData = result["financialData"]?.objectValue else {
            return .object([:])
        }

        var output: [String: YFJSONValue] = [:]
        for (key, value) in financialData {
            if key.hasPrefix("target") {
                let name = key
                    .replacingOccurrences(of: "target", with: "")
                    .replacingOccurrences(of: "Price", with: "")
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                    .lowercased()
                output[name] = Self.unwrapRaw(value)
            } else if key == "currentPrice" {
                output["current"] = Self.unwrapRaw(value)
            }
        }
        return .object(output)
    }

    public func earningsEstimate() async throws -> YFJSONValue {
        try await periodicTrendTable(key: "earningsEstimate")
    }

    public func revenueEstimate() async throws -> YFJSONValue {
        try await periodicTrendTable(key: "revenueEstimate")
    }

    public func epsTrend() async throws -> YFJSONValue {
        try await periodicTrendTable(key: "epsTrend")
    }

    public func epsRevisions() async throws -> YFJSONValue {
        try await periodicTrendTable(key: "epsRevisions")
    }

    public func earningsHistory() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["earningsHistory"])
        return result["earningsHistory"]?["history"] ?? .array([])
    }

    public func growthEstimates() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["earningsTrend", "industryTrend", "sectorTrend", "indexTrend"])
        return .object([
            "earningsTrend": result["earningsTrend"] ?? .object([:]),
            "industryTrend": result["industryTrend"] ?? .object([:]),
            "sectorTrend": result["sectorTrend"] ?? .object([:]),
            "indexTrend": result["indexTrend"] ?? .object([:]),
        ])
    }

    public func earnings(freq: YFFinancialFrequency = .yearly) async throws -> YFJSONValue {
        let module: String
        switch freq {
        case .yearly:
            module = "earnings"
        case .quarterly:
            module = "earnings"
        case .trailing:
            module = "earningsTrend"
        }
        let result = try await summaryResult(modules: [module])
        return result[module] ?? .object([:])
    }

    public func incomeStmt(freq: YFFinancialFrequency = .yearly) async throws -> YFJSONValue {
        switch freq {
        case .yearly:
            let result = try await summaryResult(modules: ["incomeStatementHistory"])
            return result["incomeStatementHistory"] ?? .object([:])
        case .quarterly:
            let result = try await summaryResult(modules: ["incomeStatementHistoryQuarterly"])
            return result["incomeStatementHistoryQuarterly"] ?? .object([:])
        case .trailing:
            return try await client.fundamentalsTimeSeries(
                symbol: symbol,
                types: [
                    "trailingTotalRevenue",
                    "trailingGrossProfit",
                    "trailingOperatingIncome",
                    "trailingEBITDA",
                    "trailingNetIncome",
                    "trailingDilutedEPS",
                ],
                period1: Date(timeIntervalSince1970: 1_483_142_400), // Jan 1, 2017 UTC
                period2: Date()
            )
        }
    }

    public func balanceSheet(freq: YFFinancialFrequency = .yearly) async throws -> YFJSONValue {
        switch freq {
        case .yearly:
            let result = try await summaryResult(modules: ["balanceSheetHistory"])
            return result["balanceSheetHistory"] ?? .object([:])
        case .quarterly:
            let result = try await summaryResult(modules: ["balanceSheetHistoryQuarterly"])
            return result["balanceSheetHistoryQuarterly"] ?? .object([:])
        case .trailing:
            throw YFinanceError.invalidRequest("Trailing balance sheet is not supported by Yahoo quoteSummary API")
        }
    }

    public func cashFlow(freq: YFFinancialFrequency = .yearly) async throws -> YFJSONValue {
        switch freq {
        case .yearly:
            let result = try await summaryResult(modules: ["cashFlowStatementHistory"])
            return result["cashFlowStatementHistory"] ?? .object([:])
        case .quarterly:
            let result = try await summaryResult(modules: ["cashFlowStatementHistoryQuarterly"])
            return result["cashFlowStatementHistoryQuarterly"] ?? .object([:])
        case .trailing:
            return try await client.fundamentalsTimeSeries(
                symbol: symbol,
                types: [
                    "trailingOperatingCashFlow",
                    "trailingFreeCashFlow",
                    "trailingCapitalExpenditure",
                ],
                period1: Date(timeIntervalSince1970: 1_483_142_400), // Jan 1, 2017 UTC
                period2: Date()
            )
        }
    }

    public func financials(freq: YFFinancialFrequency = .yearly) async throws -> YFJSONValue {
        try await incomeStmt(freq: freq)
    }

    public func earnings(freq: String) async throws -> YFJSONValue {
        try await earnings(freq: Self.parseFinancialFrequency(freq))
    }

    public func incomeStmt(freq: String) async throws -> YFJSONValue {
        try await incomeStmt(freq: Self.parseFinancialFrequency(freq))
    }

    public func financials(freq: String) async throws -> YFJSONValue {
        try await financials(freq: Self.parseFinancialFrequency(freq))
    }

    public func balanceSheet(freq: String) async throws -> YFJSONValue {
        try await balanceSheet(freq: Self.parseFinancialFrequency(freq))
    }

    public func cashFlow(freq: String) async throws -> YFJSONValue {
        try await cashFlow(freq: Self.parseFinancialFrequency(freq))
    }

    // Python parity convenience methods that mirror Ticker property names.
    public func quarterlyEarnings() async throws -> YFJSONValue {
        try await earnings(freq: .quarterly)
    }

    public func quarterlyIncomeStmt() async throws -> YFJSONValue {
        try await incomeStmt(freq: .quarterly)
    }

    public func ttmIncomeStmt() async throws -> YFJSONValue {
        try await incomeStmt(freq: .trailing)
    }

    public func incomestmt(freq: YFFinancialFrequency = .yearly) async throws -> YFJSONValue {
        try await incomeStmt(freq: freq)
    }

    public func quarterlyIncomestmt() async throws -> YFJSONValue {
        try await incomeStmt(freq: .quarterly)
    }

    public func ttmIncomestmt() async throws -> YFJSONValue {
        try await incomeStmt(freq: .trailing)
    }

    public func quarterlyFinancials() async throws -> YFJSONValue {
        try await financials(freq: .quarterly)
    }

    public func ttmFinancials() async throws -> YFJSONValue {
        try await financials(freq: .trailing)
    }

    public func quarterlyBalanceSheet() async throws -> YFJSONValue {
        try await balanceSheet(freq: .quarterly)
    }

    public func balancesheet(freq: YFFinancialFrequency = .yearly) async throws -> YFJSONValue {
        try await balanceSheet(freq: freq)
    }

    public func quarterlyBalancesheet() async throws -> YFJSONValue {
        try await balanceSheet(freq: .quarterly)
    }

    public func quarterlyCashFlow() async throws -> YFJSONValue {
        try await cashFlow(freq: .quarterly)
    }

    public func ttmCashFlow() async throws -> YFJSONValue {
        try await cashFlow(freq: .trailing)
    }

    public func cashflow(freq: YFFinancialFrequency = .yearly) async throws -> YFJSONValue {
        try await cashFlow(freq: freq)
    }

    public func quarterlyCashflow() async throws -> YFJSONValue {
        try await cashFlow(freq: .quarterly)
    }

    public func ttmCashflow() async throws -> YFJSONValue {
        try await cashFlow(freq: .trailing)
    }

    public func shares() async throws -> YFJSONValue {
        let quote = try await self.quote()
        if let shares = quote?.marketCap, let price = quote?.regularMarketPrice, price > 0 {
            return .number(shares / price)
        }
        let result = try await summaryResult(modules: ["defaultKeyStatistics"])
        return result["defaultKeyStatistics"]?["sharesOutstanding"] ?? .null
    }

    public func sharesFull(
        start: Date = Date().addingTimeInterval(-548 * 24 * 60 * 60),
        end: Date = Date()
    ) async throws -> YFJSONValue {
        try await client.fundamentalsTimeSeries(
            symbol: symbol,
            types: ["shares_out"],
            period1: start,
            period2: end
        )
    }

    public func dividends(period: YFinanceClient.Range = .max) async throws -> YFJSONValue {
        let raw = try await client.historyRaw(
            symbol: symbol,
            range: period,
            interval: .oneDay,
            includePrePost: false,
            events: [.dividends]
        )
        return Self.sortedEventArray(raw["chart"]?["result"]?[0]?["events"]?["dividends"])
    }

    public func capitalGains(period: YFinanceClient.Range = .max) async throws -> YFJSONValue {
        let raw = try await client.historyRaw(
            symbol: symbol,
            range: period,
            interval: .oneDay,
            includePrePost: false,
            events: [.capitalGains]
        )
        return Self.sortedEventArray(raw["chart"]?["result"]?[0]?["events"]?["capitalGains"])
    }

    public func splits(period: YFinanceClient.Range = .max) async throws -> YFJSONValue {
        let raw = try await client.historyRaw(
            symbol: symbol,
            range: period,
            interval: .oneDay,
            includePrePost: false,
            events: [.splits]
        )
        return Self.sortedEventArray(raw["chart"]?["result"]?[0]?["events"]?["splits"])
    }

    public func actions(period: YFinanceClient.Range = .max) async throws -> YFJSONValue {
        let dividends = try await self.dividends(period: period).arrayValue ?? []
        let splits = try await self.splits(period: period).arrayValue ?? []
        let gains = try await self.capitalGains(period: period).arrayValue ?? []
        return .object([
            "dividends": .array(dividends),
            "splits": .array(splits),
            "capitalGains": .array(gains),
        ])
    }

    public func news(count: Int = 10, tab: YFNewsTab = .news) async throws -> YFJSONValue {
        let raw = try await client.tickerNews(symbol: symbol, count: count, tab: tab.rawValue)
        let items = raw["data"]?["tickerStream"]?["stream"]?.arrayValue ?? []
        let filtered = items.filter { item in
            if let isAd = item["ad"]?.boolValue {
                return !isAd
            }
            return true
        }
        return .array(filtered)
    }

    public func earningsDates(limit: Int = 12, offset: Int = 0) async throws -> YFJSONValue {
        if limit > 100 {
            throw YFinanceError.invalidRequest("Yahoo caps limit at 100")
        }

        let safeLimit = max(limit, 1)
        let safeOffset = max(offset, 0)

        if let scraped = try? await scrapeEarningsDates(limit: safeLimit, offset: safeOffset),
           Self.hasEarningsDocumentRows(scraped) {
            return scraped
        }

        let body = Self.earningsDateVisualizationBody(symbol: symbol, limit: safeLimit, offset: safeOffset)
        return try await client.visualization(body: body)
    }

    public func fundsDataRaw() async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["quoteType", "summaryProfile", "topHoldings", "fundProfile"])
        return .object([
            "quoteType": result["quoteType"] ?? .object([:]),
            "summaryProfile": result["summaryProfile"] ?? .object([:]),
            "topHoldings": result["topHoldings"] ?? .object([:]),
            "fundProfile": result["fundProfile"] ?? .object([:]),
        ])
    }

    public func fundsData() async throws -> YFFundsData {
        let raw = try await fundsDataRaw()
        return YFFundsData(symbol: symbol, raw: raw)
    }

    public func earningsTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable {
        let raw = try await earnings(freq: freq)
        return Self.statementTable(from: raw)
    }

    public func incomeStmtTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable {
        let raw = try await incomeStmt(freq: freq)
        return Self.statementTable(from: raw)
    }

    public func balanceSheetTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable {
        let raw = try await balanceSheet(freq: freq)
        return Self.statementTable(from: raw)
    }

    public func cashFlowTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable {
        let raw = try await cashFlow(freq: freq)
        return Self.statementTable(from: raw)
    }

    public func financialsTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable {
        let raw = try await financials(freq: freq)
        return Self.statementTable(from: raw)
    }

    public func recommendationsTable() async throws -> YFTable {
        try await recommendations().toTable()
    }

    public func upgradesDowngradesTable() async throws -> YFTable {
        try await upgradesDowngrades().toTable()
    }

    public func institutionalHoldersTable() async throws -> YFTable {
        try await institutionalHolders().toTable()
    }

    public func majorHoldersTable() async throws -> YFTable {
        try await majorHolders().toTable()
    }

    public func mutualfundHoldersTable() async throws -> YFTable {
        try await mutualfundHolders().toTable()
    }

    public func insiderPurchasesTable() async throws -> YFTable {
        try await insiderPurchases().toTable()
    }

    public func insiderTransactionsTable() async throws -> YFTable {
        try await insiderTransactions().toTable()
    }

    public func insiderRosterHoldersTable() async throws -> YFTable {
        try await insiderRosterHolders().toTable()
    }

    public func sustainabilityTable() async throws -> YFTable {
        try await sustainability().toTable()
    }

    public func analystPriceTargetsTable() async throws -> YFTable {
        try await analystPriceTargets().toTable()
    }

    public func earningsEstimateTable() async throws -> YFTable {
        try await earningsEstimate().toTable()
    }

    public func revenueEstimateTable() async throws -> YFTable {
        try await revenueEstimate().toTable()
    }

    public func earningsHistoryTable() async throws -> YFTable {
        try await earningsHistory().toTable()
    }

    public func epsTrendTable() async throws -> YFTable {
        try await epsTrend().toTable()
    }

    public func epsRevisionsTable() async throws -> YFTable {
        try await epsRevisions().toTable()
    }

    public func growthEstimatesTable() async throws -> YFTable {
        try await growthEstimates().toTable()
    }

    public func dividendsTable(period: YFinanceClient.Range = .max) async throws -> YFTable {
        // Python yfinance get_dividends(): returns daily-history dividends filtered to non-zero rows.
        let series = try await history(
            period: period,
            interval: .oneDay,
            prepost: true,
            actions: true,
            autoAdjust: true,
            backAdjust: false,
            repair: false,
            keepNa: false,
            rounding: false
        )
        let table = series.historyTable(includeActions: true)
        let rows: [[String: YFJSONValue]] = table.rows.compactMap { row in
            let value = row["Dividends"]?.doubleValue ?? 0
            guard value != 0 else { return nil }
            return [
                "date": row["date"] ?? .null,
                "Dividends": .number(value),
            ]
        }
        return YFTable(columns: ["date", "Dividends"], rows: rows)
    }

    public func capitalGainsTable(period: YFinanceClient.Range = .max) async throws -> YFTable {
        // Python yfinance get_capital_gains(): returns daily-history capital gains filtered to non-zero rows.
        let series = try await history(
            period: period,
            interval: .oneDay,
            prepost: true,
            actions: true,
            autoAdjust: true,
            backAdjust: false,
            repair: false,
            keepNa: false,
            rounding: false
        )
        let table = series.historyTable(includeActions: true)
        guard table.columns.contains("Capital Gains") else {
            return YFTable(columns: ["date", "Capital Gains"], rows: [])
        }
        let rows: [[String: YFJSONValue]] = table.rows.compactMap { row in
            let value = row["Capital Gains"]?.doubleValue ?? 0
            guard value != 0 else { return nil }
            return [
                "date": row["date"] ?? .null,
                "Capital Gains": .number(value),
            ]
        }
        return YFTable(columns: ["date", "Capital Gains"], rows: rows)
    }

    public func splitsTable(period: YFinanceClient.Range = .max) async throws -> YFTable {
        // Python yfinance get_splits(): returns daily-history splits filtered to non-zero rows.
        let series = try await history(
            period: period,
            interval: .oneDay,
            prepost: true,
            actions: true,
            autoAdjust: true,
            backAdjust: false,
            repair: false,
            keepNa: false,
            rounding: false
        )
        let table = series.historyTable(includeActions: true)
        let rows: [[String: YFJSONValue]] = table.rows.compactMap { row in
            let value = row["Stock Splits"]?.doubleValue ?? 0
            guard value != 0 else { return nil }
            return [
                "date": row["date"] ?? .null,
                "Stock Splits": .number(value),
            ]
        }
        return YFTable(columns: ["date", "Stock Splits"], rows: rows)
    }

    public func actionsTable(period: YFinanceClient.Range = .max) async throws -> YFTable {
        // Python yfinance derives actions from daily history and filters to non-zero rows.
        let series = try await history(
            period: period,
            interval: .oneDay,
            prepost: true,
            actions: true,
            autoAdjust: true,
            backAdjust: false,
            repair: false,
            keepNa: false,
            rounding: false
        )

        let table = series.historyTable(includeActions: true)
        var actionColumns: [String] = []
        if table.columns.contains("Dividends") {
            actionColumns.append("Dividends")
        }
        if table.columns.contains("Stock Splits") {
            actionColumns.append("Stock Splits")
        }
        if table.columns.contains("Capital Gains") {
            actionColumns.append("Capital Gains")
        }

        let columns = ["date"] + actionColumns
        let rows: [[String: YFJSONValue]] = table.rows.compactMap { row in
            var output: [String: YFJSONValue] = ["date": row["date"] ?? .null]
            var anyNonZero = false
            for column in actionColumns {
                let value = row[column]?.doubleValue ?? 0
                if value != 0 {
                    anyNonZero = true
                }
                output[column] = .number(value)
            }
            return anyNonZero ? output : nil
        }

        return YFTable(columns: columns, rows: rows)
    }

    public func earningsDatesTable(limit: Int = 12, offset: Int = 0) async throws -> YFTable {
        let raw = try await earningsDates(limit: limit, offset: offset)
        guard
            let document = raw["finance"]?["result"]?[0]?["documents"]?[0],
            let columns = document["columns"]?.arrayValue?.map({ $0["label"]?.stringValue ?? "" }),
            let rows = document["rows"]?.arrayValue
        else {
            return YFTable(columns: [], rows: [])
        }

        let filteredColumns = columns.filter { !$0.isEmpty }
        let mapped: [[String: YFJSONValue]] = rows.compactMap { row in
            guard let values = row.arrayValue else { return nil }
            var object: [String: YFJSONValue] = [:]
            for (index, column) in columns.enumerated() where !column.isEmpty {
                if values.indices.contains(index) {
                    object[column] = values[index]
                }
            }
            return object
        }

        let normalized = Self.normalizedEarningsDatesTable(from: mapped)
        if !normalized.rows.isEmpty {
            return normalized
        }

        return YFTable(columns: filteredColumns, rows: mapped)
    }

    public func newsTable(count: Int = 10, tab: YFNewsTab = .news) async throws -> YFTable {
        try await news(count: count, tab: tab).toTable()
    }

    public func quoteSummaryRaw(modules: [String]) async throws -> YFJSONValue {
        try await client.quoteSummary(symbol: symbol, modules: modules)
    }

    public func webSocket() -> YFWebSocket {
        let socket = YFWebSocket()
        socket.subscribe(symbol)
        return socket
    }

    public func live(
        _ handler: (@Sendable (YFStreamingMessage) -> Void)?,
        verbose: Bool = true
    ) -> YFWebSocket {
        let socket = YFWebSocket(verbose: verbose)
        socket.subscribe(symbol)
        socket.listen(handler)
        return socket
    }

    public func asyncWebSocket() async throws -> YFAsyncWebSocket {
        let socket = YFAsyncWebSocket()
        try await socket.subscribe(symbol)
        return socket
    }

    // Python-style alias methods for migration ergonomics.
    public func getIsin() async throws -> String? { try await isin() }
    public func getInfo() async throws -> YFJSONValue { try await info() }
    public func getFastInfo() async throws -> YFJSONValue { try await fastInfo() }
    public func getProfileFundamentals() async throws -> YFProfileFundamentals { try await profileFundamentals() }
    public func get_profile_fundamentals() async throws -> YFProfileFundamentals { try await profileFundamentals() }
    public func getRecommendations(asDict _: Bool = false) async throws -> YFJSONValue { try await recommendations() }
    public func getRecommendationsSummary(asDict _: Bool = false) async throws -> YFJSONValue {
        try await recommendationsSummary()
    }
    public func getUpgradesDowngrades(asDict _: Bool = false) async throws -> YFJSONValue {
        try await upgradesDowngrades()
    }
    public func getCalendar() async throws -> YFJSONValue { try await calendar() }
    public func getSecFilings() async throws -> YFJSONValue { try await secFilings() }
    public func getMajorHolders(asDict _: Bool = false) async throws -> YFJSONValue { try await majorHolders() }
    public func getInstitutionalHolders(asDict _: Bool = false) async throws -> YFJSONValue {
        try await institutionalHolders()
    }
    public func getMutualfundHolders(asDict _: Bool = false) async throws -> YFJSONValue {
        try await mutualfundHolders()
    }
    public func getInsiderTransactions(asDict _: Bool = false) async throws -> YFJSONValue {
        try await insiderTransactions()
    }
    public func getInsiderPurchases(asDict _: Bool = false) async throws -> YFJSONValue {
        try await insiderPurchases()
    }
    public func getInsiderRosterHolders(asDict _: Bool = false) async throws -> YFJSONValue {
        try await insiderRosterHolders()
    }
    public func getDividends() async throws -> YFJSONValue { try await dividends() }
    public func getDividends(period: YFinanceClient.Range) async throws -> YFJSONValue { try await dividends(period: period) }
    public func getDividends(period: String) async throws -> YFJSONValue {
        guard let parsed = YFinanceClient.Range(pythonValue: period) else {
            throw YFinanceError.invalidRequest("Invalid period '\(period)'")
        }
        return try await dividends(period: parsed)
    }
    public func getCapitalGains() async throws -> YFJSONValue { try await capitalGains() }
    public func getCapitalGains(period: YFinanceClient.Range) async throws -> YFJSONValue { try await capitalGains(period: period) }
    public func getCapitalGains(period: String) async throws -> YFJSONValue {
        guard let parsed = YFinanceClient.Range(pythonValue: period) else {
            throw YFinanceError.invalidRequest("Invalid period '\(period)'")
        }
        return try await capitalGains(period: parsed)
    }
    public func getSplits() async throws -> YFJSONValue { try await splits() }
    public func getSplits(period: YFinanceClient.Range) async throws -> YFJSONValue { try await splits(period: period) }
    public func getSplits(period: String) async throws -> YFJSONValue {
        guard let parsed = YFinanceClient.Range(pythonValue: period) else {
            throw YFinanceError.invalidRequest("Invalid period '\(period)'")
        }
        return try await splits(period: parsed)
    }
    public func getActions() async throws -> YFJSONValue { try await actions() }
    public func getActions(period: YFinanceClient.Range) async throws -> YFJSONValue { try await actions(period: period) }
    public func getActions(period: String) async throws -> YFJSONValue {
        guard let parsed = YFinanceClient.Range(pythonValue: period) else {
            throw YFinanceError.invalidRequest("Invalid period '\(period)'")
        }
        return try await actions(period: parsed)
    }
    public func getShares() async throws -> YFJSONValue { try await shares() }
    public func getSharesFull(start: Date, end: Date = Date()) async throws -> YFJSONValue {
        try await sharesFull(start: start, end: end)
    }
    public func getAnalystPriceTargets() async throws -> YFJSONValue { try await analystPriceTargets() }
    public func getEarningsEstimate(asDict _: Bool = false) async throws -> YFJSONValue { try await earningsEstimate() }
    public func getRevenueEstimate(asDict _: Bool = false) async throws -> YFJSONValue { try await revenueEstimate() }
    public func getEarningsHistory(asDict _: Bool = false) async throws -> YFJSONValue { try await earningsHistory() }
    public func getEpsTrend(asDict _: Bool = false) async throws -> YFJSONValue { try await epsTrend() }
    public func getEpsRevisions(asDict _: Bool = false) async throws -> YFJSONValue { try await epsRevisions() }
    public func getGrowthEstimates(asDict _: Bool = false) async throws -> YFJSONValue { try await growthEstimates() }
    public func getSustainability(asDict _: Bool = false) async throws -> YFJSONValue { try await sustainability() }
    public func getEarnings(asDict _: Bool = false, freq: YFFinancialFrequency) async throws -> YFJSONValue {
        try await earnings(freq: freq)
    }
    public func getIncomeStmt(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await incomeStmt(freq: freq)
    }
    public func getIncomestmt(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await incomestmt(freq: freq)
    }
    public func getFinancials(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await financials(freq: freq)
    }
    public func getBalanceSheet(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await balanceSheet(freq: freq)
    }
    public func getBalancesheet(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await balancesheet(freq: freq)
    }
    public func getCashFlow(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await cashFlow(freq: freq)
    }
    public func getCashflow(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await cashflow(freq: freq)
    }
    public func getEarnings(asDict _: Bool = false, freq: String = "yearly") async throws -> YFJSONValue {
        try await earnings(freq: freq)
    }
    public func getIncomeStmt(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await incomeStmt(freq: freq)
    }
    public func getIncomestmt(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await incomestmt(freq: Self.parseFinancialFrequency(freq))
    }
    public func getFinancials(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await financials(freq: freq)
    }
    public func getBalanceSheet(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await balanceSheet(freq: freq)
    }
    public func getBalancesheet(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await balancesheet(freq: Self.parseFinancialFrequency(freq))
    }
    public func getCashFlow(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await cashFlow(freq: freq)
    }
    public func getCashflow(
        asDict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await cashflow(freq: Self.parseFinancialFrequency(freq))
    }
    public func getEarningsTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await earningsTable(freq: freq) }
    public func getIncomeStmtTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await incomeStmtTable(freq: freq) }
    public func getFinancialsTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await financialsTable(freq: freq) }
    public func getBalanceSheetTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await balanceSheetTable(freq: freq) }
    public func getCashFlowTable(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await cashFlowTable(freq: freq) }
    public func getMajorHoldersTable() async throws -> YFTable { try await majorHoldersTable() }
    public func getInstitutionalHoldersTable() async throws -> YFTable { try await institutionalHoldersTable() }
    public func getMutualfundHoldersTable() async throws -> YFTable { try await mutualfundHoldersTable() }
    public func getInsiderPurchasesTable() async throws -> YFTable { try await insiderPurchasesTable() }
    public func getInsiderTransactionsTable() async throws -> YFTable { try await insiderTransactionsTable() }
    public func getInsiderRosterHoldersTable() async throws -> YFTable { try await insiderRosterHoldersTable() }
    public func getSustainabilityTable() async throws -> YFTable { try await sustainabilityTable() }
    public func getAnalystPriceTargetsTable() async throws -> YFTable { try await analystPriceTargetsTable() }
    public func getEarningsEstimateTable() async throws -> YFTable { try await earningsEstimateTable() }
    public func getRevenueEstimateTable() async throws -> YFTable { try await revenueEstimateTable() }
    public func getEarningsHistoryTable() async throws -> YFTable { try await earningsHistoryTable() }
    public func getEpsTrendTable() async throws -> YFTable { try await epsTrendTable() }
    public func getEpsRevisionsTable() async throws -> YFTable { try await epsRevisionsTable() }
    public func getGrowthEstimatesTable() async throws -> YFTable { try await growthEstimatesTable() }
    public func getNews(count: Int = 10, tab: YFNewsTab = .news) async throws -> YFJSONValue { try await news(count: count, tab: tab) }
    public func getEarningsDates(limit: Int = 12, offset: Int = 0) async throws -> YFJSONValue { try await earningsDates(limit: limit, offset: offset) }
    public func getHistoryMetadata() async throws -> YFJSONValue { try await historyMetadata() }
    public func getFundsData() async throws -> YFFundsData { try await fundsData() }
    public func getFundsDataRaw() async throws -> YFJSONValue { try await fundsDataRaw() }
    public func getDividendsTable(period: YFinanceClient.Range = .max) async throws -> YFTable { try await dividendsTable(period: period) }
    public func getCapitalGainsTable(period: YFinanceClient.Range = .max) async throws -> YFTable { try await capitalGainsTable(period: period) }
    public func getSplitsTable(period: YFinanceClient.Range = .max) async throws -> YFTable { try await splitsTable(period: period) }
    public func getActionsTable(period: YFinanceClient.Range = .max) async throws -> YFTable { try await actionsTable(period: period) }
    public func getHistory(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
        try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
        try await history(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
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
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
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
            rounding: rounding
        )
    }

    // Python-style snake_case aliases.
    public func option_chain(_ expirationDate: String? = nil, tz: TimeZone? = nil) async throws -> YFOptionsChain {
        try await optionChain(expirationDate: expirationDate, tz: tz)
    }
    public func get_isin() async throws -> String? { try await getIsin() }
    public func major_holders() async throws -> YFJSONValue { try await majorHolders() }
    public func institutional_holders() async throws -> YFJSONValue { try await institutionalHolders() }
    public func mutualfund_holders() async throws -> YFJSONValue { try await mutualfundHolders() }
    public func major_holders_table() async throws -> YFTable { try await majorHoldersTable() }
    public func institutional_holders_table() async throws -> YFTable { try await institutionalHoldersTable() }
    public func mutualfund_holders_table() async throws -> YFTable { try await mutualfundHoldersTable() }
    public func insider_purchases() async throws -> YFJSONValue { try await insiderPurchases() }
    public func insider_transactions() async throws -> YFJSONValue { try await insiderTransactions() }
    public func insider_roster_holders() async throws -> YFJSONValue { try await insiderRosterHolders() }
    public func insider_purchases_table() async throws -> YFTable { try await insiderPurchasesTable() }
    public func insider_transactions_table() async throws -> YFTable { try await insiderTransactionsTable() }
    public func insider_roster_holders_table() async throws -> YFTable { try await insiderRosterHoldersTable() }
    public func capital_gains(period: YFinanceClient.Range = .max) async throws -> YFJSONValue { try await capitalGains(period: period) }
    public func fast_info() async throws -> YFJSONValue { try await fastInfo() }
    public func sec_filings() async throws -> YFJSONValue { try await secFilings() }
    public func recommendations_summary() async throws -> YFJSONValue { try await recommendationsSummary() }
    public func upgrades_downgrades() async throws -> YFJSONValue { try await upgradesDowngrades() }
    public func analyst_price_targets() async throws -> YFJSONValue { try await analystPriceTargets() }
    public func analyst_price_targets_table() async throws -> YFTable { try await analystPriceTargetsTable() }
    public func earnings_estimate() async throws -> YFJSONValue { try await earningsEstimate() }
    public func earnings_estimate_table() async throws -> YFTable { try await earningsEstimateTable() }
    public func revenue_estimate() async throws -> YFJSONValue { try await revenueEstimate() }
    public func revenue_estimate_table() async throws -> YFTable { try await revenueEstimateTable() }
    public func earnings_history() async throws -> YFJSONValue { try await earningsHistory() }
    public func earnings_history_table() async throws -> YFTable { try await earningsHistoryTable() }
    public func eps_trend() async throws -> YFJSONValue { try await epsTrend() }
    public func eps_trend_table() async throws -> YFTable { try await epsTrendTable() }
    public func eps_revisions() async throws -> YFJSONValue { try await epsRevisions() }
    public func eps_revisions_table() async throws -> YFTable { try await epsRevisionsTable() }
    public func growth_estimates() async throws -> YFJSONValue { try await growthEstimates() }
    public func growth_estimates_table() async throws -> YFTable { try await growthEstimatesTable() }
    public func sustainability_table() async throws -> YFTable { try await sustainabilityTable() }
    public func earnings_dates(limit: Int = 12, offset: Int = 0) async throws -> YFJSONValue { try await earningsDates(limit: limit, offset: offset) }
    public func history_metadata() async throws -> YFJSONValue { try await historyMetadata() }
    public func funds_data_raw() async throws -> YFJSONValue { try await fundsDataRaw() }
    public func shares_full(start: Date = Date().addingTimeInterval(-548 * 24 * 60 * 60), end: Date = Date()) async throws -> YFJSONValue {
        try await sharesFull(start: start, end: end)
    }
    public func quarterly_earnings() async throws -> YFJSONValue { try await quarterlyEarnings() }
    public func income_stmt() async throws -> YFJSONValue { try await incomeStmt(freq: .yearly) }
    public func quarterly_income_stmt() async throws -> YFJSONValue { try await quarterlyIncomeStmt() }
    public func ttm_income_stmt() async throws -> YFJSONValue { try await ttmIncomeStmt() }
    public func incomestmt() async throws -> YFJSONValue { try await incomestmt(freq: .yearly) }
    public func quarterly_incomestmt() async throws -> YFJSONValue { try await quarterlyIncomestmt() }
    public func ttm_incomestmt() async throws -> YFJSONValue { try await ttmIncomestmt() }
    public func quarterly_financials() async throws -> YFJSONValue { try await quarterlyFinancials() }
    public func ttm_financials() async throws -> YFJSONValue { try await ttmFinancials() }
    public func balance_sheet() async throws -> YFJSONValue { try await balanceSheet(freq: .yearly) }
    public func quarterly_balance_sheet() async throws -> YFJSONValue { try await quarterlyBalanceSheet() }
    public func balancesheet() async throws -> YFJSONValue { try await balancesheet(freq: .yearly) }
    public func quarterly_balancesheet() async throws -> YFJSONValue { try await quarterlyBalancesheet() }
    public func cash_flow() async throws -> YFJSONValue { try await cashFlow(freq: .yearly) }
    public func quarterly_cash_flow() async throws -> YFJSONValue { try await quarterlyCashFlow() }
    public func ttm_cash_flow() async throws -> YFJSONValue { try await ttmCashFlow() }
    public func cashflow() async throws -> YFJSONValue { try await cashflow(freq: .yearly) }
    public func quarterly_cashflow() async throws -> YFJSONValue { try await quarterlyCashflow() }
    public func ttm_cashflow() async throws -> YFJSONValue { try await ttmCashflow() }
    public func funds_data() async throws -> YFFundsData { try await fundsData() }
    public func get_info() async throws -> YFJSONValue { try await getInfo() }
    public func get_fast_info() async throws -> YFJSONValue { try await getFastInfo() }
    public func get_recommendations(as_dict _: Bool = false) async throws -> YFJSONValue { try await getRecommendations() }
    public func get_recommendations_summary(as_dict _: Bool = false) async throws -> YFJSONValue {
        try await getRecommendationsSummary()
    }
    public func get_upgrades_downgrades(as_dict _: Bool = false) async throws -> YFJSONValue {
        try await getUpgradesDowngrades()
    }
    public func get_calendar() async throws -> YFJSONValue { try await getCalendar() }
    public func get_sec_filings() async throws -> YFJSONValue { try await getSecFilings() }
    public func get_major_holders(as_dict _: Bool = false) async throws -> YFJSONValue { try await getMajorHolders() }
    public func get_institutional_holders(as_dict _: Bool = false) async throws -> YFJSONValue {
        try await getInstitutionalHolders()
    }
    public func get_mutualfund_holders(as_dict _: Bool = false) async throws -> YFJSONValue {
        try await getMutualfundHolders()
    }
    public func get_insider_transactions(as_dict _: Bool = false) async throws -> YFJSONValue {
        try await getInsiderTransactions()
    }
    public func get_insider_purchases(as_dict _: Bool = false) async throws -> YFJSONValue {
        try await getInsiderPurchases()
    }
    public func get_insider_roster_holders(as_dict _: Bool = false) async throws -> YFJSONValue {
        try await getInsiderRosterHolders()
    }
    public func get_dividends() async throws -> YFJSONValue { try await getDividends() }
    public func get_dividends(period: String) async throws -> YFJSONValue { try await getDividends(period: period) }
    public func get_capital_gains() async throws -> YFJSONValue { try await getCapitalGains() }
    public func get_capital_gains(period: String) async throws -> YFJSONValue { try await getCapitalGains(period: period) }
    public func get_splits() async throws -> YFJSONValue { try await getSplits() }
    public func get_splits(period: String) async throws -> YFJSONValue { try await getSplits(period: period) }
    public func get_actions() async throws -> YFJSONValue { try await getActions() }
    public func get_actions(period: String) async throws -> YFJSONValue { try await getActions(period: period) }
    public func get_shares() async throws -> YFJSONValue { try await getShares() }
    public func get_shares_full(start: Date, end: Date = Date()) async throws -> YFJSONValue {
        try await getSharesFull(start: start, end: end)
    }
    public func get_analyst_price_targets() async throws -> YFJSONValue { try await getAnalystPriceTargets() }
    public func get_earnings_estimate(as_dict _: Bool = false) async throws -> YFJSONValue { try await getEarningsEstimate() }
    public func get_revenue_estimate(as_dict _: Bool = false) async throws -> YFJSONValue { try await getRevenueEstimate() }
    public func get_earnings_history(as_dict _: Bool = false) async throws -> YFJSONValue { try await getEarningsHistory() }
    public func get_eps_trend(as_dict _: Bool = false) async throws -> YFJSONValue { try await getEpsTrend() }
    public func get_eps_revisions(as_dict _: Bool = false) async throws -> YFJSONValue { try await getEpsRevisions() }
    public func get_growth_estimates(as_dict _: Bool = false) async throws -> YFJSONValue { try await getGrowthEstimates() }
    public func get_sustainability(as_dict _: Bool = false) async throws -> YFJSONValue { try await getSustainability() }
    public func get_earnings(as_dict _: Bool = false, freq: YFFinancialFrequency) async throws -> YFJSONValue {
        try await getEarnings(freq: freq)
    }
    public func get_income_stmt(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await getIncomeStmt(freq: freq)
    }
    public func get_incomestmt(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await getIncomestmt(freq: freq)
    }
    public func get_financials(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await getFinancials(freq: freq)
    }
    public func get_balance_sheet(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await getBalanceSheet(freq: freq)
    }
    public func get_balancesheet(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await getBalancesheet(freq: freq)
    }
    public func get_cash_flow(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await getCashFlow(freq: freq)
    }
    public func get_cashflow(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: YFFinancialFrequency
    ) async throws -> YFJSONValue {
        try await getCashflow(freq: freq)
    }
    public func get_earnings(as_dict _: Bool = false, freq: String = "yearly") async throws -> YFJSONValue {
        try await getEarnings(freq: freq)
    }
    public func get_income_stmt(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await getIncomeStmt(freq: freq)
    }
    public func get_incomestmt(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await getIncomestmt(freq: freq)
    }
    public func get_financials(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await getFinancials(freq: freq)
    }
    public func get_balance_sheet(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await getBalanceSheet(freq: freq)
    }
    public func get_balancesheet(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await getBalancesheet(freq: freq)
    }
    public func get_cash_flow(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await getCashFlow(freq: freq)
    }
    public func get_cashflow(
        as_dict _: Bool = false,
        pretty _: Bool = false,
        freq: String = "yearly"
    ) async throws -> YFJSONValue {
        try await getCashflow(freq: freq)
    }
    public func get_earnings_table(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await getEarningsTable(freq: freq) }
    public func get_income_stmt_table(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await getIncomeStmtTable(freq: freq) }
    public func get_financials_table(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await getFinancialsTable(freq: freq) }
    public func get_balance_sheet_table(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await getBalanceSheetTable(freq: freq) }
    public func get_cash_flow_table(freq: YFFinancialFrequency = .yearly) async throws -> YFTable { try await getCashFlowTable(freq: freq) }
    public func get_major_holders_table() async throws -> YFTable { try await getMajorHoldersTable() }
    public func get_institutional_holders_table() async throws -> YFTable { try await getInstitutionalHoldersTable() }
    public func get_mutualfund_holders_table() async throws -> YFTable { try await getMutualfundHoldersTable() }
    public func get_insider_purchases_table() async throws -> YFTable { try await getInsiderPurchasesTable() }
    public func get_insider_transactions_table() async throws -> YFTable { try await getInsiderTransactionsTable() }
    public func get_insider_roster_holders_table() async throws -> YFTable { try await getInsiderRosterHoldersTable() }
    public func get_sustainability_table() async throws -> YFTable { try await getSustainabilityTable() }
    public func get_analyst_price_targets_table() async throws -> YFTable { try await getAnalystPriceTargetsTable() }
    public func get_earnings_estimate_table() async throws -> YFTable { try await getEarningsEstimateTable() }
    public func get_revenue_estimate_table() async throws -> YFTable { try await getRevenueEstimateTable() }
    public func get_earnings_history_table() async throws -> YFTable { try await getEarningsHistoryTable() }
    public func get_eps_trend_table() async throws -> YFTable { try await getEpsTrendTable() }
    public func get_eps_revisions_table() async throws -> YFTable { try await getEpsRevisionsTable() }
    public func get_growth_estimates_table() async throws -> YFTable { try await getGrowthEstimatesTable() }
    public func get_news(count: Int = 10, tab: YFNewsTab = .news) async throws -> YFJSONValue { try await getNews(count: count, tab: tab) }
    public func get_earnings_dates(limit: Int = 12, offset: Int = 0) async throws -> YFJSONValue { try await getEarningsDates(limit: limit, offset: offset) }
    public func get_history_metadata() async throws -> YFJSONValue { try await getHistoryMetadata() }
    public func get_funds_data() async throws -> YFFundsData { try await getFundsData() }
    public func get_funds_data_raw() async throws -> YFJSONValue { try await getFundsDataRaw() }
    public func get_dividends_table(period: YFinanceClient.Range = .max) async throws -> YFTable { try await getDividendsTable(period: period) }
    public func get_capital_gains_table(period: YFinanceClient.Range = .max) async throws -> YFTable { try await getCapitalGainsTable(period: period) }
    public func get_splits_table(period: YFinanceClient.Range = .max) async throws -> YFTable { try await getSplitsTable(period: period) }
    public func get_actions_table(period: YFinanceClient.Range = .max) async throws -> YFTable { try await getActionsTable(period: period) }
    public func get_history(
        period: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        prepost: Bool = false,
        actions: Bool = true,
        autoAdjust: Bool = true,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
        try await getHistory(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
        try await getHistory(
            period: period,
            interval: interval,
            prepost: prepost,
            actions: actions,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
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
            rounding: rounding
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
        rounding: Bool = false
    ) async throws -> YFHistorySeries {
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
            rounding: rounding
        )
    }

    private static func parseFinancialFrequency(_ value: String) throws -> YFFinancialFrequency {
        guard let parsed = YFFinancialFrequency(pythonValue: value) else {
            throw YFinanceError.invalidRequest(
                "Invalid frequency '\(value)'. Expected one of: yearly, quarterly, trailing, ttm"
            )
        }
        return parsed
    }

    private func summaryResult(modules: [String]) async throws -> YFJSONValue {
        let response = try await quoteSummaryRaw(modules: modules)
        return response["quoteSummary"]?["result"]?[0] ?? .object([:])
    }

    private func periodicTrendTable(key: String) async throws -> YFJSONValue {
        let result = try await summaryResult(modules: ["earningsTrend"])
        let trendRows = result["earningsTrend"]?["trend"]?.arrayValue ?? []
        var parsed: [YFJSONValue] = []

        for row in trendRows.prefix(4) {
            guard let detail = row[key]?.objectValue else {
                continue
            }
            var object: [String: YFJSONValue] = [:]
            object["period"] = row["period"] ?? .null

            for (k, v) in detail {
                object[k] = Self.unwrapRaw(v)
            }
            parsed.append(.object(object))
        }

        return .array(parsed)
    }

    static func parseProfileFundamentals(from result: YFJSONValue) -> YFProfileFundamentals {
        let summaryProfile = result["summaryProfile"]
        let summaryDetail = result["summaryDetail"]
        let financialData = result["financialData"]
        let defaultKeyStatistics = result["defaultKeyStatistics"]
        let price = result["price"]

        let about = firstNonEmptyString([
            summaryProfile?["longBusinessSummary"],
            summaryProfile?["description"],
            summaryProfile?["longDescription"],
        ])
        let sector = firstNonEmptyString([
            summaryProfile?["sector"],
            summaryProfile?["category"],
        ])
        let industry = firstNonEmptyString([
            summaryProfile?["industry"],
            summaryProfile?["industryDisp"],
        ])
        let website = firstNonEmptyString([
            summaryProfile?["website"],
        ])
        let marketCap = firstFiniteNumber([
            price?["marketCap"],
            summaryDetail?["marketCap"],
            financialData?["marketCap"],
            defaultKeyStatistics?["marketCap"],
        ])
        let peRatio = firstFiniteNumber([
            summaryDetail?["trailingPE"],
            summaryDetail?["forwardPE"],
            financialData?["trailingPE"],
            financialData?["forwardPE"],
            defaultKeyStatistics?["trailingPE"],
            defaultKeyStatistics?["forwardPE"],
        ])
        let currency = firstNonEmptyString([
            price?["currency"],
            summaryDetail?["currency"],
            financialData?["financialCurrency"],
        ])

        return YFProfileFundamentals(
            about: about,
            sector: sector,
            industry: industry,
            website: website,
            marketCap: marketCap,
            peRatio: peRatio,
            currency: currency
        )
    }

    private static func firstNonEmptyString(_ values: [YFJSONValue?]) -> String? {
        for value in values {
            guard let value else {
                continue
            }
            let unwrapped = unwrapRaw(value)
            if let text = unwrapped.stringValue?.trimmingCharacters(in: .whitespacesAndNewlines), !text.isEmpty {
                return text
            }
        }
        return nil
    }

    private static func firstFiniteNumber(_ values: [YFJSONValue?]) -> Double? {
        for value in values {
            guard let value else {
                continue
            }
            let unwrapped = unwrapRaw(value)
            if let number = unwrapped.doubleValue, number.isFinite {
                return number
            }
        }
        return nil
    }

    private static func parseOptions(raw: YFJSONValue) -> YFOptionsChain {
        let result = raw["optionChain"]?["result"]?[0]
        let expirationDates = (result?["expirationDates"]?.arrayValue ?? [])
            .compactMap { $0.doubleValue }
            .map { Date(timeIntervalSince1970: $0) }
        let underlying = result?["quote"]

        let options = result?["options"]?[0]
        let calls = (options?["calls"]?.arrayValue ?? []).map { YFOptionContract(raw: $0) }
        let puts = (options?["puts"]?.arrayValue ?? []).map { YFOptionContract(raw: $0) }

        return YFOptionsChain(
            expirationDates: expirationDates,
            underlying: underlying,
            calls: calls,
            puts: puts
        )
    }

    private static func expirationDateMap(from raw: YFJSONValue) -> [String: Int] {
        let values = raw["optionChain"]?["result"]?[0]?["expirationDates"]?.arrayValue ?? []
        var output: [String: Int] = [:]
        output.reserveCapacity(values.count)
        for value in values {
            guard let epoch = value.intValue else {
                continue
            }
            let date = Date(timeIntervalSince1970: TimeInterval(epoch))
            output[formatExpirationDate(date)] = epoch
        }
        return output
    }

    private static func mergeInfo(summary: YFJSONValue, additionalQuote: YFJSONValue, symbol: String) -> [String: YFJSONValue] {
        let summaryResult = summary["quoteSummary"]?["result"]?[0]?.objectValue ?? [:]
        let quoteResult = additionalQuote["quoteResponse"]?["result"]?[0]?.objectValue ?? [:]

        var merged: [String: YFJSONValue] = ["symbol": .string(symbol)]
        for (key, value) in summaryResult {
            merged[key] = unwrapRaw(value)
        }
        for (key, value) in quoteResult {
            merged[key] = unwrapRaw(value)
        }
        return merged
    }

    private static func unwrapRaw(_ value: YFJSONValue) -> YFJSONValue {
        if let object = value.objectValue {
            if let raw = object["raw"] {
                return raw
            }
            if let fmt = object["fmt"] {
                return fmt
            }

            var mapped: [String: YFJSONValue] = [:]
            for (key, child) in object {
                mapped[key] = unwrapRaw(child)
            }
            return .object(mapped)
        }

        if let array = value.arrayValue {
            return .array(array.map { unwrapRaw($0) })
        }

        return value
    }

    private static func sortedEventArray(_ value: YFJSONValue?) -> YFJSONValue {
        guard let dictionary = value?.objectValue else {
            return .array([])
        }

        let sortedValues = dictionary
            .map { $0.value }
            .sorted {
                let lhs = $0["date"]?.doubleValue ?? 0
                let rhs = $1["date"]?.doubleValue ?? 0
                return lhs < rhs
            }

        return .array(sortedValues)
    }

    private static func statementTable(from raw: YFJSONValue) -> YFTable {
        tabularValue(from: raw).toTable()
    }

    private static func tabularValue(from value: YFJSONValue) -> YFJSONValue {
        if case .array = value {
            return value
        }

        guard let object = value.objectValue else {
            return value
        }

        let preferredArrayKeys = [
            "incomeStatementHistory",
            "incomeStatementHistoryQuarterly",
            "balanceSheetStatements",
            "balanceSheetHistory",
            "balanceSheetHistoryQuarterly",
            "cashflowStatements",
            "cashFlowStatements",
            "cashflowStatementHistory",
            "cashFlowStatementHistory",
            "cashflowStatementHistoryQuarterly",
            "cashFlowStatementHistoryQuarterly",
            "history",
            "trend",
            "result",
        ]

        for key in preferredArrayKeys {
            if let array = object[key]?.arrayValue {
                return .array(array)
            }
        }

        for child in object.values {
            if case .array = child {
                return child
            }
        }

        for child in object.values {
            let nested = tabularValue(from: child)
            if case .array(let arr) = nested, !arr.isEmpty {
                return nested
            }
        }

        return value
    }

    private static func splitRatioValue(_ event: YFJSONValue) -> YFJSONValue {
        if let numerator = event["numerator"]?.doubleValue,
           let denominator = event["denominator"]?.doubleValue,
           denominator != 0 {
            return .number(numerator / denominator)
        }

        if let splitRatio = event["splitRatio"]?.stringValue {
            let parts = splitRatio.split(separator: "/")
            if parts.count == 2,
               let lhs = Double(parts[0]),
               let rhs = Double(parts[1]),
               rhs != 0 {
                return .number(lhs / rhs)
            }
        }

        return .null
    }

    private static func earningsDateVisualizationBody(symbol: String, limit: Int, offset: Int) -> YFJSONValue {
        .object([
            "size": .number(Double(min(max(limit, 1), 100))),
            "query": .object([
                "operator": .string("eq"),
                "operands": .array([.string("ticker"), .string(symbol)]),
            ]),
            "sortField": .string("startdatetime"),
            "sortType": .string("DESC"),
            "entityIdType": .string("earnings"),
            "offset": .number(Double(max(offset, 0))),
            "includeFields": .array([
                .string("startdatetime"),
                .string("timeZoneShortName"),
                .string("epsestimate"),
                .string("epsactual"),
                .string("epssurprisepct"),
                .string("eventtype"),
            ]),
        ])
    }

    private static func hasEarningsDocumentRows(_ raw: YFJSONValue) -> Bool {
        let rows = raw["finance"]?["result"]?[0]?["documents"]?[0]?["rows"]?.arrayValue ?? []
        return !rows.isEmpty
    }

    private func scrapeEarningsDates(limit: Int, offset: Int) async throws -> YFJSONValue? {
        guard let encodedSymbol = symbol.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed),
              let url = URL(
                  string: "https://finance.yahoo.com/calendar/earnings?symbol=\(encodedSymbol)&offset=\(max(offset, 0))&size=\(Self.scrapeCalendarSize(limit: limit))"
              )
        else {
            return nil
        }

        let html = try await client.rawText(url: url)
        guard let tableHTML = Self.firstHTMLTable(in: html) else {
            return nil
        }

        let parsed = Self.parseHTMLTable(tableHTML)
        guard !parsed.headers.isEmpty else {
            return nil
        }

        let symbolColumn = parsed.headers.firstIndex { $0.caseInsensitiveCompare("Symbol") == .orderedSame }
        var dropColumns: Set<Int> = []
        for (index, header) in parsed.headers.enumerated() {
            let normalized = header.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            if normalized == "symbol" || normalized == "company" || normalized == "follow" {
                dropColumns.insert(index)
            }
        }

        let keepColumns = parsed.headers.enumerated().compactMap { index, header -> (Int, String)? in
            guard !dropColumns.contains(index) else { return nil }
            return (index, Self.normalizedScrapedColumnName(header))
        }

        var rows: [[YFJSONValue]] = []
        rows.reserveCapacity(parsed.rows.count)

        for row in parsed.rows {
            if let symbolColumn,
               row.indices.contains(symbolColumn) {
                let rowSymbol = row[symbolColumn]
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                    .uppercased()
                if !rowSymbol.isEmpty, rowSymbol != symbol {
                    continue
                }
            }

            var outputRow: [YFJSONValue] = []
            outputRow.reserveCapacity(keepColumns.count)
            for (columnIndex, _) in keepColumns {
                if row.indices.contains(columnIndex) {
                    outputRow.append(Self.convertScrapedEarningsCell(row[columnIndex]))
                } else {
                    outputRow.append(.null)
                }
            }
            rows.append(outputRow)
        }

        guard !rows.isEmpty else {
            return nil
        }

        let safeOffset = max(offset, 0)
        if safeOffset > 0 {
            rows = Array(rows.dropFirst(min(safeOffset, rows.count)))
        }
        if limit > 0, rows.count > limit {
            rows = Array(rows.prefix(limit))
        }

        let columnObjects: [YFJSONValue] = keepColumns.map { _, label in
            .object(["label": .string(label)])
        }
        let rowValues: [YFJSONValue] = rows.map { .array($0) }

        return .object([
            "finance": .object([
                "result": .array([
                    .object([
                        "documents": .array([
                            .object([
                                "columns": .array(columnObjects),
                                "rows": .array(rowValues),
                            ]),
                        ]),
                    ]),
                ]),
            ]),
        ])
    }

    private static func scrapeCalendarSize(limit: Int) -> Int {
        if limit <= 25 {
            return 25
        }
        if limit <= 50 {
            return 50
        }
        return 100
    }

    private static func firstHTMLTable(in html: String) -> String? {
        guard let regex = try? NSRegularExpression(
            pattern: "(?is)<table\\b[^>]*>.*?</table>"
        ) else {
            return nil
        }
        let range = NSRange(html.startIndex..<html.endIndex, in: html)
        guard let match = regex.firstMatch(in: html, options: [], range: range),
              let matchRange = Range(match.range, in: html) else {
            return nil
        }
        return String(html[matchRange])
    }

    private static func parseHTMLTable(_ html: String) -> (headers: [String], rows: [[String]]) {
        guard let rowRegex = try? NSRegularExpression(
            pattern: "(?is)<tr\\b[^>]*>(.*?)</tr>"
        ), let cellRegex = try? NSRegularExpression(
            pattern: "(?is)<t[hd]\\b[^>]*>(.*?)</t[hd]>"
        ) else {
            return ([], [])
        }

        let fullRange = NSRange(html.startIndex..<html.endIndex, in: html)
        let rowMatches = rowRegex.matches(in: html, options: [], range: fullRange)

        var headers: [String] = []
        var rows: [[String]] = []

        for rowMatch in rowMatches {
            guard let rowRange = Range(rowMatch.range, in: html) else {
                continue
            }
            let rowHTML = String(html[rowRange])

            let rowCellRange = NSRange(rowHTML.startIndex..<rowHTML.endIndex, in: rowHTML)
            let cellMatches = cellRegex.matches(in: rowHTML, options: [], range: rowCellRange)
            if cellMatches.isEmpty {
                continue
            }

            let cells: [String] = cellMatches.compactMap { match in
                guard match.numberOfRanges > 1,
                      let valueRange = Range(match.range(at: 1), in: rowHTML) else {
                    return nil
                }
                return normalizeHTMLText(String(rowHTML[valueRange]))
            }

            if cells.isEmpty {
                continue
            }

            let isHeaderRow = rowHTML.range(of: "<th", options: [.caseInsensitive]) != nil
            if isHeaderRow && headers.isEmpty {
                headers = cells
            } else {
                rows.append(cells)
            }
        }

        return (headers, rows)
    }

    private static func normalizeHTMLText(_ html: String) -> String {
        guard let tagRegex = try? NSRegularExpression(pattern: "(?is)<[^>]+>") else {
            return decodeHTMLEntities(html).trimmingCharacters(in: .whitespacesAndNewlines)
        }
        let range = NSRange(html.startIndex..<html.endIndex, in: html)
        let stripped = tagRegex.stringByReplacingMatches(in: html, options: [], range: range, withTemplate: " ")
        let decoded = decodeHTMLEntities(stripped)
        return decoded.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func decodeHTMLEntities(_ text: String) -> String {
        var output = text
        let entities: [String: String] = [
            "&amp;": "&",
            "&quot;": "\"",
            "&#39;": "'",
            "&apos;": "'",
            "&lt;": "<",
            "&gt;": ">",
            "&nbsp;": " ",
        ]
        for (entity, replacement) in entities {
            output = output.replacingOccurrences(of: entity, with: replacement)
        }
        return output
    }

    private static func normalizedScrapedColumnName(_ name: String) -> String {
        let normalized = name.trimmingCharacters(in: .whitespacesAndNewlines)
        if normalized.caseInsensitiveCompare("Surprise(%)") == .orderedSame {
            return "Surprise (%)"
        }
        return normalized
    }

    private static func convertScrapedEarningsCell(_ raw: String) -> YFJSONValue {
        let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if value.isEmpty || value == "-" {
            return .null
        }

        let numeric = value
            .replacingOccurrences(of: ",", with: "")
            .replacingOccurrences(of: "%", with: "")
        if let number = Double(numeric) {
            return .number(number)
        }

        return .string(value)
    }

    private static func normalizedEarningsDatesTable(from rows: [[String: YFJSONValue]]) -> YFTable {
        let columns = ["Earnings Date", "EPS Estimate", "Reported EPS", "Surprise(%)", "Event Type"]
        var mappedRows: [[String: YFJSONValue]] = []
        mappedRows.reserveCapacity(rows.count)

        for row in rows {
            let timezone = value(in: row, forAny: ["Timezone short name", "timeZoneShortName"])
            let earningsDate = normalizedEarningsDate(
                value: value(in: row, forAny: ["Earnings Date", "Event Start Date", "startdatetime"]),
                timezoneValue: timezone
            )
            let epsEstimate = normalizedNumericValue(value(in: row, forAny: ["EPS Estimate", "epsestimate"]))
            let reportedEPS = normalizedNumericValue(value(in: row, forAny: ["Reported EPS", "epsactual"]))
            let surprise = normalizedNumericValue(value(in: row, forAny: ["Surprise(%)", "Surprise (%)", "epssurprisepct"]))
            let eventType = normalizedEarningsEventType(
                value(
                    in: row,
                    forAny: ["Event Type", "eventtype", "Event Name", "Earnings Call Time", "startdatetimetype"]
                )
            )

            let hasData = !isNullLike(earningsDate)
                || !isNullLike(epsEstimate)
                || !isNullLike(reportedEPS)
                || !isNullLike(surprise)
                || !isNullLike(eventType)

            guard hasData else {
                continue
            }

            mappedRows.append([
                "Earnings Date": earningsDate,
                "EPS Estimate": epsEstimate,
                "Reported EPS": reportedEPS,
                "Surprise(%)": surprise,
                "Event Type": eventType,
            ])
        }

        return YFTable(columns: columns, rows: mappedRows)
    }

    private static func value(in row: [String: YFJSONValue], forAny keys: [String]) -> YFJSONValue? {
        for key in keys {
            if let value = row[key], !isNullLike(value) {
                return value
            }
        }

        let lowercasedRow = Dictionary(uniqueKeysWithValues: row.map { ($0.key.lowercased(), $0.value) })
        for key in keys {
            if let value = lowercasedRow[key.lowercased()], !isNullLike(value) {
                return value
            }
        }
        return nil
    }

    private static func normalizedNumericValue(_ value: YFJSONValue?) -> YFJSONValue {
        guard let value else {
            return .null
        }
        if let number = value.doubleValue {
            return .number(number)
        }
        if let text = value.stringValue {
            let cleaned = text
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .replacingOccurrences(of: ",", with: "")
                .replacingOccurrences(of: "%", with: "")
            if cleaned.isEmpty || cleaned == "-" {
                return .null
            }
            if let number = Double(cleaned) {
                return .number(number)
            }
        }
        return value
    }

    private static func normalizedEarningsEventType(_ value: YFJSONValue?) -> YFJSONValue {
        guard let value else {
            return .null
        }

        if let number = value.intValue {
            switch number {
            case 1:
                return .string("Call")
            case 2:
                return .string("Earnings")
            case 11:
                return .string("Meeting")
            default:
                return value
            }
        }

        if let raw = value.stringValue {
            let normalized = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            let upper = normalized.uppercased()
            switch upper {
            case "1":
                return .string("Call")
            case "2", "EAD", "ERA":
                return .string("Earnings")
            case "11":
                return .string("Meeting")
            default:
                return normalized.isEmpty ? .null : .string(normalized)
            }
        }

        return value
    }

    private static func normalizedEarningsDate(value: YFJSONValue?, timezoneValue: YFJSONValue?) -> YFJSONValue {
        guard let value else {
            return .null
        }
        if case .number = value {
            return value
        }
        guard let text = value.stringValue?.trimmingCharacters(in: .whitespacesAndNewlines),
              !text.isEmpty else {
            return value
        }

        let timezone = normalizedEarningsTimeZone(from: timezoneValue)

        let isoFormatter = ISO8601DateFormatter()
        isoFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = isoFormatter.date(from: text) {
            return .number(date.timeIntervalSince1970)
        }
        isoFormatter.formatOptions = [.withInternetDateTime]
        if let date = isoFormatter.date(from: text) {
            return .number(date.timeIntervalSince1970)
        }

        let readableFormatter = DateFormatter()
        readableFormatter.calendar = Calendar(identifier: .gregorian)
        readableFormatter.locale = Locale(identifier: "en_US_POSIX")
        readableFormatter.timeZone = timezone
        readableFormatter.dateFormat = "MMMM d, yyyy 'at' h a"

        let normalizedText = text
            .replacingOccurrences(of: "EDT", with: "America/New_York")
            .replacingOccurrences(of: "EST", with: "America/New_York")
        if let date = readableFormatter.date(from: normalizedText) {
            return .number(date.timeIntervalSince1970)
        }

        return .string(text)
    }

    private static func normalizedEarningsTimeZone(from value: YFJSONValue?) -> TimeZone {
        guard let value else {
            return TimeZone(secondsFromGMT: 0) ?? .current
        }

        if let name = value.stringValue?.trimmingCharacters(in: .whitespacesAndNewlines),
           !name.isEmpty {
            if let timezone = TimeZone(identifier: name) {
                return timezone
            }
            switch name.uppercased() {
            case "EDT", "EST":
                return TimeZone(identifier: "America/New_York") ?? (TimeZone(secondsFromGMT: 0) ?? .current)
            default:
                break
            }
        }

        return TimeZone(secondsFromGMT: 0) ?? .current
    }

    private static func isNullLike(_ value: YFJSONValue?) -> Bool {
        guard let value else {
            return true
        }
        if case .null = value {
            return true
        }
        return false
    }

    private static func parseExpirationDate(_ value: String) -> Date? {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.date(from: value)
    }

    private static func formatExpirationDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.string(from: date)
    }
}
