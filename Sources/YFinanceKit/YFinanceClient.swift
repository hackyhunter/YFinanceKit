import Foundation

public actor YFinanceClient {
    public enum EndpointHost: Sendable {
        case query1
        case query2
        case root
    }

    public enum Interval: String, Sendable {
        case oneMinute = "1m"
        case twoMinutes = "2m"
        case fiveMinutes = "5m"
        case fifteenMinutes = "15m"
        case thirtyMinutes = "30m"
        case sixtyMinutes = "60m"
        case ninetyMinutes = "90m"
        case oneHour = "1h"
        case oneDay = "1d"
        case fiveDays = "5d"
        case oneWeek = "1wk"
        case oneMonth = "1mo"
        case threeMonths = "3mo"

        public init?(pythonValue: String) {
            let normalized = pythonValue
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .lowercased()
            switch normalized {
            case "1m":
                self = .oneMinute
            case "2m":
                self = .twoMinutes
            case "5m":
                self = .fiveMinutes
            case "15m":
                self = .fifteenMinutes
            case "30m":
                self = .thirtyMinutes
            case "60m":
                self = .sixtyMinutes
            case "90m":
                self = .ninetyMinutes
            case "1h":
                self = .oneHour
            case "1d":
                self = .oneDay
            case "5d":
                self = .fiveDays
            case "1wk", "1w":
                self = .oneWeek
            case "1mo":
                self = .oneMonth
            case "3mo":
                self = .threeMonths
            default:
                return nil
            }
        }
    }

    public enum Range: String, Sendable {
        case oneDay = "1d"
        case fiveDays = "5d"
        case oneMonth = "1mo"
        case threeMonths = "3mo"
        case sixMonths = "6mo"
        case oneYear = "1y"
        case twoYears = "2y"
        case fiveYears = "5y"
        case tenYears = "10y"
        case yearToDate = "ytd"
        case max = "max"

        public init?(pythonValue: String) {
            let normalized = pythonValue
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .lowercased()
            switch normalized {
            case "1d":
                self = .oneDay
            case "5d":
                self = .fiveDays
            case "1mo":
                self = .oneMonth
            case "3mo":
                self = .threeMonths
            case "6mo":
                self = .sixMonths
            case "1y":
                self = .oneYear
            case "2y":
                self = .twoYears
            case "5y":
                self = .fiveYears
            case "10y":
                self = .tenYears
            case "ytd":
                self = .yearToDate
            case "max":
                self = .max
            default:
                return nil
            }
        }
    }

    public enum HistoryEvent: String, Sendable {
        case dividends = "div"
        case splits = "splits"
        case capitalGains = "capitalGains"
    }

    private let session: URLSession
    private let decoder: JSONDecoder
    private let userAgent: String
    private let query1BaseURL: URL
    private let query2BaseURL: URL
    private let rootBaseURL: URL
    private let crumbStore: YFCrumbStore

    private func normalizedTimeout(_ timeout: TimeInterval?) -> TimeInterval? {
        guard let timeout, timeout > 0 else {
            return nil
        }
        return timeout
    }

    public init(
        session: URLSession = .shared,
        userAgent: String = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        query1BaseURL: URL = URL(string: "https://query1.finance.yahoo.com")!,
        query2BaseURL: URL = URL(string: "https://query2.finance.yahoo.com")!,
        rootBaseURL: URL = URL(string: "https://finance.yahoo.com")!
    ) {
        self.session = session
        self.userAgent = userAgent
        self.query1BaseURL = query1BaseURL
        self.query2BaseURL = query2BaseURL
        self.rootBaseURL = rootBaseURL
        self.decoder = JSONDecoder()
        self.crumbStore = YFCrumbStore(session: session, userAgent: userAgent)
    }

    nonisolated public func ticker(_ symbol: String) -> YFTicker {
        YFTicker(symbol: symbol, client: self)
    }

    nonisolated public func ticker(_ ticker: (String, String)) throws -> YFTicker {
        try YFTicker(ticker, client: self)
    }

    nonisolated public func tickers(_ symbols: [String]) -> YFTickers {
        YFTickers(symbols: symbols, client: self)
    }

    nonisolated public func tickers(_ tickers: [(String, String)]) throws -> YFTickers {
        try YFTickers(tickers, client: self)
    }

    public func quote(symbol: String) async throws -> YFQuote? {
        try await quote(symbols: [symbol]).first
    }

    public func quote(symbols: [String]) async throws -> [YFQuote] {
        let cleaned = normalizedSymbols(symbols)
        if cleaned.isEmpty {
            return []
        }

        let response: YFQuoteResponse = try await requestJSON(
            host: .query1,
            path: "/v7/finance/quote",
            queryItems: [
                URLQueryItem(name: "symbols", value: cleaned.joined(separator: ",")),
                URLQueryItem(name: "formatted", value: "false"),
            ],
            requiresCrumb: true
        )

        if let error = response.quoteResponse.error {
            throw YFinanceError.serverError(code: error.code ?? "unknown", description: error.description ?? "Unknown Yahoo error")
        }
        return response.quoteResponse.result
    }

    public func history(
        symbol: String,
        period: String,
        interval: String = "1d",
        includePrePost: Bool = false,
        events: Set<HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFHistorySeries {
        guard let parsedRange = Range(pythonValue: period) else {
            throw YFinanceError.invalidRequest("Invalid period '\(period)'")
        }
        guard let parsedInterval = Interval(pythonValue: interval) else {
            throw YFinanceError.invalidRequest("Invalid interval '\(interval)'")
        }
        return try await history(
            symbol: symbol,
            range: parsedRange,
            interval: parsedInterval,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            timeout: timeout
        )
    }

    public func history(
        symbol: String,
        range: Range = .oneMonth,
        interval: Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFHistorySeries {
        let targetInterval = interval
        let requestInterval = try historyRequestInterval(targetInterval, repair: repair)
        let response: YFChartResponse = try await requestJSON(
            host: .query2,
            path: "/v8/finance/chart/\(normalizedSymbol(symbol))",
            queryItems: [
                URLQueryItem(name: "range", value: range.rawValue),
                URLQueryItem(name: "interval", value: requestInterval.rawValue),
                URLQueryItem(name: "includePrePost", value: includePrePost ? "true" : "false"),
                URLQueryItem(name: "events", value: events.map(\.rawValue).sorted().joined(separator: ",")),
            ],
            requiresCrumb: false,
            timeout: timeout
        )
        return try await parseHistory(
            symbol: normalizedSymbol(symbol),
            response: response,
            requestInterval: requestInterval,
            targetInterval: targetInterval,
            includePrePost: includePrePost,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            includeEvents: !events.isEmpty,
            timeout: timeout
        )
    }

    public func history(
        symbol: String,
        start: Date,
        end: Date = Date(),
        interval: String,
        includePrePost: Bool = false,
        events: Set<HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFHistorySeries {
        guard let parsedInterval = Interval(pythonValue: interval) else {
            throw YFinanceError.invalidRequest("Invalid interval '\(interval)'")
        }
        return try await history(
            symbol: symbol,
            start: start,
            end: end,
            interval: parsedInterval,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            timeout: timeout
        )
    }

    public func history(
        symbol: String,
        start: Date,
        end: Date = Date(),
        interval: Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFHistorySeries {
        guard start < end else {
            throw YFinanceError.invalidRequest("start must be earlier than end")
        }
        let targetInterval = interval
        let requestInterval = try historyRequestInterval(targetInterval, repair: repair)

        let response: YFChartResponse = try await requestJSON(
            host: .query2,
            path: "/v8/finance/chart/\(normalizedSymbol(symbol))",
            queryItems: [
                URLQueryItem(name: "period1", value: String(Int(start.timeIntervalSince1970))),
                URLQueryItem(name: "period2", value: String(Int(end.timeIntervalSince1970))),
                URLQueryItem(name: "interval", value: requestInterval.rawValue),
                URLQueryItem(name: "includePrePost", value: includePrePost ? "true" : "false"),
                URLQueryItem(name: "events", value: events.map(\.rawValue).sorted().joined(separator: ",")),
            ],
            requiresCrumb: false,
            timeout: timeout
        )
        return try await parseHistory(
            symbol: normalizedSymbol(symbol),
            response: response,
            requestInterval: requestInterval,
            targetInterval: targetInterval,
            includePrePost: includePrePost,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            includeEvents: !events.isEmpty,
            timeout: timeout
        )
    }

    public func historyRaw(
        symbol: String,
        range: Range = .oneMonth,
        interval: Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<HistoryEvent> = [.dividends, .splits],
        timeout: TimeInterval? = nil
    ) async throws -> YFJSONValue {
        try await rawGet(
            host: .query2,
            path: "/v8/finance/chart/\(normalizedSymbol(symbol))",
            queryItems: [
                URLQueryItem(name: "range", value: range.rawValue),
                URLQueryItem(name: "interval", value: interval.rawValue),
                URLQueryItem(name: "includePrePost", value: includePrePost ? "true" : "false"),
                URLQueryItem(name: "events", value: events.map(\.rawValue).sorted().joined(separator: ",")),
            ],
            timeout: timeout
        )
    }

    public func tickerTimeZone(symbol: String) async throws -> String? {
        let cleaned = normalizedSymbol(symbol)
        if let cached = await YFCacheStores.tz.lookup(cleaned),
           !cached.isEmpty {
            if TimeZone(identifier: cached) != nil {
                return cached
            }
            await YFCacheStores.tz.set(nil, for: cleaned)
        }

        let response: YFChartResponse = try await requestJSON(
            host: .query2,
            path: "/v8/finance/chart/\(cleaned)",
            queryItems: [
                URLQueryItem(name: "range", value: Range.fiveDays.rawValue),
                URLQueryItem(name: "interval", value: Interval.oneDay.rawValue),
                URLQueryItem(name: "includePrePost", value: "false"),
            ],
            requiresCrumb: false
        )

        if let chartError = response.chart.error {
            throw YFinanceError.serverError(
                code: chartError.code ?? "chart_error",
                description: chartError.description ?? "Unknown chart error"
            )
        }

        guard let chart = response.chart.result?.first else {
            return nil
        }

        let meta = chart.meta
        let tzName = meta.exchangeTimezoneName ?? meta.timezone
        guard let tzName, !tzName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return nil
        }

        if TimeZone(identifier: tzName) != nil {
            await YFCacheStores.tz.set(tzName, for: cleaned)
        }

        return tzName
    }

    public func search(
        query: String,
        quotesCount: Int = 8,
        newsCount: Int = 8,
        listsCount: Int = 8,
        includeCompanyBreakdown: Bool = true,
        includeNavLinks: Bool = false,
        includeResearchReports: Bool = false,
        includeCulturalAssets: Bool = false,
        enableFuzzyQuery: Bool = false,
        recommendedCount: Int = 8,
        timeout: TimeInterval? = nil
    ) async throws -> YFSearchResult {
        let cleaned = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !cleaned.isEmpty else {
            throw YFinanceError.invalidRequest("query cannot be empty")
        }

        return try await requestJSON(
            host: .query2,
            path: "/v1/finance/search",
            queryItems: [
                URLQueryItem(name: "q", value: cleaned),
                URLQueryItem(name: "quotesCount", value: String(max(quotesCount, 0))),
                URLQueryItem(name: "enableFuzzyQuery", value: enableFuzzyQuery ? "true" : "false"),
                URLQueryItem(name: "newsCount", value: String(max(newsCount, 0))),
                URLQueryItem(name: "quotesQueryId", value: "tss_match_phrase_query"),
                URLQueryItem(name: "newsQueryId", value: "news_cie_vespa"),
                URLQueryItem(name: "listsCount", value: String(max(listsCount, 0))),
                URLQueryItem(name: "enableCb", value: includeCompanyBreakdown ? "true" : "false"),
                URLQueryItem(name: "enableNavLinks", value: includeNavLinks ? "true" : "false"),
                URLQueryItem(name: "enableResearchReports", value: includeResearchReports ? "true" : "false"),
                URLQueryItem(name: "enableCulturalAssets", value: includeCulturalAssets ? "true" : "false"),
                URLQueryItem(name: "recommendedCount", value: String(max(recommendedCount, 0))),
            ],
            requiresCrumb: false,
            timeout: timeout
        )
    }

    public func searchRaw(
        query: String,
        quotesCount: Int = 8,
        newsCount: Int = 8,
        listsCount: Int = 8,
        includeCompanyBreakdown: Bool = true,
        includeNavLinks: Bool = false,
        includeResearchReports: Bool = false,
        includeCulturalAssets: Bool = false,
        enableFuzzyQuery: Bool = false,
        recommendedCount: Int = 8,
        timeout: TimeInterval? = nil
    ) async throws -> YFJSONValue {
        try await rawGet(
            host: .query2,
            path: "/v1/finance/search",
            queryItems: [
                URLQueryItem(name: "q", value: query),
                URLQueryItem(name: "quotesCount", value: String(max(quotesCount, 0))),
                URLQueryItem(name: "enableFuzzyQuery", value: enableFuzzyQuery ? "true" : "false"),
                URLQueryItem(name: "newsCount", value: String(max(newsCount, 0))),
                URLQueryItem(name: "quotesQueryId", value: "tss_match_phrase_query"),
                URLQueryItem(name: "newsQueryId", value: "news_cie_vespa"),
                URLQueryItem(name: "listsCount", value: String(max(listsCount, 0))),
                URLQueryItem(name: "enableCb", value: includeCompanyBreakdown ? "true" : "false"),
                URLQueryItem(name: "enableNavLinks", value: includeNavLinks ? "true" : "false"),
                URLQueryItem(name: "enableResearchReports", value: includeResearchReports ? "true" : "false"),
                URLQueryItem(name: "enableCulturalAssets", value: includeCulturalAssets ? "true" : "false"),
                URLQueryItem(name: "recommendedCount", value: String(max(recommendedCount, 0))),
            ],
            requiresCrumb: false,
            timeout: timeout
        )
    }

    public func quoteSummary(symbol: String, modules: [String]) async throws -> YFJSONValue {
        let cleanedModules = modules.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
        if cleanedModules.isEmpty {
            throw YFinanceError.invalidRequest("modules cannot be empty")
        }

        return try await rawGet(
            host: .query2,
            path: "/v10/finance/quoteSummary/\(normalizedSymbol(symbol))",
            queryItems: [
                URLQueryItem(name: "modules", value: cleanedModules.joined(separator: ",")),
                URLQueryItem(name: "corsDomain", value: "finance.yahoo.com"),
                URLQueryItem(name: "formatted", value: "false"),
                URLQueryItem(name: "symbol", value: normalizedSymbol(symbol)),
            ],
            requiresCrumb: true
        )
    }

    public func options(
        symbol: String,
        expirationEpoch: Int? = nil
    ) async throws -> YFJSONValue {
        var queryItems: [URLQueryItem] = []
        if let expirationEpoch {
            queryItems.append(URLQueryItem(name: "date", value: String(expirationEpoch)))
        }

        return try await rawGet(
            host: .query2,
            path: "/v7/finance/options/\(normalizedSymbol(symbol))",
            queryItems: queryItems,
            requiresCrumb: true
        )
    }

    public func lookup(
        query: String,
        type: String = "all",
        count: Int = 25,
        start: Int = 0,
        timeout: TimeInterval? = nil
    ) async throws -> YFJSONValue {
        try await rawGet(
            host: .query1,
            path: "/v1/finance/lookup",
            queryItems: [
                URLQueryItem(name: "query", value: query),
                URLQueryItem(name: "type", value: type),
                URLQueryItem(name: "start", value: String(max(start, 0))),
                URLQueryItem(name: "count", value: String(max(count, 0))),
                URLQueryItem(name: "formatted", value: "false"),
                URLQueryItem(name: "fetchPricingData", value: "true"),
                URLQueryItem(name: "lang", value: "en-US"),
                URLQueryItem(name: "region", value: "US"),
            ],
            requiresCrumb: false,
            timeout: timeout
        )
    }

    public func marketSummary(market: String, timeout: TimeInterval? = nil) async throws -> YFJSONValue {
        try await rawGet(
            host: .query1,
            path: "/v6/finance/quote/marketSummary",
            queryItems: [
                URLQueryItem(name: "fields", value: "shortName,regularMarketPrice,regularMarketChange,regularMarketChangePercent"),
                URLQueryItem(name: "formatted", value: "false"),
                URLQueryItem(name: "lang", value: "en-US"),
                URLQueryItem(name: "market", value: market),
            ],
            timeout: timeout
        )
    }

    public func marketTime(market: String, timeout: TimeInterval? = nil) async throws -> YFJSONValue {
        try await rawGet(
            host: .query1,
            path: "/v6/finance/markettime",
            queryItems: [
                URLQueryItem(name: "formatted", value: "true"),
                URLQueryItem(name: "key", value: "finance"),
                URLQueryItem(name: "lang", value: "en-US"),
                URLQueryItem(name: "market", value: market),
            ],
            timeout: timeout
        )
    }

    public func domainEntity(type: String, key: String) async throws -> YFJSONValue {
        try await rawGet(
            host: .query1,
            path: "/v1/finance/\(type)/\(key)",
            queryItems: [
                URLQueryItem(name: "formatted", value: "true"),
                URLQueryItem(name: "withReturns", value: "true"),
                URLQueryItem(name: "lang", value: "en-US"),
                URLQueryItem(name: "region", value: "US"),
            ]
        )
    }

    public func screenerPredefined(
        id: String,
        offset: Int? = nil,
        count: Int? = nil,
        sortField: String? = nil,
        sortAsc: Bool? = nil,
        userId: String? = nil,
        userIdType: String? = nil
    ) async throws -> YFJSONValue {
        var queryItems: [URLQueryItem] = [
            URLQueryItem(name: "corsDomain", value: "finance.yahoo.com"),
            URLQueryItem(name: "formatted", value: "false"),
            URLQueryItem(name: "lang", value: "en-US"),
            URLQueryItem(name: "region", value: "US"),
            URLQueryItem(name: "scrIds", value: id),
        ]

        if let offset { queryItems.append(URLQueryItem(name: "offset", value: String(offset))) }
        if let count { queryItems.append(URLQueryItem(name: "count", value: String(count))) }
        if let sortField { queryItems.append(URLQueryItem(name: "sortField", value: sortField)) }
        if let sortAsc { queryItems.append(URLQueryItem(name: "sortAsc", value: sortAsc ? "true" : "false")) }
        if let userId { queryItems.append(URLQueryItem(name: "userId", value: userId)) }
        if let userIdType { queryItems.append(URLQueryItem(name: "userIdType", value: userIdType)) }

        return try await rawGet(
            host: .query1,
            path: "/v1/finance/screener/predefined/saved",
            queryItems: queryItems
        )
    }

    public func screener(
        body: YFJSONValue,
        lang: String = "en-US",
        region: String = "US"
    ) async throws -> YFJSONValue {
        try await rawPost(
            host: .query1,
            path: "/v1/finance/screener",
            queryItems: [
                URLQueryItem(name: "corsDomain", value: "finance.yahoo.com"),
                URLQueryItem(name: "formatted", value: "false"),
                URLQueryItem(name: "lang", value: lang),
                URLQueryItem(name: "region", value: region),
            ],
            body: body
        )
    }

    public func visualization(
        body: YFJSONValue,
        lang: String = "en-US",
        region: String = "US"
    ) async throws -> YFJSONValue {
        try await rawPost(
            host: .query1,
            path: "/v1/finance/visualization",
            queryItems: [
                URLQueryItem(name: "lang", value: lang),
                URLQueryItem(name: "region", value: region),
            ],
            body: body
        )
    }

    public func fundamentalsTimeSeries(
        symbol: String,
        types: [String],
        period1: Date,
        period2: Date
    ) async throws -> YFJSONValue {
        let cleanedTypes = types.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
        if cleanedTypes.isEmpty {
            throw YFinanceError.invalidRequest("types cannot be empty")
        }

        return try await rawGet(
            host: .query2,
            path: "/ws/fundamentals-timeseries/v1/finance/timeseries/\(normalizedSymbol(symbol))",
            queryItems: [
                URLQueryItem(name: "symbol", value: normalizedSymbol(symbol)),
                URLQueryItem(name: "type", value: cleanedTypes.joined(separator: ",")),
                URLQueryItem(name: "period1", value: String(Int(period1.timeIntervalSince1970))),
                URLQueryItem(name: "period2", value: String(Int(period2.timeIntervalSince1970))),
            ],
            requiresCrumb: false
        )
    }

    public func tickerNews(
        symbol: String,
        count: Int = 10,
        tab: String = "news"
    ) async throws -> YFJSONValue {
        let queryRefMap: [String: String] = [
            "all": "newsAll",
            "news": "latestNews",
            "press releases": "pressRelease",
        ]

        guard let queryRef = queryRefMap[tab.lowercased()] else {
            throw YFinanceError.invalidRequest("Invalid tab: \(tab)")
        }

        let payload = YFJSONValue.object([
            "serviceConfig": .object([
                "snippetCount": .number(Double(max(count, 0))),
                "s": .array([.string(normalizedSymbol(symbol))]),
            ]),
        ])

        return try await rawPost(
            host: .root,
            path: "/xhr/ncp",
            queryItems: [
                URLQueryItem(name: "queryRef", value: queryRef),
                URLQueryItem(name: "serviceKey", value: "ncp_fin"),
            ],
            body: payload,
            requiresCrumb: false
        )
    }

    public func rawGet(
        host: EndpointHost,
        path: String,
        queryItems: [URLQueryItem] = [],
        requiresCrumb: Bool = false,
        headers: [String: String] = [:],
        timeout: TimeInterval? = nil
    ) async throws -> YFJSONValue {
        let data = try await requestData(
            host: host,
            path: path,
            queryItems: queryItems,
            method: "GET",
            body: nil,
            headers: headers,
            requiresCrumb: requiresCrumb,
            timeout: timeout
        )
        return try YFJSONValue.decode(data: data)
    }

    public func rawGet(
        url: URL,
        headers: [String: String] = [:],
        timeout: TimeInterval? = nil
    ) async throws -> YFJSONValue {
        let data = try await requestData(url: url, method: "GET", body: nil, headers: headers, timeout: timeout)
        return try YFJSONValue.decode(data: data)
    }

    public func rawText(
        url: URL,
        headers: [String: String] = [:],
        timeout: TimeInterval? = nil
    ) async throws -> String {
        let data = try await requestData(url: url, method: "GET", body: nil, headers: headers, timeout: timeout)
        return String(decoding: data, as: UTF8.self)
    }

    public func rawPost(
        host: EndpointHost,
        path: String,
        queryItems: [URLQueryItem] = [],
        body: YFJSONValue? = nil,
        requiresCrumb: Bool = false,
        headers: [String: String] = [:],
        timeout: TimeInterval? = nil
    ) async throws -> YFJSONValue {
        let encodedBody = try body.map { try YFJSONValue.encode($0) }
        let data = try await requestData(
            host: host,
            path: path,
            queryItems: queryItems,
            method: "POST",
            body: encodedBody,
            headers: headers,
            requiresCrumb: requiresCrumb,
            timeout: timeout
        )
        return try YFJSONValue.decode(data: data)
    }

    private func requestJSON<T: Decodable>(
        host: EndpointHost,
        path: String,
        queryItems: [URLQueryItem],
        requiresCrumb: Bool,
        timeout: TimeInterval? = nil
    ) async throws -> T {
        let data = try await requestData(
            host: host,
            path: path,
            queryItems: queryItems,
            method: "GET",
            body: nil,
            headers: [:],
            requiresCrumb: requiresCrumb,
            timeout: timeout
        )

        do {
            return try decoder.decode(T.self, from: data)
        } catch {
            throw YFinanceError.decoding(error)
        }
    }

    private func requestData(
        host: EndpointHost,
        path: String,
        queryItems: [URLQueryItem],
        method: String,
        body: Data?,
        headers: [String: String],
        requiresCrumb: Bool,
        timeout: TimeInterval? = nil
    ) async throws -> Data {
        let baseURL = baseURL(for: host)
        let crumb = requiresCrumb ? (try await crumbStore.currentCrumb()) : nil
        let effectiveTimeout = normalizedTimeout(timeout)

        do {
            return try await executeRequest(
                baseURL: baseURL,
                path: path,
                queryItems: withCrumb(queryItems, crumb: crumb),
                method: method,
                body: body,
                headers: headers,
                timeout: effectiveTimeout
            )
        } catch {
            guard requiresCrumb else {
                throw error
            }

            await crumbStore.invalidate()
            let refreshedCrumb = try await crumbStore.currentCrumb(forceRefresh: true)
            return try await executeRequest(
                baseURL: baseURL,
                path: path,
                queryItems: withCrumb(queryItems, crumb: refreshedCrumb),
                method: method,
                body: body,
                headers: headers,
                timeout: effectiveTimeout
            )
        }
    }

    private func executeRequest(
        baseURL: URL,
        path: String,
        queryItems: [URLQueryItem],
        method: String,
        body: Data?,
        headers: [String: String],
        timeout: TimeInterval?
    ) async throws -> Data {
        let url = try buildURL(baseURL: baseURL, path: path, queryItems: queryItems)
        let debugEnabled = await YFConfigStore.shared.debug.enabled
        if debugEnabled {
            print("[YFinanceKit] \(method) \(redactedURLString(url))")
        }

        var request = URLRequest(url: url)
        request.httpMethod = method
        request.httpShouldHandleCookies = true
        request.httpBody = body
        if let timeout {
            request.timeoutInterval = timeout
        }
        request.setValue(userAgent, forHTTPHeaderField: "User-Agent")
        request.setValue("application/json,text/plain,*/*", forHTTPHeaderField: "Accept")
        request.setValue("en-US,en;q=0.9", forHTTPHeaderField: "Accept-Language")

        if body != nil {
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        for (header, value) in headers {
            request.setValue(value, forHTTPHeaderField: header)
        }

        let retries = await YFConfigStore.shared.network.retries
        var attempt = 0
        var lastError: Error?

        while attempt <= retries {
            do {
                let (data, response) = try await session.data(for: request)
                guard let httpResponse = response as? HTTPURLResponse else {
                    throw YFinanceError.missingData("Expected HTTPURLResponse")
                }
                if debugEnabled {
                    print("[YFinanceKit] \(method) \(redactedURLString(url)) -> \(httpResponse.statusCode) (\(data.count) bytes)")
                }
                try validateResponse(data: data, response: httpResponse)
                return data
            } catch {
                lastError = error
                let mappedError = mapTransport(error)
                if attempt < retries, shouldRetry(error: mappedError) {
                    attempt += 1
                    continue
                }
                throw mappedError
            }
        }

        throw (lastError.map { mapTransport($0) } ?? YFinanceError.missingData("Unknown network error"))
    }

    private func requestData(
        url: URL,
        method: String,
        body: Data?,
        headers: [String: String],
        timeout: TimeInterval? = nil
    ) async throws -> Data {
        let debugEnabled = await YFConfigStore.shared.debug.enabled
        if debugEnabled {
            print("[YFinanceKit] \(method) \(redactedURLString(url))")
        }

        var request = URLRequest(url: url)
        request.httpMethod = method
        request.httpShouldHandleCookies = true
        request.httpBody = body
        if let timeout = normalizedTimeout(timeout) {
            request.timeoutInterval = timeout
        }
        request.setValue(userAgent, forHTTPHeaderField: "User-Agent")
        request.setValue("application/json,text/plain,*/*", forHTTPHeaderField: "Accept")
        request.setValue("en-US,en;q=0.9", forHTTPHeaderField: "Accept-Language")
        if body != nil {
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        for (header, value) in headers {
            request.setValue(value, forHTTPHeaderField: header)
        }

        let retries = await YFConfigStore.shared.network.retries
        var attempt = 0
        var lastError: Error?

        while attempt <= retries {
            do {
                let (data, response) = try await session.data(for: request)
                guard let httpResponse = response as? HTTPURLResponse else {
                    throw YFinanceError.missingData("Expected HTTPURLResponse")
                }
                if debugEnabled {
                    print("[YFinanceKit] \(method) \(redactedURLString(url)) -> \(httpResponse.statusCode) (\(data.count) bytes)")
                }
                try validateResponse(data: data, response: httpResponse)
                return data
            } catch {
                lastError = error
                let mappedError = mapTransport(error)
                if attempt < retries, shouldRetry(error: mappedError) {
                    attempt += 1
                    continue
                }
                throw mappedError
            }
        }

        throw (lastError.map { mapTransport($0) } ?? YFinanceError.missingData("Unknown network error"))
    }

    private func validateResponse(data: Data, response: HTTPURLResponse) throws {
        if let envelope = try? decoder.decode(YFFinanceErrorEnvelope.self, from: data),
           let yahooError = envelope.finance?.error {
            throw YFinanceError.serverError(
                code: yahooError.code ?? "unknown",
                description: yahooError.description ?? "Unknown Yahoo error"
            )
        }

        guard (200...299).contains(response.statusCode) else {
            throw YFinanceError.httpStatus(response.statusCode)
        }
    }

    private func baseURL(for host: EndpointHost) -> URL {
        switch host {
        case .query1:
            return query1BaseURL
        case .query2:
            return query2BaseURL
        case .root:
            return rootBaseURL
        }
    }

    private func redactedURLString(_ url: URL) -> String {
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url.absoluteString
        }

        if let items = components.queryItems, !items.isEmpty {
            components.queryItems = items.map { item in
                if item.name == "crumb" {
                    return URLQueryItem(name: item.name, value: "<redacted>")
                }
                return item
            }
        }

        return components.url?.absoluteString ?? url.absoluteString
    }

    private func withCrumb(_ queryItems: [URLQueryItem], crumb: String?) -> [URLQueryItem] {
        guard let crumb else {
            return queryItems
        }
        return queryItems + [URLQueryItem(name: "crumb", value: crumb)]
    }

    private func buildURL(
        baseURL: URL,
        path: String,
        queryItems: [URLQueryItem]
    ) throws -> URL {
        guard var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else {
            throw YFinanceError.invalidURL(baseURL.absoluteString)
        }

        let cleanPath = path.hasPrefix("/") ? path : "/\(path)"
        let basePath = components.path.hasSuffix("/") ? String(components.path.dropLast()) : components.path
        components.path = basePath + cleanPath
        components.queryItems = queryItems

        guard let url = components.url else {
            throw YFinanceError.invalidURL("\(baseURL.absoluteString)\(path)")
        }
        return url
    }

    private func parseHistory(
        symbol: String,
        response: YFChartResponse,
        requestInterval: Interval,
        targetInterval: Interval,
        includePrePost: Bool,
        autoAdjust: Bool,
        backAdjust: Bool,
        repair: Bool,
        keepNa: Bool,
        rounding: Bool,
        includeEvents: Bool,
        timeout: TimeInterval?,
        reconstructionDepth: Int = 0
    ) async throws -> YFHistorySeries {
        if let chartError = response.chart.error {
            throw YFinanceError.serverError(
                code: chartError.code ?? "chart_error",
                description: chartError.description ?? "Unknown chart error"
            )
        }

        guard let chart = response.chart.result?.first else {
            throw YFinanceError.missingData("No chart result for \(symbol)")
        }

        var meta = chart.meta

        let timestamps = chart.timestamp ?? []
        let quote = chart.indicators.quote.first
        let adjustedClose = chart.indicators.adjclose?.first?.adjclose

        var bars: [YFHistoryBar] = []
        bars.reserveCapacity(timestamps.count)

        for (index, timestamp) in timestamps.enumerated() {
            let volumeValue = value(at: quote?.volume, index: index).map { Int($0) }
            bars.append(
                YFHistoryBar(
                    date: Date(timeIntervalSince1970: TimeInterval(timestamp)),
                    open: value(at: quote?.open, index: index),
                    high: value(at: quote?.high, index: index),
                    low: value(at: quote?.low, index: index),
                    close: value(at: quote?.close, index: index),
                    adjustedClose: value(at: adjustedClose, index: index),
                    volume: volumeValue
                )
            )
        }

        if requestInterval == .fifteenMinutes, targetInterval == .thirtyMinutes {
            bars = resampleToThirtyMinutes(bars)
        }

        let workingInterval: Interval = (requestInterval == .fifteenMinutes && targetInterval == .thirtyMinutes)
            ? .thirtyMinutes
            : requestInterval

        let exchangeTimeZone = resolveExchangeTimeZone(from: meta)
        var exchangeCalendar = Calendar(identifier: .gregorian)
        exchangeCalendar.timeZone = exchangeTimeZone

        bars = fixYahooDSTIssue(bars: bars, interval: workingInterval, calendar: exchangeCalendar)
        if !includePrePost {
            bars = fixYahooReturningPrepostUnrequested(
                bars: bars,
                interval: workingInterval,
                meta: meta,
                calendar: exchangeCalendar
            )
        }
        let fixed = fixYahooReturningLiveSeparate(
            bars: bars,
            interval: workingInterval,
            exchangeTimeZone: exchangeTimeZone,
            repair: repair,
            currency: meta.currency
        )
        bars = deduplicatedBars(fixed.bars)
        if let dropped = fixed.dropped {
            meta = metaWithLastTrade(
                meta,
                lastTrade: YFHistoryLastTrade(
                    price: dropped.close,
                    time: Int(dropped.date.timeIntervalSince1970)
                )
            )
        }

        var historyEvents = parseHistoryEvents(chart.events)
        let intervalIsIntraday = workingInterval.rawValue.hasSuffix("m") || workingInterval.rawValue.hasSuffix("h")
        if intervalIsIntraday, !bars.isEmpty, !historyEvents.isEmpty {
            let startDay = dayKey(for: bars.first!.date, calendar: exchangeCalendar)
            let endDay = dayKey(for: bars.last!.date, calendar: exchangeCalendar)
            historyEvents = historyEvents.filter { event in
                let day = dayKey(for: event.date, calendar: exchangeCalendar)
                return day >= startDay && day <= endDay
            }
        }

        if repair {
            let standardized = standardizeSubunitCurrencyIfNeeded(
                bars: bars,
                events: historyEvents,
                meta: meta,
                calendar: exchangeCalendar
            )
            bars = standardized.bars
            historyEvents = standardized.events
            meta = standardized.meta

            historyEvents = await convertDividendCurrenciesIfNeeded(
                events: historyEvents,
                meta: meta,
                timeout: timeout
            )

            let repaired = repairCorporateActionAdjustments(
                bars: bars,
                events: historyEvents,
                currency: meta.currency,
                calendar: exchangeCalendar
            )
            bars = repaired.bars
            historyEvents = repaired.events

            // Python yfinance repairs the latest/last interval before attempting 100x/split repairs,
            // because those algorithms depend on a sane baseline near "now".
            if reconstructionDepth < 2, bars.count >= 2 {
                let tailCount = min(50, bars.count)
                let tailStart = max(0, bars.count - tailCount)
                let tail = Array(bars[tailStart..<bars.count])

                let repairedTail = await reconstructMissingBarsIfNeeded(
                    symbol: meta.symbol ?? symbol,
                    bars: tail,
                    interval: workingInterval,
                    events: historyEvents,
                    includePrePost: includePrePost,
                    meta: meta,
                    timeout: timeout,
                    reconstructionDepth: reconstructionDepth
                )

                if repairedTail.count == tail.count {
                    bars.replaceSubrange(tailStart..<bars.count, with: repairedTail)
                }
            }

            bars = repairHundredXAnomalies(bars)

            let unitSwitchRepaired = repairUnitSwitchIfNeeded(
                bars: bars,
                events: historyEvents,
                meta: meta,
                interval: workingInterval,
                calendar: exchangeCalendar
            )
            bars = unitSwitchRepaired.bars
            historyEvents = unitSwitchRepaired.events

            let splitRepaired = repairBadStockSplitsIfNeeded(
                bars: bars,
                events: historyEvents,
                interval: workingInterval,
                calendar: exchangeCalendar
            )
            bars = splitRepaired.bars
            historyEvents = splitRepaired.events

            let unitMixupsTagged = tagSporadicUnitMixupsForReconstructionIfNeeded(bars)
            bars = unitMixupsTagged.bars

            if reconstructionDepth < 2 {
                bars = await reconstructMissingBarsIfNeeded(
                    symbol: meta.symbol ?? symbol,
                    bars: bars,
                    interval: workingInterval,
                    events: historyEvents,
                    includePrePost: includePrePost,
                    meta: meta,
                    timeout: timeout,
                    reconstructionDepth: reconstructionDepth
                )
            }

            bars = applySporadicUnitMixupFallbacksIfNeeded(bars, tags: unitMixupsTagged.tags)
        }

        if autoAdjust || backAdjust {
            bars = applyAdjustments(
                bars,
                autoAdjust: autoAdjust,
                backAdjust: backAdjust && !autoAdjust
            )
        }

        if requestInterval == .oneDay, targetInterval != .oneDay {
            bars = resampleFromDaily(bars, targetInterval: targetInterval, calendar: exchangeCalendar)
        }

        if rounding, let priceHint = chart.meta.priceHint, priceHint >= 0 {
            bars = roundBars(bars, digits: priceHint)
        }

        if !keepNa {
            bars = dropEmptyBars(bars)
        }

        return YFHistorySeries(
            symbol: meta.symbol ?? symbol,
            meta: meta,
            interval: targetInterval,
            bars: bars,
            events: includeEvents ? historyEvents : [],
            repairEnabled: repair
        )
    }

    private func normalizedSymbol(_ symbol: String) -> String {
        let cleaned = symbol.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        if cleaned.isEmpty {
            return symbol
        }
        return cleaned
    }

    private func normalizedSymbols(_ symbols: [String]) -> [String] {
        symbols
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).uppercased() }
            .filter { !$0.isEmpty }
    }

    private func value<T>(at array: [T?]?, index: Int) -> T? {
        guard let array, array.indices.contains(index) else {
            return nil
        }
        return array[index]
    }

    private func historyRequestInterval(_ targetInterval: Interval, repair: Bool) throws -> Interval {
        if targetInterval == .thirtyMinutes {
            return .fifteenMinutes
        }

        guard repair else {
            return targetInterval
        }

        switch targetInterval {
        case .fiveDays:
            return .oneDay
        case .oneWeek, .oneMonth, .threeMonths:
            return .oneDay
        default:
            return targetInterval
        }
    }

    private func resampleToThirtyMinutes(_ bars: [YFHistoryBar]) -> [YFHistoryBar] {
        struct Bucket {
            var date: Date
            var open: Double?
            var high: Double?
            var low: Double?
            var close: Double?
            var adjustedClose: Double?
            var volume: Int = 0
            var volumeSeen = false
            var repaired = false
        }

        var buckets: [Int: Bucket] = [:]
        for bar in bars {
            let timestamp = Int(bar.date.timeIntervalSince1970)
            let key = timestamp / 1800
            let bucketDate = Date(timeIntervalSince1970: TimeInterval(key * 1800))

            if var existing = buckets[key] {
                existing.repaired = existing.repaired || bar.repaired
                if existing.open == nil {
                    existing.open = bar.open
                }
                if let high = bar.high {
                    existing.high = maxOptional(existing.high, high)
                }
                if let low = bar.low {
                    existing.low = minOptional(existing.low, low)
                }
                if bar.close != nil {
                    existing.close = bar.close
                }
                if bar.adjustedClose != nil {
                    existing.adjustedClose = bar.adjustedClose
                }
                if let volume = bar.volume {
                    existing.volume += volume
                    existing.volumeSeen = true
                }
                buckets[key] = existing
            } else {
                var bucket = Bucket(
                    date: bucketDate,
                    open: bar.open,
                    high: bar.high,
                    low: bar.low,
                    close: bar.close,
                    adjustedClose: bar.adjustedClose,
                    repaired: bar.repaired
                )
                if let volume = bar.volume {
                    bucket.volume = volume
                    bucket.volumeSeen = true
                }
                buckets[key] = bucket
            }
        }

        return buckets
            .keys
            .sorted()
            .map { key in
                let bucket = buckets[key]!
                return YFHistoryBar(
                    date: bucket.date,
                    open: bucket.open,
                    high: bucket.high,
                    low: bucket.low,
                    close: bucket.close,
                    adjustedClose: bucket.adjustedClose,
                    volume: bucket.volumeSeen ? bucket.volume : nil,
                    repaired: bucket.repaired
                )
            }
    }

    private func resampleFromDaily(_ bars: [YFHistoryBar], targetInterval: Interval, calendar: Calendar) -> [YFHistoryBar] {
        guard !bars.isEmpty else {
            return bars
        }
        guard [.fiveDays, .oneWeek, .oneMonth, .threeMonths].contains(targetInterval) else {
            return bars
        }

        let sortedBars = bars.sorted { $0.date < $1.date }
        let anchorStart = calendar.startOfDay(for: sortedBars[0].date)

        struct Bucket {
            var date: Date
            var open: Double?
            var high: Double?
            var low: Double?
            var close: Double?
            var adjustedClose: Double?
            var volume: Int = 0
            var volumeSeen = false
            var repaired = false
        }

        var buckets: [Date: Bucket] = [:]
        for bar in sortedBars {
            let key = periodBucketStart(
                for: bar.date,
                targetInterval: targetInterval,
                anchor: anchorStart,
                calendar: calendar
            )

            if var existing = buckets[key] {
                existing.repaired = existing.repaired || bar.repaired
                if existing.open == nil {
                    existing.open = bar.open
                }
                if let high = bar.high {
                    existing.high = maxOptional(existing.high, high)
                }
                if let low = bar.low {
                    existing.low = minOptional(existing.low, low)
                }
                if bar.close != nil {
                    existing.close = bar.close
                }
                if bar.adjustedClose != nil {
                    existing.adjustedClose = bar.adjustedClose
                }
                if let volume = bar.volume {
                    existing.volume += volume
                    existing.volumeSeen = true
                }
                buckets[key] = existing
            } else {
                var bucket = Bucket(
                    date: key,
                    open: bar.open,
                    high: bar.high,
                    low: bar.low,
                    close: bar.close,
                    adjustedClose: bar.adjustedClose,
                    repaired: bar.repaired
                )
                if let volume = bar.volume {
                    bucket.volume = volume
                    bucket.volumeSeen = true
                }
                buckets[key] = bucket
            }
        }

        return buckets.keys.sorted().map { key in
            let bucket = buckets[key]!
            return YFHistoryBar(
                date: bucket.date,
                open: bucket.open,
                high: bucket.high,
                low: bucket.low,
                close: bucket.close,
                adjustedClose: bucket.adjustedClose,
                volume: bucket.volumeSeen ? bucket.volume : nil,
                repaired: bucket.repaired
            )
        }
    }

    private func periodBucketStart(
        for date: Date,
        targetInterval: Interval,
        anchor: Date,
        calendar: Calendar
    ) -> Date {
        let dayStart = calendar.startOfDay(for: date)
        switch targetInterval {
        case .fiveDays:
            let offsetDays = calendar.dateComponents([.day], from: anchor, to: dayStart).day ?? 0
            let block = max(0, offsetDays / 5)
            return calendar.date(byAdding: .day, value: block * 5, to: anchor) ?? dayStart
        case .oneWeek:
            return mondayStart(for: dayStart, calendar: calendar)
        case .oneMonth:
            var comps = calendar.dateComponents([.year, .month], from: dayStart)
            comps.day = 1
            return calendar.date(from: comps) ?? dayStart
        case .threeMonths:
            var comps = calendar.dateComponents([.year, .month], from: dayStart)
            let month = comps.month ?? 1
            comps.month = ((month - 1) / 3) * 3 + 1
            comps.day = 1
            return calendar.date(from: comps) ?? dayStart
        default:
            return dayStart
        }
    }

    private func mondayStart(for date: Date, calendar: Calendar) -> Date {
        let dayStart = calendar.startOfDay(for: date)
        let weekday = calendar.component(.weekday, from: dayStart)
        let daysFromMonday = (weekday + 5) % 7
        return calendar.date(byAdding: .day, value: -daysFromMonday, to: dayStart) ?? dayStart
    }

    private func anchoredWeekStart(for date: Date, calendar: Calendar, weekday: Int) -> Date {
        let dayStart = calendar.startOfDay(for: date)
        let currentWeekday = calendar.component(.weekday, from: dayStart)

        // Swift weekday uses 1=Sunday...7=Saturday.
        let anchorWeekday = ((weekday - 1) % 7) + 1
        var delta = currentWeekday - anchorWeekday
        if delta < 0 {
            delta += 7
        }

        return calendar.date(byAdding: .day, value: -delta, to: dayStart) ?? dayStart
    }

    private func utcCalendar() -> Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0) ?? .current
        return calendar
    }

    private func applyAdjustments(
        _ bars: [YFHistoryBar],
        autoAdjust: Bool,
        backAdjust: Bool
    ) -> [YFHistoryBar] {
        guard !bars.isEmpty else {
            return bars
        }

        // Match Python yfinance semantics:
        // - auto_adjust: Open/High/Low/Close are scaled by AdjClose/Close and "Adj Close" is dropped.
        // - back_adjust: Open/High/Low are scaled by AdjClose/Close, Close stays unadjusted, and "Adj Close" is dropped.
        return bars.map { bar in
            let ratio: Double = {
                guard let adj = sanitizePrice(bar.adjustedClose),
                      let close = sanitizePrice(bar.close),
                      close != 0 else {
                    return 1
                }
                return adj / close
            }()

            if autoAdjust {
                return YFHistoryBar(
                    date: bar.date,
                    open: bar.open.map { $0 * ratio },
                    high: bar.high.map { $0 * ratio },
                    low: bar.low.map { $0 * ratio },
                    close: bar.close.map { $0 * ratio },
                    adjustedClose: nil,
                    volume: bar.volume,
                    repaired: bar.repaired
                )
            }

            if backAdjust {
                return YFHistoryBar(
                    date: bar.date,
                    open: bar.open.map { $0 * ratio },
                    high: bar.high.map { $0 * ratio },
                    low: bar.low.map { $0 * ratio },
                    close: bar.close,
                    adjustedClose: nil,
                    volume: bar.volume,
                    repaired: bar.repaired
                )
            }

            return bar
        }
    }

    private func roundBars(_ bars: [YFHistoryBar], digits: Int) -> [YFHistoryBar] {
        bars.map { bar in
            YFHistoryBar(
                date: bar.date,
                open: rounded(bar.open, digits: digits),
                high: rounded(bar.high, digits: digits),
                low: rounded(bar.low, digits: digits),
                close: rounded(bar.close, digits: digits),
                adjustedClose: rounded(bar.adjustedClose, digits: digits),
                volume: bar.volume,
                repaired: bar.repaired
            )
        }
    }

    private func dropEmptyBars(_ bars: [YFHistoryBar]) -> [YFHistoryBar] {
        bars.filter { bar in
            let prices = [bar.open, bar.high, bar.low, bar.close, bar.adjustedClose]
            let hasPrice = prices.contains { value in
                guard let value else { return false }
                return value != 0
            }
            let hasVolume = (bar.volume ?? 0) != 0
            return hasPrice || hasVolume
        }
    }

    private func resolveExchangeTimeZone(from meta: YFHistoryMeta) -> TimeZone {
        if let exchangeTimezoneName = meta.exchangeTimezoneName,
           let tz = TimeZone(identifier: exchangeTimezoneName) {
            return tz
        }
        if let timezone = meta.timezone,
           let tz = TimeZone(identifier: timezone) {
            return tz
        }
        if let offset = meta.gmtoffset,
           let tz = TimeZone(secondsFromGMT: offset) {
            return tz
        }
        return TimeZone(secondsFromGMT: 0) ?? .current
    }

    private func deduplicatedBars(_ bars: [YFHistoryBar]) -> [YFHistoryBar] {
        let sortedBars = bars.sorted { $0.date < $1.date }
        var seen: Set<Int> = []
        var output: [YFHistoryBar] = []
        output.reserveCapacity(sortedBars.count)

        for bar in sortedBars {
            let key = Int(bar.date.timeIntervalSince1970)
            if seen.contains(key) {
                continue
            }
            seen.insert(key)
            output.append(bar)
        }

        return output
    }

    private func fixYahooDSTIssue(
        bars: [YFHistoryBar],
        interval: Interval,
        calendar: Calendar
    ) -> [YFHistoryBar] {
        // Python yfinance fix_Yahoo_dst_issue. Only applies to daily/weekly style bars.
        guard interval == .oneDay || interval == .oneWeek else {
            return bars
        }

        return bars.map { bar in
            let comps = calendar.dateComponents([.hour, .minute], from: bar.date)
            guard comps.minute == 0,
                  let hour = comps.hour,
                  hour == 22 || hour == 23 else {
                return bar
            }

            let hoursToAdd = 24 - hour
            let adjustedDate = bar.date.addingTimeInterval(TimeInterval(hoursToAdd * 60 * 60))
            return YFHistoryBar(
                date: adjustedDate,
                open: bar.open,
                high: bar.high,
                low: bar.low,
                close: bar.close,
                adjustedClose: bar.adjustedClose,
                volume: bar.volume,
                repaired: bar.repaired
            )
        }
    }

    private func fixYahooReturningPrepostUnrequested(
        bars: [YFHistoryBar],
        interval: Interval,
        meta: YFHistoryMeta,
        calendar: Calendar
    ) -> [YFHistoryBar] {
        // Python yfinance fix_Yahoo_returning_prepost_unrequested.
        let isIntraday = interval.rawValue.hasSuffix("m") || interval.rawValue.hasSuffix("h")
        guard isIntraday else {
            return bars
        }

        guard let tradingPeriods = meta.tradingPeriods,
              !tradingPeriods.regular.isEmpty else {
            return bars
        }

        var sessionByDay: [Int: (start: Date, end: Date)] = [:]
        sessionByDay.reserveCapacity(tradingPeriods.regular.count)

        for period in tradingPeriods.regular {
            guard let startEpoch = period.start,
                  let endEpoch = period.end else {
                continue
            }
            let start = Date(timeIntervalSince1970: TimeInterval(startEpoch))
            let end = Date(timeIntervalSince1970: TimeInterval(endEpoch))
            let day = dayKey(for: start, calendar: calendar)

            if let existing = sessionByDay[day] {
                let mergedStart = min(existing.start, start)
                let mergedEnd = max(existing.end, end)
                sessionByDay[day] = (start: mergedStart, end: mergedEnd)
            } else {
                sessionByDay[day] = (start: start, end: end)
            }
        }

        guard !sessionByDay.isEmpty else {
            return bars
        }

        let td = estimatedIntervalSeconds(for: interval)
        return bars.filter { bar in
            let day = dayKey(for: bar.date, calendar: calendar)
            guard let session = sessionByDay[day] else {
                return true
            }

            if bar.date >= session.end {
                return false
            }
            if bar.date.addingTimeInterval(td) <= session.start {
                return false
            }
            return true
        }
    }

    private func fixYahooReturningLiveSeparate(
        bars: [YFHistoryBar],
        interval: Interval,
        exchangeTimeZone: TimeZone,
        repair: Bool,
        currency: String?
    ) -> (bars: [YFHistoryBar], dropped: YFHistoryBar?) {
        guard bars.count > 1 else {
            return (bars, nil)
        }

        let sortedBars = bars.sorted { $0.date < $1.date }
        let last = sortedBars[sortedBars.count - 1]
        let prev = sortedBars[sortedBars.count - 2]

        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = exchangeTimeZone

        if interval == .oneDay {
            if isSameExchangeDay(prev.date, last.date, calendar: calendar) {
                var output = sortedBars
                output.remove(at: output.count - 2)
                return (output, prev)
            }
            return (sortedBars, nil)
        }

        guard [.fiveDays, .oneWeek, .oneMonth, .threeMonths].contains(interval) else {
            return (sortedBars, nil)
        }

        if dtsInSameInterval(
            start: prev.date,
            candidate: last.date,
            interval: interval,
            calendar: calendar
        ) {
            if Int(prev.date.timeIntervalSince1970) == Int(last.date.timeIntervalSince1970) {
                return (sortedBars, nil)
            }

            var base = prev
            if repair {
                let currencyDivide: Double = (currency == "KWF") ? 1000 : 100
                var ratios: [Double] = []
                ratios.reserveCapacity(4)

                let pairs: [(Double?, Double?)] = [
                    (last.open, base.open),
                    (last.high, base.high),
                    (last.low, base.low),
                    (last.close, base.close),
                ]
                for (newRaw, oldRaw) in pairs {
                    guard let newValue = sanitizePrice(newRaw),
                          let oldValue = sanitizePrice(oldRaw),
                          oldValue > 0 else {
                        continue
                    }
                    ratios.append(newValue / oldValue)
                }

                if ratios.count >= 2 {
                    let newerIs100x = ratios.allSatisfy { ratio in
                        abs((ratio / currencyDivide) - 1) < 0.05
                    }
                    let newerIs0_01x = ratios.allSatisfy { ratio in
                        abs((ratio * currencyDivide) - 1) < 0.05
                    }

                    if newerIs100x {
                        base = scaleBar(base, factor: currencyDivide)
                    } else if newerIs0_01x {
                        base = scaleBar(base, factor: 1 / currencyDivide)
                    }
                }
            }

            let merged = normalizeOHLC(mergeLiveBar(into: base, live: last))
            var output = sortedBars
            output[output.count - 2] = merged
            output.removeLast()
            return (output, last)
        }

        return (sortedBars, nil)
    }

    private func isSameExchangeDay(_ lhs: Date, _ rhs: Date, calendar: Calendar) -> Bool {
        let l = calendar.dateComponents([.year, .month, .day], from: lhs)
        let r = calendar.dateComponents([.year, .month, .day], from: rhs)
        return l.year == r.year && l.month == r.month && l.day == r.day
    }

    private func dtsInSameInterval(
        start: Date,
        candidate: Date,
        interval: Interval,
        calendar: Calendar
    ) -> Bool {
        let startDay = calendar.startOfDay(for: start)
        let candidateDay = calendar.startOfDay(for: candidate)

        switch interval {
        case .oneDay:
            return isSameExchangeDay(startDay, candidateDay, calendar: calendar)
        case .fiveDays:
            let days = calendar.dateComponents([.day], from: startDay, to: candidateDay).day ?? 0
            return days < 5
        case .oneWeek:
            let days = calendar.dateComponents([.day], from: startDay, to: candidateDay).day ?? 0
            return days < 7
        case .oneMonth:
            let c1 = calendar.dateComponents([.year, .month], from: startDay)
            let c2 = calendar.dateComponents([.year, .month], from: candidateDay)
            return c1.year == c2.year && c1.month == c2.month
        case .threeMonths:
            let y1 = calendar.component(.year, from: startDay)
            let y2 = calendar.component(.year, from: candidateDay)
            let m1 = calendar.component(.month, from: startDay)
            let m2 = calendar.component(.month, from: candidateDay)
            let shift = (m1 % 3) - 1
            let q1 = (m1 - shift - 1) / 3 + 1
            let q2 = (m2 - shift - 1) / 3 + 1
            let quarterDiff = (q2 - q1) + 4 * (y2 - y1)
            return quarterDiff == 0
        default:
            return false
        }
    }

    private func mergeLiveBar(into base: YFHistoryBar, live: YFHistoryBar) -> YFHistoryBar {
        let volumeSeen = (base.volume != nil) || (live.volume != nil)
        let mergedVolume = (base.volume ?? 0) + (live.volume ?? 0)

        let open = base.open ?? live.open
        var high = base.high
        if let liveHigh = live.high {
            high = maxOptional(high, liveHigh)
        }
        var low = base.low
        if let liveLow = live.low {
            low = minOptional(low, liveLow)
        }
        let close = live.close ?? base.close
        let adjustedClose = live.adjustedClose ?? base.adjustedClose

        return YFHistoryBar(
            date: base.date,
            open: open,
            high: high,
            low: low,
            close: close,
            adjustedClose: adjustedClose,
            volume: volumeSeen ? mergedVolume : nil,
            repaired: base.repaired || live.repaired
        )
    }

    private func historyForReconstruction(
        symbol: String,
        start: Date,
        end: Date,
        interval: Interval,
        includePrePost: Bool,
        events: Set<HistoryEvent>,
        timeout: TimeInterval?,
        reconstructionDepth: Int
    ) async throws -> YFHistorySeries {
        let cleanedSymbol = normalizedSymbol(symbol)
        let targetInterval = interval
        let requestInterval = try historyRequestInterval(targetInterval, repair: true)

        let response: YFChartResponse = try await requestJSON(
            host: .query2,
            path: "/v8/finance/chart/\(cleanedSymbol)",
            queryItems: [
                URLQueryItem(name: "period1", value: String(Int(start.timeIntervalSince1970))),
                URLQueryItem(name: "period2", value: String(Int(end.timeIntervalSince1970))),
                URLQueryItem(name: "interval", value: requestInterval.rawValue),
                URLQueryItem(name: "includePrePost", value: includePrePost ? "true" : "false"),
                URLQueryItem(name: "events", value: events.map(\.rawValue).sorted().joined(separator: ",")),
            ],
            requiresCrumb: false,
            timeout: timeout
        )

        return try await parseHistory(
            symbol: cleanedSymbol,
            response: response,
            requestInterval: requestInterval,
            targetInterval: targetInterval,
            includePrePost: includePrePost,
            autoAdjust: false,
            backAdjust: false,
            repair: true,
            keepNa: true,
            rounding: false,
            includeEvents: false,
            timeout: timeout,
            reconstructionDepth: reconstructionDepth
        )
    }

    private func reconstructMissingBarsIfNeeded(
        symbol: String,
        bars: [YFHistoryBar],
        interval: Interval,
        events: [YFHistoryEvent],
        includePrePost: Bool,
        meta: YFHistoryMeta,
        timeout: TimeInterval?,
        reconstructionDepth: Int
    ) async -> [YFHistoryBar] {
        guard let subInterval = reconstructionSubInterval(for: interval) else {
            return bars
        }
        guard !bars.isEmpty else {
            return bars
        }

        let sortedBars = bars.sorted { $0.date < $1.date }

        let exchangeTimeZone = resolveExchangeTimeZone(from: meta)
        var exchangeCalendar = Calendar(identifier: .gregorian)
        exchangeCalendar.timeZone = exchangeTimeZone
        let weekStartWeekday: Int? = (interval == .oneWeek)
            ? exchangeCalendar.component(.weekday, from: exchangeCalendar.startOfDay(for: sortedBars[0].date))
            : nil

        let splitDays: Set<Int> = Set(
            events.compactMap { event in
                guard event.kind == .split else {
                    return nil
                }
                return dayKey(for: event.date, calendar: exchangeCalendar)
            }
        )

        // Python yfinance _fix_zeroes() ignores intraday days where >50% of rows have NaN/zero prices.
        // Without this, a single bad day can trigger huge reconstruction attempts.
        let intervalIsIntraday = interval.rawValue.hasSuffix("m") || interval.rawValue.hasSuffix("h")
        var ignoredDayKeys: Set<Int> = []
        if intervalIsIntraday {
            struct DayCounts {
                var bad: Int = 0
                var total: Int = 0
            }

            var countsByDay: [Int: DayCounts] = [:]
            countsByDay.reserveCapacity(min(32, sortedBars.count / 20))

            for bar in sortedBars {
                let day = dayKey(for: bar.date, calendar: exchangeCalendar)
                var counts = countsByDay[day] ?? DayCounts()
                counts.total += 1
                let priceBad = sanitizePrice(bar.open) == nil
                    || sanitizePrice(bar.high) == nil
                    || sanitizePrice(bar.low) == nil
                    || sanitizePrice(bar.close) == nil
                if priceBad {
                    counts.bad += 1
                }
                countsByDay[day] = counts
            }

            for (day, counts) in countsByDay {
                guard counts.total > 0 else { continue }
                let pctBad = Double(counts.bad) / Double(counts.total)
                if pctBad > 0.5 {
                    ignoredDayKeys.insert(day)
                }
            }
        }

        let minDate: Date? = reconstructionLookbackSeconds(forSubInterval: subInterval).map {
            Date().addingTimeInterval(-$0)
        }

        func isGoodCalibrationBar(_ bar: YFHistoryBar) -> Bool {
            sanitizePrice(bar.open) != nil
                && sanitizePrice(bar.high) != nil
                && sanitizePrice(bar.low) != nil
                && sanitizePrice(bar.close) != nil
        }

        // Map coarse bars to reconstruction bucket keys so reconstruction can calibrate fine-grained prices.
        // Prefer a good-quality representative bar for calibration when available.
        var coarseBarByKey: [Int: YFHistoryBar] = [:]
        coarseBarByKey.reserveCapacity(min(512, sortedBars.count))
        for bar in sortedBars {
            let key = reconstructionBucketKey(
                for: bar.date,
                interval: interval,
                calendar: exchangeCalendar,
                weekStartWeekday: weekStartWeekday
            )
            if coarseBarByKey[key] == nil || (!isGoodCalibrationBar(coarseBarByKey[key]!) && isGoodCalibrationBar(bar)) {
                coarseBarByKey[key] = bar
            }
        }

        var indicesByKey: [Int: [Int]] = [:]
        var badBarCount = 0

        for index in sortedBars.indices {
            let previous = index > 0 ? sortedBars[index - 1] : nil
            if intervalIsIntraday {
                let day = dayKey(for: sortedBars[index].date, calendar: exchangeCalendar)
                if ignoredDayKeys.contains(day) {
                    continue
                }
            }
            guard barNeedsReconstruction(
                sortedBars[index],
                previous: previous,
                interval: interval,
                symbol: symbol,
                splitDays: splitDays,
                calendar: exchangeCalendar
            ) else {
                continue
            }

            if let minDate, sortedBars[index].date < minDate {
                continue
            }

            let key = reconstructionBucketKey(
                for: sortedBars[index].date,
                interval: interval,
                calendar: exchangeCalendar,
                weekStartWeekday: weekStartWeekday
            )
            indicesByKey[key, default: []].append(index)
            badBarCount += 1
        }

        let badKeys = indicesByKey.keys.sorted()
        guard !badKeys.isEmpty else {
            return sortedBars
        }

        // Need at least some good data to calibrate; if everything is bad, don't attempt reconstruction.
        if badBarCount >= sortedBars.count {
            return sortedBars
        }

        // Avoid extremely large reconstruction requests.
        if badKeys.count > 250 {
            return sortedBars
        }

        let groups = groupKeys(
            badKeys,
            maxSpanSeconds: reconstructionGroupSpanSeconds(forSubInterval: subInterval)
        )

        let ratios = adjustedCloseRatios(for: sortedBars)
        var repaired = sortedBars

        for group in groups {
            guard let firstKey = group.first, let lastKey = group.last else {
                continue
            }

            let buffer: TimeInterval = 24 * 60 * 60
            let fetchStart = Date(timeIntervalSince1970: TimeInterval(firstKey)).addingTimeInterval(-buffer)
            let fetchEnd = Date(timeIntervalSince1970: TimeInterval(lastKey))
                .addingTimeInterval(estimatedIntervalSeconds(for: interval) + buffer)

            let finePrePost = reconstructionIncludePrePost(
                targetInterval: interval,
                includePrePost: includePrePost
            )

            let fineSeries: YFHistorySeries
            do {
                fineSeries = try await historyForReconstruction(
                    symbol: symbol,
                    start: fetchStart,
                    end: fetchEnd,
                    interval: subInterval,
                    includePrePost: finePrePost,
                    events: [.dividends, .splits, .capitalGains],
                    timeout: timeout,
                    reconstructionDepth: reconstructionDepth + 1
                )
            } catch {
                continue
            }

            let fineBuckets = groupFineBarsByBucketKey(
                fineSeries.bars,
                targetInterval: interval,
                calendar: exchangeCalendar,
                weekStartWeekday: weekStartWeekday
            )

            // Calibrate fine-grained aggregates to match the split-adjustment/currency scaling of coarse bars.
            // Port of Python yfinance _reconstruct_intervals_batch() calibration step.
            var priceScale: Double = 1
            var volumeScale: Double = 1
            do {
                var ratioSamples: [Double] = []
                ratioSamples.reserveCapacity(min(64, fineBuckets.count))

                for (key, fineBars) in fineBuckets {
                    guard let coarse = coarseBarByKey[key],
                          isGoodCalibrationBar(coarse) else {
                        continue
                    }
                    let fineAgg = aggregateBars(fineBars)
                    guard let coarseClose = sanitizePrice(coarse.close),
                          let fineClose = sanitizePrice(fineAgg.close),
                          fineClose > 0 else {
                        continue
                    }
                    let ratio = coarseClose / fineClose
                    guard ratio.isFinite, ratio > 0 else { continue }
                    ratioSamples.append(ratio)
                    if ratioSamples.count >= 80 {
                        break
                    }
                }

                if ratioSamples.count >= 2,
                   let ratio = median(ratioSamples),
                   ratio.isFinite,
                   ratio > 0 {
                    let ratioRounded = (ratio * 10).rounded() / 10
                    let ratioRcpRounded = ((1 / ratio) * 10).rounded() / 10
                    if !(ratioRounded == 1 && ratioRcpRounded == 1) {
                        if ratioRounded > 1 {
                            priceScale = ratioRounded
                            volumeScale = 1 / ratioRounded
                        } else if ratioRcpRounded > 1 {
                            priceScale = 1 / ratioRcpRounded
                            volumeScale = ratioRcpRounded
                        }
                    }
                }
            }

            func scaledAggregate(_ aggregate: YFAggregatedBar) -> YFAggregatedBar {
                let volume: Int? = aggregate.volume.flatMap { value in
                    let scaled = Double(value) * volumeScale
                    guard scaled.isFinite, scaled >= 0 else { return value }
                    return Int(scaled.rounded())
                }
                return YFAggregatedBar(
                    open: aggregate.open.map { $0 * priceScale },
                    high: aggregate.high.map { $0 * priceScale },
                    low: aggregate.low.map { $0 * priceScale },
                    close: aggregate.close.map { $0 * priceScale },
                    volume: volume
                )
            }

            for key in group {
                guard let indices = indicesByKey[key] else {
                    continue
                }
                guard let fineBars = fineBuckets[key], !fineBars.isEmpty else {
                    continue
                }

                let aggregate = scaledAggregate(aggregateBars(fineBars))

                for index in indices {
                    let original = repaired[index]

                    let originalOpen = sanitizePrice(original.open)
                    let originalHigh = sanitizePrice(original.high)
                    let originalLow = sanitizePrice(original.low)
                    let originalClose = sanitizePrice(original.close)
                    let originalAdjustedClose = sanitizePrice(original.adjustedClose)

                    var open = originalOpen
                    var high = originalHigh
                    var low = originalLow
                    var close = originalClose
                    var adjustedClose = originalAdjustedClose

                    if open == nil { open = sanitizePrice(aggregate.open) }
                    if high == nil { high = sanitizePrice(aggregate.high) }
                    if low == nil { low = sanitizePrice(aggregate.low) }
                    if close == nil { close = sanitizePrice(aggregate.close) }

                    let volumeSeen = (original.volume != nil) || (aggregate.volume != nil)
                    let originalVolume = original.volume ?? 0
                    let volume: Int?
                    if !volumeSeen {
                        volume = nil
                    } else if originalVolume > 0 {
                        volume = originalVolume
                    } else if let reconstructed = aggregate.volume {
                        volume = reconstructed
                    } else {
                        volume = original.volume
                    }

                    if open == nil {
                        open = close
                    }

                    if adjustedClose == nil, let close,
                       let ratio = nearestAdjustedCloseRatio(in: ratios, around: index) {
                        adjustedClose = close * ratio
                    }

                    let didRepair = (open != originalOpen)
                        || (high != originalHigh)
                        || (low != originalLow)
                        || (close != originalClose)
                        || (adjustedClose != originalAdjustedClose)
                        || (volume != original.volume)

                    let updated = normalizeOHLC(
                        YFHistoryBar(
                            date: original.date,
                            open: open,
                            high: high,
                            low: low,
                            close: close,
                            adjustedClose: adjustedClose,
                            volume: volume,
                            repaired: original.repaired || didRepair
                        )
                    )

                    repaired[index] = updated
                }
            }
        }

        return repaired
    }

    private func reconstructionSubInterval(for interval: Interval) -> Interval? {
        switch interval {
        case .oneWeek:
            return .oneDay
        case .oneDay:
            return .oneHour
        case .oneHour, .sixtyMinutes, .ninetyMinutes:
            return .thirtyMinutes
        case .thirtyMinutes:
            return .fifteenMinutes
        case .fifteenMinutes:
            return .fiveMinutes
        case .fiveMinutes:
            return .twoMinutes
        case .twoMinutes:
            return .oneMinute
        default:
            return nil
        }
    }

    private func reconstructionLookbackSeconds(forSubInterval interval: Interval) -> TimeInterval? {
        switch interval {
        case .oneHour:
            return TimeInterval(730 * 24 * 60 * 60)
        case .thirtyMinutes, .fifteenMinutes, .fiveMinutes, .twoMinutes:
            return TimeInterval(60 * 24 * 60 * 60)
        case .oneMinute:
            return TimeInterval(30 * 24 * 60 * 60)
        default:
            return nil
        }
    }

    private func reconstructionGroupSpanSeconds(forSubInterval interval: Interval) -> TimeInterval {
        switch interval {
        case .oneDay:
            return TimeInterval(2 * 365 * 24 * 60 * 60)
        case .oneHour:
            return TimeInterval(365 * 24 * 60 * 60)
        case .thirtyMinutes, .fifteenMinutes, .fiveMinutes, .twoMinutes:
            return TimeInterval(30 * 24 * 60 * 60)
        case .oneMinute:
            return TimeInterval(5 * 24 * 60 * 60)
        default:
            return TimeInterval(30 * 24 * 60 * 60)
        }
    }

    private func estimatedIntervalSeconds(for interval: Interval) -> TimeInterval {
        switch interval {
        case .oneMinute:
            return 60
        case .twoMinutes:
            return 2 * 60
        case .fiveMinutes:
            return 5 * 60
        case .fifteenMinutes:
            return 15 * 60
        case .thirtyMinutes:
            return 30 * 60
        case .sixtyMinutes, .oneHour:
            return 60 * 60
        case .ninetyMinutes:
            return 90 * 60
        case .oneDay:
            return 24 * 60 * 60
        case .oneWeek:
            return 7 * 24 * 60 * 60
        case .fiveDays:
            return 5 * 24 * 60 * 60
        case .oneMonth:
            return 31 * 24 * 60 * 60
        case .threeMonths:
            return 93 * 24 * 60 * 60
        }
    }

    private func reconstructionIncludePrePost(targetInterval: Interval, includePrePost: Bool) -> Bool {
        switch targetInterval {
        case .oneWeek, .oneDay, .fiveDays, .oneMonth, .threeMonths:
            // Python yfinance treats interday data as always including pre/post.
            return true
        default:
            return includePrePost
        }
    }

    private func groupKeys(_ keys: [Int], maxSpanSeconds: TimeInterval) -> [[Int]] {
        guard !keys.isEmpty else { return [] }
        var groups: [[Int]] = [[keys[0]]]
        for key in keys.dropFirst() {
            guard let groupStart = groups.last?.first else {
                groups.append([key])
                continue
            }
            let span = TimeInterval(key - groupStart)
            if span <= maxSpanSeconds {
                groups[groups.count - 1].append(key)
            } else {
                groups.append([key])
            }
        }
        return groups
    }

    private func reconstructionBucketKey(
        for date: Date,
        interval: Interval,
        calendar: Calendar,
        weekStartWeekday: Int?
    ) -> Int {
        switch interval {
        case .oneDay:
            let start = calendar.startOfDay(for: date)
            return Int(start.timeIntervalSince1970)
        case .oneWeek:
            let dayStart = calendar.startOfDay(for: date)
            let weekStart: Date
            if let weekStartWeekday {
                weekStart = anchoredWeekStart(for: dayStart, calendar: calendar, weekday: weekStartWeekday)
            } else {
                weekStart = mondayStart(for: dayStart, calendar: calendar)
            }
            return Int(weekStart.timeIntervalSince1970)
        case .oneHour, .sixtyMinutes:
            let seconds = 60 * 60
            let ts = Int(date.timeIntervalSince1970)
            return (ts / seconds) * seconds
        case .ninetyMinutes:
            let seconds = 90 * 60
            let ts = Int(date.timeIntervalSince1970)
            return (ts / seconds) * seconds
        case .thirtyMinutes:
            let seconds = 30 * 60
            let ts = Int(date.timeIntervalSince1970)
            return (ts / seconds) * seconds
        case .fifteenMinutes:
            let seconds = 15 * 60
            let ts = Int(date.timeIntervalSince1970)
            return (ts / seconds) * seconds
        case .fiveMinutes:
            let seconds = 5 * 60
            let ts = Int(date.timeIntervalSince1970)
            return (ts / seconds) * seconds
        case .twoMinutes:
            let seconds = 2 * 60
            let ts = Int(date.timeIntervalSince1970)
            return (ts / seconds) * seconds
        case .oneMinute:
            let seconds = 60
            let ts = Int(date.timeIntervalSince1970)
            return (ts / seconds) * seconds
        default:
            return Int(date.timeIntervalSince1970)
        }
    }

    private func barNeedsReconstruction(
        _ bar: YFHistoryBar,
        previous: YFHistoryBar?,
        interval: Interval,
        symbol: String,
        splitDays: Set<Int>,
        calendar: Calendar
    ) -> Bool {
        let open = sanitizePrice(bar.open)
        let high = sanitizePrice(bar.high)
        let low = sanitizePrice(bar.low)
        let close = sanitizePrice(bar.close)

        let priceBad = (open == nil) || (high == nil) || (low == nil) || (close == nil)
        let splitDay = dayKey(for: bar.date, calendar: calendar)
        let splitExpectedButMissing = splitDays.contains(splitDay)
            && high != nil
            && low != nil
            && high == low

        var volumeBad = false
        if !symbol.hasSuffix("=X") {
            let volume = bar.volume ?? 0
            if volume == 0 {
                if let high, let low, high != low {
                    volumeBad = true
                } else if interval.rawValue.hasSuffix("m") || interval.rawValue.hasSuffix("h") {
                    volumeBad = false
                } else if let previous,
                          let prevClose = sanitizePrice(previous.close),
                          let close,
                          prevClose > 0 {
                    let pct = abs(close - prevClose) / prevClose
                    if pct > 0.05 {
                        volumeBad = true
                    }
                }
            }
        }

        return priceBad || splitExpectedButMissing || volumeBad
    }

    private func adjustedCloseRatios(for bars: [YFHistoryBar]) -> [Double?] {
        bars.map { bar in
            guard let close = sanitizePrice(bar.close),
                  let adj = sanitizePrice(bar.adjustedClose),
                  close > 0 else {
                return nil
            }
            return adj / close
        }
    }

    private func nearestAdjustedCloseRatio(in ratios: [Double?], around index: Int) -> Double? {
        if ratios.indices.contains(index), let direct = ratios[index] {
            return direct
        }

        for offset in 1...10 {
            let left = index - offset
            if ratios.indices.contains(left), let value = ratios[left] {
                return value
            }
            let right = index + offset
            if ratios.indices.contains(right), let value = ratios[right] {
                return value
            }
        }

        return ratios.compactMap { $0 }.first
    }

    private func groupFineBarsByBucketKey(
        _ bars: [YFHistoryBar],
        targetInterval: Interval,
        calendar: Calendar,
        weekStartWeekday: Int?
    ) -> [Int: [YFHistoryBar]] {
        guard !bars.isEmpty else { return [:] }

        var output: [Int: [YFHistoryBar]] = [:]
        output.reserveCapacity(min(bars.count, 64))

        for bar in bars {
            let key = reconstructionBucketKey(
                for: bar.date,
                interval: targetInterval,
                calendar: calendar,
                weekStartWeekday: weekStartWeekday
            )
            output[key, default: []].append(bar)
        }

        let keys = Array(output.keys)
        for key in keys {
            output[key] = (output[key] ?? []).sorted { $0.date < $1.date }
        }

        return output
    }

    private struct YFAggregatedBar: Sendable {
        let open: Double?
        let high: Double?
        let low: Double?
        let close: Double?
        let volume: Int?
    }

    private func aggregateBars(_ bars: [YFHistoryBar]) -> YFAggregatedBar {
        let sorted = bars.sorted { $0.date < $1.date }

        let open = sorted.compactMap { sanitizePrice($0.open) }.first
        let close = sorted.reversed().compactMap { sanitizePrice($0.close) }.first
        let highs = sorted.compactMap { sanitizePrice($0.high) }
        let lows = sorted.compactMap { sanitizePrice($0.low) }

        let high = highs.max()
        let low = lows.min()

        let volumes = sorted.compactMap { $0.volume }
        let volume = volumes.isEmpty ? nil : volumes.reduce(0, +)

        return YFAggregatedBar(open: open, high: high, low: low, close: close, volume: volume)
    }

    private func parseHistoryEvents(_ events: [String: [String: YFChartEventData]]?) -> [YFHistoryEvent] {
        guard let events else {
            return []
        }

        var output: [YFHistoryEvent] = []

        for (eventType, payload) in events {
            let kind: YFHistoryEventKind?
            switch eventType {
            case "dividends":
                kind = .dividend
            case "splits":
                kind = .split
            case "capitalGains":
                kind = .capitalGain
            default:
                kind = nil
            }
            guard let kind else { continue }

            for (timestampKey, event) in payload {
                let fallbackTimestamp = Int(timestampKey)
                guard let timestamp = event.date ?? fallbackTimestamp else {
                    continue
                }
                let date = Date(timeIntervalSince1970: TimeInterval(timestamp))

                let ratio: Double?
                if kind == .split {
                    if let numerator = event.numerator, let denominator = event.denominator, denominator != 0 {
                        ratio = numerator / denominator
                    } else if let splitRatio = event.splitRatio {
                        let components = splitRatio.split(separator: "/")
                        if components.count == 2,
                           let lhs = Double(components[0]),
                           let rhs = Double(components[1]),
                           rhs != 0 {
                            ratio = lhs / rhs
                        } else {
                            ratio = nil
                        }
                    } else {
                        ratio = nil
                    }
                } else {
                    ratio = nil
                }

                var rawObject: [String: YFJSONValue] = [
                    "date": .number(Double(timestamp)),
                ]
                if let amount = event.amount {
                    rawObject["amount"] = .number(amount)
                }
                if let currency = event.currency,
                   !currency.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    rawObject["currency"] = .string(currency)
                }
                if let numerator = event.numerator {
                    rawObject["numerator"] = .number(numerator)
                }
                if let denominator = event.denominator {
                    rawObject["denominator"] = .number(denominator)
                }
                if let splitRatio = event.splitRatio {
                    rawObject["splitRatio"] = .string(splitRatio)
                }

                output.append(
                    YFHistoryEvent(
                        kind: kind,
                        date: date,
                        value: event.amount,
                        ratio: ratio,
                        raw: .object(rawObject)
                    )
                )
            }
        }

        return output.sorted { $0.date < $1.date }
    }

    private func standardizeSubunitCurrencyIfNeeded(
        bars: [YFHistoryBar],
        events: [YFHistoryEvent],
        meta: YFHistoryMeta,
        calendar: Calendar
    ) -> (bars: [YFHistoryBar], events: [YFHistoryEvent], meta: YFHistoryMeta) {
        guard !bars.isEmpty else {
            return (bars, events, meta)
        }

        guard let currency = meta.currency else {
            return (bars, events, meta)
        }

        let targetCurrency: String
        let factor: Double
        switch currency {
        case "GBp":
            targetCurrency = "GBP"
            factor = 0.01
        case "ZAc":
            targetCurrency = "ZAR"
            factor = 0.01
        case "ILA":
            targetCurrency = "ILS"
            factor = 0.01
        default:
            return (bars, events, meta)
        }

        // Use latest row with actual volume, because volume=0 rows can be on the wrong scale.
        guard let lastIndex = bars.indices.reversed().first(where: { (bars[$0].volume ?? 0) > 0 }),
              let lastClose = sanitizePrice(bars[lastIndex].close),
              lastClose > 0 else {
            return (bars, events, meta)
        }

        var pricesInSubunits = true
        if let regularMarketPrice = sanitizePrice(meta.regularMarketPrice),
           regularMarketPrice > 0,
           bars[lastIndex].date > Date().addingTimeInterval(-30 * 24 * 60 * 60) {
            let ratio = regularMarketPrice / lastClose
            if abs((ratio * factor) - 1) < 0.1 {
                // Within 10% of 100x, assume prices are already in the major currency.
                pricesInSubunits = false
            }
        }

        let scaledBars = pricesInSubunits ? bars.map { scaleBar($0, factor: factor) } : bars

        // Some exchanges return dividends in the same subunits as prices. Use the same heuristic
        // as yfinance: if average dividend yield is ridiculous after scaling, scale dividends too.
        var scaledEvents = events
        if pricesInSubunits {
            var barIndexByDay: [Int: Int] = [:]
            for (index, bar) in scaledBars.enumerated() {
                let day = dayKey(for: bar.date, calendar: calendar)
                if barIndexByDay[day] == nil {
                    barIndexByDay[day] = index
                }
            }

            var yields: [Double] = []
            yields.reserveCapacity(events.count)

            var dividendEventIndices: [Int] = []
            dividendEventIndices.reserveCapacity(events.count)

            for (eventIndex, event) in scaledEvents.enumerated() where event.kind == .dividend {
                guard let amount = sanitizePrice(event.value), amount > 0 else {
                    continue
                }
                let day = dayKey(for: event.date, calendar: calendar)
                guard let barIndex = barIndexByDay[day], barIndex > 0,
                      let prevClose = sanitizePrice(scaledBars[barIndex - 1].close),
                      prevClose > 0 else {
                    continue
                }
                yields.append(amount / prevClose)
                dividendEventIndices.append(eventIndex)
            }

            if !yields.isEmpty {
                let averageYield = yields.reduce(0, +) / Double(yields.count)
                if averageYield > 1 {
                    for index in dividendEventIndices {
                        guard let amount = sanitizePrice(scaledEvents[index].value) else {
                            continue
                        }
                        scaledEvents[index] = replacingAmount(in: scaledEvents[index], value: amount * factor)
                    }
                }
            }
        }

        let updatedMeta = metaWithCurrency(meta, currency: targetCurrency, scale: pricesInSubunits ? factor : nil)
        return (scaledBars, scaledEvents, updatedMeta)
    }

    private func convertDividendCurrenciesIfNeeded(
        events: [YFHistoryEvent],
        meta: YFHistoryMeta,
        timeout: TimeInterval?
    ) async -> [YFHistoryEvent] {
        guard let priceCurrency = meta.currency?.trimmingCharacters(in: .whitespacesAndNewlines),
              !priceCurrency.isEmpty else {
            return events
        }

        var indicesByCurrency: [String: [Int]] = [:]
        for (index, event) in events.enumerated() where event.kind == .dividend {
            guard let rawCurrency = event.raw.objectValue?["currency"]?.stringValue?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !rawCurrency.isEmpty,
                  rawCurrency != priceCurrency else {
                continue
            }
            indicesByCurrency[rawCurrency, default: []].append(index)
        }

        guard !indicesByCurrency.isEmpty else {
            return events
        }

        func latestFXClose(_ symbol: String) async throws -> Double? {
            let series = try await history(
                symbol: symbol,
                range: .oneMonth,
                interval: .oneDay,
                includePrePost: false,
                events: [],
                autoAdjust: false,
                backAdjust: false,
                repair: false,
                keepNa: true,
                rounding: false,
                timeout: timeout
            )
            return series.bars.reversed().compactMap { sanitizePrice($0.close) }.first
        }

        let majorCurrencies: Set<String> = ["USD", "JPY", "EUR", "CNY", "GBP", "CAD"]

        var repaired = events
        for (divCurrency, eventIndices) in indicesByCurrency {
            var fxTicker: String
            var reverse: Bool
            var fx2Ticker: String?

            if divCurrency == "USD" {
                fxTicker = "\(priceCurrency)=X"
                reverse = false
            } else if priceCurrency == "USD" {
                // Python yfinance attempts to handle this case but uses USD=X which always returns 1.0.
                // Use the direct USD->currency quote and invert it.
                fxTicker = "\(divCurrency)=X"
                reverse = true
            } else if majorCurrencies.contains(divCurrency), majorCurrencies.contains(priceCurrency) {
                fxTicker = "\(divCurrency)\(priceCurrency)=X"
                reverse = false
            } else {
                fxTicker = "\(divCurrency)=X"
                reverse = true
                fx2Ticker = "\(priceCurrency)=X"
            }

            let fxClose: Double?
            do {
                fxClose = try await latestFXClose(fxTicker)
            } catch {
                continue
            }
            guard var fxRate = fxClose, fxRate.isFinite, fxRate > 0 else {
                continue
            }
            if reverse {
                fxRate = 1 / fxRate
            }

            if let fx2Ticker {
                let fx2Close: Double?
                do {
                    fx2Close = try await latestFXClose(fx2Ticker)
                } catch {
                    continue
                }
                if let fx2Rate = fx2Close, fx2Rate.isFinite, fx2Rate > 0 {
                    fxRate *= fx2Rate
                }
            }

            for eventIndex in eventIndices {
                guard repaired.indices.contains(eventIndex),
                      let amount = sanitizePrice(repaired[eventIndex].value) else {
                    continue
                }
                let convertedAmount = amount * fxRate

                var updated = replacingAmount(in: repaired[eventIndex], value: convertedAmount)
                var rawObject = updated.raw.objectValue ?? [:]
                rawObject["currency"] = .string(priceCurrency)
                updated = YFHistoryEvent(
                    kind: updated.kind,
                    date: updated.date,
                    value: updated.value,
                    ratio: updated.ratio,
                    raw: .object(rawObject)
                )
                repaired[eventIndex] = updated
            }
        }

        return repaired
    }

    private func repairUnitSwitchIfNeeded(
        bars: [YFHistoryBar],
        events: [YFHistoryEvent],
        meta: YFHistoryMeta,
        interval: Interval,
        calendar: Calendar
    ) -> (bars: [YFHistoryBar], events: [YFHistoryEvent]) {
        guard bars.count >= 30 else {
            return (bars, events)
        }

        let n: Double = (meta.currency == "KWF") ? 1000 : 100
        let window = max(5, min(20, bars.count / 10))

        struct Candidate {
            let breakIndex: Int
            let ratio: Double
            let distance: Double
        }

        var best: Candidate?
        for i in window..<(bars.count - window) {
            let before = bars[(i - window)..<i].compactMap { sanitizePrice($0.close) }
            let after = bars[i..<(i + window)].compactMap { sanitizePrice($0.close) }
            guard before.count >= 3,
                  after.count >= 3,
                  let beforeMedian = median(before),
                  let afterMedian = median(after),
                  beforeMedian > 0,
                  afterMedian > 0 else {
                continue
            }

            let ratio = afterMedian / beforeMedian
            let dist = min(relativeDistance(ratio, n), relativeDistance(ratio, 1 / n))
            guard dist < 0.15 else {
                continue
            }

            if let currentBest = best {
                if dist < currentBest.distance {
                    best = Candidate(breakIndex: i, ratio: ratio, distance: dist)
                }
            } else {
                best = Candidate(breakIndex: i, ratio: ratio, distance: dist)
            }
        }

        guard let candidate = best,
              candidate.ratio.isFinite,
              candidate.ratio > 0 else {
            // Fallback to the more general sudden-change repair (Python: _fix_unit_switch -> _fix_prices_sudden_change).
            let sudden = repairPricesSuddenChangeIfNeeded(
                bars: bars,
                events: events,
                interval: interval,
                calendar: calendar,
                change: n,
                correctVolume: false,
                correctDividend: true
            )
            if sudden.didRepair {
                return (sudden.bars, sudden.events)
            }
            return (bars, events)
        }

        let lastClose = bars.last.flatMap { sanitizePrice($0.close) }
        let marketPrice = sanitizePrice(meta.regularMarketPrice)
        var scaleSuffix = false
        if let marketPrice, let lastClose, lastClose > 0 {
            let ratio = marketPrice / lastClose
            if relativeDistance(ratio, n) < 0.2 || relativeDistance(ratio, 1 / n) < 0.2 {
                scaleSuffix = true
            }
        }

        let priceFactor = scaleSuffix ? (1 / candidate.ratio) : candidate.ratio
        guard priceFactor.isFinite,
              priceFactor > 0,
              relativeDistance(priceFactor, 1) > 0.05 else {
            return (bars, events)
        }

        var repairedBars = bars
        if scaleSuffix {
            for index in candidate.breakIndex..<repairedBars.count {
                repairedBars[index] = markRepaired(normalizeOHLC(scaleBar(repairedBars[index], factor: priceFactor)))
            }
        } else {
            for index in 0..<candidate.breakIndex {
                repairedBars[index] = markRepaired(normalizeOHLC(scaleBar(repairedBars[index], factor: priceFactor)))
            }
        }

        // Python yfinance corrects dividends along with unit-switch repairs.
        let breakDate = repairedBars[candidate.breakIndex].date
        var repairedEvents = events
        for index in repairedEvents.indices {
            let event = repairedEvents[index]
            guard event.kind == .dividend else {
                continue
            }
            let shouldScale = scaleSuffix ? (event.date >= breakDate) : (event.date < breakDate)
            guard shouldScale,
                  let amount = sanitizePrice(event.value) else {
                continue
            }
            repairedEvents[index] = replacingAmount(in: event, value: amount * priceFactor)
        }

        return (repairedBars, repairedEvents)
    }

    private func repairBadStockSplitsIfNeeded(
        bars: [YFHistoryBar],
        events: [YFHistoryEvent],
        interval: Interval,
        calendar: Calendar
    ) -> (bars: [YFHistoryBar], events: [YFHistoryEvent]) {
        guard !bars.isEmpty else {
            return (bars, events)
        }
        guard [.oneDay, .oneWeek, .oneMonth, .threeMonths].contains(interval) else {
            return (bars, events)
        }

        var splitRatioByDay: [Int: Double] = [:]
        for event in events where event.kind == .split {
            guard let ratio = sanitizePrice(event.ratio), ratio > 0 else {
                continue
            }
            let day = dayKey(for: event.date, calendar: calendar)
            splitRatioByDay[day, default: 1] *= ratio
        }
        guard !splitRatioByDay.isEmpty else {
            return (bars, events)
        }

        var barIndexByDay: [Int: Int] = [:]
        for (index, bar) in bars.enumerated() {
            let day = dayKey(for: bar.date, calendar: calendar)
            if barIndexByDay[day] == nil {
                barIndexByDay[day] = index
            }
        }

        // Port of Python yfinance _fix_bad_stock_splits(): for each split event,
        // run sudden-change repair on a limited prefix window and merge back in.
        var repairedBars = bars
        var repairedEvents = events

        let daysAfterSplit = ([.oneWeek, .oneMonth, .threeMonths].contains(interval)) ? 1 : 5

        for day in splitRatioByDay.keys.sorted() {
            guard let splitRatio = splitRatioByDay[day],
                  splitRatio > 0 else {
                continue
            }

            // Ignore tiny split ratios that could be confused with normal volatility.
            if splitRatio > 0.8 && splitRatio < 1.25 {
                continue
            }

            guard let splitIndex = barIndexByDay[day],
                  splitIndex > 0 else {
                continue
            }

            let cutoffIndex = min(repairedBars.count - 1, splitIndex + daysAfterSplit)
            let prefix = Array(repairedBars[0...cutoffIndex])

            let repairedPrefix = repairPricesSuddenChangeIfNeeded(
                bars: prefix,
                events: repairedEvents,
                interval: interval,
                calendar: calendar,
                change: splitRatio,
                correctVolume: true,
                correctDividend: true
            )

            guard repairedPrefix.didRepair else {
                continue
            }

            var merged: [YFHistoryBar] = []
            merged.reserveCapacity(repairedBars.count)
            merged.append(contentsOf: repairedPrefix.bars)
            if cutoffIndex + 1 < repairedBars.count {
                merged.append(contentsOf: repairedBars[(cutoffIndex + 1)..<repairedBars.count])
            }

            repairedBars = merged
            repairedEvents = repairedPrefix.events
        }

        return (repairedBars, repairedEvents)
    }

    private func repairPricesSuddenChangeIfNeeded(
        bars: [YFHistoryBar],
        events: [YFHistoryEvent],
        interval: Interval,
        calendar: Calendar,
        change: Double,
        correctVolume: Bool,
        correctDividend: Bool
    ) -> (bars: [YFHistoryBar], events: [YFHistoryEvent], didRepair: Bool) {
        // Focuses on Python yfinance _fix_prices_sudden_change(...) behavior, including
        // practical false-positive suppression for volume spikes, suspension-like rows,
        // and local-volatility confirmation.
        guard bars.count >= 2 else {
            return (bars, events, false)
        }
        guard change.isFinite, change > 0 else {
            return (bars, events, false)
        }

        // Do not attempt repair when change is too close to 1.0 (indistinguishable from normal volatility).
        if change > 0.8 && change < 1.25 {
            return (bars, events, false)
        }

        let split = change
        let splitRcp = 1.0 / split
        let splitMax = max(split, splitRcp)
        let isIntraday = interval.rawValue.hasSuffix("m") || interval.rawValue.hasSuffix("h")
        let isInterday = !isIntraday

        let descBars = bars.sorted { $0.date > $1.date }
        let n = descBars.count

        // Use adjusted prices when available to reduce dividend-volatility false positives.
        var adjustedPrices: [Double] = Array(repeating: 1.0, count: n)
        for i in 0..<n {
            if let adj = sanitizePrice(descBars[i].adjustedClose) {
                adjustedPrices[i] = adj
            } else if let close = sanitizePrice(descBars[i].close) {
                adjustedPrices[i] = close
            } else {
                adjustedPrices[i] = 1.0
            }
        }

        let correctColumnsIndividually = isInterday
            && interval != .oneDay
            && change != 100.0
            && change != 0.01
            && change != 0.001

        let priceAccessors: [(YFHistoryBar) -> Double?] = if correctColumnsIndividually {
            [
                { [self] in self.sanitizePrice($0.open) },
                { [self] in self.sanitizePrice($0.close) }
            ]
        } else {
            [
                { [self] in self.sanitizePrice($0.open) },
                { [self] in self.sanitizePrice($0.high) },
                { [self] in self.sanitizePrice($0.low) },
                { [self] in self.sanitizePrice($0.close) }
            ]
        }

        let columnCount = priceAccessors.count
        var oneStepChangeByColumn = Array(repeating: Array(repeating: 1.0, count: n), count: columnCount)

        func priceScaleFactor(at index: Int) -> Double {
            guard index < adjustedPrices.count, index < n else {
                return 1.0
            }
            let base = adjustedPrices[index]
            let close = sanitizePrice(descBars[index].close)
            guard let close, close > 0 else {
                return 1.0
            }
            guard base > 0 else {
                return 1.0
            }
            return base / close
        }

        for col in 0..<columnCount {
            for i in 1..<n {
                let accessor = priceAccessors[col]
                guard let current = accessor(descBars[i]).map({ $0 * priceScaleFactor(at: i) }),
                      let previous = accessor(descBars[i - 1]).map({ $0 * priceScaleFactor(at: i - 1) }),
                      previous > 0,
                      current > 0 else {
                    continue
                }

                let ratio = current / previous
                oneStepChangeByColumn[col][i] = (ratio.isFinite && ratio > 0) ? ratio : 1.0
            }
        }

        var oneStepChange: [Double] = Array(repeating: 1.0, count: n)
        if n > 1 {
            for i in 1..<n {
                let row = oneStepChangeByColumn.map { $0[i] }.filter { $0.isFinite && $0 > 0 }
                if correctColumnsIndividually {
                    oneStepChange[i] = row.isEmpty ? 1.0 : row.reduce(0, +) / Double(row.count)
                } else if let med = median(row) {
                    oneStepChange[i] = med
                } else {
                    oneStepChange[i] = 1.0
                }
            }
        }

        let noActivity = descBars.map { bar -> Bool in
            let allPricesMissing = sanitizePrice(bar.open) == nil
                && sanitizePrice(bar.high) == nil
                && sanitizePrice(bar.low) == nil
                && sanitizePrice(bar.close) == nil
            let hasZeroVolume = (bar.volume ?? 0) == 0
            return allPricesMissing || hasZeroVolume
        }
        let appearsSuspended = noActivity.first == true

        // If all changes are far from split ratio, exit early.
        let splitWindowThreshold = (splitMax - 1) * 0.5 + 1
        if let maxChange = oneStepChange.max(),
           let minChange = oneStepChange.min(),
           maxChange < splitWindowThreshold && minChange > (1.0 / splitWindowThreshold) {
            return (bars, events, false)
        }

        // Estimate typical 1-step volatility using IQR-filtered ratios.
        let ratioSamples = Array(oneStepChange.dropFirst()).filter { $0.isFinite && $0 > 0 }
        guard ratioSamples.count >= 4,
              let q1 = percentile(ratioSamples, 25),
              let q3 = percentile(ratioSamples, 75) else {
            return (bars, events, false)
        }

        let iqr = q3 - q1
        let lowerBound = q1 - 1.5 * iqr
        let upperBound = q3 + 1.5 * iqr
        let filtered = ratioSamples.filter { $0 >= lowerBound && $0 <= upperBound }
        guard !filtered.isEmpty else {
            return (bars, events, false)
        }

        let avg = filtered.reduce(0, +) / Double(filtered.count)
        let sd = standardDeviation(filtered, mean: avg)
        let sdPct = (avg != 0) ? (sd / avg) : 0

        // Only proceed if split ratio far exceeds typical volatility.
        var largestChangePct = 5 * sdPct
        if isInterday && interval != .oneDay {
            largestChangePct *= 3
            if interval == .oneMonth || interval == .threeMonths {
                largestChangePct *= 2
            }
        }
        if splitMax < 1.0 + largestChangePct {
            return (bars, events, false)
        }

        let threshold = (splitMax + 1.0 + largestChangePct) * 0.5
        guard threshold.isFinite, threshold > 1 else {
            return (bars, events, false)
        }

        var fDown = Array(repeating: false, count: n)
        var fUp = Array(repeating: false, count: n)
        var fDownByCol: [[Bool]] = []
        var fUpByCol: [[Bool]] = []

        if correctColumnsIndividually {
            fDownByCol = Array(repeating: Array(repeating: false, count: n), count: columnCount)
            fUpByCol = Array(repeating: Array(repeating: false, count: n), count: columnCount)

            for col in 0..<columnCount {
                for i in 1..<n {
                    let r = oneStepChangeByColumn[col][i]
                    if r < (1.0 / threshold) { fDownByCol[col][i] = true }
                    if r > threshold { fUpByCol[col][i] = true }
                }
            }

            for i in 0..<n {
                let hasDown = fDownByCol.contains(where: { $0[i] })
                let hasUp = fUpByCol.contains(where: { $0[i] })
                fDown[i] = hasDown
                fUp[i] = hasUp
            }
        } else if n > 1 {
            for i in 1..<n {
                let r = oneStepChange[i]
                if r < (1.0 / threshold) { fDown[i] = true }
                if r > threshold { fUp[i] = true }
            }
        }
        var f = zip(fDown, fUp).map { $0 || $1 }

        // Skip 100x/0.01x repairs if a detected signal is too soon after a real split (suspicious false-positive).
        if (change == 100.0 || change == 0.01), f.contains(true) {
            let splitDates = events.compactMap { event -> Date? in
                guard event.kind == .split else { return nil }
                return event.date
            }
            if !splitDates.isEmpty {
                let maxSignalGap: TimeInterval = 30 * 24 * 60 * 60
                var hasNearSplitSignal = false
                for idx in 0..<n where f[idx] {
                    let dt = descBars[idx].date
                    for splitDate in splitDates where splitDate <= dt {
                        if dt.timeIntervalSince(splitDate) <= maxSignalGap {
                            hasNearSplitSignal = true
                            break
                        }
                    }
                    if hasNearSplitSignal { break }
                }
                if hasNearSplitSignal {
                    return (bars, events, false)
                }
            }
        }
        if f[0] {
            f[0] = false
            fDown[0] = false
            fUp[0] = false
            if correctColumnsIndividually {
                for col in 0..<columnCount {
                    fDownByCol[col][0] = false
                    fUpByCol[col][0] = false
                }
            }
        }

        if appearsSuspended {
            for i in 0..<n {
                if !f[i] {
                    continue
                }
                if noActivity[i] {
                    f[i] = false
                    fDown[i] = false
                    fUp[i] = false
                    if correctColumnsIndividually {
                        for col in 0..<columnCount {
                            fDownByCol[col][i] = false
                            fUpByCol[col][i] = false
                        }
                    }
                }
            }
            // If everything is tied to suspended rows, abort.
            if !f.contains(true) {
                return (bars, events, false)
            }
        }

        func zScore(of value: Double, in values: [Double]) -> Double? {
            guard values.count >= 2 else { return nil }
            let mean = values.reduce(0, +) / Double(values.count)
            let variance = values.reduce(0.0) { sum, val in
                let delta = val - mean
                return sum + delta * delta
            } / Double(max(1, values.count - 1))
            let std = sqrt(max(0.0, variance))
            guard std > 0 else { return nil }
            return (value - mean) / std
        }

        func windowedValues<T>(_ start: Int, _ end: Int, _ keyPath: (YFHistoryBar) -> T?) -> [T] {
            guard start <= end else { return [] }
            let lower = max(0, start)
            let upper = min(end, n - 1)
            guard lower <= upper else { return [] }
            return (lower...upper).compactMap { index in
                keyPath(descBars[index])
            }
        }

        // Suppress large single-point spikes likely tied to short-lived volume events.
        if fDown.contains(true) || fUp.contains(true) {
            for idx in 1..<n {
                if !f[idx] { continue }
                if fUp[idx] {
                    let pivot = idx - 1
                    guard let volume = descBars[pivot].volume, volume > 0 else { continue }
                    let start = pivot - 15
                    let end = min(n - 1, pivot + 15)
                    let neighbors = windowedValues(start, end, \.volume).compactMap { raw in
                        Double(raw)
                    }
                    guard let score = zScore(of: Double(volume), in: neighbors), score.isFinite else {
                        continue
                    }
                    if score > 2.5 {
                        f[idx] = false
                        fDown[idx] = false
                        fUp[idx] = false
                        if correctColumnsIndividually {
                            for col in 0..<columnCount {
                                fDownByCol[col][idx] = false
                                fUpByCol[col][idx] = false
                            }
                        }
                    }
                }
            }
        }

        // Confirm local volatility context so we don't repair normal large price moves.
        for idx in 1..<n where f[idx] {
            let lookback = isInterday ? 10 : (isIntraday ? 100 : 3)
            let start = max(0, idx - lookback)
            let end = min(n, idx + 2)

            if correctColumnsIndividually {
                for col in 0..<columnCount {
                    let local = Array(oneStepChangeByColumn[col][start..<end]).filter { $0.isFinite && $0 > 0 }
                    guard !local.isEmpty else { continue }
                    let localAvg = local.reduce(0, +) / Double(local.count)
                    let localSd = standardDeviation(local, mean: localAvg)
                    guard localAvg != 0 else { continue }
                    var localLargest = 5 * (localSd / localAvg)
                    if isInterday && interval != .oneDay {
                        localLargest *= 3
                        if interval == .oneMonth || interval == .threeMonths {
                            localLargest *= 2
                        }
                    }
                    let localThreshold = (splitMax + 1.0 + localLargest) * 0.5
                    let signalRatio = oneStepChangeByColumn[col][idx]
                    if localThreshold > 1.0 && signalRatio < localThreshold && signalRatio > (1.0 / localThreshold) {
                        fDownByCol[col][idx] = false
                        fUpByCol[col][idx] = false
                    }
                }
        } else {
            let local = Array(oneStepChange[start..<end])
            let localClean = local.filter { $0.isFinite && $0 > 0 }
            guard !localClean.isEmpty else { continue }
            let localAvg = localClean.reduce(0, +) / Double(localClean.count)
            let localSd = standardDeviation(localClean, mean: localAvg)
                guard localAvg != 0 else { continue }
                var localLargest = 5 * (localSd / localAvg)
                if isInterday && interval != .oneDay {
                    localLargest *= 3
                    if interval == .oneMonth || interval == .threeMonths {
                        localLargest *= 2
                    }
                }
                let localThreshold = (splitMax + 1.0 + localLargest) * 0.5
                let signalRatio = oneStepChange[idx]
                if localThreshold > 1.0 && signalRatio < localThreshold && signalRatio > (1.0 / localThreshold) {
                    f[idx] = false
                    fDown[idx] = false
                    fUp[idx] = false
                }
            }
        }

        if correctColumnsIndividually {
            for i in 0..<n {
                let hasDown = fDownByCol.contains { $0[i] }
                let hasUp = fUpByCol.contains { $0[i] }
                fDown[i] = hasDown
                fUp[i] = hasUp
                f[i] = hasDown || hasUp
            }
        }

        if !f.contains(true) {
            return (bars, events, false)
        }

        struct RangeAdjustment {
            let start: Int
            let end: Int
            let useSplit: Bool
        }

        let idxFirstF = f.firstIndex(where: { $0 }) ?? 0

        func mapSignalsToRanges(_ signal: [Bool], _ up: [Bool], _ down: [Bool], offset: Int = 0) -> [RangeAdjustment] {
            guard signal.count == up.count,
                  signal.count == down.count,
                  !signal.isEmpty else {
                return []
            }

            var fSignal = signal
            var fUp = up
            var fDown = down
            if fSignal.first == true {
                fSignal[0] = false
                fUp[0] = false
                fDown[0] = false
            }

            let signalIndices = fSignal.indices.filter { fSignal[$0] }
            guard !signalIndices.isEmpty else {
                return []
            }

            var ranges: [RangeAdjustment] = []
            ranges.reserveCapacity((signalIndices.count + 1) / 2)

            var i = 0
            while i + 1 < signalIndices.count {
                let idx = signalIndices[i]
                let next = signalIndices[i + 1]
                let useSplit = split > 1.0 ? fDown[idx] : !fDown[idx]
                ranges.append(RangeAdjustment(start: idx + offset, end: next + offset, useSplit: useSplit))
                i += 2
            }

            if signalIndices.count % 2 == 1, let last = signalIndices.last {
                let useSplit = split > 1.0 ? fDown[last] : !fDown[last]
                ranges.append(RangeAdjustment(start: last + offset, end: min(offset + signal.count, n), useSplit: useSplit))
            }

            return ranges
        }

        func rollAndReverse(_ values: [Bool]) -> [Bool] {
            guard !values.isEmpty else { return [] }
            var shifted = Array(values.dropFirst())
            shifted.append(values[0])
            return Array(shifted.reversed())
        }

        let idxLatestActive: Int? = {
            if noActivity.allSatisfy({ $0 }) { return nil }
            for i in 0..<n where !noActivity[i] {
                if !noActivity[(i + n - 1) % n] {
                    return i
                }
            }
            return (0..<n).first(where: { !noActivity[$0] })
        }()

        let idxRevLatestActive = idxLatestActive.map { n - 1 - $0 }
        let doSuspensionSplit = appearsSuspended
            && idxLatestActive != nil
            && idxLatestActive! >= idxFirstF

        func splitRangesBySuspension(
            _ fSignal: [Bool],
            _ fUp: [Bool],
            _ fDown: [Bool],
            idxActive: Int,
            idxRevActive: Int
        ) -> [RangeAdjustment] {
            guard !fSignal.isEmpty else { return [] }

            let beforeSignal = Array(fSignal[idxActive..<n])
            let beforeUp = Array(fUp[idxActive..<n])
            let beforeDown = Array(fDown[idxActive..<n])
            var ranges = mapSignalsToRanges(beforeSignal, beforeUp, beforeDown, offset: idxActive)

            let revDown = rollAndReverse(fDown)
            let revUp = rollAndReverse(fUp)
            let revF = zip(revDown, revUp).map { $0 || $1 }
            let afterSignal = Array(revF[idxRevActive..<n])
            let afterUp = Array(revUp[idxRevActive..<n])
            let afterDown = Array(revDown[idxRevActive..<n])
            let afterRanges = mapSignalsToRanges(afterSignal, afterUp, afterDown, offset: idxRevActive)
            if !afterRanges.isEmpty {
                for r in afterRanges {
                    ranges.append(RangeAdjustment(start: n - r.end, end: n - r.start, useSplit: r.useSplit))
                }
            }

            return ranges
        }

        if correctColumnsIndividually {
            var rangesByColumn: [[RangeAdjustment]?] = Array(repeating: nil, count: columnCount)

            for col in 0..<columnCount {
                let fCol = zip(fDownByCol[col], fUpByCol[col]).map { $0 || $1 }
                let ranges: [RangeAdjustment]
                if doSuspensionSplit,
                   let idxActive = idxLatestActive,
                   let idxRevActive = idxRevLatestActive {
                    ranges = splitRangesBySuspension(
                        fCol,
                        fUpByCol[col],
                        fDownByCol[col],
                        idxActive: idxActive,
                        idxRevActive: idxRevActive
                    )
                } else {
                    ranges = mapSignalsToRanges(fCol, fUpByCol[col], fDownByCol[col])
                }
                if !ranges.isEmpty {
                    rangesByColumn[col] = ranges
                }
            }

            if rangesByColumn.filter({ $0 != nil && !$0!.isEmpty }).count < 2 {
                return (bars, events, false)
            }

            func scaleOneColumn(_ bar: YFHistoryBar, column: Int, factor: Double) -> YFHistoryBar {
                var open = bar.open
                let high = bar.high
                let low = bar.low
                var close = bar.close
                var adjustedClose = bar.adjustedClose

                switch column {
                case 0:
                    open = open.map { $0 * factor }
                case 1:
                    close = close.map { $0 * factor }
                    adjustedClose = adjustedClose.map { $0 * factor }
                default:
                    break
                }

                return YFHistoryBar(
                    date: bar.date,
                    open: open,
                    high: high,
                    low: low,
                    close: close,
                    adjustedClose: adjustedClose,
                    volume: bar.volume,
                    repaired: bar.repaired
                )
            }

            var repairedDescBars = descBars
            var didRepair = false
            var openVolumeScale: [Double?] = Array(repeating: nil, count: n)
            var closeVolumeScale: [Double?] = Array(repeating: nil, count: n)

            for col in 0..<columnCount {
                guard let ranges = rangesByColumn[col] else { continue }
                for range in ranges {
                    let m = range.useSplit ? split : splitRcp
                    let mRcp = range.useSplit ? splitRcp : split
                    guard m.isFinite, m > 0 else { continue }

                    for idx in range.start..<range.end {
                        guard repairedDescBars.indices.contains(idx) else { continue }
                        repairedDescBars[idx] = markRepaired(normalizeOHLC(scaleOneColumn(repairedDescBars[idx], column: col, factor: m)))
                        didRepair = true
                        if correctVolume {
                            if col == 0 {
                                openVolumeScale[idx] = mRcp
                            } else if col == 1 {
                                closeVolumeScale[idx] = mRcp
                            }
                        }
                    }
                }
            }

            guard didRepair else {
                return (bars, events, false)
            }

            if correctVolume {
                for idx in 0..<n {
                    let openFactor = openVolumeScale[idx]
                    let closeFactor = closeVolumeScale[idx]
                    let effectiveVolumeFactor: Double?

                    if let openScale = openFactor, let closeScale = closeFactor {
                        effectiveVolumeFactor = (openScale == closeScale) ? openScale : (openScale + closeScale) * 0.5
                    } else if let openScale = openFactor {
                        effectiveVolumeFactor = 0.5 * openScale
                    } else if let closeScale = closeFactor {
                        effectiveVolumeFactor = 0.5 * closeScale
                    } else {
                        effectiveVolumeFactor = nil
                    }

                    if let factor = effectiveVolumeFactor, factor.isFinite, factor > 0 {
                        repairedDescBars[idx] = scaleBarVolume(repairedDescBars[idx], factor: factor)
                    }
                }
            }

            return (repairedDescBars.sorted { $0.date < $1.date }, events, true)
        }

        var ranges = mapSignalsToRanges(f, fUp, fDown)
        if doSuspensionSplit, let idxActive = idxLatestActive, let idxRevActive = idxRevLatestActive {
            ranges = splitRangesBySuspension(f, fUp, fDown, idxActive: idxActive, idxRevActive: idxRevActive)
        }

        guard !ranges.isEmpty else {
            return (bars, events, false)
        }

        // For split repairs (non-100x), prune ranges that are too old: 1y before oldest split in this window.
        if change != 100.0 && change != 0.01 {
            let newest = descBars.first?.date
            let oldest = descBars.last?.date
            if let newest, let oldest {
                let splitDates = events.compactMap { event -> Date? in
                    guard event.kind == .split else { return nil }
                    guard event.date >= oldest && event.date <= newest else { return nil }
                    return event.date
                }
                if let minSplit = splitDates.min(),
                   let startMin = calendar.date(byAdding: .year, value: -1, to: minSplit) {
                    ranges = ranges.filter { range in
                        guard descBars.indices.contains(range.start) else { return false }
                        return descBars[range.start].date >= startMin
                    }
                }
            }
        }

        guard !ranges.isEmpty else {
            return (bars, events, false)
        }

        var barDayKeys: [Int] = []
        barDayKeys.reserveCapacity(n)
        for bar in descBars {
            barDayKeys.append(dayKey(for: bar.date, calendar: calendar))
        }

        var scaleByDay: [Int: Double] = [:]
        scaleByDay.reserveCapacity(n)

        var repairedDescBars = descBars
        var didRepair = false

        for range in ranges {
            let m = range.useSplit ? split : splitRcp
            let mRcp = range.useSplit ? splitRcp : split
            guard m.isFinite, m > 0 else { continue }

            var affectedDays: Set<Int> = []
            affectedDays.reserveCapacity(min(64, range.end - range.start))

            for idx in range.start..<range.end {
                guard repairedDescBars.indices.contains(idx) else { continue }
                var updated = normalizeOHLC(scaleBar(repairedDescBars[idx], factor: m))
                if correctVolume {
                    updated = scaleBarVolume(updated, factor: mRcp)
                }
                repairedDescBars[idx] = markRepaired(updated)
                affectedDays.insert(barDayKeys[idx])
                didRepair = true
            }

            if correctDividend, !affectedDays.isEmpty {
                for day in affectedDays {
                    scaleByDay[day, default: 1] *= m
                }
            }
        }

        guard didRepair else {
            return (bars, events, false)
        }

        var repairedEvents = events
        if correctDividend, !scaleByDay.isEmpty {
            for index in repairedEvents.indices {
                let event = repairedEvents[index]
                guard event.kind == .dividend else {
                    continue
                }
                guard let amount = sanitizePrice(event.value) else {
                    continue
                }
                let day = dayKey(for: event.date, calendar: calendar)
                guard let m = scaleByDay[day] else { continue }
                repairedEvents[index] = replacingAmount(in: event, value: amount * m)
            }
        }

        return (repairedDescBars.sorted { $0.date < $1.date }, repairedEvents, true)
    }

    private func metaWithCurrency(
        _ meta: YFHistoryMeta,
        currency: String,
        scale: Double?
    ) -> YFHistoryMeta {
        let factor = scale ?? 1
        return YFHistoryMeta(
            currency: currency,
            symbol: meta.symbol,
            exchangeName: meta.exchangeName,
            instrumentType: meta.instrumentType,
            timezone: meta.timezone,
            exchangeTimezoneName: meta.exchangeTimezoneName,
            regularMarketPrice: meta.regularMarketPrice.map { $0 * factor },
            chartPreviousClose: meta.chartPreviousClose.map { $0 * factor },
            previousClose: meta.previousClose.map { $0 * factor },
            gmtoffset: meta.gmtoffset,
            dataGranularity: meta.dataGranularity,
            priceHint: meta.priceHint,
            range: meta.range,
            validRanges: meta.validRanges,
            lastTrade: meta.lastTrade,
            tradingPeriods: meta.tradingPeriods
        )
    }

    private func metaWithLastTrade(_ meta: YFHistoryMeta, lastTrade: YFHistoryLastTrade?) -> YFHistoryMeta {
        YFHistoryMeta(
            currency: meta.currency,
            symbol: meta.symbol,
            exchangeName: meta.exchangeName,
            instrumentType: meta.instrumentType,
            timezone: meta.timezone,
            exchangeTimezoneName: meta.exchangeTimezoneName,
            regularMarketPrice: meta.regularMarketPrice,
            chartPreviousClose: meta.chartPreviousClose,
            previousClose: meta.previousClose,
            gmtoffset: meta.gmtoffset,
            dataGranularity: meta.dataGranularity,
            priceHint: meta.priceHint,
            range: meta.range,
            validRanges: meta.validRanges,
            lastTrade: lastTrade,
            tradingPeriods: meta.tradingPeriods
        )
    }

    private func repairHundredXAnomalies(_ bars: [YFHistoryBar]) -> [YFHistoryBar] {
        guard !bars.isEmpty else {
            return bars
        }

        var repaired = bars
        for index in repaired.indices {
            let baseline = neighborhoodBaseline(repaired, around: index)
            repaired[index] = repairBar(repaired[index], baseline: baseline)
        }

        return repaired.map(normalizeOHLC)
    }

    private enum UnitMixupScaleDirection {
        case down
        case up
    }

    private struct UnitMixupTaggedValue {
        let original: Double
        let direction: UnitMixupScaleDirection
    }

    private struct UnitMixupTag {
        var open: UnitMixupTaggedValue?
        var high: UnitMixupTaggedValue?
        var low: UnitMixupTaggedValue?
        var close: UnitMixupTaggedValue?
        var adjustedClose: UnitMixupTaggedValue?
    }

    private func tagSporadicUnitMixupsForReconstructionIfNeeded(
        _ bars: [YFHistoryBar]
    ) -> (bars: [YFHistoryBar], tags: [Int: UnitMixupTag]) {
        // Port of Python yfinance _fix_unit_random_mixups(...) tagging stage.
        // We tag suspicious values by setting them to nil so the reconstruction step can fill
        // them from finer-grained data. Any remaining nils are handled via crude fallbacks.
        guard bars.count > 1 else {
            return (bars, [:])
        }

        let includeAdjClose = bars.contains { sanitizePrice($0.adjustedClose) != nil }
        let columnCount = includeAdjClose ? 5 : 4

        var values: [[Double?]] = Array(repeating: Array(repeating: nil, count: columnCount), count: bars.count)
        for (index, bar) in bars.enumerated() {
            values[index][0] = sanitizePrice(bar.high)
            values[index][1] = sanitizePrice(bar.open)
            values[index][2] = sanitizePrice(bar.low)
            values[index][3] = sanitizePrice(bar.close)
            if includeAdjClose {
                values[index][4] = sanitizePrice(bar.adjustedClose)
            }
        }

        func roundedToNearest(_ value: Double, step: Double) -> Double {
            guard value.isFinite, step > 0 else { return value }
            return (value / step).rounded() * step
        }

        var taggedBars = bars
        var tags: [Int: UnitMixupTag] = [:]
        tags.reserveCapacity(min(32, bars.count / 10))

        let n = bars.count
        let m = columnCount

        for i in 0..<n {
            for j in 0..<m {
                guard let value = values[i][j], value.isFinite, value > 0 else {
                    continue
                }

                var neighborhood: [Double] = []
                neighborhood.reserveCapacity(9)
                for di in -1...1 {
                    let ni = (i + di + n) % n
                    for dj in -1...1 {
                        let nj = (j + dj + m) % m
                        if let v = values[ni][nj], v.isFinite, v > 0 {
                            neighborhood.append(v)
                        }
                    }
                }

                guard let med = median(neighborhood), med.isFinite, med > 0 else {
                    continue
                }

                let ratio = value / med
                guard ratio.isFinite, ratio > 0 else {
                    continue
                }

                let ratioRounded = roundedToNearest(ratio, step: 20)
                let rcpRounded = roundedToNearest(1 / ratio, step: 20)

                let tooBig = ratioRounded == 100
                let tooSmall = rcpRounded == 100
                guard tooBig || tooSmall else {
                    continue
                }

                let direction: UnitMixupScaleDirection = tooBig ? .down : .up
                let timestamp = Int(bars[i].date.timeIntervalSince1970)
                var tag = tags[timestamp] ?? UnitMixupTag()

                func record(_ current: inout UnitMixupTaggedValue?, original: Double) {
                    if current == nil {
                        current = UnitMixupTaggedValue(original: original, direction: direction)
                    }
                }

                switch j {
                case 0: // High
                    if let original = values[i][j] {
                        record(&tag.high, original: original)
                        taggedBars[i] = YFHistoryBar(
                            date: taggedBars[i].date,
                            open: taggedBars[i].open,
                            high: nil,
                            low: taggedBars[i].low,
                            close: taggedBars[i].close,
                            adjustedClose: taggedBars[i].adjustedClose,
                            volume: taggedBars[i].volume,
                            repaired: taggedBars[i].repaired
                        )
                    }
                case 1: // Open
                    if let original = values[i][j] {
                        record(&tag.open, original: original)
                        taggedBars[i] = YFHistoryBar(
                            date: taggedBars[i].date,
                            open: nil,
                            high: taggedBars[i].high,
                            low: taggedBars[i].low,
                            close: taggedBars[i].close,
                            adjustedClose: taggedBars[i].adjustedClose,
                            volume: taggedBars[i].volume,
                            repaired: taggedBars[i].repaired
                        )
                    }
                case 2: // Low
                    if let original = values[i][j] {
                        record(&tag.low, original: original)
                        taggedBars[i] = YFHistoryBar(
                            date: taggedBars[i].date,
                            open: taggedBars[i].open,
                            high: taggedBars[i].high,
                            low: nil,
                            close: taggedBars[i].close,
                            adjustedClose: taggedBars[i].adjustedClose,
                            volume: taggedBars[i].volume,
                            repaired: taggedBars[i].repaired
                        )
                    }
                case 3: // Close
                    if let original = values[i][j] {
                        record(&tag.close, original: original)
                        if includeAdjClose, let adj = values[i][4] {
                            record(&tag.adjustedClose, original: adj)
                        }
                        taggedBars[i] = YFHistoryBar(
                            date: taggedBars[i].date,
                            open: taggedBars[i].open,
                            high: taggedBars[i].high,
                            low: taggedBars[i].low,
                            close: nil,
                            adjustedClose: nil,
                            volume: taggedBars[i].volume,
                            repaired: taggedBars[i].repaired
                        )
                    }
                case 4: // Adj Close
                    if let original = values[i][j] {
                        record(&tag.adjustedClose, original: original)
                        taggedBars[i] = YFHistoryBar(
                            date: taggedBars[i].date,
                            open: taggedBars[i].open,
                            high: taggedBars[i].high,
                            low: taggedBars[i].low,
                            close: taggedBars[i].close,
                            adjustedClose: nil,
                            volume: taggedBars[i].volume,
                            repaired: taggedBars[i].repaired
                        )
                    }
                default:
                    break
                }

                tags[timestamp] = tag
            }
        }

        return (taggedBars, tags)
    }

    private func applySporadicUnitMixupFallbacksIfNeeded(
        _ bars: [YFHistoryBar],
        tags: [Int: UnitMixupTag]
    ) -> [YFHistoryBar] {
        guard !tags.isEmpty else {
            return bars
        }

        func scaled(_ original: Double, direction: UnitMixupScaleDirection) -> Double {
            switch direction {
            case .down:
                return original * 0.01
            case .up:
                return original * 100
            }
        }

        return bars.map { bar in
            let ts = Int(bar.date.timeIntervalSince1970)
            guard let tag = tags[ts] else {
                return bar
            }

            let originalOpen = sanitizePrice(bar.open)
            let originalHigh = sanitizePrice(bar.high)
            let originalLow = sanitizePrice(bar.low)
            let originalClose = sanitizePrice(bar.close)
            let originalAdjustedClose = sanitizePrice(bar.adjustedClose)

            var open = bar.open
            var high = bar.high
            var low = bar.low
            var close = bar.close
            var adjustedClose = bar.adjustedClose

            if sanitizePrice(open) == nil, let tagged = tag.open {
                open = scaled(tagged.original, direction: tagged.direction)
            }
            if sanitizePrice(close) == nil, let tagged = tag.close {
                close = scaled(tagged.original, direction: tagged.direction)
            }
            if sanitizePrice(adjustedClose) == nil, let tagged = tag.adjustedClose {
                adjustedClose = scaled(tagged.original, direction: tagged.direction)
            }

            if sanitizePrice(high) == nil, let tagged = tag.high {
                if let o = sanitizePrice(open), let c = sanitizePrice(close) {
                    high = max(o, c)
                } else {
                    high = scaled(tagged.original, direction: tagged.direction)
                }
            }
            if sanitizePrice(low) == nil, let tagged = tag.low {
                if let o = sanitizePrice(open), let c = sanitizePrice(close) {
                    low = min(o, c)
                } else {
                    low = scaled(tagged.original, direction: tagged.direction)
                }
            }

            let didFallback = (sanitizePrice(open) != originalOpen)
                || (sanitizePrice(high) != originalHigh)
                || (sanitizePrice(low) != originalLow)
                || (sanitizePrice(close) != originalClose)
                || (sanitizePrice(adjustedClose) != originalAdjustedClose)

            return normalizeOHLC(
                YFHistoryBar(
                    date: bar.date,
                    open: open,
                    high: high,
                    low: low,
                    close: close,
                    adjustedClose: adjustedClose,
                    volume: bar.volume,
                    repaired: bar.repaired || didFallback
                )
            )
        }
    }

    private func repairCorporateActionAdjustments(
        bars: [YFHistoryBar],
        events: [YFHistoryEvent],
        currency: String?,
        calendar: Calendar
    ) -> (bars: [YFHistoryBar], events: [YFHistoryEvent]) {
        guard !bars.isEmpty, !events.isEmpty else {
            return (bars, events)
        }

        var repairedBars = bars
        var repairedEvents = events
        let currencyDivide: Double = (currency == "KWF") ? 1000 : 100

        var barIndexByDay: [Int: Int] = [:]
        for (index, bar) in repairedBars.enumerated() {
            let day = dayKey(for: bar.date, calendar: calendar)
            if barIndexByDay[day] == nil {
                barIndexByDay[day] = index
            }
        }

        var dividendsByDay: [Int: Double] = [:]
        var capitalGainsByDay: [Int: Double] = [:]
        var splitRatioByDay: [Int: Double] = [:]
        var dividendEventIndicesByDay: [Int: [Int]] = [:]

        for (index, event) in repairedEvents.enumerated() {
            guard let amount = sanitizePrice(event.value) else {
                if event.kind == .split,
                   let ratio = sanitizePrice(event.ratio) {
                    let day = dayKey(for: event.date, calendar: calendar)
                    splitRatioByDay[day, default: 1] *= ratio
                }
                continue
            }
            let day = dayKey(for: event.date, calendar: calendar)
            switch event.kind {
            case .dividend:
                dividendsByDay[day, default: 0] += amount
                dividendEventIndicesByDay[day, default: []].append(index)
            case .capitalGain:
                capitalGainsByDay[day, default: 0] += amount
            case .split:
                if let ratio = sanitizePrice(event.ratio) {
                    splitRatioByDay[day, default: 1] *= ratio
                }
            }
        }

        // Very rarely Yahoo returns Close already dividend-adjusted (Close < Low by ~dividend).
        // Port of the first fix in Python _fix_bad_div_adjust.
        for day in dividendsByDay.keys.sorted() {
            guard let dividend = dividendsByDay[day],
                  dividend > 0,
                  let barIndex = barIndexByDay[day],
                  barIndex > 0 else {
                continue
            }

            let prevIndex = barIndex - 1
            let prev = repairedBars[prevIndex]
            guard let prevLow = sanitizePrice(prev.low),
                  let prevHigh = sanitizePrice(prev.high),
                  let prevClose = sanitizePrice(prev.close) else {
                continue
            }

            let diff = prevLow - prevClose
            guard diff > 0,
                  abs((diff / dividend) - 1) < 0.01 else {
                continue
            }

            let newClose = prevClose + dividend
            guard newClose >= prevLow, newClose <= prevHigh else {
                continue
            }

            let cur = repairedBars[barIndex]
            guard let curClose = sanitizePrice(cur.close),
                  let curAdjClose = sanitizePrice(cur.adjustedClose),
                  curClose > 0 else {
                continue
            }

            let adjAfter = curAdjClose / curClose
            let adj = adjAfter * max(0.000_000_1, 1 - (dividend / newClose))
            let newAdjClose = newClose * adj

            repairedBars[prevIndex] = normalizeOHLC(
                YFHistoryBar(
                    date: prev.date,
                    open: prev.open,
                    high: prev.high,
                    low: prev.low,
                    close: newClose,
                    adjustedClose: newAdjClose,
                    volume: prev.volume,
                    repaired: true
                )
            )
        }

        func applyCorrectionToBars(before day: Int, correction: Double) {
            guard correction.isFinite, correction > 0.1, correction < 10,
                  let barIndex = barIndexByDay[day], barIndex > 0 else {
                return
            }
            for index in 0..<barIndex {
                repairedBars[index] = scaledAdjustedCloseBar(repairedBars[index], factor: correction)
            }
        }

        func setDividendTotal(for day: Int, oldTotal: Double, newTotal: Double) {
            guard oldTotal > 0, newTotal >= 0 else {
                return
            }
            dividendsByDay[day] = newTotal

            let scale = newTotal / oldTotal
            for eventIndex in dividendEventIndicesByDay[day] ?? [] {
                guard let current = repairedEvents[eventIndex].value else {
                    continue
                }
                let updatedAmount = max(0, current * scale)
                repairedEvents[eventIndex] = replacingAmount(in: repairedEvents[eventIndex], value: updatedAmount)
            }
        }

        // Repair obvious 100x dividend unit mixups that leak into adjusted-close factors.
        for day in dividendsByDay.keys.sorted() {
            guard let dividend = dividendsByDay[day],
                  let barIndex = barIndexByDay[day], barIndex > 0,
                  let prevClose = sanitizePrice(repairedBars[barIndex - 1].close),
                  prevClose > 0 else {
                continue
            }

            let correctedDividend = dividend / currencyDivide
            let dividendYield = dividend / prevClose
            let correctedYield = correctedDividend / prevClose
            let likelyUnitMixup = dividend > prevClose || (dividendYield > 0.5 && correctedYield < 0.2)
            guard likelyUnitMixup else {
                continue
            }

            guard correctedDividend > 0, correctedDividend < prevClose else {
                continue
            }

            let oldFactor = max(0.000_000_1, 1 - (dividend / prevClose))
            let newFactor = max(0.000_000_1, 1 - (correctedDividend / prevClose))
            let correction = newFactor / oldFactor

            applyCorrectionToBars(before: day, correction: correction)
            setDividendTotal(for: day, oldTotal: dividend, newTotal: correctedDividend)
        }

        // Repair dividends that are ~100x too small compared to the adjusted-close factor.
        for day in dividendsByDay.keys.sorted() {
            guard let dividend = dividendsByDay[day],
                  dividend > 0,
                  let barIndex = barIndexByDay[day],
                  barIndex > 0,
                  splitRatioByDay[day] == nil,
                  (capitalGainsByDay[day] ?? 0) == 0,
                  let prevClose = sanitizePrice(repairedBars[barIndex - 1].close),
                  let prevAdjClose = sanitizePrice(repairedBars[barIndex - 1].adjustedClose),
                  let close = sanitizePrice(repairedBars[barIndex].close),
                  let adjClose = sanitizePrice(repairedBars[barIndex].adjustedClose),
                  prevClose > 0,
                  close > 0 else {
                continue
            }

            let scaledDividend = dividend * currencyDivide
            guard scaledDividend.isFinite,
                  scaledDividend > 0,
                  scaledDividend < prevClose else {
                continue
            }

            let observedFactor = (prevAdjClose / prevClose) / (adjClose / close)
            guard observedFactor.isFinite, observedFactor > 0 else {
                continue
            }

            let expectedOld = max(0.000_000_1, 1 - (dividend / prevClose))
            let expectedScaled = max(0.000_000_1, 1 - (scaledDividend / prevClose))
            if relativeDistance(observedFactor, expectedOld) < 0.05 {
                continue
            }
            if relativeDistance(observedFactor, expectedScaled) >= 0.05 {
                continue
            }

            setDividendTotal(for: day, oldTotal: dividend, newTotal: scaledDividend)
        }

        // Detect and repair double-counted capital gains in adjusted-close history.
        var priceDropMean: Double = 0
        do {
            var changes: [Double] = []
            changes.reserveCapacity(repairedBars.count)
            for index in 1..<repairedBars.count {
                let day = dayKey(for: repairedBars[index].date, calendar: calendar)
                let hasDistributions = (dividendsByDay[day] ?? 0) > 0 || (capitalGainsByDay[day] ?? 0) > 0
                guard !hasDistributions,
                      let prevClose = sanitizePrice(repairedBars[index - 1].close),
                      let close = sanitizePrice(repairedBars[index].close),
                      prevClose > 0 else {
                    continue
                }
                changes.append(abs(close - prevClose) / prevClose)
            }
            if !changes.isEmpty {
                priceDropMean = changes.reduce(0, +) / Double(changes.count)
            }
        }

        var candidateDays: [Int] = []
        var suspectedDoubleCount: [Int: Bool] = [:]

        for day in capitalGainsByDay.keys.sorted() {
            guard let capitalGain = capitalGainsByDay[day],
                  capitalGain > 0,
                  let dividend = dividendsByDay[day],
                  dividend >= capitalGain,
                  let barIndex = barIndexByDay[day],
                  barIndex > 0,
                  let prevClose = sanitizePrice(repairedBars[barIndex - 1].close),
                  let close = sanitizePrice(repairedBars[barIndex].close),
                  prevClose > 0 else {
                continue
            }

            let priceDropPct = (prevClose - close) / prevClose
            let priceDropExcludingVolatility = priceDropPct - priceDropMean
            let dividendPct = dividend / prevClose
            let dividendPlusGainPct = (dividend + capitalGain) / prevClose
            let looksDoubleCounted = abs(priceDropExcludingVolatility - dividendPct) < abs(priceDropExcludingVolatility - dividendPlusGainPct)

            candidateDays.append(day)
            suspectedDoubleCount[day] = looksDoubleCounted
        }

        let suspectedCount = candidateDays.filter { suspectedDoubleCount[$0] == true }.count
        if !candidateDays.isEmpty, suspectedCount * 3 >= candidateDays.count * 2 {
            for day in candidateDays where suspectedDoubleCount[day] == true {
                guard let capitalGain = capitalGainsByDay[day],
                      let dividend = dividendsByDay[day],
                      dividend >= capitalGain,
                      let barIndex = barIndexByDay[day],
                      barIndex > 0,
                      let prevClose = sanitizePrice(repairedBars[barIndex - 1].close),
                      let prevAdjClose = sanitizePrice(repairedBars[barIndex - 1].adjustedClose),
                      let close = sanitizePrice(repairedBars[barIndex].close),
                      let adjClose = sanitizePrice(repairedBars[barIndex].adjustedClose),
                      prevClose > 0,
                      close > 0 else {
                    continue
                }

                let observedFactor = (prevAdjClose / prevClose) / (adjClose / close)
                guard observedFactor.isFinite, observedFactor > 0 else {
                    continue
                }

                let desiredFactor = max(0.000_000_1, 1 - (dividend / prevClose))
                let correction = desiredFactor / observedFactor

                applyCorrectionToBars(before: day, correction: correction)
                setDividendTotal(for: day, oldTotal: dividend, newTotal: max(0, dividend - capitalGain))
            }
        }

        // Validate adjusted-close split factors and repair obvious missing split adjustments.
        for day in splitRatioByDay.keys.sorted() {
            guard let splitRatio = splitRatioByDay[day],
                  splitRatio > 0,
                  let barIndex = barIndexByDay[day],
                  barIndex > 0,
                  let prevClose = sanitizePrice(repairedBars[barIndex - 1].close),
                  let prevAdjClose = sanitizePrice(repairedBars[barIndex - 1].adjustedClose),
                  let close = sanitizePrice(repairedBars[barIndex].close),
                  let adjClose = sanitizePrice(repairedBars[barIndex].adjustedClose),
                  prevClose > 0,
                  close > 0 else {
                continue
            }

            let expectedFactor = max(0.000_000_1, 1 / splitRatio)
            let observedFactor = (prevAdjClose / prevClose) / (adjClose / close)
            guard observedFactor.isFinite, observedFactor > 0 else {
                continue
            }

            if relativeDistance(observedFactor, expectedFactor) < 0.15 {
                continue
            }

            let correction = expectedFactor / observedFactor
            applyCorrectionToBars(before: day, correction: correction)
        }

        // Repair missing / wrong dividend adjustments in adjusted-close factors.
        for day in dividendsByDay.keys.sorted() {
            guard let dividend = dividendsByDay[day],
                  dividend > 0,
                  let barIndex = barIndexByDay[day],
                  barIndex > 0,
                  let prevClose = sanitizePrice(repairedBars[barIndex - 1].close),
                  let prevAdjClose = sanitizePrice(repairedBars[barIndex - 1].adjustedClose),
                  let close = sanitizePrice(repairedBars[barIndex].close),
                  let adjClose = sanitizePrice(repairedBars[barIndex].adjustedClose),
                  prevClose > 0,
                  close > 0 else {
                continue
            }

            if let splitRatio = splitRatioByDay[day], abs(splitRatio - 1) > 0.000_001 {
                continue
            }
            if let capitalGain = capitalGainsByDay[day], capitalGain > 0 {
                continue
            }

            let dividendPct = dividend / prevClose
            if dividendPct < 0.0005 {
                continue
            }

            let observedFactor = (prevAdjClose / prevClose) / (adjClose / close)
            let expectedFactor = max(0.000_000_1, 1 - dividendPct)
            guard observedFactor.isFinite, observedFactor > 0 else {
                continue
            }

            if relativeDistance(observedFactor, expectedFactor) < 0.05 {
                continue
            }

            let correction = expectedFactor / observedFactor
            applyCorrectionToBars(before: day, correction: correction)
        }

        return (repairedBars, repairedEvents)
    }

    private func replacingAmount(in event: YFHistoryEvent, value: Double) -> YFHistoryEvent {
        var rawObject = event.raw.objectValue ?? [:]
        rawObject["amount"] = .number(value)
        return YFHistoryEvent(
            kind: event.kind,
            date: event.date,
            value: value,
            ratio: event.ratio,
            raw: .object(rawObject)
        )
    }

    private func markRepaired(_ bar: YFHistoryBar) -> YFHistoryBar {
        guard !bar.repaired else { return bar }
        return YFHistoryBar(
            date: bar.date,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            adjustedClose: bar.adjustedClose,
            volume: bar.volume,
            repaired: true
        )
    }

    private func scaledAdjustedCloseBar(_ bar: YFHistoryBar, factor: Double) -> YFHistoryBar {
        YFHistoryBar(
            date: bar.date,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            adjustedClose: bar.adjustedClose.map { $0 * factor },
            volume: bar.volume,
            repaired: true
        )
    }

    private func dayKey(for date: Date, calendar: Calendar) -> Int {
        let comps = calendar.dateComponents([.year, .month, .day], from: date)
        let year = comps.year ?? 0
        let month = comps.month ?? 0
        let day = comps.day ?? 0
        return year * 10_000 + month * 100 + day
    }

    private func scaleBar(_ bar: YFHistoryBar, factor: Double) -> YFHistoryBar {
        YFHistoryBar(
            date: bar.date,
            open: bar.open.map { $0 * factor },
            high: bar.high.map { $0 * factor },
            low: bar.low.map { $0 * factor },
            close: bar.close.map { $0 * factor },
            adjustedClose: bar.adjustedClose.map { $0 * factor },
            volume: bar.volume,
            repaired: bar.repaired
        )
    }

    private func scaleBarVolume(_ bar: YFHistoryBar, factor: Double) -> YFHistoryBar {
        guard let volume = bar.volume else {
            return bar
        }
        let scaled = Double(volume) * factor
        guard scaled.isFinite, scaled >= 0 else {
            return bar
        }
        return YFHistoryBar(
            date: bar.date,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            adjustedClose: bar.adjustedClose,
            volume: Int(scaled.rounded()),
            repaired: bar.repaired
        )
    }

    private func repairBar(_ bar: YFHistoryBar, baseline: Double?) -> YFHistoryBar {
        var open = sanitizePrice(bar.open)
        var high = sanitizePrice(bar.high)
        var low = sanitizePrice(bar.low)
        var close = sanitizePrice(bar.close)
        var adjustedClose = sanitizePrice(bar.adjustedClose)

        let originalOpen = open
        let originalHigh = high
        let originalLow = low
        let originalClose = close
        let originalAdjustedClose = adjustedClose

        let candidateBaseline = baseline ?? representativePrice(
            open: open,
            high: high,
            low: low,
            close: close,
            adjustedClose: adjustedClose
        )

        if let baselineValue = candidateBaseline, baselineValue > 0 {
            if let rowPrice = representativePrice(
                open: open,
                high: high,
                low: low,
                close: close,
                adjustedClose: adjustedClose
            ) {
                let currentDistance = relativeDistance(rowPrice, baselineValue)
                let downScaledDistance = relativeDistance(rowPrice / 100, baselineValue)
                let upScaledDistance = relativeDistance(rowPrice * 100, baselineValue)

                if currentDistance > 10, downScaledDistance < 0.25 {
                    return markRepaired(normalizeOHLC(scaleBar(bar, factor: 0.01)))
                }
                if currentDistance > 10, upScaledDistance < 0.25 {
                    return markRepaired(normalizeOHLC(scaleBar(bar, factor: 100)))
                }
            }

            open = sanitizeRelativeOutlier(open, baseline: baselineValue)
            high = sanitizeRelativeOutlier(high, baseline: baselineValue)
            low = sanitizeRelativeOutlier(low, baseline: baselineValue)
            close = sanitizeRelativeOutlier(close, baseline: baselineValue)
            adjustedClose = sanitizeRelativeOutlier(adjustedClose, baseline: baselineValue)
        }

        if close == nil, let baseline = candidateBaseline {
            close = baseline
        }
        if open == nil {
            open = close
        }

        return YFHistoryBar(
            date: bar.date,
            open: open,
            high: high,
            low: low,
            close: close,
            adjustedClose: adjustedClose,
            volume: bar.volume,
            repaired: bar.repaired || (open != originalOpen)
                || (high != originalHigh)
                || (low != originalLow)
                || (close != originalClose)
                || (adjustedClose != originalAdjustedClose)
        )
    }

    private func normalizeOHLC(_ bar: YFHistoryBar) -> YFHistoryBar {
        let open = sanitizePrice(bar.open)
        var high = sanitizePrice(bar.high)
        var low = sanitizePrice(bar.low)
        let close = sanitizePrice(bar.close)

        let values = [open, high, low, close].compactMap { $0 }
        if values.isEmpty {
            return YFHistoryBar(
                date: bar.date,
                open: open,
                high: high,
                low: low,
                close: close,
                adjustedClose: sanitizePrice(bar.adjustedClose),
                volume: bar.volume,
                repaired: bar.repaired
            )
        }

        let maxValue = values.max()!
        let minValue = values.min()!

        if high == nil || (high ?? 0) < maxValue {
            high = maxValue
        }
        if low == nil || (low ?? 0) > minValue {
            low = minValue
        }

        return YFHistoryBar(
            date: bar.date,
            open: open,
            high: high,
            low: low,
            close: close,
            adjustedClose: sanitizePrice(bar.adjustedClose),
            volume: bar.volume,
            repaired: bar.repaired
        )
    }

    private func neighborhoodBaseline(_ bars: [YFHistoryBar], around index: Int) -> Double? {
        guard bars.indices.contains(index) else {
            return nil
        }

        var neighbors: [Double] = []
        for offset in 1...2 {
            let left = index - offset
            if bars.indices.contains(left), let value = representativePrice(bar: bars[left]) {
                neighbors.append(value)
            }
            let right = index + offset
            if bars.indices.contains(right), let value = representativePrice(bar: bars[right]) {
                neighbors.append(value)
            }
        }

        return median(neighbors)
    }

    private func representativePrice(bar: YFHistoryBar) -> Double? {
        representativePrice(
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            adjustedClose: bar.adjustedClose
        )
    }

    private func representativePrice(
        open: Double?,
        high: Double?,
        low: Double?,
        close: Double?,
        adjustedClose: Double?
    ) -> Double? {
        if let close = sanitizePrice(close) {
            return close
        }
        if let adjustedClose = sanitizePrice(adjustedClose) {
            return adjustedClose
        }
        let values = [open, high, low].compactMap { sanitizePrice($0) }
        guard !values.isEmpty else {
            return nil
        }
        return values.reduce(0, +) / Double(values.count)
    }

    private func sanitizePrice(_ value: Double?) -> Double? {
        guard let value, value.isFinite, value > 0 else {
            return nil
        }
        return value
    }

    private func sanitizeRelativeOutlier(_ value: Double?, baseline: Double) -> Double? {
        guard let value = sanitizePrice(value), baseline > 0 else {
            return nil
        }

        let directDistance = relativeDistance(value, baseline)
        let downScaledDistance = relativeDistance(value / 100, baseline)
        let upScaledDistance = relativeDistance(value * 100, baseline)

        if directDistance > 10, downScaledDistance < 0.25 {
            return value / 100
        }
        if directDistance > 10, upScaledDistance < 0.25 {
            return value * 100
        }
        return value
    }

    private func relativeDistance(_ lhs: Double, _ rhs: Double) -> Double {
        guard rhs != 0 else {
            return .infinity
        }
        return abs(lhs - rhs) / abs(rhs)
    }

    private func percentile(_ values: [Double], _ percent: Double) -> Double? {
        let filtered = values.filter { $0.isFinite }.sorted()
        guard !filtered.isEmpty else { return nil }

        let clamped = min(100.0, max(0.0, percent))
        if filtered.count == 1 {
            return filtered[0]
        }

        let rank = (clamped / 100.0) * Double(filtered.count - 1)
        let lowerIndex = Int(rank.rounded(.down))
        let upperIndex = Int(rank.rounded(.up))
        if lowerIndex == upperIndex {
            return filtered[lowerIndex]
        }

        let weight = rank - Double(lowerIndex)
        return filtered[lowerIndex] + (filtered[upperIndex] - filtered[lowerIndex]) * weight
    }

    private func standardDeviation(_ values: [Double], mean: Double) -> Double {
        guard !values.isEmpty else { return 0 }
        let variance = values.reduce(0.0) { partial, value in
            let delta = value - mean
            return partial + delta * delta
        } / Double(values.count)
        return sqrt(variance)
    }

    private func median(_ values: [Double]) -> Double? {
        let sorted = values.filter { $0.isFinite && $0 > 0 }.sorted()
        guard !sorted.isEmpty else {
            return nil
        }
        let middle = sorted.count / 2
        if sorted.count % 2 == 1 {
            return sorted[middle]
        }
        return (sorted[middle - 1] + sorted[middle]) / 2
    }

    private func rounded(_ value: Double?, digits: Int) -> Double? {
        guard let value else {
            return nil
        }
        let factor = pow(10.0, Double(max(0, digits)))
        return (value * factor).rounded() / factor
    }

    private func maxOptional(_ lhs: Double?, _ rhs: Double) -> Double {
        guard let lhs else { return rhs }
        return max(lhs, rhs)
    }

    private func minOptional(_ lhs: Double?, _ rhs: Double) -> Double {
        guard let lhs else { return rhs }
        return min(lhs, rhs)
    }

    private func mapTransport(_ error: Error) -> Error {
        if error is YFinanceError {
            return error
        }
        return YFinanceError.transport(error)
    }

    private func shouldRetry(error: Error) -> Bool {
        guard let yfError = error as? YFinanceError else {
            return false
        }

        switch yfError {
        case .transport:
            return true
        case .httpStatus(let status):
            return status >= 500 || status == 429
        default:
            return false
        }
    }
}
