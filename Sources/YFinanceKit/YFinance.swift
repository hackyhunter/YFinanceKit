import Foundation

public enum YF {
    public static func ticker(_ symbol: String, client: YFinanceClient = YFinanceClient()) -> YFTicker {
        YFTicker(symbol: symbol, client: client)
    }

    public static func ticker(_ ticker: (String, String), client: YFinanceClient = YFinanceClient()) throws -> YFTicker {
        try YFTicker(ticker, client: client)
    }

    public static func tickers(_ symbols: [String], client: YFinanceClient = YFinanceClient()) -> YFTickers {
        YFTickers(symbols: symbols, client: client)
    }

    public static func tickers(_ symbols: String, client: YFinanceClient = YFinanceClient()) -> YFTickers {
        YFTickers(symbols, client: client)
    }

    public static func tickers(_ tickers: [(String, String)], client: YFinanceClient = YFinanceClient()) throws -> YFTickers {
        try YFTickers(tickers, client: client)
    }

    public static func search(
        _ query: String,
        maxResults: Int = 8,
        newsCount: Int = 8,
        listsCount: Int = 8,
        includeCompanyBreakdown: Bool = true,
        includeNavLinks: Bool = false,
        includeResearch: Bool = false,
        includeCulturalAssets: Bool = false,
        enableFuzzyQuery: Bool = false,
        recommendedCount: Int = 8,
        client: YFinanceClient = YFinanceClient()
    ) async throws -> YFSearchResult {
        try await client.search(
            query: query,
            quotesCount: maxResults,
            newsCount: newsCount,
            listsCount: listsCount,
            includeCompanyBreakdown: includeCompanyBreakdown,
            includeNavLinks: includeNavLinks,
            includeResearchReports: includeResearch,
            includeCulturalAssets: includeCulturalAssets,
            enableFuzzyQuery: enableFuzzyQuery,
            recommendedCount: recommendedCount
        )
    }

    public static func search(
        _ query: String,
        max_results: Int,
        news_count: Int = 8,
        lists_count: Int = 8,
        include_cb: Bool = true,
        include_nav_links: Bool = false,
        include_research: Bool = false,
        include_cultural_assets: Bool = false,
        enable_fuzzy_query: Bool = false,
        recommended: Int = 8,
        client: YFinanceClient = YFinanceClient()
    ) async throws -> YFSearchResult {
        try await search(
            query,
            maxResults: max_results,
            newsCount: news_count,
            listsCount: lists_count,
            includeCompanyBreakdown: include_cb,
            includeNavLinks: include_nav_links,
            includeResearch: include_research,
            includeCulturalAssets: include_cultural_assets,
            enableFuzzyQuery: enable_fuzzy_query,
            recommendedCount: recommended,
            client: client
        )
    }

    public static func searchObject(
        _ query: String,
        maxResults: Int = 8,
        newsCount: Int = 8,
        listsCount: Int = 8,
        includeCompanyBreakdown: Bool = true,
        includeNavLinks: Bool = false,
        includeResearch: Bool = false,
        includeCulturalAssets: Bool = false,
        enableFuzzyQuery: Bool = false,
        recommendedCount: Int = 8,
        timeout: TimeInterval = 30,
        raiseErrors: Bool = true,
        client: YFinanceClient = YFinanceClient()
    ) -> YFSearch {
        YFSearch(
            query: query,
            maxResults: maxResults,
            newsCount: newsCount,
            listsCount: listsCount,
            includeCompanyBreakdown: includeCompanyBreakdown,
            includeNavLinks: includeNavLinks,
            includeResearch: includeResearch,
            includeCulturalAssets: includeCulturalAssets,
            enableFuzzyQuery: enableFuzzyQuery,
            recommendedCount: recommendedCount,
            timeout: timeout,
            raiseErrors: raiseErrors,
            client: client
        )
    }

    public static func searchObject(
        _ query: String,
        max_results: Int,
        news_count: Int = 8,
        lists_count: Int = 8,
        include_cb: Bool = true,
        include_nav_links: Bool = false,
        include_research: Bool = false,
        include_cultural_assets: Bool = false,
        enable_fuzzy_query: Bool = false,
        recommended: Int = 8,
        timeout: TimeInterval = 30,
        raise_errors: Bool = true,
        client: YFinanceClient = YFinanceClient()
    ) -> YFSearch {
        searchObject(
            query,
            maxResults: max_results,
            newsCount: news_count,
            listsCount: lists_count,
            includeCompanyBreakdown: include_cb,
            includeNavLinks: include_nav_links,
            includeResearch: include_research,
            includeCulturalAssets: include_cultural_assets,
            enableFuzzyQuery: enable_fuzzy_query,
            recommendedCount: recommended,
            timeout: timeout,
            raiseErrors: raise_errors,
            client: client
        )
    }

    public static func lookup(
        _ query: String,
        timeout: TimeInterval = 30,
        raiseErrors: Bool = true,
        client: YFinanceClient = YFinanceClient()
    ) -> YFLookup {
        YFLookup(query: query, timeout: timeout, raiseErrors: raiseErrors, client: client)
    }

    public static func lookup(
        _ query: String,
        timeout: TimeInterval = 30,
        raise_errors: Bool,
        client: YFinanceClient = YFinanceClient()
    ) -> YFLookup {
        lookup(query, timeout: timeout, raiseErrors: raise_errors, client: client)
    }

    public static func market(
        _ market: String,
        timeout: TimeInterval = 30,
        client: YFinanceClient = YFinanceClient()
    ) -> YFMarket {
        YFMarket(market: market, timeout: timeout, client: client)
    }

    public static func sector(_ key: String, client: YFinanceClient = YFinanceClient()) -> YFSector {
        YFSector(key: key, client: client)
    }

    public static func industry(_ key: String, client: YFinanceClient = YFinanceClient()) -> YFIndustry {
        YFIndustry(key: key, client: client)
    }

    public static func calendars(
        start: Date? = nil,
        end: Date? = nil,
        client: YFinanceClient = YFinanceClient()
    ) -> YFCalendars {
        YFCalendars(start: start, end: end, client: client)
    }

    public static func screener(client: YFinanceClient = YFinanceClient()) -> YFScreener {
        YFScreener(client: client)
    }

    public static func webSocket(
        url: URL = URL(string: "wss://streamer.finance.yahoo.com/?version=2")!,
        verbose: Bool = true
    ) -> YFWebSocket {
        YFWebSocket(url: url, verbose: verbose)
    }

    public static func asyncWebSocket(
        url: URL = URL(string: "wss://streamer.finance.yahoo.com/?version=2")!,
        verbose: Bool = true
    ) -> YFAsyncWebSocket {
        YFAsyncWebSocket(url: url, verbose: verbose)
    }

    public static func download(
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

    public static func download(
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
            parsedSymbols(symbols),
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

    public static func download(
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

    public static func download(
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
            parsedSymbols(symbols),
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

    public static func download(
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

    public static func download(
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
            parsedSymbols(symbols),
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

    public static func download(
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

    public static func download(
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
            parsedSymbols(symbols),
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

    public static func download(
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
            parsedSymbols(symbols),
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

    public static func download(
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

    public static func download(
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

    public static func download(
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

    public static func download(
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

    public static func download(
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

    public static func download(
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

    public static func download(
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

    public static func download(
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

    public static func download(
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

    public static func downloadTable(
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

    public static func downloadTable(
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
        try await downloadTable(
            parsedSymbols(symbols),
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

    public static func downloadTable(
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

    public static func downloadTable(
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
        try await downloadTable(
            parsedSymbols(symbols),
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

    public static func downloadTable(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func download_table(
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

    public static func downloadTable(
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
        try await downloadTable(
            parsedSymbols(symbols),
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

    public static func downloadTable(
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

    public static func downloadTable(
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
        try await downloadTable(
            parsedSymbols(symbols),
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

    public static func download(
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

    public static func download(
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
        let parsedSymbols = parsedSymbols(symbols)
        return try await download(
            parsedSymbols,
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

    private static func parsedSymbols(_ symbols: String) -> [String] {
        symbols
            .replacingOccurrences(of: ",", with: " ")
            .split(separator: " ")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).uppercased() }
            .filter { !$0.isEmpty }
    }

    public static func config() -> YFConfigStore {
        YFConfigStore.shared
    }

    public static func client() async -> YFinanceClient {
        await YFinanceClient.configured()
    }

    public static func setConfig(proxy: String? = nil, retries: Int? = nil) async {
        await YFConfigStore.shared.setConfig(proxy: proxy, retries: retries)
    }

    public static func setTZCacheLocation(_ path: String) async {
        await YFConfigStore.shared.setTZCacheLocation(path)
    }

    public static func setCacheLocation(_ path: String) async {
        await setTZCacheLocation(path)
    }

    public static func set_tz_cache_location(_ path: String) async {
        await setTZCacheLocation(path)
    }

    public static func set_cache_location(_ path: String) async {
        await setTZCacheLocation(path)
    }

    public static func set_config(proxy: String? = nil, retries: Int? = nil) async {
        await setConfig(proxy: proxy, retries: retries)
    }

    public static func enableDebugMode(_ enabled: Bool = true) async {
        await YFConfigStore.shared.enableDebugMode(enabled)
    }
}
