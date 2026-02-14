import Foundation

public actor YFSearch {
    public let query: String
    public let maxResults: Int
    public let newsCount: Int
    public let listsCount: Int
    public let includeCompanyBreakdown: Bool
    public let includeNavLinks: Bool
    public let includeResearch: Bool
    public let includeCulturalAssets: Bool
    public let enableFuzzyQuery: Bool
    public let recommendedCount: Int
    public let timeout: TimeInterval
    public let raiseErrors: Bool

    private let client: YFinanceClient
    private var cached: YFSearchResult?
    private var cachedRaw: YFJSONValue?

    public init(
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
    ) {
        self.init(
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

    public init(
        query: String,
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
    ) {
        self.query = query
        self.maxResults = maxResults
        self.newsCount = newsCount
        self.listsCount = listsCount
        self.includeCompanyBreakdown = includeCompanyBreakdown
        self.includeNavLinks = includeNavLinks
        self.includeResearch = includeResearch
        self.includeCulturalAssets = includeCulturalAssets
        self.enableFuzzyQuery = enableFuzzyQuery
        self.recommendedCount = recommendedCount
        self.timeout = timeout
        self.raiseErrors = raiseErrors
        self.client = client
    }

    // Python-style initializer labels.
    public init(
        query: String,
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
    ) {
        self.init(
            query: query,
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

    @discardableResult
    public func search() async throws -> YFSearchResult {
        do {
            let result = try await client.search(
                query: query,
                quotesCount: maxResults,
                newsCount: newsCount,
                listsCount: listsCount,
                includeCompanyBreakdown: includeCompanyBreakdown,
                includeNavLinks: includeNavLinks,
                includeResearchReports: includeResearch,
                includeCulturalAssets: includeCulturalAssets,
                enableFuzzyQuery: enableFuzzyQuery,
                recommendedCount: recommendedCount,
                timeout: timeout
            )
            cached = result
            cachedRaw = nil
            return result
        } catch {
            if raiseErrors {
                throw error
            }
            await logSuppressedError(error, context: "Search.search()")
            let empty = YFSearchResult.empty
            cached = empty
            cachedRaw = .object([:])
            return empty
        }
    }

    public func quotes() async throws -> [YFSearchQuote] {
        let result = try await ensure()
        return result.quotes
    }

    public func quotesTable() async throws -> YFTable {
        let raw = try await response()
        return (raw["quotes"] ?? .array([])).toTable()
    }

    public func news() async throws -> [YFSearchNews] {
        let result = try await ensure()
        return result.news
    }

    public func newsTable() async throws -> YFTable {
        let raw = try await response()
        return (raw["news"] ?? .array([])).toTable()
    }

    public func lists() async throws -> [YFSearchList] {
        let result = try await ensure()
        return result.lists
    }

    public func research() async throws -> [YFSearchResearchReport] {
        let result = try await ensure()
        return result.researchReports
    }

    public func nav() async throws -> [YFSearchNavLink] {
        let result = try await ensure()
        return result.nav
    }

    public func all() async throws -> YFSearchResult {
        try await ensure()
    }

    public func response() async throws -> YFJSONValue {
        if let cachedRaw {
            return cachedRaw
        }
        do {
            let raw = try await client.searchRaw(
                query: query,
                quotesCount: maxResults,
                newsCount: newsCount,
                listsCount: listsCount,
                includeCompanyBreakdown: includeCompanyBreakdown,
                includeNavLinks: includeNavLinks,
                includeResearchReports: includeResearch,
                includeCulturalAssets: includeCulturalAssets,
                enableFuzzyQuery: enableFuzzyQuery,
                recommendedCount: recommendedCount,
                timeout: timeout
            )
            cachedRaw = raw
            return raw
        } catch {
            if raiseErrors {
                throw error
            }
            await logSuppressedError(error, context: "Search.response()")
            let empty = YFJSONValue.object([:])
            cachedRaw = empty
            if cached == nil {
                cached = .empty
            }
            return empty
        }
    }

    // Python-style aliases.
    public func getQuotes() async throws -> [YFSearchQuote] { try await quotes() }
    public func getNews() async throws -> [YFSearchNews] { try await news() }
    public func getLists() async throws -> [YFSearchList] { try await lists() }
    public func getResearch() async throws -> [YFSearchResearchReport] { try await research() }
    public func getNav() async throws -> [YFSearchNavLink] { try await nav() }
    public func getAll() async throws -> YFSearchResult { try await all() }
    public func getResponse() async throws -> YFJSONValue { try await response() }
    public func get_quotes() async throws -> [YFSearchQuote] { try await getQuotes() }
    public func get_news() async throws -> [YFSearchNews] { try await getNews() }
    public func get_lists() async throws -> [YFSearchList] { try await getLists() }
    public func get_research() async throws -> [YFSearchResearchReport] { try await getResearch() }
    public func get_nav() async throws -> [YFSearchNavLink] { try await getNav() }
    public func get_all() async throws -> YFSearchResult { try await getAll() }
    public func get_response() async throws -> YFJSONValue { try await getResponse() }

    private func ensure() async throws -> YFSearchResult {
        if let cached {
            return cached
        }
        return try await search()
    }

    private func logSuppressedError(_ error: Error, context: String) async {
        let debugEnabled = await YFConfigStore.shared.debug.enabled
        if debugEnabled {
            print("[YFinanceKit] Suppressed \(context) error: \(error)")
        }
    }
}
