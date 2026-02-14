import Foundation

// Python yfinance compatibility helpers (utils.py).

public func is_isin(_ string: String) -> Bool {
    let cleaned = string.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
    guard cleaned.count == 12 else { return false }
    return cleaned.range(of: #"^([A-Z]{2})([A-Z0-9]{9})([0-9])$"#, options: .regularExpression) != nil
}

public func get_all_by_isin(
    _ isin: String,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFJSONValue {
    let cleaned = isin.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
    guard is_isin(cleaned) else {
        throw YFinanceError.invalidRequest("Invalid ISIN number")
    }

    let raw = try await client.searchRaw(
        query: cleaned,
        quotesCount: 1,
        newsCount: 8,
        listsCount: 8,
        includeCompanyBreakdown: true,
        includeNavLinks: false,
        includeResearchReports: false,
        includeCulturalAssets: false,
        enableFuzzyQuery: false,
        recommendedCount: 8
    )

    let quotes = raw["quotes"]?.arrayValue ?? []
    let quote = quotes.first { item in
        if let symbol = item["symbol"]?.stringValue?.trimmingCharacters(in: .whitespacesAndNewlines),
           !symbol.isEmpty {
            return true
        }
        return false
    }

    if let symbol = quote?["symbol"]?.stringValue?.trimmingCharacters(in: .whitespacesAndNewlines),
       !symbol.isEmpty {
        await YFCacheStores.isin.set(symbol.uppercased(), for: cleaned)
    }

    let tickerObject: [String: YFJSONValue] = [
        "symbol": .string(quote?["symbol"]?.stringValue ?? ""),
        "shortname": .string(quote?["shortname"]?.stringValue ?? ""),
        "longname": .string(quote?["longname"]?.stringValue ?? ""),
        "type": .string(quote?["quoteType"]?.stringValue ?? ""),
        "exchange": .string(quote?["exchDisp"]?.stringValue ?? ""),
    ]

    return .object([
        "ticker": .object(tickerObject),
        "news": raw["news"] ?? .array([]),
    ])
}

public func get_ticker_by_isin(
    _ isin: String,
    client: YFinanceClient = YFinanceClient()
) async throws -> String {
    let cleaned = isin.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
    guard is_isin(cleaned) else {
        throw YFinanceError.invalidRequest("Invalid ISIN number")
    }

    if let cached = await YFCacheStores.isin.lookup(cleaned), !cached.isEmpty {
        return cached
    }

    let all = try await get_all_by_isin(cleaned, client: client)
    let symbol = all["ticker"]?["symbol"]?.stringValue ?? ""
    let resolved = symbol.trimmingCharacters(in: .whitespacesAndNewlines)
    if !resolved.isEmpty {
        await YFCacheStores.isin.set(resolved.uppercased(), for: cleaned)
    }
    return resolved
}

public func get_info_by_isin(
    _ isin: String,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFJSONValue {
    let all = try await get_all_by_isin(isin, client: client)
    return all["ticker"] ?? .object([:])
}

public func get_news_by_isin(
    _ isin: String,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFJSONValue {
    let all = try await get_all_by_isin(isin, client: client)
    return all["news"] ?? .array([])
}
