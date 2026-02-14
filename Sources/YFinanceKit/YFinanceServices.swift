import Foundation

public actor YFLookup {
    public let query: String
    public let timeout: TimeInterval
    public let raiseErrors: Bool
    private let client: YFinanceClient
    private var cache: [String: YFJSONValue] = [:]

    public init(
        _ query: String,
        timeout: TimeInterval = 30,
        raiseErrors: Bool = true,
        client: YFinanceClient = YFinanceClient()
    ) {
        self.init(query: query, timeout: timeout, raiseErrors: raiseErrors, client: client)
    }

    public init(
        query: String,
        timeout: TimeInterval = 30,
        raiseErrors: Bool = true,
        client: YFinanceClient = YFinanceClient()
    ) {
        self.query = query
        self.timeout = timeout
        self.raiseErrors = raiseErrors
        self.client = client
    }

    public init(
        query: String,
        timeout: TimeInterval = 30,
        raise_errors: Bool,
        client: YFinanceClient = YFinanceClient()
    ) {
        self.init(query: query, timeout: timeout, raiseErrors: raise_errors, client: client)
    }

    public func fetch(type: YFLookupType = .all, count: Int = 25) async throws -> YFJSONValue {
        let cacheKey = "\(type.rawValue)|\(count)"
        if let cached = cache[cacheKey] {
            return cached
        }

        do {
            let raw = try await client.lookup(query: query, type: type.rawValue, count: count, timeout: timeout)
            cache[cacheKey] = raw
            return raw
        } catch {
            if raiseErrors {
                throw error
            }
            await logSuppressedError(error, context: "Lookup.fetch(type:\(type.rawValue))")
            let empty = YFJSONValue.object([:])
            cache[cacheKey] = empty
            return empty
        }
    }

    public func fetchTable(type: YFLookupType = .all, count: Int = 25) async throws -> YFTable {
        let raw = try await fetch(type: type, count: count)
        let docs = raw["finance"]?["result"]?[0]?["documents"] ?? .array([])
        let table = docs.toTable()
        guard table.columns.contains("symbol") else {
            return YFTable(columns: [], rows: [])
        }
        let filteredRows = table.rows.filter { row in
            if let symbol = row["symbol"]?.stringValue?.trimmingCharacters(in: .whitespacesAndNewlines), !symbol.isEmpty {
                return true
            }
            return false
        }
        return YFTable(columns: table.columns, rows: filteredRows)
    }

    public func fetchIndexedTable(type: YFLookupType = .all, count: Int = 25) async throws -> YFIndexedTable {
        try await fetchTable(type: type, count: count).index(by: "symbol")
    }

    public func all(count: Int = 25) async throws -> YFJSONValue { try await fetch(type: .all, count: count) }
    public func stock(count: Int = 25) async throws -> YFJSONValue { try await fetch(type: .equity, count: count) }
    public func mutualfund(count: Int = 25) async throws -> YFJSONValue { try await fetch(type: .mutualfund, count: count) }
    public func etf(count: Int = 25) async throws -> YFJSONValue { try await fetch(type: .etf, count: count) }
    public func index(count: Int = 25) async throws -> YFJSONValue { try await fetch(type: .index, count: count) }
    public func future(count: Int = 25) async throws -> YFJSONValue { try await fetch(type: .future, count: count) }
    public func currency(count: Int = 25) async throws -> YFJSONValue { try await fetch(type: .currency, count: count) }
    public func cryptocurrency(count: Int = 25) async throws -> YFJSONValue { try await fetch(type: .cryptocurrency, count: count) }
    public func allTable(count: Int = 25) async throws -> YFTable { try await fetchTable(type: .all, count: count) }
    public func stockTable(count: Int = 25) async throws -> YFTable { try await fetchTable(type: .equity, count: count) }
    public func mutualfundTable(count: Int = 25) async throws -> YFTable { try await fetchTable(type: .mutualfund, count: count) }
    public func etfTable(count: Int = 25) async throws -> YFTable { try await fetchTable(type: .etf, count: count) }
    public func indexTable(count: Int = 25) async throws -> YFTable { try await fetchTable(type: .index, count: count) }
    public func futureTable(count: Int = 25) async throws -> YFTable { try await fetchTable(type: .future, count: count) }
    public func currencyTable(count: Int = 25) async throws -> YFTable { try await fetchTable(type: .currency, count: count) }
    public func cryptocurrencyTable(count: Int = 25) async throws -> YFTable { try await fetchTable(type: .cryptocurrency, count: count) }

    // Python-style aliases
    public func getAll(count: Int = 25) async throws -> YFJSONValue { try await all(count: count) }
    public func getStock(count: Int = 25) async throws -> YFJSONValue { try await stock(count: count) }
    public func getMutualfund(count: Int = 25) async throws -> YFJSONValue { try await mutualfund(count: count) }
    public func getEtf(count: Int = 25) async throws -> YFJSONValue { try await etf(count: count) }
    public func getIndex(count: Int = 25) async throws -> YFJSONValue { try await index(count: count) }
    public func getFuture(count: Int = 25) async throws -> YFJSONValue { try await future(count: count) }
    public func getCurrency(count: Int = 25) async throws -> YFJSONValue { try await currency(count: count) }
    public func getCryptocurrency(count: Int = 25) async throws -> YFJSONValue { try await cryptocurrency(count: count) }
    public func getAllTable(count: Int = 25) async throws -> YFTable { try await allTable(count: count) }
    public func getStockTable(count: Int = 25) async throws -> YFTable { try await stockTable(count: count) }
    public func getMutualfundTable(count: Int = 25) async throws -> YFTable { try await mutualfundTable(count: count) }
    public func getEtfTable(count: Int = 25) async throws -> YFTable { try await etfTable(count: count) }
    public func getIndexTable(count: Int = 25) async throws -> YFTable { try await indexTable(count: count) }
    public func getFutureTable(count: Int = 25) async throws -> YFTable { try await futureTable(count: count) }
    public func getCurrencyTable(count: Int = 25) async throws -> YFTable { try await currencyTable(count: count) }
    public func getCryptocurrencyTable(count: Int = 25) async throws -> YFTable { try await cryptocurrencyTable(count: count) }
    public func get_all(count: Int = 25) async throws -> YFJSONValue { try await getAll(count: count) }
    public func get_stock(count: Int = 25) async throws -> YFJSONValue { try await getStock(count: count) }
    public func get_mutualfund(count: Int = 25) async throws -> YFJSONValue { try await getMutualfund(count: count) }
    public func get_etf(count: Int = 25) async throws -> YFJSONValue { try await getEtf(count: count) }
    public func get_index(count: Int = 25) async throws -> YFJSONValue { try await getIndex(count: count) }
    public func get_future(count: Int = 25) async throws -> YFJSONValue { try await getFuture(count: count) }
    public func get_currency(count: Int = 25) async throws -> YFJSONValue { try await getCurrency(count: count) }
    public func get_cryptocurrency(count: Int = 25) async throws -> YFJSONValue { try await getCryptocurrency(count: count) }
    public func get_all_table(count: Int = 25) async throws -> YFTable { try await getAllTable(count: count) }
    public func get_stock_table(count: Int = 25) async throws -> YFTable { try await getStockTable(count: count) }
    public func get_mutualfund_table(count: Int = 25) async throws -> YFTable { try await getMutualfundTable(count: count) }
    public func get_etf_table(count: Int = 25) async throws -> YFTable { try await getEtfTable(count: count) }
    public func get_index_table(count: Int = 25) async throws -> YFTable { try await getIndexTable(count: count) }
    public func get_future_table(count: Int = 25) async throws -> YFTable { try await getFutureTable(count: count) }
    public func get_currency_table(count: Int = 25) async throws -> YFTable { try await getCurrencyTable(count: count) }
    public func get_cryptocurrency_table(count: Int = 25) async throws -> YFTable { try await getCryptocurrencyTable(count: count) }
}

public struct YFMarket: Sendable {
    public let market: String
    public let timeout: TimeInterval
    private let client: YFinanceClient

    public init(
        _ market: String,
        timeout: TimeInterval = 30,
        client: YFinanceClient = YFinanceClient()
    ) {
        self.init(market: market, timeout: timeout, client: client)
    }

    public init(
        market: String,
        timeout: TimeInterval = 30,
        client: YFinanceClient = YFinanceClient()
    ) {
        self.market = market
        self.timeout = timeout
        self.client = client
    }

    public func summary() async throws -> YFJSONValue {
        let raw = try await client.marketSummary(market: market, timeout: timeout)
        return raw["marketSummaryResponse"]?["result"] ?? .array([])
    }

    public func summaryTable() async throws -> YFTable {
        try await summary().toTable()
    }

    public func status() async throws -> YFJSONValue {
        let raw = try await client.marketTime(market: market, timeout: timeout)
        return raw["finance"]?["marketTimes"]?[0]?["marketTime"]?[0] ?? .object([:])
    }

    public func statusTable() async throws -> YFTable {
        try await status().toTable()
    }

    // Python-style aliases.
    public func getSummary() async throws -> YFJSONValue { try await summary() }
    public func getStatus() async throws -> YFJSONValue { try await status() }
    public func get_summary() async throws -> YFJSONValue { try await getSummary() }
    public func get_status() async throws -> YFJSONValue { try await getStatus() }
}

extension YFLookup {
    private func logSuppressedError(_ error: Error, context: String) async {
        let debugEnabled = await YFConfigStore.shared.debug.enabled
        if debugEnabled {
            print("[YFinanceKit] Suppressed \(context) error: \(error)")
        }
    }
}

public struct YFSector: Sendable, CustomStringConvertible {
    public let key: String
    private let client: YFinanceClient

    public init(_ key: String, client: YFinanceClient = YFinanceClient()) {
        self.init(key: key, client: client)
    }

    public init(key: String, client: YFinanceClient = YFinanceClient()) {
        self.key = key
        self.client = client
    }

    public func data() async throws -> YFDomainData {
        let raw = try await client.domainEntity(type: YFDomainType.sectors.rawValue, key: key)
        return Self.parseDomain(key: key, raw: raw)
    }

    public func topEtfs() async throws -> YFJSONValue {
        (try await data()).raw["data"]?["topETFs"] ?? .array([])
    }

    public func topMutualFunds() async throws -> YFJSONValue {
        (try await data()).raw["data"]?["topMutualFunds"] ?? .array([])
    }

    public func industries() async throws -> YFJSONValue {
        (try await data()).raw["data"]?["industries"] ?? .array([])
    }

    public func industriesTable() async throws -> YFTable {
        let items = try await industries().arrayValue ?? []
        let rows: [[String: YFJSONValue]] = items.compactMap { item in
            let name = item["name"]?.stringValue ?? ""
            if name == "All Industries" {
                return nil
            }
            return [
                "key": item["key"] ?? .null,
                "name": item["name"] ?? .null,
                "symbol": item["symbol"] ?? .null,
                "market weight": item["marketWeight"]?["raw"] ?? item["marketWeight"] ?? .null,
            ]
        }
        return YFTable(columns: ["key", "name", "symbol", "market weight"], rows: rows)
    }

    // Python-style aliases.
    public func getTopEtfs() async throws -> YFJSONValue { try await topEtfs() }
    public func getTopMutualFunds() async throws -> YFJSONValue { try await topMutualFunds() }
    public func getIndustries() async throws -> YFJSONValue { try await industries() }
    public func getIndustriesTable() async throws -> YFTable { try await industriesTable() }
    public func top_etfs() async throws -> YFJSONValue { try await topEtfs() }
    public func top_mutual_funds() async throws -> YFJSONValue { try await topMutualFunds() }
    public func get_top_etfs() async throws -> YFJSONValue { try await getTopEtfs() }
    public func get_top_mutual_funds() async throws -> YFJSONValue { try await getTopMutualFunds() }
    public func get_industries() async throws -> YFJSONValue { try await getIndustries() }
    public func get_industries_table() async throws -> YFTable { try await getIndustriesTable() }

    public var description: String {
        "yfinance.Sector object <\(key)>"
    }
}

public struct YFIndustry: Sendable, CustomStringConvertible {
    public let key: String
    private let client: YFinanceClient

    public init(_ key: String, client: YFinanceClient = YFinanceClient()) {
        self.init(key: key, client: client)
    }

    public init(key: String, client: YFinanceClient = YFinanceClient()) {
        self.key = key
        self.client = client
    }

    public func data() async throws -> YFDomainData {
        let raw = try await client.domainEntity(type: YFDomainType.industries.rawValue, key: key)
        return Self.parseDomain(key: key, raw: raw)
    }

    public func sectorKey() async throws -> String? {
        (try await data()).raw["data"]?["sectorKey"]?.stringValue
    }

    public func sectorName() async throws -> String? {
        (try await data()).raw["data"]?["sectorName"]?.stringValue
    }

    public func topPerformingCompanies() async throws -> YFJSONValue {
        (try await data()).raw["data"]?["topPerformingCompanies"] ?? .array([])
    }

    public func topGrowthCompanies() async throws -> YFJSONValue {
        (try await data()).raw["data"]?["topGrowthCompanies"] ?? .array([])
    }

    public func topPerformingCompaniesTable() async throws -> YFTable {
        let items = try await topPerformingCompanies().arrayValue ?? []
        let rows: [[String: YFJSONValue]] = items.map { item in
            [
                "symbol": item["symbol"] ?? .null,
                "name": item["name"] ?? .null,
                "ytd return": item["ytdReturn"]?["raw"] ?? item["ytdReturn"] ?? .null,
                "last price": item["lastPrice"]?["raw"] ?? item["lastPrice"] ?? .null,
                "target price": item["targetPrice"]?["raw"] ?? item["targetPrice"] ?? .null,
            ]
        }
        return YFTable(columns: ["symbol", "name", "ytd return", "last price", "target price"], rows: rows)
    }

    public func topGrowthCompaniesTable() async throws -> YFTable {
        let items = try await topGrowthCompanies().arrayValue ?? []
        let rows: [[String: YFJSONValue]] = items.map { item in
            [
                "symbol": item["symbol"] ?? .null,
                "name": item["name"] ?? .null,
                "ytd return": item["ytdReturn"]?["raw"] ?? item["ytdReturn"] ?? .null,
                "growth estimate": item["growthEstimate"]?["raw"] ?? item["growthEstimate"] ?? .null,
            ]
        }
        return YFTable(columns: ["symbol", "name", "ytd return", "growth estimate"], rows: rows)
    }

    // Python-style aliases.
    public func getTopPerformingCompanies() async throws -> YFJSONValue { try await topPerformingCompanies() }
    public func getTopGrowthCompanies() async throws -> YFJSONValue { try await topGrowthCompanies() }
    public func getTopPerformingCompaniesTable() async throws -> YFTable { try await topPerformingCompaniesTable() }
    public func getTopGrowthCompaniesTable() async throws -> YFTable { try await topGrowthCompaniesTable() }
    public func sector_key() async throws -> String? { try await sectorKey() }
    public func sector_name() async throws -> String? { try await sectorName() }
    public func top_performing_companies() async throws -> YFJSONValue { try await topPerformingCompanies() }
    public func top_growth_companies() async throws -> YFJSONValue { try await topGrowthCompanies() }
    public func get_top_performing_companies() async throws -> YFJSONValue { try await getTopPerformingCompanies() }
    public func get_top_growth_companies() async throws -> YFJSONValue { try await getTopGrowthCompanies() }
    public func get_top_performing_companies_table() async throws -> YFTable { try await getTopPerformingCompaniesTable() }
    public func get_top_growth_companies_table() async throws -> YFTable { try await getTopGrowthCompaniesTable() }
    public func get_sector_key() async throws -> String? { try await sectorKey() }
    public func get_sector_name() async throws -> String? { try await sectorName() }

    public var description: String {
        "yfinance.Industry object <\(key)>"
    }
}

private extension YFSector {
    static func parseDomain(key: String, raw: YFJSONValue) -> YFDomainData {
        let data = raw["data"]
        return YFDomainData(
            key: key,
            name: data?["name"]?.stringValue,
            symbol: data?["symbol"]?.stringValue,
            overview: data?["overview"],
            topCompanies: data?["topCompanies"]?.arrayValue ?? [],
            researchReports: data?["researchReports"]?.arrayValue ?? [],
            raw: raw
        )
    }
}

private extension YFIndustry {
    static func parseDomain(key: String, raw: YFJSONValue) -> YFDomainData {
        let data = raw["data"]
        return YFDomainData(
            key: key,
            name: data?["name"]?.stringValue,
            symbol: data?["symbol"]?.stringValue,
            overview: data?["overview"],
            topCompanies: data?["topCompanies"]?.arrayValue ?? [],
            researchReports: data?["researchReports"]?.arrayValue ?? [],
            raw: raw
        )
    }
}
