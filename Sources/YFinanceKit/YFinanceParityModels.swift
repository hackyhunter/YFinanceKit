import Foundation

public enum YFQuoteSummaryModule: String, CaseIterable, Sendable {
    case summaryProfile
    case summaryDetail
    case assetProfile
    case fundProfile
    case price
    case quoteType
    case esgScores
    case incomeStatementHistory
    case incomeStatementHistoryQuarterly
    case balanceSheetHistory
    case balanceSheetHistoryQuarterly
    case cashFlowStatementHistory
    case cashFlowStatementHistoryQuarterly
    case defaultKeyStatistics
    case financialData
    case calendarEvents
    case secFilings
    case upgradeDowngradeHistory
    case institutionOwnership
    case fundOwnership
    case majorDirectHolders
    case majorHoldersBreakdown
    case insiderTransactions
    case insiderHolders
    case netSharePurchaseActivity
    case earnings
    case earningsHistory
    case earningsTrend
    case industryTrend
    case indexTrend
    case sectorTrend
    case recommendationTrend
    case futuresChain
}

public let QUOTE_SUMMARY_VALID_MODULES: [String] = YFQuoteSummaryModule.allCases.map(\.rawValue)

public enum YFFinancialFrequency: String, Sendable {
    case yearly
    case quarterly
    case trailing

    public init?(pythonValue: String) {
        switch pythonValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "yearly", "annual", "year":
            self = .yearly
        case "quarterly", "quarter", "q":
            self = .quarterly
        case "trailing", "ttm":
            self = .trailing
        default:
            return nil
        }
    }
}

public enum YFNewsTab: String, Sendable {
    case news
    case all
    case pressReleases = "press releases"
}

public enum YFGroupBy: String, Sendable {
    case column
    case ticker

    public init?(pythonValue: String) {
        switch pythonValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "column":
            self = .column
        case "ticker":
            self = .ticker
        default:
            return nil
        }
    }
}

public struct YFOptionsChain: Sendable {
    public let expirationDates: [Date]
    public let underlying: YFJSONValue?
    public let calls: [YFOptionContract]
    public let puts: [YFOptionContract]

    public init(
        expirationDates: [Date],
        underlying: YFJSONValue?,
        calls: [YFOptionContract],
        puts: [YFOptionContract]
    ) {
        self.expirationDates = expirationDates
        self.underlying = underlying
        self.calls = calls
        self.puts = puts
    }

    public func callsTable() -> YFTable {
        Self.contractsTable(calls)
    }

    public func putsTable() -> YFTable {
        Self.contractsTable(puts)
    }

    private static func contractsTable(_ contracts: [YFOptionContract]) -> YFTable {
        let columns = [
            "contractSymbol",
            "lastTradeDate",
            "strike",
            "lastPrice",
            "bid",
            "ask",
            "change",
            "percentChange",
            "volume",
            "openInterest",
            "impliedVolatility",
            "inTheMoney",
            "contractSize",
            "currency",
        ]
        let rows: [[String: YFJSONValue]] = contracts.map { contract in
            var row: [String: YFJSONValue] = [:]
            row["contractSymbol"] = contract.contractSymbol.map { .string($0) } ?? .null
            row["lastTradeDate"] = contract.lastTradeDate.map { .number($0.timeIntervalSince1970) } ?? .null
            row["strike"] = contract.strike.map { .number($0) } ?? .null
            row["lastPrice"] = contract.lastPrice.map { .number($0) } ?? .null
            row["bid"] = contract.bid.map { .number($0) } ?? .null
            row["ask"] = contract.ask.map { .number($0) } ?? .null
            row["change"] = contract.change.map { .number($0) } ?? .null
            row["percentChange"] = contract.percentChange.map { .number($0) } ?? .null
            row["volume"] = contract.volume.map { .number(Double($0)) } ?? .null
            row["openInterest"] = contract.openInterest.map { .number(Double($0)) } ?? .null
            row["impliedVolatility"] = contract.impliedVolatility.map { .number($0) } ?? .null
            row["inTheMoney"] = contract.inTheMoney.map { .bool($0) } ?? .null
            row["contractSize"] = contract.contractSize.map { .string($0) } ?? .null
            row["currency"] = contract.currency.map { .string($0) } ?? .null
            return row
        }
        return YFTable(columns: columns, rows: rows)
    }
}

public struct YFOptionContract: Sendable {
    public let contractSymbol: String?
    public let lastTradeDate: Date?
    public let strike: Double?
    public let lastPrice: Double?
    public let bid: Double?
    public let ask: Double?
    public let change: Double?
    public let percentChange: Double?
    public let volume: Int?
    public let openInterest: Int?
    public let impliedVolatility: Double?
    public let inTheMoney: Bool?
    public let contractSize: String?
    public let currency: String?
    public let raw: YFJSONValue

    init(raw: YFJSONValue) {
        self.raw = raw
        self.contractSymbol = raw["contractSymbol"]?.stringValue
        if let timestamp = raw["lastTradeDate"]?.doubleValue {
            self.lastTradeDate = Date(timeIntervalSince1970: timestamp)
        } else {
            self.lastTradeDate = nil
        }
        self.strike = raw["strike"]?.doubleValue
        self.lastPrice = raw["lastPrice"]?.doubleValue
        self.bid = raw["bid"]?.doubleValue
        self.ask = raw["ask"]?.doubleValue
        self.change = raw["change"]?.doubleValue
        self.percentChange = raw["percentChange"]?.doubleValue
        self.volume = raw["volume"]?.intValue
        self.openInterest = raw["openInterest"]?.intValue
        self.impliedVolatility = raw["impliedVolatility"]?.doubleValue
        self.inTheMoney = raw["inTheMoney"]?.boolValue
        self.contractSize = raw["contractSize"]?.stringValue
        self.currency = raw["currency"]?.stringValue
    }
}

public enum YFLookupType: String, CaseIterable, Sendable {
    case all
    case equity
    case mutualfund
    case etf
    case index
    case future
    case currency
    case cryptocurrency
}

public let LOOKUP_TYPES: [String] = YFLookupType.allCases.map(\.rawValue)

public enum YFDomainType: String, Sendable {
    case sectors
    case industries
}

public enum YFCalendarType: String, CaseIterable, Sendable {
    case spEarnings = "sp_earnings"
    case ipoInfo = "ipo_info"
    case economicEvent = "economic_event"
    case splits
}

public let PREDEFINED_CALENDARS: [String: YFJSONValue] = [
    "sp_earnings": .object([
        "sortField": .string("intradaymarketcap"),
        "includeFields": .array([
            .string("ticker"),
            .string("companyshortname"),
            .string("intradaymarketcap"),
            .string("eventname"),
            .string("startdatetime"),
            .string("startdatetimetype"),
            .string("epsestimate"),
            .string("epsactual"),
            .string("epssurprisepct"),
        ]),
        "nan_cols": .array([
            .string("Surprise (%)"),
            .string("EPS Estimate"),
            .string("Reported EPS"),
        ]),
        "datetime_cols": .array([
            .string("Event Start Date"),
        ]),
        "df_index": .string("Symbol"),
        "renames": .object([
            "Surprise (%)": .string("Surprise(%)"),
            "Company Name": .string("Company"),
            "Market Cap (Intraday)": .string("Marketcap"),
        ]),
    ]),
    "ipo_info": .object([
        "sortField": .string("startdatetime"),
        "includeFields": .array([
            .string("ticker"),
            .string("companyshortname"),
            .string("exchange_short_name"),
            .string("filingdate"),
            .string("startdatetime"),
            .string("amendeddate"),
            .string("pricefrom"),
            .string("priceto"),
            .string("offerprice"),
            .string("currencyname"),
            .string("shares"),
            .string("dealtype"),
        ]),
        "nan_cols": .array([
            .string("Price From"),
            .string("Price To"),
            .string("Price"),
            .string("Shares"),
        ]),
        "datetime_cols": .array([
            .string("Filing Date"),
            .string("Date"),
            .string("Amended Date"),
        ]),
        "df_index": .string("Symbol"),
        "renames": .object([
            "Exchange Short Name": .string("Exchange"),
        ]),
    ]),
    "economic_event": .object([
        "sortField": .string("startdatetime"),
        "includeFields": .array([
            .string("econ_release"),
            .string("country_code"),
            .string("startdatetime"),
            .string("period"),
            .string("after_release_actual"),
            .string("consensus_estimate"),
            .string("prior_release_actual"),
            .string("originally_reported_actual"),
        ]),
        "nan_cols": .array([
            .string("Actual"),
            .string("Market Expectation"),
            .string("Prior to This"),
            .string("Revised from"),
        ]),
        "datetime_cols": .array([
            .string("Event Time"),
        ]),
        "df_index": .string("Event"),
        "renames": .object([
            "Country Code": .string("Region"),
            "Market Expectation": .string("Expected"),
            "Prior to This": .string("Last"),
            "Revised from": .string("Revised"),
        ]),
    ]),
    "splits": .object([
        "sortField": .string("startdatetime"),
        "includeFields": .array([
            .string("ticker"),
            .string("companyshortname"),
            .string("startdatetime"),
            .string("optionable"),
            .string("old_share_worth"),
            .string("share_worth"),
        ]),
        "nan_cols": .array([]),
        "datetime_cols": .array([
            .string("Payable On"),
        ]),
        "df_index": .string("Symbol"),
        "renames": .object([
            "Optionable?": .string("Optionable"),
        ]),
    ]),
]

public let PREDEFINED_CALENDAR_IDS: [String] = YFCalendarType.allCases.map(\.rawValue)

public enum YFScreenerQuoteType: String, Sendable {
    case equity = "EQUITY"
    case mutualFund = "MUTUALFUND"
}

public enum YFPredefinedScreenerQuery: String, CaseIterable, Sendable {
    case aggressiveSmallCaps = "aggressive_small_caps"
    case dayGainers = "day_gainers"
    case dayLosers = "day_losers"
    case growthTechnologyStocks = "growth_technology_stocks"
    case mostActives = "most_actives"
    case mostShortedStocks = "most_shorted_stocks"
    case smallCapGainers = "small_cap_gainers"
    case undervaluedGrowthStocks = "undervalued_growth_stocks"
    case undervaluedLargeCaps = "undervalued_large_caps"
    case conservativeForeignFunds = "conservative_foreign_funds"
    case highYieldBond = "high_yield_bond"
    case portfolioAnchors = "portfolio_anchors"
    case solidLargeGrowthFunds = "solid_large_growth_funds"
    case solidMidcapGrowthFunds = "solid_midcap_growth_funds"
    case topMutualFunds = "top_mutual_funds"
}

internal struct YFPredefinedScreenerDefinition: Sendable {
    let id: String
    let quoteType: YFScreenerQuoteType
    let sortField: String
    let sortType: String
    let query: YFScreenerQuery
    let count: Int?
    let offset: Int?
}

// The source-of-truth definitions used to power PREDEFINED_SCREENER_QUERIES and
// to emulate Python's "offset switches to non-predefined API" behavior.
internal let _PREDEFINED_SCREENER_DEFINITIONS: [String: YFPredefinedScreenerDefinition] = {
    func and(_ queries: [YFScreenerQuery]) -> YFScreenerQuery {
        YFQueryBuilder.and(queries)
    }

    func eq(_ field: String, _ value: YFScreenerOperand) -> YFScreenerQuery {
        YFQueryBuilder.eq(field, value)
    }

    func gt(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFQueryBuilder.gt(field, value)
    }

    func gte(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFQueryBuilder.gte(field, value)
    }

    func lt(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFQueryBuilder.lt(field, value)
    }

    func btwn(_ field: String, _ low: Double, _ high: Double) -> YFScreenerQuery {
        YFQueryBuilder.btwn(field, low, high)
    }

    func isIn(_ field: String, _ values: [YFScreenerOperand]) -> YFScreenerQuery {
        YFQueryBuilder.isIn(field, values)
    }

    func def(
        _ id: String,
        quoteType: YFScreenerQuoteType,
        sortField: String,
        sortType: String,
        query: YFScreenerQuery,
        count: Int? = nil,
        offset: Int? = nil
    ) -> (String, YFPredefinedScreenerDefinition) {
        (
            id,
            YFPredefinedScreenerDefinition(
                id: id,
                quoteType: quoteType,
                sortField: sortField,
                sortType: sortType,
                query: query,
                count: count,
                offset: offset
            )
        )
    }

    // Mirror yfinance's PREDEFINED_SCREENER_QUERIES as of Dec-2024.
    // Source: yfinance/screener/screener.py
    return Dictionary(uniqueKeysWithValues: [
        def(
            "aggressive_small_caps",
            quoteType: .equity,
            sortField: "eodvolume",
            sortType: "desc",
            query: and([
                isIn("exchange", [.string("NMS"), .string("NYQ")]),
                lt("epsgrowth.lasttwelvemonths", 15),
            ])
        ),
        def(
            "day_gainers",
            quoteType: .equity,
            sortField: "percentchange",
            sortType: "DESC",
            query: and([
                gt("percentchange", 3),
                eq("region", .string("us")),
                gte("intradaymarketcap", 2_000_000_000),
                gte("intradayprice", 5),
                gt("dayvolume", 15_000),
            ])
        ),
        def(
            "day_losers",
            quoteType: .equity,
            sortField: "percentchange",
            sortType: "ASC",
            query: and([
                lt("percentchange", -2.5),
                eq("region", .string("us")),
                gte("intradaymarketcap", 2_000_000_000),
                gte("intradayprice", 5),
                gt("dayvolume", 20_000),
            ])
        ),
        def(
            "growth_technology_stocks",
            quoteType: .equity,
            sortField: "eodvolume",
            sortType: "desc",
            query: and([
                gte("quarterlyrevenuegrowth.quarterly", 25),
                gte("epsgrowth.lasttwelvemonths", 25),
                eq("sector", .string("Technology")),
                isIn("exchange", [.string("NMS"), .string("NYQ")]),
            ])
        ),
        def(
            "most_actives",
            quoteType: .equity,
            sortField: "dayvolume",
            sortType: "DESC",
            query: and([
                eq("region", .string("us")),
                gte("intradaymarketcap", 2_000_000_000),
                gt("dayvolume", 5_000_000),
            ])
        ),
        def(
            "most_shorted_stocks",
            quoteType: .equity,
            sortField: "short_percentage_of_shares_outstanding.value",
            sortType: "DESC",
            query: and([
                eq("region", .string("us")),
                gt("intradayprice", 1),
                gt("avgdailyvol3m", 200_000),
            ]),
            count: 25,
            offset: 0
        ),
        def(
            "small_cap_gainers",
            quoteType: .equity,
            sortField: "eodvolume",
            sortType: "desc",
            query: and([
                lt("intradaymarketcap", 2_000_000_000),
                isIn("exchange", [.string("NMS"), .string("NYQ")]),
            ])
        ),
        def(
            "undervalued_growth_stocks",
            quoteType: .equity,
            sortField: "eodvolume",
            sortType: "DESC",
            query: and([
                btwn("peratio.lasttwelvemonths", 0, 20),
                lt("pegratio_5y", 1),
                gte("epsgrowth.lasttwelvemonths", 25),
                isIn("exchange", [.string("NMS"), .string("NYQ")]),
            ])
        ),
        def(
            "undervalued_large_caps",
            quoteType: .equity,
            sortField: "eodvolume",
            sortType: "desc",
            query: and([
                btwn("peratio.lasttwelvemonths", 0, 20),
                lt("pegratio_5y", 1),
                btwn("intradaymarketcap", 10_000_000_000, 100_000_000_000),
                isIn("exchange", [.string("NMS"), .string("NYQ")]),
            ])
        ),
        def(
            "conservative_foreign_funds",
            quoteType: .mutualFund,
            sortField: "fundnetassets",
            sortType: "DESC",
            query: and([
                isIn("categoryname", [
                    .string("Foreign Large Value"),
                    .string("Foreign Large Blend"),
                    .string("Foreign Large Growth"),
                    .string("Foreign Small/Mid Growth"),
                    .string("Foreign Small/Mid Blend"),
                    .string("Foreign Small/Mid Value"),
                ]),
                isIn("performanceratingoverall", [.int(4), .int(5)]),
                lt("initialinvestment", 100_001),
                lt("annualreturnnavy1categoryrank", 50),
                isIn("riskratingoverall", [.int(1), .int(2), .int(3)]),
                eq("exchange", .string("NAS")),
            ])
        ),
        def(
            "high_yield_bond",
            quoteType: .mutualFund,
            sortField: "fundnetassets",
            sortType: "DESC",
            query: and([
                isIn("performanceratingoverall", [.int(4), .int(5)]),
                lt("initialinvestment", 100_001),
                lt("annualreturnnavy1categoryrank", 50),
                isIn("riskratingoverall", [.int(1), .int(2), .int(3)]),
                eq("categoryname", .string("High Yield Bond")),
                eq("exchange", .string("NAS")),
            ])
        ),
        def(
            "portfolio_anchors",
            quoteType: .mutualFund,
            sortField: "fundnetassets",
            sortType: "DESC",
            query: and([
                eq("categoryname", .string("Large Blend")),
                isIn("performanceratingoverall", [.int(4), .int(5)]),
                lt("initialinvestment", 100_001),
                lt("annualreturnnavy1categoryrank", 50),
                eq("exchange", .string("NAS")),
            ])
        ),
        def(
            "solid_large_growth_funds",
            quoteType: .mutualFund,
            sortField: "fundnetassets",
            sortType: "DESC",
            query: and([
                eq("categoryname", .string("Large Growth")),
                isIn("performanceratingoverall", [.int(4), .int(5)]),
                lt("initialinvestment", 100_001),
                lt("annualreturnnavy1categoryrank", 50),
                eq("exchange", .string("NAS")),
            ])
        ),
        def(
            "solid_midcap_growth_funds",
            quoteType: .mutualFund,
            sortField: "fundnetassets",
            sortType: "DESC",
            query: and([
                eq("categoryname", .string("Mid-Cap Growth")),
                isIn("performanceratingoverall", [.int(4), .int(5)]),
                lt("initialinvestment", 100_001),
                lt("annualreturnnavy1categoryrank", 50),
                eq("exchange", .string("NAS")),
            ])
        ),
        def(
            "top_mutual_funds",
            quoteType: .mutualFund,
            sortField: "percentchange",
            sortType: "DESC",
            query: and([
                gt("intradayprice", 15),
                isIn("performanceratingoverall", [.int(4), .int(5)]),
                gt("initialinvestment", 1_000),
                eq("exchange", .string("NAS")),
            ])
        ),
    ])
}()

public let PREDEFINED_SCREENER_QUERIES: [String: YFJSONValue] = {
    var output: [String: YFJSONValue] = [:]
    output.reserveCapacity(_PREDEFINED_SCREENER_DEFINITIONS.count)

    for (id, def) in _PREDEFINED_SCREENER_DEFINITIONS {
        var object: [String: YFJSONValue] = [
            "sortField": .string(def.sortField),
            "sortType": .string(def.sortType),
            "query": def.query.toJSONValue(),
        ]
        if let count = def.count {
            object["count"] = .number(Double(count))
        }
        if let offset = def.offset {
            object["offset"] = .number(Double(offset))
        }
        output[id] = .object(object)
    }

    return output
}()

public let PREDEFINED_SCREENER_QUERY_IDS: [String] = YFPredefinedScreenerQuery.allCases.map(\.rawValue)

public let PREDEFINED_SCREENER_BODY_DEFAULTS: [String: YFJSONValue] = [
    "offset": .number(0),
    "count": .number(25),
    "userId": .string(""),
    "userIdType": .string("guid"),
]

public struct YFScreenerQuery: Sendable {
    public let `operator`: String
    public let operands: [YFScreenerOperand]

    public init(_ operator: String, operands: [YFScreenerOperand]) {
        self.operator = `operator`.uppercased()
        self.operands = operands
    }

    public func toJSONValue() -> YFJSONValue {
        // Python yfinance expands IS-IN into an OR of EQ operations before sending to Yahoo.
        if `operator` == "IS-IN", operands.count >= 2 {
            let field = operands[0]
            let eqQueries: [YFScreenerQuery] = operands.dropFirst().map { value in
                YFScreenerQuery("EQ", operands: [field, value])
            }
            return YFScreenerQuery("OR", operands: eqQueries.map { .query($0) }).toJSONValue()
        }

        return .object([
            "operator": .string(`operator`),
            "operands": .array(operands.map { $0.toJSONValue() }),
        ])
    }
}

public indirect enum YFScreenerOperand: Sendable {
    case query(YFScreenerQuery)
    case string(String)
    case int(Int)
    case double(Double)
    case bool(Bool)

    func toJSONValue() -> YFJSONValue {
        switch self {
        case .query(let value):
            return value.toJSONValue()
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

public struct YFScreenerRequest: Sendable {
    public var offset: Int = 0
    public var size: Int?
    public var count: Int = 25
    public var sortField: String = "ticker"
    public var sortAscending: Bool = false
    public var userId: String = ""
    public var userIdType: String = "guid"
    public var quoteType: YFScreenerQuoteType?
    public var query: YFScreenerQuery

    public init(query: YFScreenerQuery) {
        self.query = query
    }

    public func toJSONValue() -> YFJSONValue {
        var object: [String: YFJSONValue] = [
            "offset": .number(Double(offset)),
            "count": .number(Double(count)),
            "sortField": .string(sortField),
            "sortType": .string(sortAscending ? "ASC" : "DESC"),
            "userId": .string(userId),
            "userIdType": .string(userIdType),
            "query": query.toJSONValue(),
        ]
        if let size {
            object["size"] = .number(Double(size))
        }
        if let quoteType {
            object["quoteType"] = .string(quoteType.rawValue)
        }
        return .object(object)
    }
}

public struct YFDomainData: Sendable {
    public let key: String
    public let name: String?
    public let symbol: String?
    public let overview: YFJSONValue?
    public let topCompanies: [YFJSONValue]
    public let researchReports: [YFJSONValue]
    public let raw: YFJSONValue
}
