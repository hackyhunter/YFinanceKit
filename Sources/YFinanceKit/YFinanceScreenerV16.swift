import Foundation

// Parity additions from yfinance 1.3.0-1.6.0. Keep these separate from the
// generated February-2026 constants so upstream fixes can be reviewed clearly.
extension YFScreenerConst {
    static let equityFieldAdditions: Set<String> = [
        "dividendyield",
        "dividendpershare.lasttwelvemonths",
        "netepsbasic.lasttwelvemonths",
        "netepsdiluted.lasttwelvemonths",
    ]

    static let equityFieldRemovals: Set<String> = [
        // Upstream #2867 fixed a missing comma which had concatenated these.
        "netepsbasic.lasttwelvemonthsnetepsdiluted.lasttwelvemonths",
    ]

    static let equityValidValueAdditions: [String: Set<String>] = [
        "exchange": [
            "AMS", "AQS", "ASE", "ASX", "ATH", "BER", "BRU", "BSE", "BTS", "BUD", "BUE", "BVB", "BVC",
            "CAI", "CCS", "CNQ", "CPH", "CSE", "CXA", "CXE", "CXI", "DOH", "DUS", "DXE", "EBS", "ENX",
            "EUX", "FKA", "FRA", "GER", "HAM", "HAN", "HEL", "HKG", "ICE", "IOB", "ISE", "IST", "JKT",
            "JNB", "JPX", "KAR", "KLS", "KOE", "KSC", "KUW", "LIS", "LIT", "LSE", "MAD", "MCE", "MCX",
            "MDD", "MEX", "MIL", "MUN", "NAE", "NCM", "NEO", "NGM", "NMS", "NSI", "NYQ", "NZE", "OEM",
            "OQB", "OQX", "OSA", "OSL", "PAR", "PCX", "PHP", "PHS", "PNK", "PRA", "RIS", "SAO", "SAP",
            "SAU", "SES", "SET", "SGO", "SHH", "SHZ", "STO", "STU", "TAI", "TAL", "TLO", "TLV", "TOR",
            "TWO", "VAN", "VIE", "VSE", "WSE", "YHD",
        ],
        "region": [
            "ae", "ar", "at", "au", "be", "br", "ca", "ch", "cl", "cn", "co", "cz", "de", "dk", "ee", "eg",
            "es", "fi", "fr", "gb", "gr", "hk", "hu", "id", "ie", "il", "in", "is", "it", "jp", "kr", "kw",
            "lk", "lt", "lv", "mx", "my", "nl", "no", "nz", "pe", "ph", "pk", "pl", "pt", "qa", "ro", "ru",
            "sa", "se", "sg", "sr", "th", "tr", "tw", "us", "ve", "vn", "za",
        ],
    ]

    static let etfFields: Set<String> = [
        "categoryname",
        "fundfamilyname",
        "region",
        "primary_sector",
        "morningstar_economic_moat",
        "morningstar_stewardship",
        "morningstar_uncertainty",
        "morningstar_moat_trend",
        "morningstar_rating_change",
        "fundnetassets",
        "ticker",
        "annualreportgrossexpenseratio",
        "annualreportnetexpenseratio",
        "turnoverratio",
        "annualreturnnavy1",
        "annualreturnnavy1categoryrank",
        "annualreturnnavy3",
        "annualreturnnavy5",
        "avgdailyvol3m",
        "dayvolume",
        "eodvolume",
        "fiftytwowkpercentchange",
        "percentchange",
        "morningstar_last_close_price_to_fair_value",
        "morningstar_rating",
        "morningstar_rating_updated_time",
        "marketcapitalvaluelong",
        "initialinvestment",
        "performanceratingoverall",
        "quarterendtrailingreturnytd",
        "riskratingoverall",
        "trailing_3m_return",
        "trailing_ytd_return",
        "exchange",
        "eodprice",
        "intradaypricechange",
        "intradayprice",
    ]

    static let etfValidValues: [String: Set<String>] = [
        "exchange": equityValidValueAdditions["exchange"] ?? [],
        "region": equityValidValueAdditions["region"] ?? [],
        "morningstar_economic_moat": ["Wide", "Narrow", "None"],
        "morningstar_stewardship": ["Exemplary", "Standard", "Poor"],
        "morningstar_uncertainty": ["Low", "Medium", "High", "Very High", "Extreme"],
        "morningstar_moat_trend": ["Stable", "Positive", "Negative"],
        "morningstar_rating_change": ["Upgrade", "Downgrade"],
    ]
}

public enum YFETFQueryBuilder {
    public static func and(_ operands: [YFScreenerQuery]) -> YFScreenerQuery {
        YFQueryBuilder.and(operands)
    }

    public static func or(_ operands: [YFScreenerQuery]) -> YFScreenerQuery {
        YFQueryBuilder.or(operands)
    }

    public static func eq(_ field: String, _ value: YFScreenerOperand) -> YFScreenerQuery {
        YFQueryBuilder.eq(field, value)
    }

    public static func gt(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFQueryBuilder.gt(field, value)
    }

    public static func gte(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFQueryBuilder.gte(field, value)
    }

    public static func lt(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFQueryBuilder.lt(field, value)
    }

    public static func lte(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFQueryBuilder.lte(field, value)
    }

    public static func btwn(_ field: String, _ low: Double, _ high: Double) -> YFScreenerQuery {
        YFQueryBuilder.btwn(field, low, high)
    }

    public static func isIn(_ field: String, _ values: [YFScreenerOperand]) -> YFScreenerQuery {
        YFQueryBuilder.isIn(field, values)
    }
}

public extension YFinanceClient {
    /// Runs an ETF screener query using Yahoo's `quoteType = ETF` payload.
    /// This mirrors yfinance's ETFQuery support added in 1.3.0 without changing
    /// the older public `YFScreenerQuoteType` enum ABI.
    func screenETF(
        query: YFScreenerQuery,
        count: Int = 25,
        offset: Int = 0,
        sortField: String = "ticker",
        sortAscending: Bool = false,
        userId: String = "",
        userIdType: String = "guid"
    ) async throws -> YFJSONValue {
        guard count > 0, count <= 250 else {
            throw YFinanceError.invalidRequest("Yahoo limits screener count to 1...250")
        }
        try query.validateETF()

        let payload: YFJSONValue = .object([
            "offset": .number(Double(max(0, offset))),
            "size": .number(Double(count)),
            "sortField": .string(sortField),
            "sortType": .string(sortAscending ? "ASC" : "DESC"),
            "quoteType": .string("ETF"),
            "query": query.toJSONValue(),
            "userId": .string(userId),
            "userIdType": .string(userIdType),
        ])

        let raw = try await screener(body: payload)
        return raw["finance"]?["result"]?[0] ?? .object([:])
    }
}

public func screenETF(
    _ query: YFScreenerQuery,
    count: Int = 25,
    offset: Int = 0,
    sortField: String = "ticker",
    sortAscending: Bool = false,
    userId: String = "",
    userIdType: String = "guid",
    client: YFinanceClient = YFinanceClient()
) async throws -> YFJSONValue {
    try await client.screenETF(
        query: query,
        count: count,
        offset: offset,
        sortField: sortField,
        sortAscending: sortAscending,
        userId: userId,
        userIdType: userIdType
    )
}
