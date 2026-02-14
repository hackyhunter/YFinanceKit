import Foundation

public struct YFFundsData: Sendable {
    public let symbol: String
    public let raw: YFJSONValue
    public let quoteType: String?
    public let description: String
    public let fundOverview: [String: YFJSONValue]
    public let fundOperations: YFTable
    public let assetClasses: [String: YFJSONValue]
    public let topHoldings: YFTable
    public let equityHoldings: YFTable
    public let bondHoldings: YFTable
    public let bondRatings: [String: YFJSONValue]
    public let sectorWeightings: [String: YFJSONValue]

    // Python-style aliases.
    public var fund_overview: [String: YFJSONValue] { fundOverview }
    public var fund_operations: YFTable { fundOperations }
    public var asset_classes: [String: YFJSONValue] { assetClasses }
    public var top_holdings: YFTable { topHoldings }
    public var equity_holdings: YFTable { equityHoldings }
    public var bond_holdings: YFTable { bondHoldings }
    public var bond_ratings: [String: YFJSONValue] { bondRatings }
    public var sector_weightings: [String: YFJSONValue] { sectorWeightings }

    public init(symbol: String, raw: YFJSONValue) {
        self.symbol = symbol
        self.raw = raw

        self.quoteType = Self.rawValue(raw["quoteType"]?["quoteType"])?.stringValue

        self.description = raw["summaryProfile"]?["longBusinessSummary"]?.stringValue ?? ""

        let fundProfile = raw["fundProfile"]
        self.fundOverview = [
            "categoryName": Self.rawValue(fundProfile?["categoryName"]) ?? .null,
            "family": Self.rawValue(fundProfile?["family"]) ?? .null,
            "legalType": Self.rawValue(fundProfile?["legalType"]) ?? .null,
        ]

        let fees = fundProfile?["feesExpensesInvestment"]
        let feesCat = fundProfile?["feesExpensesInvestmentCat"]
        self.fundOperations = YFTable(
            columns: ["Attributes", symbol, "Category Average"],
            rows: [
                [
                    "Attributes": .string("Annual Report Expense Ratio"),
                    symbol: Self.rawValue(fees?["annualReportExpenseRatio"]) ?? .null,
                    "Category Average": Self.rawValue(feesCat?["annualReportExpenseRatio"]) ?? .null,
                ],
                [
                    "Attributes": .string("Annual Holdings Turnover"),
                    symbol: Self.rawValue(fees?["annualHoldingsTurnover"]) ?? .null,
                    "Category Average": Self.rawValue(feesCat?["annualHoldingsTurnover"]) ?? .null,
                ],
                [
                    "Attributes": .string("Total Net Assets"),
                    symbol: Self.rawValue(fees?["totalNetAssets"]) ?? .null,
                    "Category Average": Self.rawValue(feesCat?["totalNetAssets"]) ?? .null,
                ],
            ]
        )

        let topHoldingsSection = raw["topHoldings"]
        self.assetClasses = [
            "cashPosition": Self.rawValue(topHoldingsSection?["cashPosition"]) ?? .null,
            "stockPosition": Self.rawValue(topHoldingsSection?["stockPosition"]) ?? .null,
            "bondPosition": Self.rawValue(topHoldingsSection?["bondPosition"]) ?? .null,
            "preferredPosition": Self.rawValue(topHoldingsSection?["preferredPosition"]) ?? .null,
            "convertiblePosition": Self.rawValue(topHoldingsSection?["convertiblePosition"]) ?? .null,
            "otherPosition": Self.rawValue(topHoldingsSection?["otherPosition"]) ?? .null,
        ]

        let holdingsRows = (topHoldingsSection?["holdings"]?.arrayValue ?? []).map { item in
            [
                "Symbol": item["symbol"] ?? .null,
                "Name": item["holdingName"] ?? .null,
                "Holding Percent": Self.rawValue(item["holdingPercent"]) ?? .null,
            ]
        }
        self.topHoldings = YFTable(columns: ["Symbol", "Name", "Holding Percent"], rows: holdingsRows)

        let equity = topHoldingsSection?["equityHoldings"]
        self.equityHoldings = YFTable(
            columns: ["Average", symbol, "Category Average"],
            rows: [
                Self.metricRow("Average", "Price/Earnings", "priceToEarnings", equity, symbol),
                Self.metricRow("Average", "Price/Book", "priceToBook", equity, symbol),
                Self.metricRow("Average", "Price/Sales", "priceToSales", equity, symbol),
                Self.metricRow("Average", "Price/Cashflow", "priceToCashflow", equity, symbol),
                Self.metricRow("Average", "Median Market Cap", "medianMarketCap", equity, symbol),
                Self.metricRow("Average", "3 Year Earnings Growth", "threeYearEarningsGrowth", equity, symbol),
            ]
        )

        let bond = topHoldingsSection?["bondHoldings"]
        self.bondHoldings = YFTable(
            columns: ["Average", symbol, "Category Average"],
            rows: [
                Self.metricRow("Average", "Duration", "duration", bond, symbol),
                Self.metricRow("Average", "Maturity", "maturity", bond, symbol),
                Self.metricRow("Average", "Credit Quality", "creditQuality", bond, symbol),
            ]
        )

        self.bondRatings = Self.flattenKeyValueArray(topHoldingsSection?["bondRatings"]?.arrayValue ?? [])
        self.sectorWeightings = Self.flattenKeyValueArray(topHoldingsSection?["sectorWeightings"]?.arrayValue ?? [])
    }

    public func rawTable() -> YFTable {
        raw.toTable()
    }

    public func quote_type() -> String? {
        quoteType
    }

    private static func metricRow(
        _ labelColumn: String,
        _ label: String,
        _ key: String,
        _ object: YFJSONValue?,
        _ symbol: String
    ) -> [String: YFJSONValue] {
        [
            labelColumn: .string(label),
            symbol: rawValue(object?[key]) ?? .null,
            "Category Average": rawValue(object?["\(key)Cat"]) ?? .null,
        ]
    }

    private static func flattenKeyValueArray(_ array: [YFJSONValue]) -> [String: YFJSONValue] {
        var output: [String: YFJSONValue] = [:]
        for item in array {
            guard let object = item.objectValue else { continue }
            for (key, value) in object {
                output[key] = rawValue(value) ?? value
            }
        }
        return output
    }

    private static func rawValue(_ value: YFJSONValue?) -> YFJSONValue? {
        guard let value else { return nil }
        if let raw = value["raw"] {
            return raw
        }
        return value
    }
}
