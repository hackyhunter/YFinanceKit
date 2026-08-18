import Foundation

public enum YFFinancialStatementKind: String, CaseIterable, Sendable {
    case income
    case balanceSheet = "balance-sheet"
    case cashFlow = "cash-flow"
}

public enum YFFundamentalsFetchMode: String, Sendable {
    case singleRequest
    case chunkedFallback
}

public struct YFFinancialPoint: Sendable, Equatable {
    public let asOfDate: String
    public let value: Double
    public let currencyCode: String?
    public let periodType: String?

    public init(asOfDate: String, value: Double, currencyCode: String? = nil, periodType: String? = nil) {
        self.asOfDate = asOfDate
        self.value = value
        self.currencyCode = currencyCode
        self.periodType = periodType
    }
}

/// A typed view over Yahoo's fundamentals-timeseries response.
///
/// `values` is keyed by the same metric names used by Python yfinance, for
/// example `TotalRevenue`, `NetIncome`, `TotalAssets`, and `FreeCashFlow`.
public struct YFFinancialStatementSeries: Sendable {
    public let symbol: String
    public let kind: YFFinancialStatementKind
    public let frequency: YFFinancialFrequency
    public let metricOrder: [String]
    public let values: [String: [YFFinancialPoint]]
    public let fetchMode: YFFundamentalsFetchMode
    public let raw: YFJSONValue

    public init(
        symbol: String,
        kind: YFFinancialStatementKind,
        frequency: YFFinancialFrequency,
        metricOrder: [String],
        values: [String: [YFFinancialPoint]],
        fetchMode: YFFundamentalsFetchMode,
        raw: YFJSONValue
    ) {
        self.symbol = symbol
        self.kind = kind
        self.frequency = frequency
        self.metricOrder = metricOrder
        self.values = values
        self.fetchMode = fetchMode
        self.raw = raw
    }

    public var dates: [String] {
        Array(Set(values.values.flatMap { $0.map(\.asOfDate) })).sorted(by: >)
    }

    public func points(for metric: String) -> [YFFinancialPoint] {
        (values[metric] ?? []).sorted { $0.asOfDate > $1.asOfDate }
    }

    public func latestValue(for metric: String) -> Double? {
        points(for: metric).first?.value
    }

    public func value(for metric: String, asOfDate: String) -> Double? {
        values[metric]?.first(where: { $0.asOfDate == asOfDate })?.value
    }

    /// Python-like statement orientation: one metric per row, periods as columns.
    public func table(maxPeriods: Int? = nil) -> YFTable {
        var selectedDates = dates
        if let maxPeriods {
            selectedDates = Array(selectedDates.prefix(max(0, maxPeriods)))
        }

        let orderedMetrics = metricOrder.filter { values[$0] != nil }
        let rows: [[String: YFJSONValue]] = orderedMetrics.map { metric in
            var row: [String: YFJSONValue] = ["Metric": .string(metric)]
            for date in selectedDates {
                row[date] = value(for: metric, asOfDate: date).map(YFJSONValue.number) ?? .null
            }
            return row
        }
        return YFTable(columns: ["Metric"] + selectedDates, rows: rows)
    }

    /// App-friendly orientation: one period per row, metrics as columns.
    public func periodTable(maxPeriods: Int? = nil) -> YFTable {
        var selectedDates = dates
        if let maxPeriods {
            selectedDates = Array(selectedDates.prefix(max(0, maxPeriods)))
        }
        let orderedMetrics = metricOrder.filter { values[$0] != nil }
        let rows: [[String: YFJSONValue]] = selectedDates.map { date in
            var row: [String: YFJSONValue] = ["Date": .string(date)]
            for metric in orderedMetrics {
                row[metric] = value(for: metric, asOfDate: date).map(YFJSONValue.number) ?? .null
            }
            return row
        }
        return YFTable(columns: ["Date"] + orderedMetrics, rows: rows)
    }
}

private enum YFFundamentalKeys {
    static let income: [String] = [
        "TaxEffectOfUnusualItems", "TaxRateForCalcs", "NormalizedEBITDA", "NormalizedDilutedEPS",
        "NormalizedBasicEPS", "TotalUnusualItems", "TotalUnusualItemsExcludingGoodwill",
        "NetIncomeFromContinuingOperationNetMinorityInterest", "ReconciledDepreciation",
        "ReconciledCostOfRevenue", "EBITDA", "EBIT", "NetInterestIncome", "InterestExpense",
        "InterestIncome", "ContinuingAndDiscontinuedDilutedEPS", "ContinuingAndDiscontinuedBasicEPS",
        "NormalizedIncome", "NetIncomeFromContinuingAndDiscontinuedOperation", "TotalExpenses",
        "RentExpenseSupplemental", "ReportedNormalizedDilutedEPS", "ReportedNormalizedBasicEPS",
        "TotalOperatingIncomeAsReported", "DividendPerShare", "DilutedAverageShares", "BasicAverageShares",
        "DilutedEPS", "DilutedEPSOtherGainsLosses", "TaxLossCarryforwardDilutedEPS",
        "DilutedAccountingChange", "DilutedExtraordinary", "DilutedDiscontinuousOperations",
        "DilutedContinuousOperations", "BasicEPS", "BasicEPSOtherGainsLosses", "TaxLossCarryforwardBasicEPS",
        "BasicAccountingChange", "BasicExtraordinary", "BasicDiscontinuousOperations",
        "BasicContinuousOperations", "DilutedNIAvailtoComStockholders", "AverageDilutionEarnings",
        "NetIncomeCommonStockholders", "OtherunderPreferredStockDividend", "PreferredStockDividends",
        "NetIncome", "MinorityInterests", "NetIncomeIncludingNoncontrollingInterests",
        "NetIncomeFromTaxLossCarryforward", "NetIncomeExtraordinary", "NetIncomeDiscontinuousOperations",
        "NetIncomeContinuousOperations", "EarningsFromEquityInterestNetOfTax", "TaxProvision",
        "PretaxIncome", "OtherIncomeExpense", "OtherNonOperatingIncomeExpenses", "SpecialIncomeCharges",
        "GainOnSaleOfPPE", "GainOnSaleOfBusiness", "OtherSpecialCharges", "WriteOff",
        "ImpairmentOfCapitalAssets", "RestructuringAndMergernAcquisition", "SecuritiesAmortization",
        "EarningsFromEquityInterest", "GainOnSaleOfSecurity", "NetNonOperatingInterestIncomeExpense",
        "TotalOtherFinanceCost", "InterestExpenseNonOperating", "InterestIncomeNonOperating",
        "OperatingIncome", "OperatingExpense", "OtherOperatingExpenses", "OtherTaxes",
        "ProvisionForDoubtfulAccounts", "DepreciationAmortizationDepletionIncomeStatement",
        "DepletionIncomeStatement", "DepreciationAndAmortizationInIncomeStatement", "Amortization",
        "AmortizationOfIntangiblesIncomeStatement", "DepreciationIncomeStatement", "ResearchAndDevelopment",
        "SellingGeneralAndAdministration", "SellingAndMarketingExpense", "GeneralAndAdministrativeExpense",
        "OtherGandA", "InsuranceAndClaims", "RentAndLandingFees", "SalariesAndWages", "GrossProfit",
        "CostOfRevenue", "TotalRevenue", "ExciseTaxes", "OperatingRevenue", "LossAdjustmentExpense",
        "NetPolicyholderBenefitsAndClaims", "PolicyholderBenefitsGross", "PolicyholderBenefitsCeded",
        "OccupancyAndEquipment", "ProfessionalExpenseAndContractServicesExpense", "OtherNonInterestExpense",
    ]

    static let balanceSheet: [String] = [
        "TreasurySharesNumber", "PreferredSharesNumber", "OrdinarySharesNumber", "ShareIssued", "NetDebt",
        "TotalDebt", "TangibleBookValue", "InvestedCapital", "WorkingCapital", "NetTangibleAssets",
        "CapitalLeaseObligations", "CommonStockEquity", "PreferredStockEquity", "TotalCapitalization",
        "TotalEquityGrossMinorityInterest", "MinorityInterest", "StockholdersEquity", "OtherEquityInterest",
        "GainsLossesNotAffectingRetainedEarnings", "OtherEquityAdjustments", "FixedAssetsRevaluationReserve",
        "ForeignCurrencyTranslationAdjustments", "MinimumPensionLiabilities", "UnrealizedGainLoss",
        "TreasuryStock", "RetainedEarnings", "AdditionalPaidInCapital", "CapitalStock", "OtherCapitalStock",
        "CommonStock", "PreferredStock", "TotalPartnershipCapital", "GeneralPartnershipCapital",
        "LimitedPartnershipCapital", "TotalLiabilitiesNetMinorityInterest",
        "TotalNonCurrentLiabilitiesNetMinorityInterest", "OtherNonCurrentLiabilities",
        "LiabilitiesHeldforSaleNonCurrent", "RestrictedCommonStock", "PreferredSecuritiesOutsideStockEquity",
        "DerivativeProductLiabilities", "EmployeeBenefits", "NonCurrentPensionAndOtherPostretirementBenefitPlans",
        "NonCurrentAccruedExpenses", "DuetoRelatedPartiesNonCurrent", "TradeandOtherPayablesNonCurrent",
        "NonCurrentDeferredLiabilities", "NonCurrentDeferredRevenue", "NonCurrentDeferredTaxesLiabilities",
        "LongTermDebtAndCapitalLeaseObligation", "LongTermCapitalLeaseObligation", "LongTermDebt",
        "LongTermProvisions", "CurrentLiabilities", "OtherCurrentLiabilities", "CurrentDeferredLiabilities",
        "CurrentDeferredRevenue", "CurrentDeferredTaxesLiabilities", "CurrentDebtAndCapitalLeaseObligation",
        "CurrentCapitalLeaseObligation", "CurrentDebt", "OtherCurrentBorrowings", "LineOfCredit",
        "CommercialPaper", "CurrentNotesPayable", "PensionandOtherPostRetirementBenefitPlansCurrent",
        "CurrentProvisions", "PayablesAndAccruedExpenses", "CurrentAccruedExpenses", "InterestPayable",
        "Payables", "OtherPayable", "DuetoRelatedPartiesCurrent", "DividendsPayable", "TotalTaxPayable",
        "IncomeTaxPayable", "AccountsPayable", "TotalAssets", "TotalNonCurrentAssets", "OtherNonCurrentAssets",
        "DefinedPensionBenefit", "NonCurrentPrepaidAssets", "NonCurrentDeferredAssets",
        "NonCurrentDeferredTaxesAssets", "DuefromRelatedPartiesNonCurrent", "NonCurrentNoteReceivables",
        "NonCurrentAccountsReceivable", "FinancialAssets", "InvestmentsAndAdvances", "OtherInvestments",
        "InvestmentinFinancialAssets", "HeldToMaturitySecurities", "AvailableForSaleSecurities",
        "FinancialAssetsDesignatedasFairValueThroughProfitorLossTotal", "TradingSecurities",
        "LongTermEquityInvestment", "InvestmentsinJointVenturesatCost",
        "InvestmentsInOtherVenturesUnderEquityMethod", "InvestmentsinAssociatesatCost",
        "InvestmentsinSubsidiariesatCost", "InvestmentProperties", "GoodwillAndOtherIntangibleAssets",
        "OtherIntangibleAssets", "Goodwill", "NetPPE", "AccumulatedDepreciation", "GrossPPE", "Leases",
        "ConstructionInProgress", "OtherProperties", "MachineryFurnitureEquipment", "BuildingsAndImprovements",
        "LandAndImprovements", "Properties", "CurrentAssets", "OtherCurrentAssets", "HedgingAssetsCurrent",
        "AssetsHeldForSaleCurrent", "CurrentDeferredAssets", "CurrentDeferredTaxesAssets", "RestrictedCash",
        "PrepaidAssets", "Inventory", "InventoriesAdjustmentsAllowances", "OtherInventories", "FinishedGoods",
        "WorkInProcess", "RawMaterials", "Receivables", "ReceivablesAdjustmentsAllowances", "OtherReceivables",
        "DuefromRelatedPartiesCurrent", "TaxesReceivable", "AccruedInterestReceivable", "NotesReceivable",
        "LoansReceivable", "AccountsReceivable", "AllowanceForDoubtfulAccountsReceivable",
        "GrossAccountsReceivable", "CashCashEquivalentsAndShortTermInvestments", "OtherShortTermInvestments",
        "CashAndCashEquivalents", "CashEquivalents", "CashFinancial", "CashCashEquivalentsAndFederalFundsSold",
        "FixedMaturityInvestments", "EquityInvestments", "NetLoan", "DeferredAssets",
    ]

    static let cashFlow: [String] = [
        "ForeignSales", "DomesticSales", "AdjustedGeographySegmentData", "FreeCashFlow",
        "RepurchaseOfCapitalStock", "RepaymentOfDebt", "IssuanceOfDebt", "IssuanceOfCapitalStock",
        "CapitalExpenditure", "InterestPaidSupplementalData", "IncomeTaxPaidSupplementalData", "EndCashPosition",
        "OtherCashAdjustmentOutsideChangeinCash", "BeginningCashPosition", "EffectOfExchangeRateChanges",
        "ChangesInCash", "OtherCashAdjustmentInsideChangeinCash", "CashFlowFromDiscontinuedOperation",
        "FinancingCashFlow", "CashFromDiscontinuedFinancingActivities", "CashFlowFromContinuingFinancingActivities",
        "NetOtherFinancingCharges", "InterestPaidCFF", "ProceedsFromStockOptionExercised", "CashDividendsPaid",
        "PreferredStockDividendPaid", "CommonStockDividendPaid", "NetPreferredStockIssuance",
        "PreferredStockPayments", "PreferredStockIssuance", "NetCommonStockIssuance", "CommonStockPayments",
        "CommonStockIssuance", "NetIssuancePaymentsOfDebt", "NetShortTermDebtIssuance", "ShortTermDebtPayments",
        "ShortTermDebtIssuance", "NetLongTermDebtIssuance", "LongTermDebtPayments", "LongTermDebtIssuance",
        "InvestingCashFlow", "CashFromDiscontinuedInvestingActivities", "CashFlowFromContinuingInvestingActivities",
        "NetOtherInvestingChanges", "InterestReceivedCFI", "DividendsReceivedCFI", "NetInvestmentPurchaseAndSale",
        "SaleOfInvestment", "PurchaseOfInvestment", "NetInvestmentPropertiesPurchaseAndSale",
        "SaleOfInvestmentProperties", "PurchaseOfInvestmentProperties", "NetBusinessPurchaseAndSale",
        "SaleOfBusiness", "PurchaseOfBusiness", "NetIntangiblesPurchaseAndSale", "SaleOfIntangibles",
        "PurchaseOfIntangibles", "NetPPEPurchaseAndSale", "SaleOfPPE", "PurchaseOfPPE",
        "CapitalExpenditureReported", "OperatingCashFlow", "CashFromDiscontinuedOperatingActivities",
        "CashFlowFromContinuingOperatingActivities", "TaxesRefundPaid", "InterestReceivedCFO", "InterestPaidCFO",
        "DividendReceivedCFO", "DividendPaidCFO", "ChangeInWorkingCapital", "ChangeInOtherWorkingCapital",
        "ChangeInOtherCurrentLiabilities", "ChangeInOtherCurrentAssets", "ChangeInPayablesAndAccruedExpense",
        "ChangeInAccruedExpense", "ChangeInInterestPayable", "ChangeInPayable", "ChangeInDividendPayable",
        "ChangeInAccountPayable", "ChangeInTaxPayable", "ChangeInIncomeTaxPayable", "ChangeInPrepaidAssets",
        "ChangeInInventory", "ChangeInReceivables", "ChangesInAccountReceivables", "OtherNonCashItems",
        "ExcessTaxBenefitFromStockBasedCompensation", "StockBasedCompensation",
        "UnrealizedGainLossOnInvestmentSecurities", "ProvisionandWriteOffofAssets", "AssetImpairmentCharge",
        "AmortizationOfSecurities", "DeferredTax", "DeferredIncomeTax", "DepreciationAmortizationDepletion",
        "Depletion", "DepreciationAndAmortization", "AmortizationCashFlow", "AmortizationOfIntangibles",
        "Depreciation", "OperatingGainsLosses", "PensionAndEmployeeBenefitExpense",
        "EarningsLossesFromEquityInvestments", "GainLossOnInvestmentSecurities",
        "NetForeignCurrencyExchangeGainLoss", "GainLossOnSaleOfPPE", "GainLossOnSaleOfBusiness",
        "NetIncomeFromContinuingOperations", "CashFlowsfromusedinOperatingActivitiesDirect",
        "TaxesRefundPaidDirect", "InterestReceivedDirect", "InterestPaidDirect", "DividendsReceivedDirect",
        "DividendsPaidDirect", "ClassesofCashPayments", "OtherCashPaymentsfromOperatingActivities",
        "PaymentsonBehalfofEmployees", "PaymentstoSuppliersforGoodsandServices",
        "ClassesofCashReceiptsfromOperatingActivities", "OtherCashReceiptsfromOperatingActivities",
        "ReceiptsfromGovernmentGrants", "ReceiptsfromCustomers",
    ]

    static func keys(for kind: YFFinancialStatementKind) -> [String] {
        switch kind {
        case .income: return income
        case .balanceSheet: return balanceSheet
        case .cashFlow: return cashFlow
        }
    }
}

public extension YFinanceClient {
    /// Fetches a complete Yahoo financial statement from the fundamentals-timeseries API.
    ///
    /// The fast path sends one request. If Yahoo returns an empty payload (or the
    /// request fails), the call falls back to 60-metric chunks, matching upstream
    /// yfinance 1.5.1+ behavior for restrictive proxies/NATs that drop long URLs.
    func financialStatement(
        symbol: String,
        kind: YFFinancialStatementKind,
        frequency: YFFinancialFrequency = .yearly,
        timeout: TimeInterval? = 15
    ) async throws -> YFFinancialStatementSeries {
        if kind == .balanceSheet && frequency == .trailing {
            throw YFinanceError.invalidRequest("Trailing balance sheet is not supported by Yahoo fundamentals-timeseries")
        }

        let cleanedSymbol = symbol.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        guard !cleanedSymbol.isEmpty else {
            throw YFinanceError.invalidRequest("symbol cannot be empty")
        }

        let prefix: String
        switch frequency {
        case .yearly: prefix = "annual"
        case .quarterly: prefix = "quarterly"
        case .trailing: prefix = "trailing"
        }

        let keys = YFFundamentalKeys.keys(for: kind)
        let types = keys.map { prefix + $0 }

        let raw: YFJSONValue
        let mode: YFFundamentalsFetchMode
        do {
            let single = try await fundamentalsPayload(
                symbol: cleanedSymbol,
                types: types,
                timeout: timeout
            )
            if Self.fundamentalsResult(from: single).isEmpty {
                throw YFinanceError.missingData("Empty fundamentals-timeseries result")
            }
            raw = single
            mode = .singleRequest
        } catch {
            guard Self.shouldFallbackToChunkedFundamentals(after: error) else {
                throw error
            }
            raw = try await fundamentalsChunkedPayload(
                symbol: cleanedSymbol,
                types: types,
                chunkSize: 60,
                timeout: timeout
            )
            mode = .chunkedFallback
        }

        let result = Self.fundamentalsResult(from: raw)
        guard !result.isEmpty else {
            throw YFinanceError.missingData("Empty fundamentals-timeseries result for \(cleanedSymbol)")
        }

        var values: [String: [YFFinancialPoint]] = [:]
        for item in result {
            guard let object = item.objectValue else { continue }
            for (typeName, encodedPoints) in object {
                guard typeName.hasPrefix(prefix), let points = encodedPoints.arrayValue else { continue }
                let metric = String(typeName.dropFirst(prefix.count))
                guard keys.contains(metric) else { continue }

                for point in points {
                    guard let date = point["asOfDate"]?.stringValue,
                          let value = point["reportedValue"]?["raw"]?.doubleValue,
                          value.isFinite else {
                        continue
                    }
                    let parsed = YFFinancialPoint(
                        asOfDate: date,
                        value: value,
                        currencyCode: point["currencyCode"]?.stringValue,
                        periodType: point["periodType"]?.stringValue
                    )
                    values[metric, default: []].append(parsed)
                }
            }
        }

        for metric in values.keys {
            var deduped: [String: YFFinancialPoint] = [:]
            for point in values[metric] ?? [] {
                deduped[point.asOfDate] = point
            }
            values[metric] = deduped.values.sorted { $0.asOfDate > $1.asOfDate }
        }

        return YFFinancialStatementSeries(
            symbol: cleanedSymbol,
            kind: kind,
            frequency: frequency,
            metricOrder: keys,
            values: values,
            fetchMode: mode,
            raw: raw
        )
    }

    private func fundamentalsPayload(
        symbol: String,
        types: [String],
        timeout: TimeInterval?
    ) async throws -> YFJSONValue {
        let start = Date(timeIntervalSince1970: 1_483_142_400) // 2016-12-31 UTC
        let end = Date().addingTimeInterval(86_400)
        return try await rawGet(
            host: .query2,
            path: "/ws/fundamentals-timeseries/v1/finance/timeseries/\(symbol)",
            queryItems: [
                URLQueryItem(name: "symbol", value: symbol),
                URLQueryItem(name: "type", value: types.joined(separator: ",")),
                URLQueryItem(name: "period1", value: String(Int(start.timeIntervalSince1970))),
                URLQueryItem(name: "period2", value: String(Int(end.timeIntervalSince1970))),
            ],
            requiresCrumb: true,
            timeout: timeout
        )
    }

    private func fundamentalsChunkedPayload(
        symbol: String,
        types: [String],
        chunkSize: Int,
        timeout: TimeInterval?
    ) async throws -> YFJSONValue {
        let safeChunkSize = max(1, chunkSize)
        var merged: [YFJSONValue] = []
        var lastError: Error?

        var startIndex = 0
        while startIndex < types.count {
            let endIndex = min(startIndex + safeChunkSize, types.count)
            let chunk = Array(types[startIndex..<endIndex])
            do {
                let payload = try await fundamentalsPayload(symbol: symbol, types: chunk, timeout: timeout)
                merged.append(contentsOf: Self.fundamentalsResult(from: payload))
            } catch {
                guard Self.shouldFallbackToChunkedFundamentals(after: error) else {
                    throw error
                }
                lastError = error
            }
            startIndex = endIndex
        }

        if merged.isEmpty {
            if let lastError { throw lastError }
            throw YFinanceError.missingData("All fundamentals-timeseries chunks were empty for \(symbol)")
        }

        return .object([
            "timeseries": .object([
                "result": .array(merged),
                "error": .null,
            ]),
        ])
    }

    private static func shouldFallbackToChunkedFundamentals(after error: Error) -> Bool {
        switch YFinanceErrorClassifier.kind(of: error) {
        case .transport, .serverUnavailable, .missingData, .decoding, .unknown:
            return true
        case .invalidRequest, .unauthorized, .forbidden, .notFound, .rateLimited, .yahooAPI:
            return false
        }
    }

    private static func fundamentalsResult(from payload: YFJSONValue) -> [YFJSONValue] {
        payload["timeseries"]?["result"]?.arrayValue ?? []
    }
}

public extension YF {
    static func financialStatement(
        _ symbol: String,
        kind: YFFinancialStatementKind,
        frequency: YFFinancialFrequency = .yearly,
        timeout: TimeInterval? = 15,
        client: YFinanceClient = YFinanceClient()
    ) async throws -> YFFinancialStatementSeries {
        try await client.financialStatement(
            symbol: symbol,
            kind: kind,
            frequency: frequency,
            timeout: timeout
        )
    }
}
