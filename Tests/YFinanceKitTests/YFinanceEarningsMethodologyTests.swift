import XCTest
@testable import YFinanceKit

final class YFinanceEarningsMethodologyTests: XCTestCase {
    func testModuleSelectionForAllFourGetters() throws {
        for key in ["earningsEstimate", "epsTrend", "epsRevisions", "history"] {
            let base = key == "history" ? "earningsHistory" : "earningsTrend"
            XCTAssertEqual(try YFEarningsMethodologyAnalysis.moduleName(key: key, methodology: nil), base)
            XCTAssertEqual(try YFEarningsMethodologyAnalysis.moduleName(key: key, methodology: "GAAP"), base + "Gaap")
            XCTAssertEqual(try YFEarningsMethodologyAnalysis.moduleName(key: key, methodology: "Normalized"), base + "NonGaap")
        }
        XCTAssertThrowsError(try YFEarningsMethodologyAnalysis.normalized("ifrs"))
        XCTAssertThrowsError(try YFEarningsMethodologyAnalysis.normalized(""))
    }

    func testSelectedModuleValuesAndLabelOverrideYahooDefault() async throws {
        for (methodology, module, expected) in [("gaap", "earningsTrendGaap", 2.0), ("normalized", "earningsTrendNonGaap", 3.0)] {
            let result = try await YFEarningsMethodologyAnalysis.fetch(key: "earningsEstimate", methodology: methodology) { modules in
                XCTAssertEqual(modules, [module])
                return .object(["quoteSummary": .object(["result": .array([.object([
                    module: .object([
                        "defaultMethodology": .string("nongaap"),
                        "trend": .array([.object([
                            "period": .string("0q"),
                            "earningsCurrency": .string("USD"),
                            "earningsEstimate": .object(["avg": .object(["raw": .number(expected)])])
                        ])])
                    ])
                ])])])])
            }
            XCTAssertEqual(result[0]?["avg"]?.doubleValue, expected)
            XCTAssertEqual(result[0]?["methodology"]?.stringValue, methodology)
            XCTAssertEqual(result[0]?["currency"]?.stringValue, "USD")
            XCTAssertEqual(result[0]?["period"]?.stringValue, "0q")
        }
    }

    func testHistoryUsesOwnModuleAndPreservesRawFields() async throws {
        let result = try await YFEarningsMethodologyAnalysis.fetch(key: "history", methodology: nil) { modules in
            XCTAssertEqual(modules, ["earningsHistory"])
            return .object(["quoteSummary": .object(["result": .array([.object([
                "earningsHistory": .object([
                    "defaultMethodology": .string("nongaap"),
                    "history": .array([.object(["epsActual": .object(["raw": .number(1.25)])])])
                ])
            ])])])])
        }
        XCTAssertEqual(result[0]?["epsActual"]?["raw"]?.doubleValue, 1.25)
        XCTAssertEqual(result[0]?["methodology"]?.stringValue, "normalized")
    }

    func testInvalidMethodologyDoesNotFetch() async {
        do {
            _ = try await YFEarningsMethodologyAnalysis.fetch(key: "epsTrend", methodology: "ifrs") { _ in
                XCTFail("Invalid input must fail before the request")
                return .null
            }
            XCTFail("Expected invalidRequest")
        } catch YFinanceError.invalidRequest {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testEmptyAndMissingModulesDoNotInventRows() {
        XCTAssertEqual(YFEarningsMethodologyAnalysis.rows(from: nil, key: "epsTrend", methodology: "gaap").arrayValue?.count, 0)
        let empty: YFJSONValue = .object(["trend": .array([.object([
            "period": .string("0q"), "epsTrend": .object([:])
        ])])])
        XCTAssertEqual(YFEarningsMethodologyAnalysis.rows(from: empty, key: "epsTrend", methodology: "gaap").arrayValue?.count, 0)
    }
}
