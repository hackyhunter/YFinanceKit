import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceAdversarialTests: XCTestCase {
    func testNonFiniteFoundationNumberIsRejected() {
        XCTAssertThrowsError(try YFJSONValue(any: NSNumber(value: Double.infinity)))
        XCTAssertThrowsError(try YFJSONValue(any: NSNumber(value: Double.nan)))
    }

    func testNonFiniteAndOutOfRangeIntegerConversionsReturnNil() {
        XCTAssertNil(YFJSONValue.number(Double.infinity).doubleValue)
        XCTAssertNil(YFJSONValue.number(Double.nan).doubleValue)
        XCTAssertNil(YFJSONValue.number(Double.greatestFiniteMagnitude).intValue)
        XCTAssertNil(YFJSONValue.number(Double(Int.max)).intValue)
        XCTAssertEqual(YFJSONValue.number(42).intValue, 42)
        XCTAssertEqual(YFJSONValue.number(42.9).intValue, 42)
        XCTAssertEqual(YFJSONValue.number(-42.9).intValue, -42)
    }

    func testProviderIntegerSafetyRejectsNegativeAndOverflowingVolume() {
        XCTAssertNil(YFNumericSafety.nonNegativeInteger(from: -1))
        XCTAssertNil(YFNumericSafety.nonNegativeInteger(from: Double(Int.max)))
        XCTAssertNil(YFNumericSafety.sumNonNegative([Int.max, 1]))
        XCTAssertNil(YFNumericSafety.sumNonNegative([1, -1]))
        XCTAssertEqual(YFNumericSafety.sumNonNegative([4, 5, 6]), 15)
    }

    func testMalformedJSONThrowsInsteadOfProducingPartialValue() {
        let malformed = Data(#"{"chart":{"result":[}"#.utf8)
        XCTAssertThrowsError(try YFJSONValue.decode(data: malformed))
    }

    func testMissingAndNullNestedPathsDegradeToNil() throws {
        let value = try YFJSONValue(any: [
            "chart": [
                "result": NSNull(),
                "error": NSNull(),
            ],
        ])

        XCTAssertNil(value["chart"]?["result"]?[0])
        XCTAssertNil(value.value(at: ["chart", "result", "meta", "currency"]))
    }

    func testHistoryIntegrityMutationMatrixNeverSilentlyPassesStructuralCorruption() {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let valid = bar(date: base, open: 100, high: 102, low: 99, close: 101, volume: 1_000)

        let mutations: [(YFHistoryIntegrityCode, YFHistoryBar)] = [
            (.negativeVolume, bar(date: base, open: 100, high: 102, low: 99, close: 101, volume: -1)),
            (.highBelowLow, bar(date: base, open: 100, high: 98, low: 99, close: 100, volume: 1_000)),
            (.openOutsideRange, bar(date: base, open: 110, high: 102, low: 99, close: 101, volume: 1_000)),
            (.closeOutsideRange, bar(date: base, open: 100, high: 102, low: 99, close: 110, volume: 1_000)),
        ]

        XCTAssertTrue(makeSeries([valid]).integrityReport().isValid)
        for (expectedCode, mutated) in mutations {
            let report = makeSeries([mutated]).integrityReport()
            XCTAssertFalse(report.isValid, "Mutation \(expectedCode.rawValue) should be structural corruption")
            XCTAssertTrue(report.contains(expectedCode))
        }
    }

    func testInteriorHundredXBlockHardeningLeavesRealEdgeSwitchUntouched() {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let interior = [50.0, 51, 5_100, 5_200, 52, 53].enumerated().map { index, close in
            bar(
                date: base.addingTimeInterval(Double(index) * 86_400),
                open: close,
                high: close,
                low: close,
                close: close,
                volume: 1_000
            )
        }
        let interiorResult = makeSeries(interior).hardened()
        XCTAssertEqual(interiorResult.repairedIndices, [2, 3])
        XCTAssertEqual(interiorResult.series.bars[2].close ?? 0, 51, accuracy: 0.001)
        XCTAssertEqual(interiorResult.series.bars[3].close ?? 0, 52, accuracy: 0.001)

        let edge = [5_000.0, 5_100, 51, 52, 53].enumerated().map { index, close in
            bar(
                date: base.addingTimeInterval(Double(index) * 86_400),
                open: close,
                high: close,
                low: close,
                close: close,
                volume: 1_000
            )
        }
        let edgeResult = makeSeries(edge).hardened()
        XCTAssertTrue(edgeResult.repairedIndices.isEmpty)
        XCTAssertEqual(edgeResult.series.bars[0].close, 5_000)
    }

    private func bar(
        date: Date,
        open: Double?,
        high: Double?,
        low: Double?,
        close: Double?,
        volume: Int?
    ) -> YFHistoryBar {
        YFHistoryBar(
            date: date,
            open: open,
            high: high,
            low: low,
            close: close,
            adjustedClose: close,
            volume: volume
        )
    }

    private func makeSeries(_ bars: [YFHistoryBar]) -> YFHistorySeries {
        let meta = YFHistoryMeta(
            currency: "USD",
            symbol: "FUZZ",
            exchangeName: "NMS",
            instrumentType: "EQUITY",
            timezone: "EST",
            exchangeTimezoneName: "America/New_York",
            regularMarketPrice: nil,
            chartPreviousClose: nil,
            previousClose: nil,
            gmtoffset: -18_000,
            dataGranularity: "1d",
            priceHint: 2,
            range: "1mo",
            validRanges: ["1mo"],
            lastTrade: nil,
            tradingPeriods: nil
        )
        return YFHistorySeries(symbol: "FUZZ", meta: meta, interval: .oneDay, bars: bars)
    }
}
