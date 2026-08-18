import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceRepairSafetyTests: XCTestCase {
    func testHeavilyRepairedResultFallsBackOnlyWhenRawShowsBoundedBlock() {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let rawCloses: [Double] = [50, 51, 5_100, 5_200, 52, 53]
        let raw = makeSeries(
            closes: rawCloses,
            repairedIndices: [],
            base: base
        )

        // Simulate a legacy whole-side decision that touched most of the table.
        let legacy = makeSeries(
            closes: [0.50, 0.51, 51, 52, 52, 53],
            repairedIndices: [0, 1, 2, 3],
            base: base
        )

        XCTAssertGreaterThan(legacy.repairedBarFraction, 0.30)
        let result = legacy.replacingSuspiciousWholeSideRepairIfNeeded(with: raw)

        XCTAssertEqual(result.repairedIndices, [2, 3])
        XCTAssertEqual(result.series.bars[0].close, 50)
        XCTAssertEqual(result.series.bars[1].close, 51)
        XCTAssertEqual(result.series.bars[2].close ?? 0, 51, accuracy: 0.001)
        XCTAssertEqual(result.series.bars[3].close ?? 0, 52, accuracy: 0.001)
        XCTAssertEqual(result.series.bars[4].close, 52)
    }

    func testHeavilyRepairedResultIsPreservedWithoutRawBoundedBlockEvidence() {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let raw = makeSeries(
            closes: [50, 51, 52, 53, 54, 55],
            repairedIndices: [],
            base: base
        )
        let legacyCloses: [Double] = [50, 51, 52, 53, 54, 55]
        let legacy = makeSeries(
            closes: legacyCloses,
            repairedIndices: [0, 1, 2, 3],
            base: base
        )

        let result = legacy.replacingSuspiciousWholeSideRepairIfNeeded(with: raw)
        XCTAssertTrue(result.repairedIndices.isEmpty)
        XCTAssertEqual(result.series.bars.map(\.close), legacy.bars.map(\.close))
    }

    func testSmallRepairDoesNotTriggerRawOverride() {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let raw = makeSeries(
            closes: [50, 51, 5_100, 5_200, 52, 53],
            repairedIndices: [],
            base: base
        )
        let legacy = makeSeries(
            closes: [50, 51, 51, 52, 52, 53],
            repairedIndices: [2],
            base: base
        )

        XCTAssertLessThan(legacy.repairedBarFraction, 0.30)
        let result = legacy.replacingSuspiciousWholeSideRepairIfNeeded(with: raw)
        XCTAssertTrue(result.repairedIndices.isEmpty)
        XCTAssertEqual(result.series.bars[2].close, 51)
    }

    private func makeSeries(
        closes: [Double],
        repairedIndices: Set<Int>,
        base: Date
    ) -> YFHistorySeries {
        let bars = closes.enumerated().map { index, close in
            YFHistoryBar(
                date: base.addingTimeInterval(Double(index) * 86_400),
                open: close,
                high: close,
                low: close,
                close: close,
                adjustedClose: close,
                volume: 1_000,
                repaired: repairedIndices.contains(index)
            )
        }

        let meta = YFHistoryMeta(
            currency: "USD",
            symbol: "TEST",
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

        return YFHistorySeries(
            symbol: "TEST",
            meta: meta,
            interval: .oneDay,
            bars: bars,
            repairEnabled: !repairedIndices.isEmpty
        )
    }
}
