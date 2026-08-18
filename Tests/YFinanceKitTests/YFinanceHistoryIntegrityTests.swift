import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceHistoryIntegrityTests: XCTestCase {
    func testDetectsBrokenOHLCAndDuplicateTimestamp() {
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let series = makeSeries(bars: [
            YFHistoryBar(
                date: date,
                open: 101,
                high: 100,
                low: 99,
                close: 98,
                adjustedClose: 98,
                volume: 100
            ),
            YFHistoryBar(
                date: date,
                open: 100,
                high: 102,
                low: 99,
                close: 101,
                adjustedClose: 101,
                volume: 100
            ),
        ])

        let report = series.integrityReport()
        XCTAssertFalse(report.isValid)
        XCTAssertTrue(report.contains(.duplicateTimestamp))
        XCTAssertTrue(report.contains(.openOutsideRange))
        XCTAssertTrue(report.contains(.closeOutsideRange))
        XCTAssertThrowsError(try series.validateIntegrity())
    }

    func testFlagsClassicHundredXJumpAsWarning() throws {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let series = makeSeries(bars: [
            bar(date: base, close: 10),
            bar(date: base.addingTimeInterval(86_400), close: 1_000),
        ])

        let report = try series.validateIntegrity()
        XCTAssertTrue(report.isValid)
        XCTAssertTrue(report.hasWarnings)
        XCTAssertTrue(report.contains(.implausibleOneBarScaleJump))
    }

    func testRepairedHundredXJumpDoesNotWarn() throws {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let series = makeSeries(bars: [
            bar(date: base, close: 10),
            bar(date: base.addingTimeInterval(86_400), close: 1_000, repaired: true),
        ])

        let report = try series.validateIntegrity()
        XCTAssertFalse(report.contains(.implausibleOneBarScaleJump))
    }

    private func bar(date: Date, close: Double, repaired: Bool = false) -> YFHistoryBar {
        YFHistoryBar(
            date: date,
            open: close,
            high: close,
            low: close,
            close: close,
            adjustedClose: close,
            volume: 100,
            repaired: repaired
        )
    }

    private func makeSeries(bars: [YFHistoryBar]) -> YFHistorySeries {
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
            range: "5d",
            validRanges: ["5d"],
            lastTrade: nil,
            tradingPeriods: nil
        )
        return YFHistorySeries(
            symbol: "TEST",
            meta: meta,
            interval: .oneDay,
            bars: bars
        )
    }
}
