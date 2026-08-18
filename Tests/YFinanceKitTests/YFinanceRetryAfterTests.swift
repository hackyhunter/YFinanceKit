import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceRetryAfterTests: XCTestCase {
    func testParsesDeltaSeconds() {
        XCTAssertEqual(YFRetryAfterParser.parse("15"), 15)
        XCTAssertEqual(YFRetryAfterParser.parse(" 2.5 "), 2.5)
        XCTAssertNil(YFRetryAfterParser.parse("-1"))
        XCTAssertNil(YFRetryAfterParser.parse("banana"))
    }

    func testCapsAbsurdDeltaSeconds() {
        XCTAssertEqual(YFRetryAfterParser.parse("999999", maximum: 60), 60)
    }

    func testParsesHTTPDateRelativeToInjectedClock() {
        let now = Date(timeIntervalSince1970: 1_784_332_800) // 2026-07-18 00:00:00 UTC
        let raw = "Sat, 18 Jul 2026 00:00:30 GMT"
        XCTAssertEqual(YFRetryAfterParser.parse(raw, now: now), 30)
    }

    func testPastHTTPDateReturnsZero() {
        let now = Date(timeIntervalSince1970: 1_784_332_800)
        let raw = "Fri, 17 Jul 2026 23:59:00 GMT"
        XCTAssertEqual(YFRetryAfterParser.parse(raw, now: now), 0)
    }
}
