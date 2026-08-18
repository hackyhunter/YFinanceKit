import XCTest
@testable import YFinanceKit

final class YFinanceErrorClassificationTests: XCTestCase {
    func testRateLimitClassification() {
        let error = YFinanceError.httpStatus(429)
        XCTAssertEqual(error.failureKind, .rateLimited)
        XCTAssertTrue(error.isRateLimited)
        XCTAssertTrue(error.isTransient)
        XCTAssertEqual(error.httpStatusCode, 429)
    }

    func testYahooRateLimitEnvelopeClassification() {
        let error = YFinanceError.serverError(
            code: "Too Many Requests",
            description: "Rate limited. Try after a while."
        )
        XCTAssertEqual(error.failureKind, .rateLimited)
        XCTAssertTrue(error.isRateLimited)
        XCTAssertEqual(error.yahooDescription, "Rate limited. Try after a while.")
    }

    func testAuthorizationClassification() {
        XCTAssertEqual(YFinanceError.httpStatus(401).failureKind, .unauthorized)
        XCTAssertEqual(YFinanceError.httpStatus(403).failureKind, .forbidden)
        XCTAssertTrue(YFinanceError.httpStatus(403).isUnauthorized)
    }
}
