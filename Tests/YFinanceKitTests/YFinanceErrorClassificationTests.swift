import XCTest
@testable import YFinanceKit

final class YFinanceErrorClassificationTests: XCTestCase {
    func testRateLimitClassification() {
        let error = YFinanceError.httpStatus(429)
        XCTAssertEqual(error.failureKind, .rateLimited)
        XCTAssertTrue(error.isRateLimited)
        XCTAssertTrue(error.isTransient)
        XCTAssertEqual(error.httpStatusCode, 429)
        XCTAssertNil(error.retryAfter)
    }

    func testStructuredRateLimitCarriesRetryAfter() {
        let error = YFinanceError.rateLimited(retryAfter: 12.5)
        XCTAssertEqual(error.failureKind, .rateLimited)
        XCTAssertEqual(error.httpStatusCode, 429)
        XCTAssertEqual(error.retryAfter, 12.5)
        XCTAssertTrue(error.isRateLimited)
    }

    func testStructuredRateLimitWithoutHeaderHasNoRetryAfter() {
        let error = YFinanceError.rateLimited()
        XCTAssertEqual(error.failureKind, .rateLimited)
        XCTAssertNil(error.retryAfter)
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
