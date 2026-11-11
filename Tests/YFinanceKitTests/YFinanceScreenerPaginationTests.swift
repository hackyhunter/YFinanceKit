import XCTest
@testable import YFinanceKit

final class YFinanceScreenerPaginationTests: XCTestCase {
    func testPaginatedPredefinedRequestSendsCountAsSize() throws {
        let definition = try XCTUnwrap(_PREDEFINED_SCREENER_DEFINITIONS["day_gainers"])
        var request = YFScreenerRequest(query: definition.query)
        request.quoteType = definition.quoteType
        request.offset = 25
        request.count = 100
        request.sortField = definition.sortField

        let body = YFScreener.postBody(for: request)
        XCTAssertEqual(body["size"]?.intValue, 100)
        XCTAssertNil(body["count"])
        XCTAssertEqual(body["offset"]?.intValue, 25)
        XCTAssertEqual(body["sortField"]?.stringValue, "percentchange")
        XCTAssertEqual(body["quoteType"]?.stringValue, "EQUITY")
        XCTAssertEqual(body["query"]?["operator"]?.stringValue, "AND")
    }

    func testExplicitSizeTakesPrecedenceOverCount() {
        var request = YFScreenerRequest(query: YFQueryBuilder.eq("region", .string("us")))
        request.count = 100
        request.size = 50
        let body = YFScreener.postBody(for: request)
        XCTAssertEqual(body["size"]?.intValue, 50)
        XCTAssertNil(body["count"])
    }

    func testDefaultAndMaximumPageSizes() {
        var request = YFScreenerRequest(query: YFQueryBuilder.eq("region", .string("us")))
        XCTAssertEqual(YFScreener.postBody(for: request)["size"]?.intValue, 25)
        request.count = 250
        XCTAssertEqual(YFScreener.postBody(for: request)["size"]?.intValue, 250)
    }

    func testRequestModelSerializationRemainsSourceCompatible() {
        var request = YFScreenerRequest(query: YFQueryBuilder.eq("region", .string("us")))
        request.count = 100
        XCTAssertEqual(request.toJSONValue()["count"]?.intValue, 100)
        XCTAssertNil(request.toJSONValue()["size"])
        _ = YFScreener.postBody(for: request)
        XCTAssertEqual(request.count, 100)
        XCTAssertNil(request.size)
    }
}
