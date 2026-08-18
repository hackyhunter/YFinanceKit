import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceFinancialsTests: XCTestCase {
    override func setUp() {
        super.setUp()
        FinancialsURLProtocol.reset()
    }

    override func tearDown() {
        FinancialsURLProtocol.reset()
        super.tearDown()
    }

    func testFinancialStatementFallsBackToChunksWhenLongRequestIsEmpty() async throws {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [FinancialsURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))

        var fullRequestSeen = false
        var chunkRequests = 0

        FinancialsURLProtocol.handler = { request in
            guard let url = request.url else { throw FinancialsTestError.missingURL }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "fundamentals-crumb", contentType: "text/plain")
            }
            if url.path.contains("/ws/fundamentals-timeseries/") {
                let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
                let types = components?.queryItems?.first(where: { $0.name == "type" })?.value?.split(separator: ",").map(String.init) ?? []
                XCTAssertEqual(
                    components?.queryItems?.first(where: { $0.name == "crumb" })?.value,
                    "fundamentals-crumb"
                )

                if types.count > 60 {
                    fullRequestSeen = true
                    return Self.response(
                        url: url,
                        body: #"{"timeseries":{"result":[],"error":null}}"#
                    )
                }

                chunkRequests += 1
                if types.contains("annualTotalRevenue") {
                    return Self.response(
                        url: url,
                        body: #"{"timeseries":{"result":[{"annualTotalRevenue":[{"asOfDate":"2025-09-30","periodType":"12M","currencyCode":"USD","reportedValue":{"raw":1000.0}}]},{"annualNetIncome":[{"asOfDate":"2025-09-30","periodType":"12M","currencyCode":"USD","reportedValue":{"raw":125.0}}]}],"error":null}}"#
                    )
                }

                // Sparse statements can legitimately have a whole chunk with no values.
                return Self.response(
                    url: url,
                    body: #"{"timeseries":{"result":[],"error":null}}"#
                )
            }

            throw FinancialsTestError.unexpectedURL(url.absoluteString)
        }

        let statement = try await client.financialStatement(
            symbol: "AAPL",
            kind: .income,
            frequency: .yearly
        )

        XCTAssertTrue(fullRequestSeen)
        XCTAssertGreaterThan(chunkRequests, 1)
        XCTAssertEqual(statement.fetchMode, .chunkedFallback)
        XCTAssertEqual(statement.latestValue(for: "TotalRevenue"), 1000.0)
        XCTAssertEqual(statement.latestValue(for: "NetIncome"), 125.0)
        XCTAssertEqual(statement.periodTable().rowCount, 1)
    }

    func testTrailingBalanceSheetIsRejected() async throws {
        let client = YFinanceClient()
        do {
            _ = try await client.financialStatement(
                symbol: "AAPL",
                kind: .balanceSheet,
                frequency: .trailing
            )
            XCTFail("Expected invalid request")
        } catch YFinanceError.invalidRequest(_) {
            // expected
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    private static func response(
        url: URL,
        status: Int = 200,
        body: String,
        contentType: String = "application/json"
    ) -> (HTTPURLResponse, Data) {
        let response = HTTPURLResponse(
            url: url,
            statusCode: status,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": contentType]
        )!
        return (response, Data(body.utf8))
    }
}

private enum FinancialsTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class FinancialsURLProtocol: URLProtocol {
    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    static func reset() { handler = nil }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "FinancialsURLProtocol", code: 1))
            return
        }
        do {
            let (response, data) = try handler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}
