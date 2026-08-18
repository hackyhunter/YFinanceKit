import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceMultiInfoTests: XCTestCase {
    override func setUp() {
        super.setUp()
        MultiInfoURLProtocol.handler = nil
    }

    override func tearDown() {
        MultiInfoURLProtocol.handler = nil
        super.tearDown()
    }

    func testOneBadTickerDoesNotDiscardSuccessfulInfo() async {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MultiInfoURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))
        let tickers = YFTickers(["AAPL", "BAD"], client: client)

        MultiInfoURLProtocol.handler = { request in
            guard let url = request.url else {
                throw MultiInfoTestError.missingURL
            }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "multi-info-crumb", contentType: "text/plain")
            }
            if url.path.hasPrefix("/v10/finance/quoteSummary/AAPL") {
                return Self.response(
                    url: url,
                    body: #"{"quoteSummary":{"result":[{}],"error":null}}"#
                )
            }
            if url.path.hasPrefix("/v10/finance/quoteSummary/BAD") {
                // A 200 with malformed payload must fail only this symbol.
                return Self.response(url: url, body: "{")
            }
            if url.path == "/v7/finance/quote" {
                let symbols = URLComponents(url: url, resolvingAgainstBaseURL: false)?
                    .queryItems?
                    .first(where: { $0.name == "symbols" })?
                    .value ?? ""
                let body = #"{"quoteResponse":{"result":[{"symbol":"\#(symbols)","regularMarketPrice":100}],"error":null}}"#
                return Self.response(url: url, body: body)
            }

            throw MultiInfoTestError.unexpectedURL(url.absoluteString)
        }

        let result = await tickers.infoResult(maxConcurrentRequests: 2)
        XCTAssertNotNil(result.values["AAPL"])
        XCTAssertNil(result.values["BAD"])
        XCTAssertNotNil(result.failures["BAD"])
        XCTAssertEqual(result.failedSymbols, ["BAD"])
        XCTAssertEqual(result.succeededSymbols, ["AAPL"])
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

private enum MultiInfoTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class MultiInfoURLProtocol: URLProtocol {
    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "MultiInfoURLProtocol", code: 1))
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
