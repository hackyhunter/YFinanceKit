import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceRepairParityTests: XCTestCase {
    func testRepairRestoresOriginalSubunitCurrency() async throws {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [RepairYahooURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))

        RepairYahooURLProtocol.handler = { request in
            guard let url = request.url else {
                throw RepairTestError.missingURL
            }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: "ok")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 200, body: "repair-crumb")
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                let body = """
                {
                  "chart": {
                    "result": [{
                      "meta": {
                        "currency": "GBp",
                        "symbol": "TEST.L",
                        "exchangeName": "LSE",
                        "instrumentType": "EQUITY",
                        "timezone": "BST",
                        "exchangeTimezoneName": "Europe/London",
                        "regularMarketPrice": 12345.0,
                        "chartPreviousClose": 12200.0,
                        "previousClose": 12200.0,
                        "gmtoffset": 3600,
                        "dataGranularity": "1d",
                        "range": "5d",
                        "validRanges": ["1d", "5d", "1mo"]
                      },
                      "timestamp": [1786982400],
                      "indicators": {
                        "quote": [{
                          "open": [12300.0],
                          "high": [12400.0],
                          "low": [12250.0],
                          "close": [12345.0],
                          "volume": [1000.0]
                        }],
                        "adjclose": [{"adjclose": [12345.0]}]
                      }
                    }],
                    "error": null
                  }
                }
                """
                return Self.response(url: url, status: 200, body: body)
            }

            throw RepairTestError.unexpectedURL(url.absoluteString)
        }
        defer { RepairYahooURLProtocol.handler = nil }

        let history = try await client.history(
            symbol: "TEST.L",
            range: .fiveDays,
            interval: .oneDay,
            repair: true,
            keepNa: true
        )

        XCTAssertEqual(history.meta.currency, "GBp")
        XCTAssertEqual(history.bars.count, 1)
        XCTAssertEqual(history.bars[0].close ?? 0, 12345.0, accuracy: 0.001)
        XCTAssertEqual(history.bars[0].open ?? 0, 12300.0, accuracy: 0.001)
    }

    private static func response(url: URL, status: Int, body: String) -> (HTTPURLResponse, Data) {
        let response = HTTPURLResponse(
            url: url,
            statusCode: status,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": "application/json"]
        )!
        return (response, Data(body.utf8))
    }
}

private enum RepairTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class RepairYahooURLProtocol: URLProtocol {
    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "RepairYahooURLProtocol", code: 1))
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
