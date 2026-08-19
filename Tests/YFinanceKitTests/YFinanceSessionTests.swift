import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceSessionTests: XCTestCase {
    override func setUp() {
        super.setUp()
        MockYahooURLProtocol.reset()
    }

    override func tearDown() {
        MockYahooURLProtocol.reset()
        super.tearDown()
    }

    func testHistoryUsesSessionCrumb() async throws {
        let session = makeSession()
        let client = YFinanceClient(session: session)

        MockYahooURLProtocol.handler = { request in
            guard let url = request.url else {
                throw TestError.missingURL
            }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: "ok")
            }

            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 200, body: "chart-crumb")
            }

            if url.path.hasPrefix("/v8/finance/chart/") {
                let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
                let crumb = components?.queryItems?.first(where: { $0.name == "crumb" })?.value
                XCTAssertEqual(crumb, "chart-crumb", "Chart/history requests must carry the session-bound Yahoo crumb")

                let body = """
                {
                  "chart": {
                    "result": [{
                      "meta": {
                        "currency": "USD",
                        "symbol": "AAPL",
                        "exchangeName": "NMS",
                        "instrumentType": "EQUITY",
                        "timezone": "EDT",
                        "exchangeTimezoneName": "America/New_York",
                        "regularMarketPrice": 200.0,
                        "chartPreviousClose": 199.0,
                        "previousClose": 199.0,
                        "gmtoffset": -14400,
                        "dataGranularity": "1d",
                        "range": "5d",
                        "validRanges": ["1d", "5d", "1mo"]
                      },
                      "timestamp": [1786996800],
                      "indicators": {
                        "quote": [{
                          "open": [199.0],
                          "high": [202.0],
                          "low": [198.0],
                          "close": [201.0],
                          "volume": [123456.0]
                        }],
                        "adjclose": [{"adjclose": [201.0]}]
                      }
                    }],
                    "error": null
                  }
                }
                """
                return Self.response(url: url, status: 200, body: body)
            }

            throw TestError.unexpectedURL(url.absoluteString)
        }

        let history = try await client.history(symbol: "AAPL", range: .fiveDays, interval: .oneDay)
        XCTAssertEqual(history.bars.count, 1)
        XCTAssertEqual(history.bars.first?.close, 201.0)
    }

    func testCrumbStoreFallsBackToCSRFConsentFlow() async throws {
        let session = makeSession()
        let store = YFCrumbStore(session: session, userAgent: "test-agent")
        var sawCollectConsent = false
        var sawCopyConsent = false

        MockYahooURLProtocol.handler = { request in
            guard let url = request.url else {
                throw TestError.missingURL
            }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: "ok")
            }

            if url.host == "query1.finance.yahoo.com", url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 401, body: "Unauthorized")
            }

            if url.host == "guce.yahoo.com", url.path == "/consent" {
                let html = """
                <html><body><form>
                <input type="hidden" value="csrf-value" name="csrfToken">
                <input name="sessionId" type="hidden" value="session-value">
                </form></body></html>
                """
                return Self.response(url: url, status: 200, body: html)
            }

            if url.host == "consent.yahoo.com", url.path == "/v2/collectConsent" {
                XCTAssertEqual(request.httpMethod, "POST")
                XCTAssertEqual(request.value(forHTTPHeaderField: "Content-Type"), "application/x-www-form-urlencoded")
                sawCollectConsent = true
                return Self.response(url: url, status: 200, body: "ok")
            }

            if url.host == "guce.yahoo.com", url.path == "/copyConsent" {
                sawCopyConsent = true
                return Self.response(url: url, status: 200, body: "ok")
            }

            if url.host == "query2.finance.yahoo.com", url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 200, body: "csrf-crumb")
            }

            throw TestError.unexpectedURL(url.absoluteString)
        }

        let crumb = try await store.currentCrumb()
        XCTAssertEqual(crumb, "csrf-crumb")
        XCTAssertTrue(sawCollectConsent)
        XCTAssertTrue(sawCopyConsent)
    }

    func testCrumbStoreRejectsRateLimitBody() async throws {
        let session = makeSession()
        let store = YFCrumbStore(session: session, userAgent: "test-agent")

        MockYahooURLProtocol.handler = { request in
            guard let url = request.url else {
                throw TestError.missingURL
            }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: "ok")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 429, body: "Edge: Too Many Requests")
            }
            throw TestError.unexpectedURL(url.absoluteString)
        }

        do {
            _ = try await store.currentCrumb()
            XCTFail("Expected HTTP 429")
        } catch YFinanceError.httpStatus(let status) {
            XCTAssertEqual(status, 429)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MockYahooURLProtocol.self]
        return URLSession(configuration: configuration)
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

    private enum TestError: Error {
        case missingURL
        case unexpectedURL(String)
    }
}

private final class MockYahooURLProtocol: URLProtocol {
    private static let handlerState = TestURLProtocolHandlerState()

    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))? {
        get { handlerState.snapshot() }
        set { handlerState.set(newValue) }
    }

    static func reset() {
        handler = nil
    }

    override class func canInit(with request: URLRequest) -> Bool {
        true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "MockYahooURLProtocol", code: 1))
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
