import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceResilientClientTests: XCTestCase {
    override func setUp() {
        super.setUp()
        ResilientURLProtocol.reset()
    }

    override func tearDown() {
        ResilientURLProtocol.reset()
        super.tearDown()
    }

    func testConcurrentIdenticalQuotesCoalesceToOneYahooRequest() async throws {
        let session = makeSession()
        let client = YFinanceClient(session: session)
        let resilient = YFResilientClient(
            client: client,
            policy: YFRequestPolicy(
                maxConcurrentRequests: 4,
                maxAttempts: 1,
                baseRetryDelay: 0,
                maxRetryDelay: 0,
                retryJitterFraction: 0,
                baseRateLimitCooldown: 1,
                maxRateLimitCooldown: 5,
                traceCapacity: 20
            )
        )

        installHappyPathHandler()

        async let first = resilient.quote(symbol: "AAPL")
        async let second = resilient.quote(symbol: "aapl")
        let (firstResult, secondResult) = try await (first, second)
        let symbols = [firstResult?.symbol, secondResult?.symbol].compactMap { $0 }

        XCTAssertEqual(symbols, ["AAPL", "AAPL"])
        XCTAssertEqual(ResilientURLProtocol.quoteRequestCount, 1)

        let diagnostics = await resilient.diagnostics()
        XCTAssertEqual(diagnostics.coalescedRequests, 1)
        XCTAssertEqual(diagnostics.logicalRequests, 1)
        XCTAssertEqual(diagnostics.successes, 1)
    }

    func testFreshQuoteCacheAvoidsSecondYahooRequest() async throws {
        let session = makeSession()
        let client = YFinanceClient(session: session)
        let resilient = YFResilientClient(client: client)
        installHappyPathHandler()

        let first = try await resilient.quoteCached(symbol: "AAPL", freshFor: 60, staleFor: 300)
        XCTAssertEqual(first.freshness, .fresh)
        XCTAssertEqual(first.value?.symbol, "AAPL")

        let second = try await resilient.quoteCached(symbol: "AAPL", freshFor: 60, staleFor: 300)
        XCTAssertEqual(second.freshness, .fresh)
        XCTAssertEqual(second.value?.symbol, "AAPL")
        XCTAssertEqual(ResilientURLProtocol.quoteRequestCount, 1)

        let diagnostics = await resilient.diagnostics()
        XCTAssertEqual(diagnostics.cacheMisses, 1)
        XCTAssertEqual(diagnostics.cacheHits, 1)
    }

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [ResilientURLProtocol.self]
        return URLSession(configuration: configuration)
    }

    private func installHappyPathHandler() {
        ResilientURLProtocol.handler = { request in
            guard let url = request.url else {
                throw ResilientTestError.missingURL
            }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 200, body: "resilient-crumb", contentType: "text/plain")
            }
            if url.path == "/v7/finance/quote" {
                ResilientURLProtocol.incrementQuoteCount()
                let body = #"{"quoteResponse":{"result":[{"symbol":"AAPL"}],"error":null}}"#
                return Self.response(url: url, status: 200, body: body)
            }

            throw ResilientTestError.unexpectedURL(url.absoluteString)
        }
    }

    private static func response(
        url: URL,
        status: Int,
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

private enum ResilientTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class ResilientURLProtocol: URLProtocol {
    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    private static let lock = NSLock()
    private static var _quoteRequestCount = 0

    static var quoteRequestCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return _quoteRequestCount
    }

    static func incrementQuoteCount() {
        lock.lock()
        _quoteRequestCount += 1
        lock.unlock()
    }

    static func reset() {
        lock.lock()
        _quoteRequestCount = 0
        lock.unlock()
        handler = nil
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "ResilientURLProtocol", code: 1))
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
