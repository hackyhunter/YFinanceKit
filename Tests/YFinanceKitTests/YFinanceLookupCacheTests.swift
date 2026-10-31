import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceLookupCacheTests: XCTestCase {
    override func setUp() {
        super.setUp()
        LookupCacheURLProtocol.reset()
    }

    override func tearDown() {
        LookupCacheURLProtocol.reset()
        super.tearDown()
    }

    func testMalformedLookupResponseIsNotCachedAndCanRecover() async throws {
        let client = YFinanceClient(session: makeSession())
        let lookup = YFLookup(query: "AAPL", raiseErrors: false, client: client)

        LookupCacheURLProtocol.handler = { request in
            guard let url = request.url else { throw LookupCacheTestError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "lookup-crumb", contentType: "text/plain")
            }
            if url.path == "/v1/finance/lookup" {
                LookupCacheURLProtocol.incrementLookupCount()
                if LookupCacheURLProtocol.lookupCount == 1 {
                    return Self.response(url: url, body: "<html>temporary error</html>", contentType: "text/html")
                }
                return Self.response(
                    url: url,
                    body: """
                    {"finance":{"error":null,"result":[{"documents":[{"symbol":"AAPL","shortName":"Apple Inc."}]}]}}
                    """
                )
            }
            throw LookupCacheTestError.unexpectedURL(url.absoluteString)
        }

        let first = try await lookup.fetch(type: .equity, count: 5)
        XCTAssertEqual(first.objectValue?.isEmpty, true)

        let second = try await lookup.fetch(type: .equity, count: 5)
        XCTAssertEqual(
            second["finance"]?["result"]?[0]?["documents"]?[0]?["symbol"]?.stringValue,
            "AAPL"
        )

        let third = try await lookup.fetch(type: .equity, count: 5)
        XCTAssertEqual(third, second)
        XCTAssertEqual(LookupCacheURLProtocol.lookupCount, 2)
    }

    func testValidEmptyLookupResponseIsCached() async throws {
        let client = YFinanceClient(session: makeSession())
        let lookup = YFLookup(query: "NORESULT", raiseErrors: false, client: client)

        LookupCacheURLProtocol.handler = { request in
            guard let url = request.url else { throw LookupCacheTestError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "lookup-crumb", contentType: "text/plain")
            }
            if url.path == "/v1/finance/lookup" {
                LookupCacheURLProtocol.incrementLookupCount()
                return Self.response(
                    url: url,
                    body: """
                    {"finance":{"error":null,"result":[{"documents":[]}]}}
                    """
                )
            }
            throw LookupCacheTestError.unexpectedURL(url.absoluteString)
        }

        let first = try await lookup.fetch(type: .equity, count: 5)
        let second = try await lookup.fetch(type: .equity, count: 5)

        XCTAssertEqual(first, second)
        XCTAssertEqual(first["finance"]?["result"]?[0]?["documents"]?.arrayValue?.count, 0)
        XCTAssertEqual(LookupCacheURLProtocol.lookupCount, 1)
    }

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [LookupCacheURLProtocol.self]
        return URLSession(configuration: configuration)
    }

    private static func response(
        url: URL,
        body: String,
        contentType: String = "application/json"
    ) -> (HTTPURLResponse, Data) {
        let response = HTTPURLResponse(
            url: url,
            statusCode: 200,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": contentType]
        )!
        return (response, Data(body.utf8))
    }
}

private enum LookupCacheTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class LookupCacheURLProtocol: URLProtocol {
    private static let handlerState = TestURLProtocolHandlerState()
    private static let lookupCounter = TestURLProtocolCounterState()

    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))? {
        get { handlerState.snapshot() }
        set { handlerState.set(newValue) }
    }

    static var lookupCount: Int { lookupCounter.snapshot() }

    static func incrementLookupCount() {
        lookupCounter.increment()
    }

    static func reset() {
        lookupCounter.reset()
        handler = nil
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "LookupCacheURLProtocol", code: 1))
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
