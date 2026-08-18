import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceCrumbHardeningTests: XCTestCase {
    override func setUp() {
        super.setUp()
        CrumbHardeningURLProtocol.reset()
    }

    override func tearDown() {
        CrumbHardeningURLProtocol.reset()
        super.tearDown()
    }

    func testBlockedFcYahooFallsBackToConsentStrategy() async throws {
        let session = makeSession()
        let store = YFCrumbStore(session: session, userAgent: "test")

        CrumbHardeningURLProtocol.handler = { request in
            guard let url = request.url else { throw CrumbHardeningError.missingURL }
            if url.host == "fc.yahoo.com" {
                throw URLError(.cannotFindHost)
            }
            if url.host == "guce.yahoo.com", url.path == "/consent" {
                return Self.response(
                    url: url,
                    body: #"<html><input name="csrfToken" value="csrf"><input value="session" name="sessionId"></html>"#,
                    contentType: "text/html"
                )
            }
            if url.host == "consent.yahoo.com", url.path == "/v2/collectConsent" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.host == "guce.yahoo.com", url.path == "/copyConsent" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.host == "query2.finance.yahoo.com", url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "fallback-crumb", contentType: "text/plain")
            }
            throw CrumbHardeningError.unexpectedURL(url.absoluteString)
        }

        let crumb = try await store.currentCrumb()
        XCTAssertEqual(crumb, "fallback-crumb")
    }

    func testConcurrentCrumbCallersReuseSingleBootstrap() async throws {
        let session = makeSession()
        let store = YFCrumbStore(session: session, userAgent: "test")

        CrumbHardeningURLProtocol.handler = { request in
            guard let url = request.url else { throw CrumbHardeningError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.host == "query1.finance.yahoo.com", url.path == "/v1/test/getcrumb" {
                CrumbHardeningURLProtocol.incrementCrumbFetchCount()
                return Self.response(url: url, body: "one-crumb", contentType: "text/plain")
            }
            throw CrumbHardeningError.unexpectedURL(url.absoluteString)
        }

        async let first = store.currentCrumb()
        async let second = store.currentCrumb()
        async let third = store.currentCrumb()
        let (a, b, c) = try await (first, second, third)

        XCTAssertEqual(a, "one-crumb")
        XCTAssertEqual(b, "one-crumb")
        XCTAssertEqual(c, "one-crumb")
        XCTAssertEqual(CrumbHardeningURLProtocol.crumbFetchCount, 1)
    }

    func testHtmlBodyIsNeverAcceptedAsCrumb() async {
        let session = makeSession()
        let store = YFCrumbStore(session: session, userAgent: "test")

        CrumbHardeningURLProtocol.handler = { request in
            guard let url = request.url else { throw CrumbHardeningError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.host == "query1.finance.yahoo.com", url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "<html>login</html>", contentType: "text/html")
            }
            if url.host == "guce.yahoo.com", url.path == "/consent" {
                // Force alternate strategy to fail too. The important invariant is
                // that the HTML from query1 is not returned as a crumb.
                return Self.response(url: url, status: 500, body: "no")
            }
            throw CrumbHardeningError.unexpectedURL(url.absoluteString)
        }

        do {
            let crumb = try await store.currentCrumb()
            XCTFail("HTML must not be accepted as crumb: \(crumb)")
        } catch {
            // Expected: no plausible crumb was available.
        }
    }

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [CrumbHardeningURLProtocol.self]
        return URLSession(configuration: configuration)
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

private enum CrumbHardeningError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class CrumbHardeningURLProtocol: URLProtocol {
    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    private static let lock = NSLock()
    private static var _crumbFetchCount = 0

    static var crumbFetchCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return _crumbFetchCount
    }

    static func incrementCrumbFetchCount() {
        lock.lock()
        _crumbFetchCount += 1
        lock.unlock()
    }

    static func reset() {
        lock.lock()
        _crumbFetchCount = 0
        lock.unlock()
        handler = nil
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "CrumbHardeningURLProtocol", code: 1))
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
