import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceCookieCrumbDegradationTests: XCTestCase {
    override func setUp() {
        super.setUp()
        CookieCrumbDegradationURLProtocol.reset()
    }

    override func tearDown() {
        CookieCrumbDegradationURLProtocol.reset()
        super.tearDown()
    }

    func testGetCrumb429DegradesToCrumbLessTargetRequest() async throws {
        let client = YFinanceClient(session: makeSession())
        CookieCrumbDegradationURLProtocol.handler = { request in
            guard let url = request.url else { throw CookieCrumbDegradationError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: "ok")
            }
            if url.host == "query1.finance.yahoo.com", url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 429, body: "Too Many Requests")
            }
            if url.host == "query2.finance.yahoo.com", url.path == "/v8/finance/chart/AAPL" {
                guard URLComponents(url: url, resolvingAgainstBaseURL: false)?
                    .queryItems?.contains(where: { $0.name == "crumb" }) != true else {
                    throw CookieCrumbDegradationError.unexpectedCrumb
                }
                CookieCrumbDegradationURLProtocol.incrementTargetCount()
                return Self.response(url: url, status: 200, body: "{}")
            }
            throw CookieCrumbDegradationError.unexpectedURL(url.absoluteString)
        }

        _ = try await client.rawGet(
            host: .query2,
            path: "/v8/finance/chart/AAPL",
            requiresCrumb: false
        )
        XCTAssertEqual(CookieCrumbDegradationURLProtocol.targetCount, 1)
    }

    func testTransientCookieAndCrumbBootstrapFailureDegrades() async throws {
        let client = YFinanceClient(session: makeSession())
        CookieCrumbDegradationURLProtocol.handler = { request in
            guard let url = request.url else { throw CookieCrumbDegradationError.missingURL }
            if url.host == "fc.yahoo.com" || url.host == "guce.yahoo.com" {
                throw URLError(.timedOut)
            }
            if url.host == "query2.finance.yahoo.com", url.path == "/v8/finance/chart/AAPL" {
                guard URLComponents(url: url, resolvingAgainstBaseURL: false)?
                    .queryItems?.contains(where: { $0.name == "crumb" }) != true else {
                    throw CookieCrumbDegradationError.unexpectedCrumb
                }
                CookieCrumbDegradationURLProtocol.incrementTargetCount()
                return Self.response(url: url, status: 200, body: "{}")
            }
            throw CookieCrumbDegradationError.unexpectedURL(url.absoluteString)
        }

        _ = try await client.rawGet(
            host: .query2,
            path: "/v8/finance/chart/AAPL",
            requiresCrumb: false
        )
        XCTAssertEqual(CookieCrumbDegradationURLProtocol.targetCount, 1)
    }

    func testTarget429RemainsTerminalAndPreservesRetryAfter() async {
        let client = YFinanceClient(session: makeSession())
        CookieCrumbDegradationURLProtocol.handler = { request in
            guard let url = request.url else { throw CookieCrumbDegradationError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: "ok")
            }
            if url.host == "query1.finance.yahoo.com", url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 429, body: "Too Many Requests")
            }
            if url.host == "query2.finance.yahoo.com", url.path == "/v8/finance/chart/AAPL" {
                CookieCrumbDegradationURLProtocol.incrementTargetCount()
                return Self.response(
                    url: url,
                    status: 429,
                    body: "Too Many Requests",
                    headers: ["Retry-After": "7"]
                )
            }
            throw CookieCrumbDegradationError.unexpectedURL(url.absoluteString)
        }

        do {
            _ = try await client.rawGet(
                host: .query2,
                path: "/v8/finance/chart/AAPL",
                requiresCrumb: false
            )
            XCTFail("Expected target rate limit")
        } catch let error as YFinanceError {
            XCTAssertEqual(error.failureKind, .rateLimited)
            XCTAssertEqual(error.retryAfter ?? -1, 7, accuracy: 0.001)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertEqual(CookieCrumbDegradationURLProtocol.targetCount, 1)
    }

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [CookieCrumbDegradationURLProtocol.self]
        return URLSession(configuration: configuration)
    }

    private static func response(
        url: URL,
        status: Int,
        body: String,
        headers: [String: String] = [:]
    ) -> (HTTPURLResponse, Data) {
        var fields = ["Content-Type": "application/json"]
        for (key, value) in headers { fields[key] = value }
        let response = HTTPURLResponse(
            url: url,
            statusCode: status,
            httpVersion: "HTTP/1.1",
            headerFields: fields
        )!
        return (response, Data(body.utf8))
    }
}

private enum CookieCrumbDegradationError: Error {
    case missingURL
    case unexpectedURL(String)
    case unexpectedCrumb
}

private final class CookieCrumbDegradationURLProtocol: URLProtocol {
    private static let handlerState = TestURLProtocolHandlerState()
    private static let targetCounter = TestURLProtocolCounterState()

    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))? {
        get { handlerState.snapshot() }
        set { handlerState.set(newValue) }
    }

    static var targetCount: Int { targetCounter.snapshot() }
    static func incrementTargetCount() { targetCounter.increment() }

    static func reset() {
        targetCounter.reset()
        handler = nil
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "CookieCrumbDegradationURLProtocol", code: 1))
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
