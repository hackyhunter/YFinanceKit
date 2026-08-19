import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceTransportTests: XCTestCase {
    override func setUp() {
        super.setUp()
        TransportURLProtocol.reset()
    }

    override func tearDown() {
        TransportURLProtocol.reset()
        super.tearDown()
    }

    func testTransportReturnsSendableStatusHeadersAndData() async throws {
        TransportURLProtocol.setHandler { request in
            guard let url = request.url else {
                throw TransportTestError.missingURL
            }
            let response = HTTPURLResponse(
                url: url,
                statusCode: 429,
                httpVersion: "HTTP/1.1",
                headerFields: [
                    "Retry-After": "7",
                    "X-Test": "ok",
                ]
            )!
            return (response, Data("rate limited".utf8))
        }

        let transport = YFURLSessionTransport(session: makeSession())
        let request = URLRequest(url: URL(string: "https://query1.finance.yahoo.com/test")!)
        let response = try await transport.send(request)

        XCTAssertEqual(response.statusCode, 429)
        XCTAssertEqual(response.header("Retry-After"), "7")
        XCTAssertEqual(response.header("retry-after"), "7")
        XCTAssertEqual(response.header("X-Test"), "ok")
        XCTAssertEqual(String(data: response.data, encoding: .utf8), "rate limited")
    }

    func testTransportRejectsNonHTTPResponse() async throws {
        TransportURLProtocol.setNonHTTP(true)
        let transport = YFURLSessionTransport(session: makeSession())
        let request = URLRequest(url: URL(string: "https://query1.finance.yahoo.com/test")!)

        do {
            _ = try await transport.send(request)
            XCTFail("Expected non-HTTP response failure")
        } catch let error as YFinanceError {
            XCTAssertEqual(error.failureKind, .invalidRequest)
        } catch {
            XCTFail("Expected YFinanceError, got \(error)")
        }
    }

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [TransportURLProtocol.self]
        return URLSession(configuration: configuration)
    }
}

private enum TransportTestError: Error {
    case missingHandler
    case missingURL
}

private final class TransportURLProtocolState: @unchecked Sendable {
    private let lock = NSLock()
    private var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?
    private var nonHTTP = false

    func reset() {
        lock.lock()
        handler = nil
        nonHTTP = false
        lock.unlock()
    }

    func setHandler(_ value: @escaping (URLRequest) throws -> (HTTPURLResponse, Data)) {
        lock.lock()
        handler = value
        lock.unlock()
    }

    func setNonHTTP(_ value: Bool) {
        lock.lock()
        nonHTTP = value
        lock.unlock()
    }

    func snapshot() -> (handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?, nonHTTP: Bool) {
        lock.lock()
        defer { lock.unlock() }
        return (handler, nonHTTP)
    }
}

private final class TransportURLProtocol: URLProtocol {
    private static let state = TransportURLProtocolState()

    static func reset() {
        state.reset()
    }

    static func setHandler(_ handler: @escaping (URLRequest) throws -> (HTTPURLResponse, Data)) {
        state.setHandler(handler)
    }

    static func setNonHTTP(_ value: Bool) {
        state.setNonHTTP(value)
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let snapshot = Self.state.snapshot()
        if snapshot.nonHTTP {
            guard let url = request.url else {
                client?.urlProtocol(self, didFailWithError: TransportTestError.missingURL)
                return
            }
            let response = URLResponse(
                url: url,
                mimeType: "text/plain",
                expectedContentLength: 0,
                textEncodingName: nil
            )
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocolDidFinishLoading(self)
            return
        }

        guard let handler = snapshot.handler else {
            client?.urlProtocol(self, didFailWithError: TransportTestError.missingHandler)
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
