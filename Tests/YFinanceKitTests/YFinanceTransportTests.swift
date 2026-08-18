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
        TransportURLProtocol.handler = { request in
            let url = try XCTUnwrap(request.url)
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
        TransportURLProtocol.nonHTTP = true
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
}

private final class TransportURLProtocol: URLProtocol {
    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?
    static var nonHTTP = false

    static func reset() {
        handler = nil
        nonHTTP = false
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        if Self.nonHTTP {
            guard let url = request.url else {
                client?.urlProtocol(self, didFailWithError: TransportTestError.missingHandler)
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

        guard let handler = Self.handler else {
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
