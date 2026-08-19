import Foundation
import XCTest
import YFinanceKit

final class YFinancePublicHistoryRawTests: XCTestCase {
    func testPublicFourArgumentHistoryRawStillPerformsRequestedHourlyFetch() async throws {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [PublicHistoryRawURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))
        let requests = PublicHistoryRawRequestRecorder()

        PublicHistoryRawURLProtocol.handler = { request in
            guard let url = request.url else { throw PublicHistoryRawTestError.missingURL }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "public-raw-crumb", contentType: "text/plain")
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                let interval = URLComponents(url: url, resolvingAgainstBaseURL: false)?
                    .queryItems?
                    .first(where: { $0.name == "interval" })?
                    .value ?? ""
                requests.record(interval)
                return Self.response(url: url, body: Self.rawHourlyBody)
            }

            throw PublicHistoryRawTestError.unexpectedURL(url.absoluteString)
        }
        defer { PublicHistoryRawURLProtocol.handler = nil }

        // This file deliberately uses `import YFinanceKit`, not `@testable import`.
        // It therefore exercises the public API surface seen by app consumers.
        let raw = try await client.historyRaw(
            symbol: "RAWTEST",
            range: .fiveDays,
            interval: .oneHour,
            includePrePost: true
        )

        XCTAssertEqual(raw["chart"]?["result"]?[0]?["meta"]?["probe"]?.stringValue, "raw-public")
        XCTAssertEqual(requests.snapshot(), ["1h"])
    }

    private static let rawHourlyBody = #"{"chart":{"result":[{"meta":{"symbol":"RAWTEST","probe":"raw-public"},"timestamp":[],"indicators":{"quote":[{}]}}],"error":null}}"#

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

private enum PublicHistoryRawTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class PublicHistoryRawRequestRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var intervals: [String] = []

    func record(_ interval: String) {
        lock.lock()
        intervals.append(interval)
        lock.unlock()
    }

    func snapshot() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return intervals
    }
}

private final class PublicHistoryRawURLProtocol: URLProtocol {
    private static let handlerState = TestURLProtocolHandlerState()

    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))? {
        get { handlerState.snapshot() }
        set { handlerState.set(newValue) }
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(
                self,
                didFailWithError: NSError(domain: "PublicHistoryRawURLProtocol", code: 1)
            )
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
