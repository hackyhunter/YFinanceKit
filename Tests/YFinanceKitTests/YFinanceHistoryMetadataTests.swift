import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceHistoryMetadataTests: XCTestCase {
    func testFallsBackToDailyWhenHourlyMetadataUnavailable() async throws {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MetadataURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))
        var sawHourly = false
        var sawDaily = false

        MetadataURLProtocol.handler = { request in
            guard let url = request.url else { throw MetadataTestError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "meta-crumb", contentType: "text/plain")
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
                let interval = components?.queryItems?.first(where: { $0.name == "interval" })?.value
                if interval == "1h" {
                    sawHourly = true
                    return Self.response(
                        url: url,
                        body: #"{"chart":{"result":null,"error":{"code":"Not Found","description":"No intraday data"}}}"#
                    )
                }
                if interval == "1d" {
                    sawDaily = true
                    return Self.response(
                        url: url,
                        body: #"{"chart":{"result":[{"meta":{"currency":"CHF","symbol":"ROG.SW","exchangeName":"EBS","instrumentType":"EQUITY","exchangeTimezoneName":"Europe/Zurich"},"timestamp":[],"indicators":{"quote":[{}]}}],"error":null}}"#
                    )
                }
            }
            throw MetadataTestError.unexpectedURL(url.absoluteString)
        }
        defer { MetadataURLProtocol.handler = nil }

        let result = try await client.robustHistoryMetadata(symbol: "ROG.SW")
        XCTAssertTrue(sawHourly)
        XCTAssertTrue(sawDaily)
        XCTAssertEqual(result.intervalUsed, .oneDay)
        XCTAssertEqual(result.metadata["currency"]?.stringValue, "CHF")
        XCTAssertEqual(result.metadata["exchangeTimezoneName"]?.stringValue, "Europe/Zurich")
        XCTAssertFalse(result.hasTradingPeriods)
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

private enum MetadataTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class MetadataURLProtocol: URLProtocol {
    private static let handlerState = TestURLProtocolHandlerState()

    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))? {
        get { handlerState.snapshot() }
        set { handlerState.set(newValue) }
    }
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "MetadataURLProtocol", code: 1))
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
