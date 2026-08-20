import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceHistoryNumericSafetyTests: XCTestCase {
    override func tearDown() {
        NumericSafetyURLProtocol.handler = nil
        super.tearDown()
    }

    func testOutOfRangeYahooVolumeIsDroppedWithoutTrapping() async throws {
        let history = try await history(
            timestamps: [1_699_999_200],
            volumes: [Double(Int.max)],
            requestedInterval: .oneDay
        )

        XCTAssertEqual(history.bars.count, 1)
        XCTAssertNil(history.bars[0].volume)
    }

    func testThirtyMinuteResampleDropsOverflowingVolumeSum() async throws {
        let largeVolume = 4_700_000_000_000_000_000.0
        XCTAssertNotNil(YFNumericSafety.nonNegativeInteger(from: largeVolume))

        let history = try await history(
            timestamps: [1_699_999_200, 1_700_000_100],
            volumes: [largeVolume, largeVolume],
            requestedInterval: .thirtyMinutes
        )

        XCTAssertEqual(history.bars.count, 1)
        XCTAssertNil(history.bars[0].volume)
    }

    private func history(
        timestamps: [Int],
        volumes: [Double],
        requestedInterval: YFinanceClient.Interval
    ) async throws -> YFHistorySeries {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [NumericSafetyURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))

        NumericSafetyURLProtocol.handler = { request in
            guard let url = request.url else {
                throw NumericSafetyTestError.missingURL
            }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: Data("ok".utf8))
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 200, body: Data("numeric-crumb".utf8))
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                let quoteRows = timestamps.indices.map { _ in 100.0 }
                let object: [String: Any] = [
                    "chart": [
                        "result": [[
                            "meta": [
                                "currency": "USD",
                                "symbol": "NUMERIC",
                                "exchangeName": "NMS",
                                "instrumentType": "EQUITY",
                                "timezone": "EST",
                                "exchangeTimezoneName": "America/New_York",
                                "gmtoffset": -18_000,
                                "dataGranularity": "15m",
                                "range": "1d",
                                "validRanges": ["1d"]
                            ],
                            "timestamp": timestamps,
                            "indicators": [
                                "quote": [[
                                    "open": quoteRows,
                                    "high": quoteRows,
                                    "low": quoteRows,
                                    "close": quoteRows,
                                    "volume": volumes
                                ]],
                                "adjclose": [["adjclose": quoteRows]]
                            ]
                        ]],
                        "error": NSNull()
                    ]
                ]
                return Self.response(
                    url: url,
                    status: 200,
                    body: try JSONSerialization.data(withJSONObject: object)
                )
            }
            throw NumericSafetyTestError.unexpectedURL(url.absoluteString)
        }

        return try await client.history(
            symbol: "NUMERIC",
            range: .oneDay,
            interval: requestedInterval,
            repair: false,
            keepNa: true
        )
    }

    private static func response(
        url: URL,
        status: Int,
        body: Data
    ) -> (HTTPURLResponse, Data) {
        let response = HTTPURLResponse(
            url: url,
            statusCode: status,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": "application/json"]
        )!
        return (response, body)
    }
}

private enum NumericSafetyTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class NumericSafetyURLProtocol: URLProtocol {
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
                didFailWithError: NSError(domain: "NumericSafetyURLProtocol", code: 1)
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
