import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceHistoryMetadataTests: XCTestCase {
    func testHistoryThenMetadataReusesChartResponse() async throws {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MetadataURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))
        let requests = MetadataRequestRecorder()

        MetadataURLProtocol.handler = { request in
            guard let url = request.url else { throw MetadataTestError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "meta-crumb", contentType: "text/plain")
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                requests.record(
                    interval: Self.interval(from: url),
                    includePrePost: Self.prePost(from: url)
                )
                return Self.response(url: url, body: Self.aaplHistoryBody)
            }
            throw MetadataTestError.unexpectedURL(url.absoluteString)
        }
        defer { MetadataURLProtocol.handler = nil }

        _ = try await client.history(
            symbol: "AAPL",
            range: .fiveDays,
            interval: .oneDay,
            events: [],
            autoAdjust: false
        )

        let ticker = client.ticker("AAPL")
        let metadata = try await ticker.historyMetadata()

        XCTAssertEqual(metadata["currency"]?.stringValue, "USD")
        XCTAssertEqual(metadata["exchangeTimezoneName"]?.stringValue, "America/New_York")
        XCTAssertEqual(requests.snapshot(), ["1d"])
        XCTAssertEqual(requests.prePostSnapshot(), ["false"])
    }

    func testTradingPeriodsFetchesHourlyOnlyWhenRequested() async throws {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MetadataURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))
        let requests = MetadataRequestRecorder()

        MetadataURLProtocol.handler = { request in
            guard let url = request.url else { throw MetadataTestError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "meta-crumb", contentType: "text/plain")
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                let interval = Self.interval(from: url)
                requests.record(
                    interval: interval,
                    includePrePost: Self.prePost(from: url)
                )
                if interval == "1h" {
                    return Self.response(url: url, body: Self.hourlyMetadataBody)
                }
                return Self.response(url: url, body: Self.dailyMetadataBody)
            }
            throw MetadataTestError.unexpectedURL(url.absoluteString)
        }
        defer { MetadataURLProtocol.handler = nil }

        let base = try await client.robustHistoryMetadata(symbol: "ROG.SW")
        XCTAssertEqual(base.metadata["currency"]?.stringValue, "CHF")
        XCTAssertFalse(base.hasTradingPeriods)
        XCTAssertEqual(base.intervalUsed, .oneDay)
        XCTAssertEqual(requests.snapshot(), ["1d"])
        XCTAssertEqual(requests.prePostSnapshot(), ["false"])

        let enriched = try await client.robustHistoryMetadata(
            symbol: "ROG.SW",
            includeTradingPeriods: true
        )
        XCTAssertTrue(enriched.hasTradingPeriods)
        XCTAssertEqual(enriched.intervalUsed, .oneDay)
        XCTAssertEqual(enriched.metadata["currency"]?.stringValue, "CHF")
        XCTAssertEqual(requests.snapshot(), ["1d", "1h"])
        XCTAssertEqual(requests.prePostSnapshot(), ["false", "true"])

        _ = try await client.robustHistoryMetadata(
            symbol: "ROG.SW",
            includeTradingPeriods: true
        )
        XCTAssertEqual(requests.snapshot(), ["1d", "1h"])
        XCTAssertEqual(requests.prePostSnapshot(), ["false", "true"])
    }

    func testFailedTradingPeriodsEnrichmentKeepsCoreMetadata() async throws {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MetadataURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))
        let requests = MetadataRequestRecorder()

        MetadataURLProtocol.handler = { request in
            guard let url = request.url else { throw MetadataTestError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "meta-crumb", contentType: "text/plain")
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                let interval = Self.interval(from: url)
                requests.record(
                    interval: interval,
                    includePrePost: Self.prePost(from: url)
                )
                if interval == "1h" {
                    return Self.response(
                        url: url,
                        body: #"{"chart":{"result":null,"error":{"code":"Not Found","description":"No intraday data"}}}"#
                    )
                }
                return Self.response(url: url, body: Self.novnDailyMetadataBody)
            }
            throw MetadataTestError.unexpectedURL(url.absoluteString)
        }
        defer { MetadataURLProtocol.handler = nil }

        let result = try await client.robustHistoryMetadata(
            symbol: "NOVN.SW",
            includeTradingPeriods: true
        )

        XCTAssertEqual(result.metadata["currency"]?.stringValue, "CHF")
        XCTAssertEqual(result.metadata["exchangeTimezoneName"]?.stringValue, "Europe/Zurich")
        XCTAssertFalse(result.hasTradingPeriods)
        XCTAssertEqual(result.intervalUsed, .oneDay)
        XCTAssertEqual(requests.snapshot(), ["1d", "1h"])
        XCTAssertEqual(requests.prePostSnapshot(), ["false", "true"])
    }

    func testTickerMetadataDefaultsToDailyAndCanOptIntoTradingPeriods() async throws {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MetadataURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))
        let requests = MetadataRequestRecorder()

        MetadataURLProtocol.handler = { request in
            guard let url = request.url else { throw MetadataTestError.missingURL }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, body: "meta-crumb", contentType: "text/plain")
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                let interval = Self.interval(from: url)
                requests.record(
                    interval: interval,
                    includePrePost: Self.prePost(from: url)
                )
                if interval == "1h" {
                    return Self.response(url: url, body: Self.hourlyMetadataBody)
                }
                return Self.response(url: url, body: Self.dailyMetadataBody)
            }
            throw MetadataTestError.unexpectedURL(url.absoluteString)
        }
        defer { MetadataURLProtocol.handler = nil }

        let ticker = client.ticker("NESN.SW")
        let core = try await ticker.historyMetadata()
        XCTAssertEqual(core["currency"]?.stringValue, "CHF")
        XCTAssertNil(core["tradingPeriods"])
        XCTAssertEqual(requests.snapshot(), ["1d"])

        let enriched = try await ticker.historyMetadata(includeTradingPeriods: true)
        XCTAssertEqual(enriched["currency"]?.stringValue, "CHF")
        XCTAssertNotNil(enriched["tradingPeriods"])
        XCTAssertEqual(requests.snapshot(), ["1d", "1h"])
        XCTAssertEqual(requests.prePostSnapshot(), ["false", "true"])
    }

    private static func interval(from url: URL) -> String {
        URLComponents(url: url, resolvingAgainstBaseURL: false)?
            .queryItems?
            .first(where: { $0.name == "interval" })?
            .value ?? ""
    }

    private static func prePost(from url: URL) -> String {
        URLComponents(url: url, resolvingAgainstBaseURL: false)?
            .queryItems?
            .first(where: { $0.name == "includePrePost" })?
            .value ?? ""
    }

    private static let dailyMetadataBody = #"{"chart":{"result":[{"meta":{"currency":"CHF","symbol":"ROG.SW","exchangeName":"EBS","instrumentType":"EQUITY","exchangeTimezoneName":"Europe/Zurich"},"timestamp":[],"indicators":{"quote":[{}]}}],"error":null}}"#

    private static let hourlyMetadataBody = #"{"chart":{"result":[{"meta":{"currency":"WRONG","symbol":"ROG.SW","exchangeName":"EBS","instrumentType":"EQUITY","exchangeTimezoneName":"Europe/Zurich","tradingPeriods":{"regular":[[{"timezone":"CET","start":1787020800,"end":1787051400,"gmtoffset":7200}]]}},"timestamp":[],"indicators":{"quote":[{}]}}],"error":null}}"#

    private static let novnDailyMetadataBody = #"{"chart":{"result":[{"meta":{"currency":"CHF","symbol":"NOVN.SW","exchangeName":"EBS","instrumentType":"EQUITY","exchangeTimezoneName":"Europe/Zurich"},"timestamp":[],"indicators":{"quote":[{}]}}],"error":null}}"#

    private static let aaplHistoryBody = """
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

private final class MetadataRequestRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var intervals: [String] = []
    private var prePostValues: [String] = []

    func record(interval: String, includePrePost: String) {
        lock.lock()
        intervals.append(interval)
        prePostValues.append(includePrePost)
        lock.unlock()
    }

    func snapshot() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return intervals
    }

    func prePostSnapshot() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return prePostValues
    }
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
