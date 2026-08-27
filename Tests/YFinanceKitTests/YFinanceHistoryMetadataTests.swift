import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceHistoryMetadataTests: XCTestCase {
    override func setUp() {
        super.setUp()
        MetadataURLProtocol.reset()
    }

    override func tearDown() {
        MetadataURLProtocol.reset()
        super.tearDown()
    }

    func testCoreMetadataUsesSingleDailyChartRequest() async throws {
        let client = makeClient()
        installMetadataHandler(
            dailyBody: Self.dailyMetadataBody(),
            hourlyBody: Self.hourlyMetadataBody()
        )

        let result = try await client.robustHistoryMetadata(symbol: "ROG.SW")

        XCTAssertEqual(MetadataURLProtocol.chartIntervals, ["1d"])
        XCTAssertEqual(result.intervalUsed, .oneDay)
        XCTAssertEqual(result.metadata["currency"]?.stringValue, "CHF")
        XCTAssertFalse(result.hasTradingPeriods)
    }

    func testTradingPeriodsEnrichmentMakesOneAdditionalHourlyRequest() async throws {
        let client = makeClient()
        installMetadataHandler(
            dailyBody: Self.dailyMetadataBody(),
            hourlyBody: Self.hourlyMetadataBody()
        )

        let result = try await client.robustHistoryMetadata(
            symbol: "ROG.SW",
            includeTradingPeriods: true
        )

        XCTAssertEqual(MetadataURLProtocol.chartIntervals, ["1d", "1h"])
        XCTAssertEqual(MetadataURLProtocol.prePostValues, ["false", "true"])
        XCTAssertEqual(result.intervalUsed, .oneDay)
        XCTAssertTrue(result.hasTradingPeriods)
        XCTAssertEqual(result.metadata["currency"]?.stringValue, "CHF", "hourly enrichment must not overwrite core daily metadata")
        XCTAssertNotNil(result.metadata["tradingPeriods"])
    }

    func testHourlyEnrichmentFailurePreservesDailyMetadata() async throws {
        let client = makeClient()
        installMetadataHandler(
            dailyBody: Self.dailyMetadataBody(),
            hourlyBody: #"{"chart":{"result":null,"error":{"code":"Not Found","description":"No intraday data"}}}"#
        )

        let result = try await client.robustHistoryMetadata(
            symbol: "ROG.SW",
            includeTradingPeriods: true
        )

        XCTAssertEqual(MetadataURLProtocol.chartIntervals, ["1d", "1h"])
        XCTAssertEqual(result.metadata["currency"]?.stringValue, "CHF")
        XCTAssertEqual(result.metadata["exchangeTimezoneName"]?.stringValue, "Europe/Zurich")
        XCTAssertFalse(result.hasTradingPeriods)
    }

    func testTickerHistoryMetadataDefaultsToDailyAndCanOptIntoTradingPeriods() async throws {
        let client = makeClient()
        let ticker = client.ticker("ROG.SW")
        installMetadataHandler(
            dailyBody: Self.dailyMetadataBody(),
            hourlyBody: Self.hourlyMetadataBody()
        )

        let core = try await ticker.historyMetadata()
        XCTAssertEqual(MetadataURLProtocol.chartIntervals, ["1d"])
        XCTAssertEqual(core["currency"]?.stringValue, "CHF")
        XCTAssertNil(core["tradingPeriods"])

        MetadataURLProtocol.resetChartRequests()
        let enriched = try await ticker.historyMetadata(includeTradingPeriods: true)
        XCTAssertEqual(MetadataURLProtocol.chartIntervals, ["1d", "1h"])
        XCTAssertNotNil(enriched["tradingPeriods"])
    }

    private func makeClient() -> YFinanceClient {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MetadataURLProtocol.self]
        return YFinanceClient(session: URLSession(configuration: configuration))
    }

    private func installMetadataHandler(dailyBody: String, hourlyBody: String) {
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
                let interval = components?.queryItems?.first(where: { $0.name == "interval" })?.value ?? ""
                let includePrePost = components?.queryItems?.first(where: { $0.name == "includePrePost" })?.value ?? ""
                MetadataURLProtocol.recordChart(interval: interval, includePrePost: includePrePost)
                if interval == "1d" {
                    return Self.response(url: url, body: dailyBody)
                }
                if interval == "1h" {
                    return Self.response(url: url, body: hourlyBody)
                }
            }
            throw MetadataTestError.unexpectedURL(url.absoluteString)
        }
    }

    private static func dailyMetadataBody() -> String {
        #"{"chart":{"result":[{"meta":{"currency":"CHF","symbol":"ROG.SW","exchangeName":"EBS","instrumentType":"EQUITY","exchangeTimezoneName":"Europe/Zurich"},"timestamp":[],"indicators":{"quote":[{}]}}],"error":null}}"#
    }

    private static func hourlyMetadataBody() -> String {
        #"{"chart":{"result":[{"meta":{"currency":"WRONG","symbol":"ROG.SW","exchangeTimezoneName":"Europe/Zurich","tradingPeriods":{"regular":[[{"start":1787800000,"end":1787820000}]]}},"timestamp":[],"indicators":{"quote":[{}]}}],"error":null}}"#
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
    private static let requestState = MetadataRequestState()

    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))? {
        get { handlerState.snapshot() }
        set { handlerState.set(newValue) }
    }

    static var chartIntervals: [String] { requestState.intervals() }
    static var prePostValues: [String] { requestState.prePostValues() }

    static func recordChart(interval: String, includePrePost: String) {
        requestState.record(interval: interval, includePrePost: includePrePost)
    }

    static func resetChartRequests() {
        requestState.reset()
    }

    static func reset() {
        handler = nil
        requestState.reset()
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

private final class MetadataRequestState: @unchecked Sendable {
    private let lock = NSLock()
    private var recordedIntervals: [String] = []
    private var recordedPrePost: [String] = []

    func record(interval: String, includePrePost: String) {
        lock.lock()
        recordedIntervals.append(interval)
        recordedPrePost.append(includePrePost)
        lock.unlock()
    }

    func intervals() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return recordedIntervals
    }

    func prePostValues() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return recordedPrePost
    }

    func reset() {
        lock.lock()
        recordedIntervals.removeAll(keepingCapacity: true)
        recordedPrePost.removeAll(keepingCapacity: true)
        lock.unlock()
    }
}
