import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceCoreRepairRegressionTests: XCTestCase {
    override func tearDown() {
        CoreRepairURLProtocol.handler = nil
        super.tearDown()
    }

    func testCoreRepairKeepsBoundedHundredXChangesLocal() async throws {
        let normal = Array(repeating: 5_000.0, count: 60)

        var partialBlock = normal
        partialBlock.replaceSubrange(20..<30, with: Array(repeating: 500_000.0, count: 10))
        let repairedPartial = try await repairedHistory(currency: "GBp", closes: partialBlock)
        assertUniform(repairedPartial, expectedClose: 5_000, repairedRange: 20..<30)

        var wronglyDividedBlock = normal
        wronglyDividedBlock.replaceSubrange(20..<30, with: Array(repeating: 50.0, count: 10))
        let repairedWronglyDivided = try await repairedHistory(currency: "GBp", closes: wronglyDividedBlock)
        assertUniform(repairedWronglyDivided, expectedClose: 5_000, repairedRange: 20..<30)

        var mixedMajorUnits = Array(repeating: 50.0, count: 60)
        mixedMajorUnits.replaceSubrange(20..<30, with: Array(repeating: 5_000.0, count: 10))
        let repairedMixed = try await repairedHistory(currency: "GBP", closes: mixedMajorUnits)
        assertUniform(repairedMixed, expectedClose: 50, repairedRange: 20..<30)
    }

    func testCoreRepairOrientsRealUnitSwitchWithoutMarketPriceAnchor() async throws {
        let switched = Array(repeating: 5_000.0, count: 30)
            + Array(repeating: 50.0, count: 30)

        let scaled = try await repairedHistory(currency: "GBp", closes: switched)
        assertUniform(scaled, expectedClose: 5_000, repairedRange: 30..<60)

        let recentEnd = Date().timeIntervalSince1970
        let recentStart = Int(recentEnd) - (59 * 86_400)
        let relabelled = try await repairedHistory(
            currency: "GBp",
            regularMarketPrice: 5_000,
            closes: switched,
            firstTimestamp: recentStart
        )
        assertUniform(relabelled, expectedClose: 50, repairedRange: 0..<30)
    }

    func testCapitalGainWithoutDividendOrPriorRowDoesNotCrashRepair() async throws {
        let closes = Array(repeating: 50.0, count: 10)
        let firstTimestamp = 1_704_067_200
        let eventTimestamps = [firstTimestamp, firstTimestamp + (5 * 86_400)]
        let history = try await repairedHistory(
            currency: "USD",
            closes: closes,
            firstTimestamp: firstTimestamp,
            capitalGainTimestamps: eventTimestamps
        )

        XCTAssertEqual(history.bars.count, closes.count)
        XCTAssertEqual(history.events.filter { $0.kind == .capitalGain }.count, 2)
        XCTAssertTrue(history.bars.allSatisfy { ($0.close ?? 0) == 50 })
    }

    private func assertUniform(
        _ history: YFHistorySeries,
        expectedClose: Double,
        repairedRange: Range<Int>,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertEqual(history.bars.count, 60, file: file, line: line)
        for (index, bar) in history.bars.enumerated() {
            XCTAssertEqual(bar.close ?? 0, expectedClose, accuracy: 0.001, file: file, line: line)
            XCTAssertEqual(bar.repaired, repairedRange.contains(index), "Unexpected repair flag at index \(index)", file: file, line: line)
        }
    }

    private func repairedHistory(
        currency: String,
        regularMarketPrice: Double? = nil,
        closes: [Double],
        firstTimestamp: Int = 1_704_067_200,
        capitalGainTimestamps: [Int] = []
    ) async throws -> YFHistorySeries {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [CoreRepairURLProtocol.self]
        let client = YFinanceClient(session: URLSession(configuration: configuration))
        let timestamps = closes.indices.map { firstTimestamp + ($0 * 86_400) }

        CoreRepairURLProtocol.handler = { request in
            guard let url = request.url else {
                throw CoreRepairTestError.missingURL
            }
            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: Data("ok".utf8))
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 200, body: Data("repair-regression-crumb".utf8))
            }
            if url.path.hasPrefix("/v8/finance/chart/") {
                var meta: [String: Any] = [
                    "currency": currency,
                    "symbol": "REPAIR",
                    "exchangeName": "LSE",
                    "instrumentType": "EQUITY",
                    "timezone": "GMT",
                    "exchangeTimezoneName": "Europe/London",
                    "gmtoffset": 0,
                    "dataGranularity": "1d",
                    "range": "3mo",
                    "validRanges": ["3mo"]
                ]
                if let regularMarketPrice {
                    meta["regularMarketPrice"] = regularMarketPrice
                }

                var events: [String: Any] = [:]
                if !capitalGainTimestamps.isEmpty {
                    events["capitalGains"] = Dictionary(uniqueKeysWithValues: capitalGainTimestamps.map { timestamp in
                        (String(timestamp), ["amount": 0.5, "date": timestamp, "currency": currency] as [String: Any])
                    })
                }

                let volumes = Array(repeating: 1_000.0, count: closes.count)
                let object: [String: Any] = [
                    "chart": [
                        "result": [[
                            "meta": meta,
                            "timestamp": timestamps,
                            "indicators": [
                                "quote": [[
                                    "open": closes,
                                    "high": closes,
                                    "low": closes,
                                    "close": closes,
                                    "volume": volumes
                                ]],
                                "adjclose": [["adjclose": closes]]
                            ],
                            "events": events
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
            throw CoreRepairTestError.unexpectedURL(url.absoluteString)
        }

        return try await client.history(
            symbol: "REPAIR",
            range: .threeMonths,
            interval: .oneDay,
            events: [.dividends, .splits, .capitalGains],
            repair: true,
            keepNa: true,
            timeout: 2
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

private enum CoreRepairTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class CoreRepairURLProtocol: URLProtocol {
    private static let handlerState = TestURLProtocolHandlerState()

    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))? {
        get { handlerState.snapshot() }
        set { handlerState.set(newValue) }
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "CoreRepairURLProtocol", code: 1))
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
