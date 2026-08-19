import Foundation
import XCTest
@testable import YFinanceKit

/// Deterministic structural fuzzing around a known-good Yahoo chart envelope.
///
/// This is not a security fuzzer and does not touch the network. Its purpose is
/// to continuously stress defensive decoding with deleted fields, nulls, type
/// shifts, truncated arrays and malformed nested shapes. Every case must either
/// return a valid/best-effort series or a structured YFinanceError, never an
/// unrelated exception or process trap.
final class YFinanceMutationFuzzTests: XCTestCase {
    override func setUp() {
        super.setUp()
        MutationFuzzURLProtocol.reset()
    }

    override func tearDown() {
        MutationFuzzURLProtocol.reset()
        super.tearDown()
    }

    func testDeterministicChartMutationCorpusNeverEscapesStructuredErrors() async throws {
        for seed in 1...96 {
            let body = try mutatedChartBody(seed: UInt64(seed))
            let session = makeSession(body: body)
            let client = YFinanceClient(session: session)

            do {
                let series = try await client.history(
                    symbol: "AAPL",
                    range: .fiveDays,
                    interval: .oneDay,
                    includePrePost: false,
                    events: [],
                    autoAdjust: false,
                    backAdjust: false,
                    repair: false,
                    keepNa: true,
                    rounding: false,
                    timeout: 2
                )
                _ = series.integrityReport()
            } catch is YFinanceError {
                // The expected defensive failure boundary.
            } catch {
                XCTFail("Seed \(seed) escaped structured YFinanceError: \(error)")
            }
        }
    }

    private func mutatedChartBody(seed: UInt64) throws -> Data {
        var object = try XCTUnwrap(baseChartObject() as? [String: Any])
        var rng = MutationFuzzRNG(seed: seed)
        let mutationCount = 1 + rng.nextInt(upperBound: 4)

        for _ in 0..<mutationCount {
            applyMutation(index: rng.nextInt(upperBound: 14), object: &object, rng: &rng)
        }

        return try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    }

    private func baseChartObject() -> Any {
        [
            "chart": [
                "result": [[
                    "meta": [
                        "currency": "USD",
                        "symbol": "AAPL",
                        "exchangeName": "NMS",
                        "instrumentType": "EQUITY",
                        "timezone": "EDT",
                        "exchangeTimezoneName": "America/New_York",
                        "regularMarketPrice": 201.25,
                        "gmtoffset": -14_400,
                        "dataGranularity": "1d",
                        "range": "5d",
                        "validRanges": ["1d", "5d", "1mo"],
                    ],
                    "timestamp": [
                        1_786_912_200,
                        1_786_998_600,
                        1_787_085_000,
                    ],
                    "indicators": [
                        "quote": [[
                            "open": [199.0, 200.0, 201.0],
                            "high": [202.0, 203.0, 204.0],
                            "low": [198.0, 199.0, 200.0],
                            "close": [201.0, 202.0, 203.0],
                            "volume": [100_000, 110_000, 120_000],
                        ]],
                        "adjclose": [[
                            "adjclose": [201.0, 202.0, 203.0],
                        ]],
                    ],
                    "events": [
                        "dividends": [:] as [String: Any],
                        "splits": [:] as [String: Any],
                    ],
                ]],
                "error": NSNull(),
            ]
        ]
    }

    private func applyMutation(
        index: Int,
        object: inout [String: Any],
        rng: inout MutationFuzzRNG
    ) {
        switch index {
        case 0:
            mutateResult(object: &object) { _ in NSNull() }
        case 1:
            mutateResult(object: &object) { _ in [] as [Any] }
        case 2:
            mutateResultObject(object: &object) { result in
                result.removeValue(forKey: "meta")
            }
        case 3:
            mutateResultObject(object: &object) { result in
                result["meta"] = NSNull()
            }
        case 4:
            mutateResultObject(object: &object) { result in
                if let timestamps = result["timestamp"] as? [Any] {
                    result["timestamp"] = timestamps.map { String(describing: $0) }
                }
            }
        case 5:
            mutateResultObject(object: &object) { result in
                result["timestamp"] = [1_786_912_200, NSNull(), "bad"]
            }
        case 6:
            mutateQuoteObject(object: &object) { quote in
                quote["close"] = [201.0]
            }
        case 7:
            mutateQuoteObject(object: &object) { quote in
                quote["open"] = NSNull()
                quote["high"] = NSNull()
                quote["low"] = NSNull()
            }
        case 8:
            mutateQuoteObject(object: &object) { quote in
                quote["volume"] = [100_000, "110000", NSNull()]
            }
        case 9:
            mutateResultObject(object: &object) { result in
                result["indicators"] = "not-an-object"
            }
        case 10:
            mutateResultObject(object: &object) { result in
                result["events"] = ["dividends": NSNull(), "splits": "bad"]
            }
        case 11:
            mutateMetaObject(object: &object) { meta in
                meta["regularMarketPrice"] = rng.nextBool() ? "201.25" : NSNull()
            }
        case 12:
            mutateMetaObject(object: &object) { meta in
                meta["validRanges"] = [NSNull(), 5, "1mo"]
            }
        default:
            mutateQuoteObject(object: &object) { quote in
                let columns = ["open", "high", "low", "close", "volume"]
                quote.removeValue(forKey: columns[rng.nextInt(upperBound: columns.count)])
            }
        }
    }

    private func mutateResult(
        object: inout [String: Any],
        mutation: (Any?) -> Any
    ) {
        guard var chart = object["chart"] as? [String: Any] else { return }
        chart["result"] = mutation(chart["result"])
        object["chart"] = chart
    }

    private func mutateResultObject(
        object: inout [String: Any],
        mutation: (inout [String: Any]) -> Void
    ) {
        guard var chart = object["chart"] as? [String: Any],
              var resultArray = chart["result"] as? [[String: Any]],
              !resultArray.isEmpty else { return }
        mutation(&resultArray[0])
        chart["result"] = resultArray
        object["chart"] = chart
    }

    private func mutateMetaObject(
        object: inout [String: Any],
        mutation: (inout [String: Any]) -> Void
    ) {
        mutateResultObject(object: &object) { result in
            guard var meta = result["meta"] as? [String: Any] else { return }
            mutation(&meta)
            result["meta"] = meta
        }
    }

    private func mutateQuoteObject(
        object: inout [String: Any],
        mutation: (inout [String: Any]) -> Void
    ) {
        mutateResultObject(object: &object) { result in
            guard var indicators = result["indicators"] as? [String: Any],
                  var quotes = indicators["quote"] as? [[String: Any]],
                  !quotes.isEmpty else { return }
            mutation(&quotes[0])
            indicators["quote"] = quotes
            result["indicators"] = indicators
        }
    }

    private func makeSession(body: Data) -> URLSession {
        MutationFuzzURLProtocol.setBody(body)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MutationFuzzURLProtocol.self]
        return URLSession(configuration: configuration)
    }
}

private struct MutationFuzzRNG {
    private var state: UInt64

    init(seed: UInt64) {
        self.state = seed == 0 ? 0x9E3779B97F4A7C15 : seed
    }

    mutating func next() -> UInt64 {
        state = state &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
        return state
    }

    mutating func nextInt(upperBound: Int) -> Int {
        precondition(upperBound > 0)
        return Int(next() % UInt64(upperBound))
    }

    mutating func nextBool() -> Bool {
        (next() & 1) == 1
    }
}

private enum MutationFuzzTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class MutationFuzzProtocolState: @unchecked Sendable {
    private let lock = NSLock()
    private var body = Data()

    func reset() {
        lock.lock()
        body = Data()
        lock.unlock()
    }

    func setBody(_ value: Data) {
        lock.lock()
        body = value
        lock.unlock()
    }

    func currentBody() -> Data {
        lock.lock()
        defer { lock.unlock() }
        return body
    }
}

private final class MutationFuzzURLProtocol: URLProtocol {
    private static let state = MutationFuzzProtocolState()

    static func reset() {
        state.reset()
    }

    static func setBody(_ body: Data) {
        state.setBody(body)
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let url = request.url else {
            client?.urlProtocol(self, didFailWithError: MutationFuzzTestError.missingURL)
            return
        }

        let status: Int
        let responseBody: Data
        let contentType: String

        if url.host == "fc.yahoo.com" {
            status = 200
            responseBody = Data("ok".utf8)
            contentType = "text/plain"
        } else if url.path == "/v1/test/getcrumb" {
            status = 200
            responseBody = Data("mutation-fuzz-crumb".utf8)
            contentType = "text/plain"
        } else if url.path.hasPrefix("/v8/finance/chart/") {
            status = 200
            responseBody = Self.state.currentBody()
            contentType = "application/json"
        } else {
            client?.urlProtocol(
                self,
                didFailWithError: MutationFuzzTestError.unexpectedURL(url.absoluteString)
            )
            return
        }

        let response = HTTPURLResponse(
            url: url,
            statusCode: status,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": contentType]
        )!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: responseBody)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}
