import Foundation
import XCTest
@testable import YFinanceKit

/// Deterministic, network-free mutations of otherwise plausible Yahoo payloads.
/// The contract is simple: malformed/schema-shifted responses may return a
/// structured error or an empty best-effort result, but must never crash.
final class YFinanceSchemaMutationTests: XCTestCase {
    override func setUp() {
        super.setUp()
        SchemaMutationURLProtocol.reset()
    }

    override func tearDown() {
        SchemaMutationURLProtocol.reset()
        super.tearDown()
    }

    func testChartNullResultFailsGracefully() async throws {
        let client = makeClient(chartBody: #"{"chart":{"result":null,"error":null}}"#)
        await assertYFinanceFailure {
            _ = try await client.history(symbol: "AAPL", range: .fiveDays, interval: .oneDay)
        }
    }

    func testChartEmptyResultFailsGracefully() async throws {
        let client = makeClient(chartBody: #"{"chart":{"result":[],"error":null}}"#)
        await assertYFinanceFailure {
            _ = try await client.history(symbol: "AAPL", range: .fiveDays, interval: .oneDay)
        }
    }

    func testChartHTMLBodyIsStructuredDecodingFailure() async throws {
        let client = makeClient(chartBody: "<html><body>temporarily unavailable</body></html>")
        do {
            _ = try await client.history(symbol: "AAPL", range: .fiveDays, interval: .oneDay)
            XCTFail("Expected decoding failure")
        } catch let error as YFinanceError {
            XCTAssertEqual(error.failureKind, .decoding)
        } catch {
            XCTFail("Expected YFinanceError, got \(error)")
        }
    }

    func testChartTruncatedJSONIsStructuredDecodingFailure() async throws {
        let client = makeClient(chartBody: #"{"chart":{"result":[{"meta":{"symbol":"AAPL"}"#)
        do {
            _ = try await client.history(symbol: "AAPL", range: .fiveDays, interval: .oneDay)
            XCTFail("Expected decoding failure")
        } catch let error as YFinanceError {
            XCTAssertEqual(error.failureKind, .decoding)
        } catch {
            XCTFail("Expected YFinanceError, got \(error)")
        }
    }

    func testChartTimestampTypeShiftDoesNotCrash() async throws {
        let body = chartBody(
            timestampJSON: #"["1786996800"]"#,
            quoteJSON: #"{"open":[199.0],"high":[202.0],"low":[198.0],"close":[201.0],"volume":[123456.0]}"#
        )
        let client = makeClient(chartBody: body)
        await assertYFinanceFailure {
            _ = try await client.history(symbol: "AAPL", range: .fiveDays, interval: .oneDay)
        }
    }

    func testChartNullQuoteArraysDoNotCrash() async throws {
        let body = chartBody(
            timestampJSON: "[1786996800]",
            quoteJSON: #"{"open":null,"high":null,"low":null,"close":null,"volume":null}"#
        )
        let client = makeClient(chartBody: body)

        do {
            let series = try await client.history(symbol: "AAPL", range: .fiveDays, interval: .oneDay)
            // Best-effort empty/sparse handling is acceptable.
            XCTAssertLessThanOrEqual(series.bars.count, 1)
        } catch let error as YFinanceError {
            XCTAssertTrue(
                [.decoding, .missingData, .yahooAPI].contains(error.failureKind),
                "Unexpected failure kind: \(error.failureKind)"
            )
        }
    }

    func testQuoteNullResultDoesNotCrash() async throws {
        let client = makeClient(
            chartBody: nil,
            quoteBody: #"{"quoteResponse":{"result":null,"error":null}}"#
        )
        await assertYFinanceFailure {
            _ = try await client.quote(symbol: "AAPL")
        }
    }

    func testQuoteNumericStringTypeShiftDoesNotCrash() async throws {
        let client = makeClient(
            chartBody: nil,
            quoteBody: #"{"quoteResponse":{"result":[{"symbol":"AAPL","regularMarketPrice":"201.25"}],"error":null}}"#
        )
        await assertYFinanceFailure {
            _ = try await client.quote(symbol: "AAPL")
        }
    }

    func testSearchNullCollectionsDegradeToEmptyArrays() async throws {
        let session = makeSession()
        let client = YFinanceClient(session: session)
        installHandler(
            chartBody: nil,
            quoteBody: nil,
            searchBody: #"{"count":0,"quotes":null,"news":null,"lists":null,"researchReports":null,"nav":null}"#
        )

        let result = try await client.search(query: "apple")
        XCTAssertTrue(result.quotes.isEmpty)
        XCTAssertTrue(result.news.isEmpty)
        XCTAssertTrue(result.lists.isEmpty)
        XCTAssertTrue(result.researchReports.isEmpty)
        XCTAssertTrue(result.nav.isEmpty)
    }

    func testYahooErrorObjectPreservesProviderReason() async throws {
        let client = makeClient(
            chartBody: #"{"chart":{"result":null,"error":{"code":"Not Found","description":"No data found, symbol may be delisted"}}}"#
        )

        do {
            _ = try await client.history(symbol: "AAPL", range: .fiveDays, interval: .oneDay)
            XCTFail("Expected Yahoo error")
        } catch let error as YFinanceError {
            XCTAssertEqual(error.failureKind, .yahooAPI)
            XCTAssertTrue((error.yahooDescription ?? "").contains("No data found"))
        } catch {
            XCTFail("Expected YFinanceError, got \(error)")
        }
    }

    private func makeClient(
        chartBody: String?,
        quoteBody: String? = nil
    ) -> YFinanceClient {
        let session = makeSession()
        installHandler(chartBody: chartBody, quoteBody: quoteBody, searchBody: nil)
        return YFinanceClient(session: session)
    }

    private func installHandler(
        chartBody: String?,
        quoteBody: String?,
        searchBody: String?
    ) {
        SchemaMutationURLProtocol.handler = { request in
            guard let url = request.url else {
                throw SchemaMutationTestError.missingURL
            }

            if url.host == "fc.yahoo.com" {
                return Self.response(url: url, status: 200, body: "ok", contentType: "text/plain")
            }
            if url.path == "/v1/test/getcrumb" {
                return Self.response(url: url, status: 200, body: "mutation-crumb", contentType: "text/plain")
            }
            if url.path.hasPrefix("/v8/finance/chart/"), let chartBody {
                return Self.response(url: url, status: 200, body: chartBody)
            }
            if url.path == "/v7/finance/quote", let quoteBody {
                return Self.response(url: url, status: 200, body: quoteBody)
            }
            if url.path == "/v1/finance/search", let searchBody {
                return Self.response(url: url, status: 200, body: searchBody)
            }

            throw SchemaMutationTestError.unexpectedURL(url.absoluteString)
        }
    }

    private func chartBody(timestampJSON: String, quoteJSON: String) -> String {
        """
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
                "gmtoffset": -14400,
                "dataGranularity": "1d",
                "range": "5d",
                "validRanges": ["1d", "5d", "1mo"]
              },
              "timestamp": \(timestampJSON),
              "indicators": {
                "quote": [\(quoteJSON)],
                "adjclose": [{"adjclose":[201.0]}]
              }
            }],
            "error": null
          }
        }
        """
    }

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [SchemaMutationURLProtocol.self]
        return URLSession(configuration: configuration)
    }

    private static func response(
        url: URL,
        status: Int,
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

    private func assertYFinanceFailure(
        _ operation: () async throws -> Void
    ) async {
        do {
            try await operation()
            XCTFail("Expected structured failure")
        } catch is YFinanceError {
            // Structured library failure is the contract.
        } catch {
            XCTFail("Expected YFinanceError, got \(error)")
        }
    }
}

private enum SchemaMutationTestError: Error {
    case missingURL
    case unexpectedURL(String)
}

private final class SchemaMutationURLProtocol: URLProtocol {
    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    static func reset() {
        handler = nil
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(
                self,
                didFailWithError: NSError(domain: "SchemaMutationURLProtocol", code: 1)
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
