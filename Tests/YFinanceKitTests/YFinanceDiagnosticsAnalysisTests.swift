import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceDiagnosticsAnalysisTests: XCTestCase {
    func testGroupsOperationalStatsByEndpoint() throws {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let traces = [
            YFRequestTrace(
                endpoint: "quote",
                resource: "AAPL",
                startedAt: base,
                duration: 0.100,
                attempts: 1,
                outcome: .success
            ),
            YFRequestTrace(
                endpoint: "quote",
                resource: "MSFT",
                startedAt: base,
                duration: 0.300,
                attempts: 2,
                outcome: .failure,
                failureKind: .serverUnavailable
            ),
            YFRequestTrace(
                endpoint: "history",
                resource: "AAPL:1mo:1d",
                startedAt: base,
                duration: 0.250,
                attempts: 1,
                outcome: .rateLimited,
                failureKind: .rateLimited
            ),
        ]
        let snapshot = YFRequestDiagnosticsSnapshot(
            logicalRequests: 3,
            attempts: 4,
            successes: 1,
            failures: 2,
            retries: 1,
            rateLimits: 1,
            coalescedRequests: 0,
            cacheHits: 0,
            cacheMisses: 0,
            activeRequests: 0,
            queuedRequests: 0,
            cooldownUntil: nil,
            recentTraces: traces
        )

        let summaries = snapshot.endpointSummaries()
        XCTAssertEqual(summaries.map(\.endpoint), ["history", "quote"])

        let quote = try XCTUnwrap(summaries.first(where: { $0.endpoint == "quote" }))
        XCTAssertEqual(quote.requestCount, 2)
        XCTAssertEqual(quote.successes, 1)
        XCTAssertEqual(quote.failures, 1)
        XCTAssertEqual(quote.totalAttempts, 3)
        XCTAssertEqual(quote.maxAttempts, 2)
        XCTAssertEqual(quote.averageDurationMilliseconds, 200)
        XCTAssertEqual(quote.maxDurationMilliseconds, 300)
        XCTAssertEqual(quote.failureKinds["serverUnavailable"], 1)

        let history = try XCTUnwrap(summaries.first(where: { $0.endpoint == "history" }))
        XCTAssertEqual(history.rateLimited, 1)
        XCTAssertEqual(history.failureKinds["rateLimited"], 1)
    }
}
