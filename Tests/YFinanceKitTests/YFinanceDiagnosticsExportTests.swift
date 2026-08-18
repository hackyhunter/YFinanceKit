import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceDiagnosticsExportTests: XCTestCase {
    func testExportContainsOnlyRedactedOperationalMetadata() throws {
        let startedAt = Date(timeIntervalSince1970: 1_700_000_000)
        let trace = YFRequestTrace(
            endpoint: "history",
            resource: "AAPL:1mo:1d",
            startedAt: startedAt,
            duration: 0.245,
            attempts: 2,
            outcome: .success,
            failureKind: nil
        )
        let snapshot = YFRequestDiagnosticsSnapshot(
            logicalRequests: 1,
            attempts: 2,
            successes: 1,
            failures: 0,
            retries: 1,
            rateLimits: 0,
            coalescedRequests: 0,
            cacheHits: 0,
            cacheMisses: 1,
            activeRequests: 0,
            queuedRequests: 0,
            cooldownUntil: nil,
            recentTraces: [trace]
        )

        let export = snapshot.redactedExport(generatedAt: startedAt)
        let data = try JSONEncoder().encode(export)
        let text = try XCTUnwrap(String(data: data, encoding: .utf8))

        XCTAssertTrue(text.contains("history"))
        XCTAssertTrue(text.contains("AAPL:1mo:1d"))
        XCTAssertFalse(text.lowercased().contains("crumb"))
        XCTAssertFalse(text.lowercased().contains("cookie"))
        XCTAssertFalse(text.lowercased().contains("authorization"))
        XCTAssertFalse(text.lowercased().contains("user-agent"))
        XCTAssertFalse(text.contains("finance.yahoo.com"))
    }

    func testTraceDurationIsMilliseconds() {
        let trace = YFRequestTrace(
            endpoint: "quote",
            resource: "MSFT",
            startedAt: Date(timeIntervalSince1970: 0),
            duration: 1.2346,
            attempts: 1,
            outcome: .failure,
            failureKind: .serverUnavailable
        )
        let export = YFDiagnosticsTraceExport(trace)
        XCTAssertEqual(export.durationMilliseconds, 1_235)
        XCTAssertEqual(export.failureKind, "serverUnavailable")
        XCTAssertEqual(export.outcome, "failure")
    }
}
