import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceResilienceTests: XCTestCase {
    func testCoordinatorRetriesServerFailuresThenSucceeds() async throws {
        let clock = TestClock(Date(timeIntervalSince1970: 1_700_000_000))
        let counter = AttemptCounter()
        let coordinator = YFRequestCoordinator(
            policy: YFRequestPolicy(
                maxConcurrentRequests: 2,
                maxAttempts: 3,
                baseRetryDelay: 0.25,
                maxRetryDelay: 1,
                retryJitterFraction: 0,
                baseRateLimitCooldown: 2,
                maxRateLimitCooldown: 30,
                traceCapacity: 20
            ),
            clock: clock,
            jitter: YFZeroJitterSource()
        )

        let value: String = try await coordinator.execute(endpoint: "test", resource: "AAPL") {
            let attempt = await counter.increment()
            if attempt < 3 {
                throw YFinanceError.httpStatus(503)
            }
            return "ok"
        }

        XCTAssertEqual(value, "ok")
        let attemptCount = await counter.value()
        XCTAssertEqual(attemptCount, 3)
        let snapshot = await coordinator.snapshot()
        XCTAssertEqual(snapshot.attempts, 3)
        XCTAssertEqual(snapshot.retries, 2)
        XCTAssertEqual(snapshot.successes, 1)
        XCTAssertEqual(snapshot.rateLimits, 0)
    }

    func testCoordinatorDoesNotRetryRateLimitAndOpensCooldown() async throws {
        let start = Date(timeIntervalSince1970: 1_700_000_000)
        let clock = TestClock(start)
        let counter = AttemptCounter()
        let coordinator = YFRequestCoordinator(
            policy: YFRequestPolicy(
                maxConcurrentRequests: 1,
                maxAttempts: 5,
                baseRetryDelay: 0,
                maxRetryDelay: 0,
                retryJitterFraction: 0,
                baseRateLimitCooldown: 4,
                maxRateLimitCooldown: 30,
                traceCapacity: 20
            ),
            clock: clock,
            jitter: YFZeroJitterSource()
        )

        do {
            let _: Int = try await coordinator.execute(endpoint: "quote", resource: "AAPL") {
                _ = await counter.increment()
                throw YFinanceError.httpStatus(429)
            }
            XCTFail("Expected rate limit")
        } catch let error as YFinanceError {
            XCTAssertTrue(error.isRateLimited)
        }

        let rateLimitAttempts = await counter.value()
        XCTAssertEqual(rateLimitAttempts, 1, "429 must never be retried by the coordinator")
        var snapshot = await coordinator.snapshot()
        XCTAssertEqual(snapshot.rateLimits, 1)
        XCTAssertEqual(snapshot.failures, 1)
        XCTAssertNotNil(snapshot.cooldownUntil)

        let value: Int = try await coordinator.execute(endpoint: "quote", resource: "MSFT") {
            7
        }
        XCTAssertEqual(value, 7)
        let timeAfterCooldown = await clock.now()
        XCTAssertGreaterThanOrEqual(timeAfterCooldown, start.addingTimeInterval(4))

        snapshot = await coordinator.snapshot()
        XCTAssertEqual(snapshot.successes, 1)
    }

    func testStaleCacheFreshStaleAndExpired() async {
        let cache = YFStaleCache<String, Int>(capacity: 4)
        let start = Date(timeIntervalSince1970: 1_700_000_000)
        await cache.store(42, for: "AAPL", at: start)

        switch await cache.lookup("AAPL", freshFor: 10, staleFor: 30, now: start.addingTimeInterval(5)) {
        case .fresh(let value, _): XCTAssertEqual(value, 42)
        default: XCTFail("Expected fresh cache hit")
        }

        switch await cache.lookup("AAPL", freshFor: 10, staleFor: 30, now: start.addingTimeInterval(20)) {
        case .stale(let value, _): XCTAssertEqual(value, 42)
        default: XCTFail("Expected stale cache hit")
        }

        switch await cache.lookup("AAPL", freshFor: 10, staleFor: 30, now: start.addingTimeInterval(31)) {
        case .miss: break
        default: XCTFail("Expected expired cache miss")
        }
    }

    func testRepairsOnlyBoundedInteriorHundredXBlock() {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let closes: [Double] = [50, 51, 52, 5_000, 5_100, 5_200, 53, 54, 55, 56]
        let series = makeSeries(
            bars: closes.enumerated().map { index, close in
                bar(date: base.addingTimeInterval(Double(index) * 86_400), close: close)
            }
        )

        let result = series.repairingInteriorUnitScaleBlocks()
        XCTAssertEqual(result.repairedIndices, [3, 4, 5])
        XCTAssertEqual(result.series.bars[3].close ?? 0, 50, accuracy: 0.001)
        XCTAssertEqual(result.series.bars[4].close ?? 0, 51, accuracy: 0.001)
        XCTAssertEqual(result.series.bars[5].close ?? 0, 52, accuracy: 0.001)
        XCTAssertFalse(result.series.bars[2].repaired)
        XCTAssertTrue(result.series.bars[3].repaired)
        XCTAssertFalse(result.series.bars[6].repaired)
    }

    func testDoesNotGuessAtSingleEdgeHundredXSwitch() {
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let closes: [Double] = [5_000, 5_100, 5_200, 53, 54, 55]
        let series = makeSeries(
            bars: closes.enumerated().map { index, close in
                bar(date: base.addingTimeInterval(Double(index) * 86_400), close: close)
            }
        )

        let result = series.repairingInteriorUnitScaleBlocks()
        XCTAssertTrue(result.repairedIndices.isEmpty)
        XCTAssertEqual(result.series.bars[0].close, 5_000)
        XCTAssertEqual(result.series.bars[3].close, 53)
    }

    func testNormalizesInvalidOHLCWithoutChangingOpenOrClose() {
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let broken = YFHistoryBar(
            date: date,
            open: 100,
            high: 99,
            low: 101,
            close: 102,
            adjustedClose: 102,
            volume: 100
        )
        let result = makeSeries(bars: [broken]).normalizingInvalidOHLC()

        XCTAssertEqual(result.normalizedIndices, [0])
        XCTAssertEqual(result.series.bars[0].open, 100)
        XCTAssertEqual(result.series.bars[0].close, 102)
        XCTAssertEqual(result.series.bars[0].high, 102)
        XCTAssertEqual(result.series.bars[0].low, 99)
        XCTAssertTrue(result.series.bars[0].repaired)
    }

    func testCustomWindowTrimsEventsAtExactBounds() {
        let start = Date(timeIntervalSince1970: 1_700_000_000)
        let end = start.addingTimeInterval(10 * 86_400)
        let events = [
            YFHistoryEvent(kind: .dividend, date: start.addingTimeInterval(-10), value: 1, ratio: nil, raw: .object([:])),
            YFHistoryEvent(kind: .dividend, date: start.addingTimeInterval(86_400), value: 2, ratio: nil, raw: .object([:])),
            YFHistoryEvent(kind: .split, date: end, value: nil, ratio: 2, raw: .object([:])),
        ]
        let series = makeSeries(bars: [], events: events)

        let trimmed = series.trimmingEvents(to: start..<end)
        XCTAssertEqual(trimmed.events.count, 1)
        XCTAssertEqual(trimmed.events.first?.value, 2)
    }

    func testYahooDateSemanticsAreIndependentOfDeviceTimezone() {
        XCTAssertEqual(YFYahooDateSemantics.utcDateString(epoch: 0), "1970-01-01")
        XCTAssertEqual(
            YFYahooDateSemantics.exchangeDateString(epoch: 0, timeZoneIdentifier: "America/Los_Angeles"),
            "1969-12-31"
        )
    }

    private func bar(date: Date, close: Double, repaired: Bool = false) -> YFHistoryBar {
        YFHistoryBar(
            date: date,
            open: close,
            high: close,
            low: close,
            close: close,
            adjustedClose: close,
            volume: 1_000,
            repaired: repaired
        )
    }

    private func makeSeries(
        bars: [YFHistoryBar],
        events: [YFHistoryEvent] = []
    ) -> YFHistorySeries {
        let meta = YFHistoryMeta(
            currency: "USD",
            symbol: "TEST",
            exchangeName: "NMS",
            instrumentType: "EQUITY",
            timezone: "EST",
            exchangeTimezoneName: "America/New_York",
            regularMarketPrice: nil,
            chartPreviousClose: nil,
            previousClose: nil,
            gmtoffset: -18_000,
            dataGranularity: "1d",
            priceHint: 2,
            range: "1mo",
            validRanges: ["1mo"],
            lastTrade: nil,
            tradingPeriods: nil
        )
        return YFHistorySeries(
            symbol: "TEST",
            meta: meta,
            interval: .oneDay,
            bars: bars,
            events: events,
            repairEnabled: false
        )
    }
}

private actor AttemptCounter {
    private var count = 0

    func increment() -> Int {
        count += 1
        return count
    }

    func value() -> Int {
        count
    }
}

private actor TestClock: YFClock {
    private var current: Date

    init(_ date: Date) {
        current = date
    }

    func now() async -> Date {
        current
    }

    func sleep(for seconds: TimeInterval) async throws {
        try Task.checkCancellation()
        current = current.addingTimeInterval(max(0, seconds))
    }
}
