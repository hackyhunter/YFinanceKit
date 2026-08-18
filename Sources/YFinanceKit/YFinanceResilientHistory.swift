import Foundation

public extension YFResilientClient {
    /// Fetches history through request coordination and applies the conservative
    /// post-repair hardening pass when repair is enabled.
    func hardenedHistory(
        symbol: String,
        range: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = true,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFHistorySeries {
        let series = try await history(
            symbol: symbol,
            range: range,
            interval: interval,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            timeout: timeout
        )
        return repair ? series.hardened().series : series
    }

    /// Stale-while-revalidate history with the same conservative post-repair pass.
    func hardenedHistoryCached(
        symbol: String,
        range: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        freshFor: TimeInterval = 60,
        staleFor: TimeInterval = 24 * 60 * 60,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = true,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFCachedResult<YFHistorySeries> {
        let cached = try await historyCached(
            symbol: symbol,
            range: range,
            interval: interval,
            freshFor: freshFor,
            staleFor: staleFor,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            timeout: timeout
        )

        let value = repair ? cached.value.hardened().series : cached.value
        return YFCachedResult(
            value: value,
            freshness: cached.freshness,
            storedAt: cached.storedAt
        )
    }

    /// Start/end variant. Corporate actions are trimmed to the exact requested
    /// half-open interval, independent of the device's local timezone.
    func hardenedHistory(
        symbol: String,
        start: Date,
        end: Date = Date(),
        interval: YFinanceClient.Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        repair: Bool = true,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil
    ) async throws -> YFHistorySeries {
        let series = try await history(
            symbol: symbol,
            start: start,
            end: end,
            interval: interval,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: repair,
            keepNa: keepNa,
            rounding: rounding,
            timeout: timeout
        )
        let trimmed = series.trimmingEvents(to: start..<end)
        return repair ? trimmed.hardened().series : trimmed
    }
}
