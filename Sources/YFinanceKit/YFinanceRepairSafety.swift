import Foundation

public extension YFHistorySeries {
    var repairedBarFraction: Double {
        guard !bars.isEmpty else { return 0 }
        let repairedCount = bars.reduce(into: 0) { count, bar in
            if bar.repaired { count += 1 }
        }
        return Double(repairedCount) / Double(bars.count)
    }

    /// Compares a heavily repaired result with an un-repaired version and only
    /// substitutes the raw-derived result when the raw series contains strong
    /// evidence of a bounded interior 100x/0.01x block.
    ///
    /// This is a defensive safety valve for the class of failure described by
    /// upstream yfinance #2927: a single-breakpoint unit-switch algorithm can
    /// mistake an interior bad block for a permanent unit switch and alter a
    /// large prefix/suffix of otherwise-good data.
    func replacingSuspiciousWholeSideRepairIfNeeded(
        with rawSeries: YFHistorySeries,
        minimumRepairedFraction: Double = 0.30
    ) -> YFHistoryHardeningResult {
        let threshold = min(max(minimumRepairedFraction, 0.05), 1)
        guard repairedBarFraction >= threshold,
              rawSeries.symbol == symbol,
              rawSeries.interval == interval else {
            return YFHistoryHardeningResult(
                series: self,
                repairedIndices: [],
                normalizedIndices: []
            )
        }

        let boundedRepair = rawSeries.repairingInteriorUnitScaleBlocks()
        guard !boundedRepair.repairedIndices.isEmpty else {
            return YFHistoryHardeningResult(
                series: self,
                repairedIndices: [],
                normalizedIndices: []
            )
        }

        let normalized = boundedRepair.series.normalizingInvalidOHLC()
        return YFHistoryHardeningResult(
            series: normalized.series,
            repairedIndices: boundedRepair.repairedIndices,
            normalizedIndices: normalized.normalizedIndices
        )
    }
}

public extension YFResilientClient {
    /// Uses the full legacy repair engine first. Only if that engine marks a large
    /// fraction of the table repaired do we spend one additional Yahoo request to
    /// compare against raw history. A raw bounded-interior 100x signature wins;
    /// otherwise the full legacy repair result is preserved.
    func safelyRepairedHistory(
        symbol: String,
        range: YFinanceClient.Range = .oneMonth,
        interval: YFinanceClient.Interval = .oneDay,
        includePrePost: Bool = false,
        events: Set<YFinanceClient.HistoryEvent> = [.dividends, .splits],
        autoAdjust: Bool = false,
        backAdjust: Bool = false,
        keepNa: Bool = false,
        rounding: Bool = false,
        timeout: TimeInterval? = nil,
        suspiciousRepairFraction: Double = 0.30
    ) async throws -> YFHistorySeries {
        let repaired = try await hardenedHistory(
            symbol: symbol,
            range: range,
            interval: interval,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: true,
            keepNa: keepNa,
            rounding: rounding,
            timeout: timeout
        )

        guard repaired.repairedBarFraction >= suspiciousRepairFraction else {
            return repaired
        }

        let raw = try await history(
            symbol: symbol,
            range: range,
            interval: interval,
            includePrePost: includePrePost,
            events: events,
            autoAdjust: autoAdjust,
            backAdjust: backAdjust,
            repair: false,
            keepNa: keepNa,
            rounding: rounding,
            timeout: timeout
        )

        return repaired.replacingSuspiciousWholeSideRepairIfNeeded(
            with: raw,
            minimumRepairedFraction: suspiciousRepairFraction
        ).series
    }
}
