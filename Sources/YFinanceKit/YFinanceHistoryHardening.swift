import Foundation

public struct YFHistoryHardeningResult: Sendable {
    public let series: YFHistorySeries
    public let repairedIndices: [Int]
    public let normalizedIndices: [Int]

    public init(series: YFHistorySeries, repairedIndices: [Int], normalizedIndices: [Int]) {
        self.series = series
        self.repairedIndices = repairedIndices
        self.normalizedIndices = normalizedIndices
    }

    public var changed: Bool {
        !repairedIndices.isEmpty || !normalizedIndices.isEmpty
    }
}

public extension YFHistorySeries {
    /// Bounds corporate actions to the actual requested window. This mirrors the
    /// upstream custom-period fix without depending on the machine's local timezone.
    func trimmingEvents(to range: Range<Date>) -> YFHistorySeries {
        let filtered = events.filter { range.contains($0.date) }
        guard filtered.count != events.count else { return self }
        return YFHistorySeries(
            symbol: symbol,
            meta: meta,
            interval: interval,
            bars: bars,
            events: filtered,
            repairEnabled: repairEnabled
        )
    }

    /// Repairs bounded interior 100x / 0.01x blocks by looking for a pair of
    /// inverse scale transitions. A single edge transition is never modified,
    /// which avoids guessing about real splits or genuine quotation-unit switches.
    /// Split-adjacent transitions are excluded when Yahoo supplied a split event.
    func repairingInteriorUnitScaleBlocks(
        relativeTolerance: Double = 0.12,
        maxSegmentFraction: Double = 0.60
    ) -> YFHistoryHardeningResult {
        guard bars.count >= 5 else {
            return YFHistoryHardeningResult(series: self, repairedIndices: [], normalizedIndices: [])
        }

        struct Transition {
            let index: Int
            let correctionFactor: Double
        }

        let tolerance = min(max(relativeTolerance, 0.01), 0.40)
        let maxFraction = min(max(maxSegmentFraction, 0.05), 0.90)
        var transitions: [Transition] = []
        transitions.reserveCapacity(8)

        for index in 1..<bars.count {
            guard let previous = yfRepresentativePrice(bars[index - 1]),
                  let current = yfRepresentativePrice(bars[index]),
                  previous > 0,
                  current > 0 else {
                continue
            }

            let ratio = current / previous
            let correctionFactor: Double?
            if yfIsNear(ratio, target: 100, tolerance: tolerance) {
                correctionFactor = 0.01
            } else if yfIsNear(ratio, target: 0.01, tolerance: tolerance) {
                correctionFactor = 100
            } else {
                correctionFactor = nil
            }

            guard let correctionFactor,
                  !yfTransitionIsSplitAdjacent(index: index) else {
                continue
            }
            transitions.append(Transition(index: index, correctionFactor: correctionFactor))
        }

        guard transitions.count >= 2 else {
            return YFHistoryHardeningResult(series: self, repairedIndices: [], normalizedIndices: [])
        }

        var output = bars
        var repaired: Set<Int> = []
        var cursor = 0

        while cursor + 1 < transitions.count {
            let first = transitions[cursor]
            let second = transitions[cursor + 1]

            // Entering and leaving a bounded bad block must require opposite
            // corrections. Two same-direction jumps are not enough evidence.
            guard first.correctionFactor != second.correctionFactor else {
                cursor += 1
                continue
            }

            let start = first.index
            let end = second.index
            let count = end - start
            guard start > 0,
                  end < output.count,
                  count >= 2,
                  Double(count) / Double(output.count) <= maxFraction else {
                cursor += 1
                continue
            }

            for index in start..<end {
                output[index] = yfScaleBar(output[index], factor: first.correctionFactor, repaired: true)
                repaired.insert(index)
            }
            cursor += 2
        }

        guard !repaired.isEmpty else {
            return YFHistoryHardeningResult(series: self, repairedIndices: [], normalizedIndices: [])
        }

        let repairedSeries = YFHistorySeries(
            symbol: symbol,
            meta: meta,
            interval: interval,
            bars: output,
            events: events,
            repairEnabled: true
        )
        return YFHistoryHardeningResult(
            series: repairedSeries,
            repairedIndices: repaired.sorted(),
            normalizedIndices: []
        )
    }

    /// Recomputes invalid high/low bounds from finite OHLC values. This is a
    /// conservative structural repair: open and close are not changed.
    func normalizingInvalidOHLC() -> YFHistoryHardeningResult {
        var output = bars
        var normalized: [Int] = []

        for index in output.indices {
            let bar = output[index]
            let candidates = [bar.open, bar.high, bar.low, bar.close].compactMap { value -> Double? in
                guard let value, value.isFinite, value > 0 else { return nil }
                return value
            }
            guard let maxValue = candidates.max(), let minValue = candidates.min() else {
                continue
            }

            let highNeedsRepair = bar.high == nil || !(bar.high?.isFinite ?? false) || (bar.high ?? -Double.infinity) < maxValue
            let lowNeedsRepair = bar.low == nil || !(bar.low?.isFinite ?? false) || (bar.low ?? Double.infinity) > minValue
            guard highNeedsRepair || lowNeedsRepair else { continue }

            output[index] = YFHistoryBar(
                date: bar.date,
                open: bar.open,
                high: highNeedsRepair ? maxValue : bar.high,
                low: lowNeedsRepair ? minValue : bar.low,
                close: bar.close,
                adjustedClose: bar.adjustedClose,
                volume: bar.volume,
                repaired: true
            )
            normalized.append(index)
        }

        let series = normalized.isEmpty ? self : YFHistorySeries(
            symbol: symbol,
            meta: meta,
            interval: interval,
            bars: output,
            events: events,
            repairEnabled: repairEnabled || !normalized.isEmpty
        )
        return YFHistoryHardeningResult(series: series, repairedIndices: [], normalizedIndices: normalized)
    }

    /// Applies the conservative post-repair passes that can be validated without
    /// pandas or hidden Yahoo state. This is safe to run after the main repair engine.
    func hardened() -> YFHistoryHardeningResult {
        let scale = repairingInteriorUnitScaleBlocks()
        let normalized = scale.series.normalizingInvalidOHLC()
        return YFHistoryHardeningResult(
            series: normalized.series,
            repairedIndices: scale.repairedIndices,
            normalizedIndices: normalized.normalizedIndices
        )
    }

    private func yfTransitionIsSplitAdjacent(index: Int) -> Bool {
        guard bars.indices.contains(index) else { return false }
        let date = bars[index].date
        let exclusionWindow: TimeInterval = 3 * 24 * 60 * 60
        return events.contains { event in
            guard event.kind == .split else { return false }
            return abs(event.date.timeIntervalSince(date)) <= exclusionWindow
        }
    }

    private func yfRepresentativePrice(_ bar: YFHistoryBar) -> Double? {
        if let close = bar.close, close.isFinite, close > 0 { return close }
        if let adjustedClose = bar.adjustedClose, adjustedClose.isFinite, adjustedClose > 0 { return adjustedClose }
        let values = [bar.open, bar.high, bar.low].compactMap { value -> Double? in
            guard let value, value.isFinite, value > 0 else { return nil }
            return value
        }
        guard !values.isEmpty else { return nil }
        return values.reduce(0, +) / Double(values.count)
    }

    private func yfIsNear(_ value: Double, target: Double, tolerance: Double) -> Bool {
        guard value.isFinite, target != 0 else { return false }
        return abs(value - target) / abs(target) <= tolerance
    }

    private func yfScaleBar(_ bar: YFHistoryBar, factor: Double, repaired: Bool) -> YFHistoryBar {
        YFHistoryBar(
            date: bar.date,
            open: bar.open.map { $0 * factor },
            high: bar.high.map { $0 * factor },
            low: bar.low.map { $0 * factor },
            close: bar.close.map { $0 * factor },
            adjustedClose: bar.adjustedClose.map { $0 * factor },
            volume: bar.volume,
            repaired: bar.repaired || repaired
        )
    }
}

public enum YFYahooDateSemantics {
    /// Yahoo calendar-event epochs are UTC instants. Formatting them with this
    /// helper avoids accidental dependence on the device's local timezone.
    public static func utcDateString(epoch: TimeInterval) -> String {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.string(from: Date(timeIntervalSince1970: epoch))
    }

    /// Computes a calendar day in the exchange timezone from an absolute epoch.
    public static func exchangeDateString(epoch: TimeInterval, timeZoneIdentifier: String) -> String? {
        guard let timeZone = TimeZone(identifier: timeZoneIdentifier) else { return nil }
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = timeZone
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.string(from: Date(timeIntervalSince1970: epoch))
    }
}
