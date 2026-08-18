import Foundation

public enum YFHistoryIntegritySeverity: Int, Comparable, Sendable {
    case info = 0
    case warning = 1
    case error = 2

    public static func < (lhs: YFHistoryIntegritySeverity, rhs: YFHistoryIntegritySeverity) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

public enum YFHistoryIntegrityCode: String, Sendable {
    case duplicateTimestamp
    case nonMonotonicTimestamp
    case nonFinitePrice
    case nonPositivePrice
    case negativeVolume
    case highBelowLow
    case openOutsideRange
    case closeOutsideRange
    case adjustedCloseNonFinite
    case implausibleOneBarScaleJump
}

public struct YFHistoryIntegrityIssue: Sendable {
    public let code: YFHistoryIntegrityCode
    public let severity: YFHistoryIntegritySeverity
    public let index: Int
    public let date: Date
    public let message: String

    public init(
        code: YFHistoryIntegrityCode,
        severity: YFHistoryIntegritySeverity,
        index: Int,
        date: Date,
        message: String
    ) {
        self.code = code
        self.severity = severity
        self.index = index
        self.date = date
        self.message = message
    }
}

public struct YFHistoryIntegrityReport: Sendable {
    public let symbol: String
    public let barCount: Int
    public let issues: [YFHistoryIntegrityIssue]

    public init(symbol: String, barCount: Int, issues: [YFHistoryIntegrityIssue]) {
        self.symbol = symbol
        self.barCount = barCount
        self.issues = issues
    }

    public var errors: [YFHistoryIntegrityIssue] { issues.filter { $0.severity == .error } }
    public var warnings: [YFHistoryIntegrityIssue] { issues.filter { $0.severity == .warning } }
    public var isValid: Bool { errors.isEmpty }
    public var hasWarnings: Bool { !warnings.isEmpty }

    public func contains(_ code: YFHistoryIntegrityCode) -> Bool {
        issues.contains { $0.code == code }
    }
}

public extension YFHistorySeries {
    /// Audits returned bars for structural problems that commonly indicate a
    /// Yahoo schema/data glitch or an incomplete repair. This does not mutate data.
    func integrityReport(
        detectImplausibleScaleJumps: Bool = true
    ) -> YFHistoryIntegrityReport {
        var issues: [YFHistoryIntegrityIssue] = []
        var seenTimestamps: Set<Int64> = []
        var previousDate: Date?
        var previousRepresentativePrice: Double?

        func add(
            _ code: YFHistoryIntegrityCode,
            _ severity: YFHistoryIntegritySeverity,
            index: Int,
            bar: YFHistoryBar,
            _ message: String
        ) {
            issues.append(
                YFHistoryIntegrityIssue(
                    code: code,
                    severity: severity,
                    index: index,
                    date: bar.date,
                    message: message
                )
            )
        }

        for (index, bar) in bars.enumerated() {
            let timestampMicros = Int64((bar.date.timeIntervalSince1970 * 1_000_000).rounded())
            if !seenTimestamps.insert(timestampMicros).inserted {
                add(.duplicateTimestamp, .error, index: index, bar: bar, "Duplicate history timestamp")
            }
            if let previousDate, bar.date < previousDate {
                add(.nonMonotonicTimestamp, .error, index: index, bar: bar, "History timestamps are not monotonic")
            }
            previousDate = bar.date

            let namedPrices: [(String, Double?)] = [
                ("open", bar.open),
                ("high", bar.high),
                ("low", bar.low),
                ("close", bar.close),
            ]
            for (name, value) in namedPrices {
                guard let value else { continue }
                if !value.isFinite {
                    add(.nonFinitePrice, .error, index: index, bar: bar, "Non-finite \(name) price")
                } else if value <= 0 {
                    // Zero/missing prices can occur in legitimate no-trade Yahoo rows,
                    // so flag them as a warning rather than declaring the series corrupt.
                    add(.nonPositivePrice, .warning, index: index, bar: bar, "Non-positive \(name) price")
                }
            }

            if let adjustedClose = bar.adjustedClose, !adjustedClose.isFinite {
                add(.adjustedCloseNonFinite, .error, index: index, bar: bar, "Non-finite adjusted close")
            }
            if let volume = bar.volume, volume < 0 {
                add(.negativeVolume, .error, index: index, bar: bar, "Negative volume")
            }

            if let high = finitePositive(bar.high), let low = finitePositive(bar.low), high < low {
                add(.highBelowLow, .error, index: index, bar: bar, "High is below low")
            }
            if let open = finitePositive(bar.open),
               let high = finitePositive(bar.high),
               let low = finitePositive(bar.low),
               (open > high || open < low) {
                add(.openOutsideRange, .error, index: index, bar: bar, "Open lies outside low/high")
            }
            if let close = finitePositive(bar.close),
               let high = finitePositive(bar.high),
               let low = finitePositive(bar.low),
               (close > high || close < low) {
                add(.closeOutsideRange, .error, index: index, bar: bar, "Close lies outside low/high")
            }

            if detectImplausibleScaleJumps,
               !bar.repaired,
               let current = representativePrice(bar),
               let previous = previousRepresentativePrice,
               previous > 0 {
                let ratio = current / previous
                // This is intentionally narrow: 100x / 0.01x is Yahoo's classic
                // currency-unit corruption signature. Splits can create large real
                // moves, so only flag ratios close to 100x rather than generic jumps.
                if isNear(ratio, target: 100, relativeTolerance: 0.08)
                    || isNear(ratio, target: 0.01, relativeTolerance: 0.08) {
                    add(
                        .implausibleOneBarScaleJump,
                        .warning,
                        index: index,
                        bar: bar,
                        "Possible 100x currency-unit jump remains after history processing"
                    )
                }
            }
            if let current = representativePrice(bar) {
                previousRepresentativePrice = current
            }
        }

        return YFHistoryIntegrityReport(symbol: symbol, barCount: bars.count, issues: issues)
    }

    /// Throws only for structural integrity errors. Warnings such as zero-price
    /// no-trade rows and possible 100x jumps remain inspectable in the report.
    @discardableResult
    func validateIntegrity(
        detectImplausibleScaleJumps: Bool = true
    ) throws -> YFHistoryIntegrityReport {
        let report = integrityReport(detectImplausibleScaleJumps: detectImplausibleScaleJumps)
        guard report.isValid else {
            let details = report.errors.prefix(5).map(\.message).joined(separator: "; ")
            throw YFinanceError.missingData(
                "History integrity validation failed for \(symbol): \(details)"
            )
        }
        return report
    }

    private func finitePositive(_ value: Double?) -> Double? {
        guard let value, value.isFinite, value > 0 else { return nil }
        return value
    }

    private func representativePrice(_ bar: YFHistoryBar) -> Double? {
        if let close = finitePositive(bar.close) { return close }
        if let adjustedClose = finitePositive(bar.adjustedClose) { return adjustedClose }
        let candidates = [bar.open, bar.high, bar.low].compactMap(finitePositive)
        guard !candidates.isEmpty else { return nil }
        return candidates.reduce(0, +) / Double(candidates.count)
    }

    private func isNear(_ value: Double, target: Double, relativeTolerance: Double) -> Bool {
        guard value.isFinite, target != 0 else { return false }
        return abs(value - target) / abs(target) <= relativeTolerance
    }
}
