import Foundation

/// Checked conversions for provider-controlled numeric values.
///
/// Yahoo commonly encodes integral values as JSON doubles. Swift's direct
/// `Double` to `Int` conversion traps when the value is outside the platform
/// integer range, so every provider-controlled conversion must be failable.
enum YFNumericSafety {
    static func integer(
        from value: Double,
        rounding rule: FloatingPointRoundingRule = .towardZero
    ) -> Int? {
        guard value.isFinite else { return nil }
        return Int(exactly: value.rounded(rule))
    }

    static func nonNegativeInteger(
        from value: Double,
        rounding rule: FloatingPointRoundingRule = .towardZero
    ) -> Int? {
        guard value >= 0,
              let integer = integer(from: value, rounding: rule),
              integer >= 0 else {
            return nil
        }
        return integer
    }

    static func sumNonNegative(_ values: [Int]) -> Int? {
        guard !values.isEmpty else { return nil }

        var total = 0
        for value in values {
            guard value >= 0 else { return nil }
            let addition = total.addingReportingOverflow(value)
            guard !addition.overflow else { return nil }
            total = addition.partialValue
        }
        return total
    }
}
