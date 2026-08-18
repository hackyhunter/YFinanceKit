import Foundation

/// Parses HTTP `Retry-After` according to RFC semantics: either delta-seconds
/// or an HTTP-date. This is transport-agnostic so the current client can adopt
/// it once response headers are exposed without changing higher-level policy.
public enum YFRetryAfterParser {
    public static func parse(
        _ rawValue: String?,
        now: Date = Date(),
        maximum: TimeInterval = 24 * 60 * 60
    ) -> TimeInterval? {
        guard let rawValue else { return nil }
        let value = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !value.isEmpty else { return nil }
        let cap = max(0, maximum)

        if let seconds = TimeInterval(value), seconds.isFinite, seconds >= 0 {
            return min(seconds, cap)
        }

        guard let date = parseHTTPDate(value) else { return nil }
        return min(max(0, date.timeIntervalSince(now)), cap)
    }

    private static func parseHTTPDate(_ value: String) -> Date? {
        // RFC 7231 preferred IMF-fixdate plus obsolete formats that still occur
        // in real HTTP infrastructure.
        let formats = [
            "EEE',' dd MMM yyyy HH':'mm':'ss zzz",
            "EEEE',' dd-MMM-yy HH':'mm':'ss zzz",
            "EEE MMM d HH':'mm':'ss yyyy",
        ]

        for format in formats {
            let formatter = DateFormatter()
            formatter.locale = Locale(identifier: "en_US_POSIX")
            formatter.calendar = Calendar(identifier: .gregorian)
            formatter.timeZone = TimeZone(secondsFromGMT: 0)
            formatter.dateFormat = format
            if let date = formatter.date(from: value) {
                return date
            }
        }
        return nil
    }
}
