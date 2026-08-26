import Foundation

enum YFRepairVolumeValidation {
    struct CandidateRange: Hashable, Sendable {
        let start: Int
        let end: Int
    }

    enum RepairKind: Sendable {
        case stockSplit
        case unitSwitch
    }

    static func filterRanges(
        _ ranges: [CandidateRange],
        volumes: [Int?],
        signalUp: [Bool],
        signalDown: [Bool],
        splitMax: Double,
        isInterday: Bool,
        interval: String,
        kind: RepairKind
    ) -> [CandidateRange] {
        guard !ranges.isEmpty else { return [] }
        guard volumes.count == signalUp.count, volumes.count == signalDown.count else {
            return ranges
        }

        let threshold = volumeUnitChangeThreshold(
            volumes: volumes,
            splitMax: splitMax,
            isInterday: isInterday,
            interval: interval
        )
        guard threshold.isFinite, threshold > 1 else {
            return ranges
        }

        var accepted: [CandidateRange] = []
        accepted.reserveCapacity(ranges.count)

        for (rangeIndex, range) in ranges.enumerated() {
            guard range.start >= 0,
                  range.end > range.start,
                  range.end <= volumes.count,
                  signalUp.indices.contains(range.start),
                  signalDown.indices.contains(range.start) else {
                continue
            }

            var during = Array(volumes[range.start..<range.end])
            var outside: [Int?] = []

            if rangeIndex == 0, range.start > 0 {
                outside.append(contentsOf: volumes[max(0, range.start - 10)..<range.start])
            } else if rangeIndex == ranges.count - 1, range.end < volumes.count {
                outside.append(contentsOf: volumes[range.end..<min(range.end + 10, volumes.count)])
            }

            if ranges.count > 1 {
                for step in 1..<ranges.count {
                    if positiveCount(outside) > 10, positiveCount(during) > 10 {
                        break
                    }

                    let leftIndex = rangeIndex - step
                    let rightIndex = rangeIndex + step

                    if leftIndex >= 0 {
                        let left = ranges[leftIndex]
                        if positiveCount(during) < 10,
                           left.start >= 0,
                           left.end > left.start,
                           left.end <= volumes.count {
                            during.insert(contentsOf: volumes[left.start..<left.end], at: 0)
                        }
                        if positiveCount(outside) < 10 {
                            let nextIndex = leftIndex + 1
                            if ranges.indices.contains(nextIndex) {
                                let next = ranges[nextIndex]
                                let gapStart = max(0, min(left.end, volumes.count))
                                let gapEnd = max(gapStart, min(next.start, volumes.count))
                                if gapEnd > gapStart {
                                    outside.insert(contentsOf: volumes[gapStart..<gapEnd], at: 0)
                                }
                            }
                        }
                    }

                    if ranges.indices.contains(rightIndex) {
                        let right = ranges[rightIndex]
                        if positiveCount(during) < 10,
                           right.start >= 0,
                           right.end > right.start,
                           right.end <= volumes.count {
                            during.append(contentsOf: volumes[right.start..<right.end])
                        }
                        if positiveCount(outside) < 10 {
                            let previousIndex = rightIndex - 1
                            if ranges.indices.contains(previousIndex) {
                                let previous = ranges[previousIndex]
                                let gapStart = max(0, min(previous.end, volumes.count))
                                let gapEnd = max(gapStart, min(right.start, volumes.count))
                                if gapEnd > gapStart {
                                    outside.append(contentsOf: volumes[gapStart..<gapEnd])
                                }
                            }
                        }
                    }
                }
            }

            let duringDenoised = denoisePositive(during)
            let outsideDenoised = denoisePositive(outside)

            // #2958: absent usable volume is not evidence against an otherwise
            // plausible repair. Only veto when usable volume contradicts it.
            guard !duringDenoised.isEmpty, !outsideDenoised.isEmpty else {
                accepted.append(range)
                continue
            }

            let duringMean = mean(duringDenoised)
            let outsideMean = mean(outsideDenoised)
            guard duringMean > 0, outsideMean > 0 else {
                accepted.append(range)
                continue
            }

            let boundaryVolumeChange = duringMean / outsideMean
            let up = signalUp[range.start]
            let down = signalDown[range.start]

            switch kind {
            case .stockSplit:
                if boundaryVolumeChange < 1 / threshold, up {
                    accepted.append(range)
                } else if boundaryVolumeChange > threshold, down {
                    accepted.append(range)
                }
            case .unitSwitch:
                let splitLikeLow = boundaryVolumeChange < 1 / threshold && up
                let splitLikeHigh = boundaryVolumeChange > threshold && down
                if !splitLikeLow, !splitLikeHigh {
                    accepted.append(range)
                }
            }
        }

        return accepted
    }

    private static func volumeUnitChangeThreshold(
        volumes: [Int?],
        splitMax: Double,
        isInterday: Bool,
        interval: String
    ) -> Double {
        guard splitMax.isFinite, splitMax > 1 else { return 1 }

        let raw = volumes.map { value -> Double in
            guard let value, value > 0 else { return 0 }
            return Double(value)
        }
        guard raw.contains(where: { $0 > 0 }) else {
            return 1 + (splitMax - 1) * 0.2
        }

        let filled = fillZeroVolumes(raw)
        let denoised = movingMedian(filled)
        guard !denoised.isEmpty else {
            return 1 + (splitMax - 1) * 0.2
        }

        var changes = Array(repeating: 1.0, count: denoised.count)
        if denoised.count > 1 {
            for index in 1..<denoised.count {
                let previous = denoised[index - 1]
                let current = denoised[index]
                if previous > 0, current > 0 {
                    changes[index] = current / previous
                }
            }
        }

        let filtered = iqrFiltered(changes)
        let sample = filtered.isEmpty ? changes : filtered
        let average = mean(sample)
        let sd = standardDeviation(sample, mean: average)
        let sdPct = average == 0 ? 0 : sd / average

        var largestVolumeChangePct = 5 * sdPct
        if isInterday, interval != "1d" {
            largestVolumeChangePct *= 3
            if interval == "1mo" || interval == "3mo" {
                largestVolumeChangePct *= 2
            }
        }

        // Follow-up 5c1f64e relaxed the upstream coefficient from 0.333 to 0.2.
        return 1 + (splitMax - 1 + largestVolumeChangePct) * 0.2
    }

    private static func positiveCount(_ values: [Int?]) -> Int {
        values.reduce(into: 0) { count, value in
            if let value, value > 0 { count += 1 }
        }
    }

    private static func denoisePositive(_ values: [Int?]) -> [Double] {
        let positive = values.compactMap { value -> Double? in
            guard let value, value > 0 else { return nil }
            return Double(value)
        }
        guard !positive.isEmpty else { return [] }
        return movingMedian(positive)
    }

    private static func fillZeroVolumes(_ values: [Double]) -> [Double] {
        guard !values.isEmpty else { return [] }
        var output = values

        var nextPositive: Double?
        for index in output.indices.reversed() {
            if output[index] > 0 {
                nextPositive = output[index]
            } else if let nextPositive {
                output[index] = nextPositive
            }
        }

        var previousPositive: Double?
        for index in output.indices {
            if output[index] > 0 {
                previousPositive = output[index]
            } else if let previousPositive {
                output[index] = previousPositive
            }
        }
        return output
    }

    private static func movingMedian(_ values: [Double]) -> [Double] {
        guard !values.isEmpty else { return [] }
        var window = min(9, values.count)
        if window.isMultiple(of: 2) { window -= 1 }
        guard window > 1 else { return values }

        let radius = window / 2
        return values.indices.map { index in
            let start = max(0, index - radius)
            let end = min(values.count, index + radius + 1)
            let sorted = values[start..<end].filter { $0.isFinite }.sorted()
            guard !sorted.isEmpty else { return values[index] }
            let middle = sorted.count / 2
            if sorted.count.isMultiple(of: 2) {
                return (sorted[middle - 1] + sorted[middle]) / 2
            }
            return sorted[middle]
        }
    }

    private static func iqrFiltered(_ values: [Double]) -> [Double] {
        let sorted = values.filter { $0.isFinite && $0 > 0 }.sorted()
        guard sorted.count >= 4,
              let q1 = percentile(sorted, 25),
              let q3 = percentile(sorted, 75) else {
            return sorted
        }
        let iqr = q3 - q1
        let lower = q1 - 1.5 * iqr
        let upper = q3 + 1.5 * iqr
        return sorted.filter { $0 >= lower && $0 <= upper }
    }

    private static func percentile(_ sortedValues: [Double], _ percent: Double) -> Double? {
        guard !sortedValues.isEmpty else { return nil }
        if sortedValues.count == 1 { return sortedValues[0] }
        let rank = (min(100, max(0, percent)) / 100) * Double(sortedValues.count - 1)
        let lower = Int(rank.rounded(.down))
        let upper = Int(rank.rounded(.up))
        if lower == upper { return sortedValues[lower] }
        let weight = rank - Double(lower)
        return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * weight
    }

    private static func mean(_ values: [Double]) -> Double {
        guard !values.isEmpty else { return 0 }
        return values.reduce(0, +) / Double(values.count)
    }

    private static func standardDeviation(_ values: [Double], mean: Double) -> Double {
        guard !values.isEmpty else { return 0 }
        let variance = values.reduce(0.0) { partial, value in
            let delta = value - mean
            return partial + delta * delta
        } / Double(values.count)
        return sqrt(max(0, variance))
    }
}
