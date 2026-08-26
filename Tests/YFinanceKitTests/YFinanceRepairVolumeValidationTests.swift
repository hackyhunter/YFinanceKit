import XCTest
@testable import YFinanceKit

final class YFinanceRepairVolumeValidationTests: XCTestCase {
    func testStockSplitUsesAggregateVolumeInsteadOfSingleBoundaryPair() {
        var volumes: [Int?] = Array(repeating: 100, count: 20)
        volumes[4] = 1_500 // Misleading one-row boundary outlier.
        volumes[5] = 1_500
        volumes[6] = 1_500
        volumes[7] = 1_500

        var up = Array(repeating: false, count: volumes.count)
        var down = Array(repeating: false, count: volumes.count)
        down[5] = true
        let range = YFRepairVolumeValidation.CandidateRange(start: 5, end: 8)

        let accepted = YFRepairVolumeValidation.filterRanges(
            [range],
            volumes: volumes,
            signalUp: up,
            signalDown: down,
            splitMax: 15,
            isInterday: true,
            interval: "1d",
            kind: .stockSplit
        )

        XCTAssertEqual(accepted, [range])
    }

    func testZeroVolumeDoesNotCountAsEvidenceAgainstSplitRepair() {
        let volumes: [Int?] = [nil, 0, 0, nil, 0, 0, nil, 0]
        var up = Array(repeating: false, count: volumes.count)
        var down = Array(repeating: false, count: volumes.count)
        down[2] = true
        let range = YFRepairVolumeValidation.CandidateRange(start: 2, end: 4)

        let accepted = YFRepairVolumeValidation.filterRanges(
            [range],
            volumes: volumes,
            signalUp: up,
            signalDown: down,
            splitMax: 15,
            isInterday: true,
            interval: "1d",
            kind: .stockSplit
        )

        XCTAssertEqual(accepted, [range])
    }

    func testUnitSwitchAcceptsNormalVolumeAndRejectsSplitLikeVolume() {
        let range = YFRepairVolumeValidation.CandidateRange(start: 5, end: 8)
        var up = Array(repeating: false, count: 20)
        var down = Array(repeating: false, count: 20)
        down[5] = true

        let normal = YFRepairVolumeValidation.filterRanges(
            [range],
            volumes: Array(repeating: Optional(100), count: 20),
            signalUp: up,
            signalDown: down,
            splitMax: 15,
            isInterday: true,
            interval: "1d",
            kind: .unitSwitch
        )
        XCTAssertEqual(normal, [range])

        var splitLike: [Int?] = Array(repeating: 100, count: 20)
        splitLike[5] = 1_500
        splitLike[6] = 1_500
        splitLike[7] = 1_500
        let rejected = YFRepairVolumeValidation.filterRanges(
            [range],
            volumes: splitLike,
            signalUp: up,
            signalDown: down,
            splitMax: 15,
            isInterday: true,
            interval: "1d",
            kind: .unitSwitch
        )
        XCTAssertTrue(rejected.isEmpty)
    }
}
