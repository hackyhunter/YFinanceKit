import XCTest
@testable import YFinanceKit

final class YFinanceUpstreamParityTests: XCTestCase {
    func testYFinance16EquityScreenerFields() throws {
        XCTAssertNoThrow(
            try YFQueryBuilder.gt("dividendyield", 1.0).validate(for: .equity)
        )
        XCTAssertNoThrow(
            try YFQueryBuilder.gt("dividendpershare.lasttwelvemonths", 1.0).validate(for: .equity)
        )
        XCTAssertNoThrow(
            try YFQueryBuilder.gt("netepsbasic.lasttwelvemonths", 1.0).validate(for: .equity)
        )
        XCTAssertNoThrow(
            try YFQueryBuilder.gt("netepsdiluted.lasttwelvemonths", 1.0).validate(for: .equity)
        )

        XCTAssertThrowsError(
            try YFQueryBuilder.gt(
                "netepsbasic.lasttwelvemonthsnetepsdiluted.lasttwelvemonths",
                1.0
            ).validate(for: .equity)
        )
    }

    func testETFQueryFieldsAndValues() throws {
        XCTAssertNoThrow(
            try YFETFQueryBuilder.and([
                YFETFQueryBuilder.gt("intradayprice", 10),
                YFETFQueryBuilder.eq("region", .string("us")),
                YFETFQueryBuilder.eq("morningstar_economic_moat", .string("Wide")),
            ]).validateETF()
        )

        XCTAssertThrowsError(
            try YFETFQueryBuilder.eq("morningstar_economic_moat", .string("Huge")).validateETF()
        )
    }
}
