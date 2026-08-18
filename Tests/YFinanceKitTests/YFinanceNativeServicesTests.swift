import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceNativeServicesTests: XCTestCase {
    func testQuoteServiceForwardsOnlyQuoteContract() async throws {
        let provider = QuoteProviderProbe()
        let service = YFQuoteService(provider: provider)

        _ = try await service.fetch("AAPL")
        _ = try await service.cached("MSFT", freshFor: 12, staleFor: 90)

        let calls = await provider.calls()
        XCTAssertEqual(calls.count, 2)
        XCTAssertEqual(calls[0], .direct(symbol: "AAPL"))
        XCTAssertEqual(calls[1], .cached(symbol: "MSFT", freshFor: 12, staleFor: 90))
    }

    func testMarketDataServicesShareOneResilientClientWithoutNetworkAtConstruction() async {
        let client = YFResilientClient()
        let services = YFMarketDataServices(client: client)
        _ = services.quotes
        _ = services.history
        _ = services.metadata
        _ = services.financials

        let diagnostics = await client.diagnostics()
        XCTAssertEqual(diagnostics.logicalRequests, 0)
        XCTAssertEqual(diagnostics.attempts, 0)
    }
}

private actor QuoteProviderProbe: YFCachedQuoteProviding {
    enum Call: Equatable {
        case direct(symbol: String)
        case cached(symbol: String, freshFor: TimeInterval, staleFor: TimeInterval)
    }

    private var recorded: [Call] = []

    func quote(symbol: String) async throws -> YFQuote? {
        recorded.append(.direct(symbol: symbol))
        return nil
    }

    func quoteCached(
        symbol: String,
        freshFor: TimeInterval,
        staleFor: TimeInterval
    ) async throws -> YFCachedResult<YFQuote?> {
        recorded.append(
            .cached(symbol: symbol, freshFor: freshFor, staleFor: staleFor)
        )
        return YFCachedResult(
            value: nil,
            freshness: .fresh,
            storedAt: Date(timeIntervalSince1970: 0)
        )
    }

    func calls() -> [Call] {
        recorded
    }
}
