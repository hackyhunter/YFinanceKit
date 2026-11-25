import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceNewsCacheTests: XCTestCase {
    private var key: YFNewsResponseCache.Key {
        .init(symbol: "AAPL", count: 10, tab: "news", lang: "en-US", region: "US")
    }

    func testCachesSuccessfulEmptyResultsAndExpiresWithoutSleeping() async {
        let cache = YFNewsResponseCache(lifetime: 60)
        let client = YFinanceClient()
        await cache.insert(.array([]), for: key, owner: client, now: 100)
        let hit = await cache.value(for: key, owner: client, now: 159)
        let expired = await cache.value(for: key, owner: client, now: 160)
        XCTAssertEqual(hit?.arrayValue?.count, 0)
        XCTAssertNil(expired)
    }

    func testAllRequestDimensionsAndClientAreIsolated() async {
        let cache = YFNewsResponseCache()
        let client = YFinanceClient()
        let otherClient = YFinanceClient()
        await cache.insert(.string("cached"), for: key, owner: client, now: 100)
        var variants: [YFNewsResponseCache.Key] = []
        var changed = key; changed.symbol = "MSFT"; variants.append(changed)
        changed = key; changed.count = 20; variants.append(changed)
        changed = key; changed.tab = "all"; variants.append(changed)
        changed = key; changed.lang = "fr-FR"; variants.append(changed)
        changed = key; changed.region = "FR"; variants.append(changed)
        for variant in variants {
            let miss = await cache.value(for: variant, owner: client, now: 101)
            XCTAssertNil(miss)
        }
        let other = await cache.value(for: key, owner: otherClient, now: 101)
        XCTAssertNil(other)
        await cache.clear(owner: client)
        let cleared = await cache.value(for: key, owner: client, now: 101)
        XCTAssertNil(cleared)
    }

    func testCapacityEvictsOldestEntry() async {
        let cache = YFNewsResponseCache(capacity: 2)
        let client = YFinanceClient()
        var second = key; second.count = 20
        var third = key; third.count = 30
        await cache.insert(.string("one"), for: key, owner: client, now: 100)
        await cache.insert(.string("two"), for: second, owner: client, now: 101)
        await cache.insert(.string("three"), for: third, owner: client, now: 102)
        let evicted = await cache.value(for: key, owner: client, now: 103)
        let retained = await cache.value(for: third, owner: client, now: 103)
        XCTAssertNil(evicted)
        XCTAssertEqual(retained?.stringValue, "three")
    }

    func testMalformedAndGraphQLErrorPagesAreNotCacheable() throws {
        let valid = try YFJSONValue.decode(data: Data("""
        {"data":{"lightyearList":{"main":{"edges":[],"pageInfo":{"hasNextPage":false}}}}}
        """.utf8))
        XCTAssertTrue(YFNewsResponseCache.isSuccessfulPage(valid))
        XCTAssertFalse(YFNewsResponseCache.isSuccessfulPage(.object([:])))
        var errorResponse = valid.objectValue ?? [:]
        errorResponse["errors"] = .array([.object(["message": .string("temporary failure")])])
        XCTAssertFalse(YFNewsResponseCache.isSuccessfulPage(.object(errorResponse)))
        errorResponse["errors"] = .string("malformed error")
        XCTAssertFalse(YFNewsResponseCache.isSuccessfulPage(.object(errorResponse)))
    }
}
