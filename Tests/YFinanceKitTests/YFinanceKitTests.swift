import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceKitTests: XCTestCase {
    func testSearchDecodeDefaultsMissingCollectionsToEmpty() throws {
        let json = """
        {
          "count": 1,
          "quotes": [
            { "symbol": "AAPL", "shortname": "Apple Inc." }
          ]
        }
        """

        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(YFSearchResult.self, from: data)

        XCTAssertEqual(decoded.count, 1)
        XCTAssertEqual(decoded.quotes.first?.symbol, "AAPL")
        XCTAssertTrue(decoded.news.isEmpty)
        XCTAssertTrue(decoded.lists.isEmpty)
        XCTAssertTrue(decoded.researchReports.isEmpty)
        XCTAssertTrue(decoded.nav.isEmpty)
    }

    func testSearchDecodeFiltersQuotesMissingSymbol() throws {
        let json = """
        {
          "quotes": [
            { "shortname": "No Symbol" },
            { "symbol": "AAPL", "shortname": "Apple Inc." }
          ]
        }
        """

        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(YFSearchResult.self, from: data)
        XCTAssertEqual(decoded.quotes.count, 1)
        XCTAssertEqual(decoded.quotes.first?.symbol, "AAPL")
    }

    func testJSONValuePathLookup() throws {
        let json = """
        {
          "a": {
            "b": {
              "c": 42
            }
          }
        }
        """
        let value = try YFJSONValue.decode(data: Data(json.utf8))
        XCTAssertEqual(value.value(at: ["a", "b", "c"])?.intValue, 42)
    }

    func testScreenerQueryEncodesNestedStructure() {
        let query = YFScreenerQuery("and", operands: [
            .query(YFScreenerQuery("gt", operands: [.string("percentchange"), .double(3.0)])),
            .query(YFScreenerQuery("eq", operands: [.string("region"), .string("us")])),
        ])
        let request = YFScreenerRequest(query: query)
        let encoded = request.toJSONValue()

        XCTAssertEqual(encoded["sortType"]?.stringValue, "DESC")
        XCTAssertEqual(encoded["query"]?["operator"]?.stringValue, "AND")
        XCTAssertEqual(encoded["query"]?["operands"]?.arrayValue?.count, 2)
    }

    func testJSONValueToTableFromObjectArray() throws {
        let value: YFJSONValue = .array([
            .object(["symbol": .string("AAPL"), "price": .number(100)]),
            .object(["symbol": .string("MSFT"), "price": .number(200)]),
        ])
        let table = value.toTable()

        XCTAssertEqual(table.rows.count, 2)
        XCTAssertTrue(table.columns.contains("symbol"))
        XCTAssertTrue(table.columns.contains("price"))
    }

    func testTableOperations() {
        let table = YFTable(
            columns: ["symbol", "price"],
            rows: [
                ["symbol": .string("AAPL"), "price": .number(120)],
                ["symbol": .string("MSFT"), "price": .number(300)],
            ]
        )

        XCTAssertEqual(table.rowCount, 2)
        XCTAssertEqual(table.columnCount, 2)
        XCTAssertEqual(table.column("symbol").first?.stringValue, "AAPL")
        XCTAssertEqual(table.head(1).rows.count, 1)
        XCTAssertEqual(table.tail(1).rows.first?["symbol"]?.stringValue, "MSFT")

        let sorted = table.sorted(by: "price", ascending: false)
        XCTAssertEqual(sorted.rows.first?["symbol"]?.stringValue, "MSFT")

        let indexed = table.index(by: "symbol")
        XCTAssertEqual(indexed["AAPL"]?["price"]?.intValue, 120)

        let transposed = table.transposed()
        XCTAssertEqual(transposed.rows.count, 2)
        XCTAssertEqual(transposed.rows.first?["column"]?.stringValue, "symbol")
    }

    func testProtobufPricingDecodeBasicFields() throws {
        var bytes: [UInt8] = []

        // field 1: id (string) => "AAPL"
        bytes.append(0x0A)
        bytes.append(0x04)
        bytes.append(contentsOf: Array("AAPL".utf8))

        // field 2: price (float) => 123.5
        bytes.append(0x15)
        let priceBits = Float(123.5).bitPattern
        bytes.append(UInt8(truncatingIfNeeded: priceBits & 0xFF))
        bytes.append(UInt8(truncatingIfNeeded: (priceBits >> 8) & 0xFF))
        bytes.append(UInt8(truncatingIfNeeded: (priceBits >> 16) & 0xFF))
        bytes.append(UInt8(truncatingIfNeeded: (priceBits >> 24) & 0xFF))

        // field 3: time (sint64) => 1700000000 (zigzag encoded)
        bytes.append(0x18)
        let zigzag = UInt64(1_700_000_000) << 1
        bytes.append(contentsOf: encodeVarint(zigzag))

        // field 4: currency (string) => "USD"
        bytes.append(0x22)
        bytes.append(0x03)
        bytes.append(contentsOf: Array("USD".utf8))

        let decoded = try YFProtobufDecoder.decodePricingData(Data(bytes))
        XCTAssertEqual(decoded.id, "AAPL")
        XCTAssertEqual(decoded.currency, "USD")
        XCTAssertEqual(decoded.time, 1_700_000_000)
        XCTAssertEqual(decoded.price ?? 0, 123.5, accuracy: 0.001)
    }

    func testStreamingSemanticEnums() {
        var data = YFPricingData()
        data.quoteType = 8
        data.marketHours = 1

        XCTAssertEqual(data.quoteTypeValue, .equity)
        XCTAssertEqual(data.marketHoursValue, .regularMarket)

        data.quoteType = 13
        data.marketHours = 4
        XCTAssertEqual(data.quoteTypeValue, .option)
        XCTAssertEqual(data.marketHoursValue, .overnightMarket)
    }

    func testHistorySeriesTables() throws {
        let metaJSON = """
        {
          "currency": "USD",
          "symbol": "AAPL",
          "exchangeName": "NMS",
          "instrumentType": "EQUITY",
          "timezone": "EST",
          "exchangeTimezoneName": "America/New_York",
          "regularMarketPrice": 100,
          "chartPreviousClose": 99,
          "previousClose": 99,
          "gmtoffset": -18000,
          "dataGranularity": "1d",
          "range": "1mo",
          "validRanges": ["1d", "5d"]
        }
        """
        let meta = try JSONDecoder().decode(YFHistoryMeta.self, from: Data(metaJSON.utf8))
        let series = YFHistorySeries(
            symbol: "AAPL",
            meta: meta,
            bars: [
                YFHistoryBar(
                    date: Date(timeIntervalSince1970: 1_700_000_000),
                    open: 100,
                    high: 102,
                    low: 99,
                    close: 101,
                    adjustedClose: 100.5,
                    volume: 1_000
                ),
            ],
            events: [
                YFHistoryEvent(
                    kind: .dividend,
                    date: Date(timeIntervalSince1970: 1_700_000_000),
                    value: 0.24,
                    ratio: nil,
                    raw: .object(["amount": .number(0.24)])
                ),
            ]
        )

        XCTAssertEqual(series.barsTable().rows.count, 1)
        XCTAssertEqual(series.eventsTable().rows.count, 1)

        let historyTable = series.historyTable(includeActions: true, ignoreTZ: false)
        XCTAssertTrue(historyTable.columns.contains("Adj Close"))
        XCTAssertFalse(historyTable.columns.contains("Capital Gains"))
        XCTAssertEqual(historyTable.rows.first?["date"]?.intValue, 1_700_000_000)
        XCTAssertEqual(historyTable.rows.first?["Dividends"]?.doubleValue ?? 0, 0.24, accuracy: 0.0001)
        XCTAssertEqual(historyTable.rows.first?["Stock Splits"]?.doubleValue ?? -1, 0, accuracy: 0.0001)
    }

    func testHistoryTableIncludesRepairedWhenRepairEnabled() throws {
        let metaJSON = """
        {
          "currency": "USD",
          "symbol": "AAPL",
          "exchangeName": "NMS",
          "instrumentType": "EQUITY",
          "timezone": "EST",
          "exchangeTimezoneName": "America/New_York",
          "regularMarketPrice": 100,
          "chartPreviousClose": 99,
          "previousClose": 99,
          "gmtoffset": -18000,
          "dataGranularity": "1d",
          "range": "1mo",
          "validRanges": ["1d", "5d"]
        }
        """
        let meta = try JSONDecoder().decode(YFHistoryMeta.self, from: Data(metaJSON.utf8))
        let series = YFHistorySeries(
            symbol: "AAPL",
            meta: meta,
            interval: .oneDay,
            bars: [
                YFHistoryBar(
                    date: Date(timeIntervalSince1970: 1_700_000_000),
                    open: 100,
                    high: 102,
                    low: 99,
                    close: 101,
                    adjustedClose: 100.5,
                    volume: 1_000,
                    repaired: true
                ),
            ],
            events: [],
            repairEnabled: true
        )

        let table = series.historyTable(includeActions: false, ignoreTZ: false)
        XCTAssertTrue(table.columns.contains("Repaired?"))
        XCTAssertEqual(table.rows.first?["Repaired?"]?.boolValue, true)
    }

    func testHistoryTableOmitsAdjCloseWhenMissing() throws {
        let metaJSON = """
        {
          "currency": "USD",
          "symbol": "AAPL",
          "exchangeName": "NMS",
          "instrumentType": "EQUITY",
          "timezone": "EST",
          "exchangeTimezoneName": "America/New_York",
          "regularMarketPrice": 100,
          "chartPreviousClose": 99,
          "previousClose": 99,
          "gmtoffset": -18000,
          "dataGranularity": "1h",
          "range": "5d",
          "validRanges": ["1d", "5d"]
        }
        """
        let meta = try JSONDecoder().decode(YFHistoryMeta.self, from: Data(metaJSON.utf8))
        let series = YFHistorySeries(
            symbol: "AAPL",
            meta: meta,
            bars: [
                YFHistoryBar(
                    date: Date(timeIntervalSince1970: 1_700_000_000),
                    open: 100,
                    high: 102,
                    low: 99,
                    close: 101,
                    adjustedClose: nil,
                    volume: 1_000
                ),
            ],
            events: []
        )

        let table = series.historyTable(includeActions: true, ignoreTZ: false)
        XCTAssertFalse(table.columns.contains("Adj Close"))
        XCTAssertFalse(table.columns.contains("Capital Gains"))
        XCTAssertEqual(table.rows.first?["date"]?.intValue, 1_700_000_000)
    }

    func testQueryBuilderProducesExpectedOperators() {
        let query = YFQueryBuilder.and([
            YFQueryBuilder.gt("percentchange", 3),
            YFQueryBuilder.eq("region", .string("us")),
        ])
        let json = query.toJSONValue()
        XCTAssertEqual(json["operator"]?.stringValue, "AND")
        XCTAssertEqual(json["operands"]?.arrayValue?.count, 2)

        var calendarQuery = YFCalendarQuery("or", operands: [])
        XCTAssertTrue(calendarQuery.isEmpty)
        calendarQuery.append("AAPL")
        XCTAssertFalse(calendarQuery.isEmpty)
        XCTAssertEqual(calendarQuery.toDict()["operator"]?.stringValue, "OR")
    }

    func testScreenerQueryValidation() throws {
        let valid = YFQueryBuilder.eq("region", .string("us"))
        XCTAssertNoThrow(try valid.validate(for: .equity))

        let invalid = YFQueryBuilder.eq("definitely_not_a_real_field", .string("x"))
        XCTAssertThrowsError(try invalid.validate(for: .equity))
    }

    func testStringIntervalAndRangeParsing() {
        XCTAssertEqual(YFinanceClient.Interval(pythonValue: "1wk"), .oneWeek)
        XCTAssertEqual(YFinanceClient.Interval(pythonValue: "60m"), .sixtyMinutes)
        XCTAssertNil(YFinanceClient.Interval(pythonValue: "7m"))

        XCTAssertEqual(YFinanceClient.Range(pythonValue: "max"), .max)
        XCTAssertEqual(YFinanceClient.Range(pythonValue: "1y"), .oneYear)
        XCTAssertNil(YFinanceClient.Range(pythonValue: "7y"))

        XCTAssertEqual(YFFinancialFrequency(pythonValue: "yearly"), .yearly)
        XCTAssertEqual(YFFinancialFrequency(pythonValue: "quarterly"), .quarterly)
        XCTAssertEqual(YFFinancialFrequency(pythonValue: "ttm"), .trailing)
        XCTAssertNil(YFFinancialFrequency(pythonValue: "monthly"))

        XCTAssertEqual(YFGroupBy(pythonValue: "column"), .column)
        XCTAssertEqual(YFGroupBy(pythonValue: "ticker"), .ticker)
        XCTAssertNil(YFGroupBy(pythonValue: "invalid"))
        XCTAssertNotNil(PREDEFINED_SCREENER_QUERIES["most_actives"])
        XCTAssertTrue(PREDEFINED_SCREENER_QUERY_IDS.contains("most_actives"))
        XCTAssertEqual(PREDEFINED_SCREENER_BODY_DEFAULTS["count"]?.intValue, 25)
        XCTAssertNotNil(PREDEFINED_CALENDARS["sp_earnings"])
        XCTAssertTrue(PREDEFINED_CALENDAR_IDS.contains("sp_earnings"))

        XCTAssertTrue(is_isin("US0378331005"))
        XCTAssertFalse(is_isin("AAPL"))
    }

    func testStartEndOverloadsCompile() {
        let client = YFinanceClient()
        let ticker = YFTicker(symbol: "AAPL", client: client)
        let tickerAliasInit = Ticker("AAPL", client: client)
        let micTickerInit = try? YFTicker(("OR", "XPAR"), client: client)
        let tickers = YFTickers(symbols: ["AAPL", "MSFT"], client: client)
        let tickersAliasInit = Tickers(["AAPL", "MSFT"], client: client)
        let micTickersInit = try? YFTickers([("OR", "XPAR"), ("VOD", "XLON")], client: client)
        let start = Date(timeIntervalSince1970: 1_700_000_000)
        let end = Date(timeIntervalSince1970: 1_700_086_400)
        let search = YFSearch(
            query: "apple",
            max_results: 8,
            news_count: 8,
            lists_count: 8,
            include_cb: true,
            include_nav_links: false,
            include_research: false,
            include_cultural_assets: false,
            enable_fuzzy_query: false,
            recommended: 8,
            timeout: 30,
            raise_errors: true,
            client: client
        )
        let searchAliasInit = Search("apple", maxResults: 8, client: client)
        let searchObject = YF.searchObject(
            "apple",
            max_results: 8,
            news_count: 8,
            lists_count: 8,
            include_cb: true,
            include_nav_links: false,
            include_research: false,
            include_cultural_assets: false,
            enable_fuzzy_query: false,
            recommended: 8,
            timeout: 30,
            raise_errors: true,
            client: client
        )
        let lookup = YF.lookup("apple", timeout: 30, raise_errors: true, client: client)
        let lookupAliasInit = Lookup("apple", timeout: 30, raiseErrors: true, client: client)
        let market = YF.market("US", timeout: 30, client: client)
        let marketAliasInit = Market("US", timeout: 30, client: client)
        let sector = YFSector(key: "technology", client: client)
        let sectorAliasInit = Sector("technology", client: client)
        let industry = YFIndustry(key: "software", client: client)
        let industryAliasInit = Industry("software", client: client)
        let calendars = YF.calendars(client: client)
        let screenQuery = YFQueryBuilder.and([
            YFQueryBuilder.gt("percentchange", 3),
            YFQueryBuilder.eq("region", .string("us")),
        ])

        let tickerCall = { () async throws -> YFHistorySeries in
            try await ticker.history(start: start, end: end, interval: "1d")
        }
        let tickerPythonHistorySignatureCall = { () async throws -> YFHistorySeries in
            try await ticker.history(
                period: nil,
                interval: "1d",
                start: start,
                end: end,
                prepost: false,
                actions: true,
                auto_adjust: true,
                back_adjust: false,
                repair: false,
                keepna: false,
                rounding: false,
                timeout: 10
            )
        }
        let tickersCall = { () async throws -> [String: YFHistorySeries] in
            try await tickers.download(start: start, end: end, interval: "1d", threads: false)
        }
        let globalCall = { () async throws -> [String: YFHistorySeries] in
            try await download(["AAPL", "MSFT"], start: start, end: end, interval: "1d", threads: false, client: client)
        }
        let globalStringDownloadCall = { () async throws -> [String: YFHistorySeries] in
            try await download("AAPL MSFT", period: .oneMonth, interval: "1d", threads: false, client: client)
        }
        let globalStringDownloadPeriodCall = { () async throws -> [String: YFHistorySeries] in
            try await YF.download("AAPL MSFT", period: .oneMonth, interval: "1d", threads: false, client: client)
        }
        let globalStringDownloadStartEndCall = { () async throws -> [String: YFHistorySeries] in
            try await download("AAPL MSFT", start: start, end: end, interval: "1d", threads: false, client: client)
        }
        let globalStringTablePeriodRangeCall = { () async throws -> YFTable in
            try await YF.downloadTable(
                "AAPL MSFT",
                period: .oneMonth,
                interval: .oneDay,
                threads: false,
                groupBy: .column,
                multiLevelIndex: false,
                client: client
            )
        }
        let globalStringTablePeriodStringCall = { () async throws -> YFTable in
            try await YF.downloadTable(
                "AAPL MSFT",
                period: "1mo",
                interval: "1d",
                threads: false,
                groupBy: .column,
                multiLevelIndex: false,
                client: client
            )
        }
        let globalStringTableStartEndCall = { () async throws -> YFTable in
            try await YF.downloadTable(
                "AAPL MSFT",
                start: start,
                end: end,
                interval: .oneDay,
                threads: false,
                groupBy: .column,
                multiLevelIndex: false,
                client: client
            )
        }
        let globalStringTableStartEndStringCall = { () async throws -> YFTable in
            try await YF.downloadTable(
                "AAPL MSFT",
                start: start,
                end: end,
                interval: "1d",
                threads: false,
                groupBy: .column,
                multiLevelIndex: false,
                client: client
            )
        }
        let globalStringSnakeTablePeriodRangeCall = { () async throws -> YFTable in
            try await download_table(
                "AAPL MSFT",
                period: .oneMonth,
                interval: .oneDay,
                threads: false,
                groupBy: .column,
                client: client
            )
        }
        let globalStringSnakeTablePeriodStringCall = { () async throws -> YFTable in
            try await download_table(
                "AAPL MSFT",
                period: "1mo",
                interval: "1d",
                threads: false,
                groupBy: .column,
                client: client
            )
        }
        let globalStringSnakeTableStartEndCall = { () async throws -> YFTable in
            try await download_table(
                "AAPL MSFT",
                start: start,
                end: end,
                interval: .oneDay,
                threads: false,
                groupBy: .column,
                client: client
            )
        }
        let globalStringSnakeTableStartEndStringCall = { () async throws -> YFTable in
            try await download_table(
                "AAPL MSFT",
                start: start,
                end: end,
                interval: "1d",
                threads: false,
                groupBy: .column,
                client: client
            )
        }
        let tickersStringCall = { () -> YFTickers in
            YF.tickers("AAPL MSFT", client: client)
        }
        let tickersTableCall = { () async throws -> YFTable in
            try await tickers.historyTable(
                start: start,
                end: end,
                interval: "1d",
                threads: false,
                groupBy: .ticker,
                multiLevelIndex: false
            )
        }
        let tickersStringGroupByTableCall = { () async throws -> YFTable in
            try await tickers.downloadTable(period: "1mo", interval: "1d", threads: false, groupBy: "column")
        }
        let tickersPythonDownloadDefaultGroupByCall = { () async throws -> YFTable in
            try await tickers.download(
                start: start,
                end: end,
                prepost: false,
                actions: true,
                auto_adjust: true,
                back_adjust: false,
                repair: false,
                keepna: false,
                rounding: false,
                threads: false,
                ignore_tz: false,
                progress: true,
                period: nil,
                interval: "1d",
                timeout: 10,
                multi_level_index: true
            )
        }
        let globalTableCall = { () async throws -> YFTable in
            try await YF.downloadTable(
                ["AAPL"],
                start: start,
                end: end,
                interval: "1d",
                threads: false,
                groupBy: .column,
                multiLevelIndex: false,
                client: client
            )
        }
        let globalSnakeTableCall = { () async throws -> YFTable in
            try await download_table(["AAPL"], period: "1mo", threads: false, client: client)
        }
        let globalSnakeTableDefaultGroupByCall = { () async throws -> YFTable in
            try await download_table(
                "AAPL MSFT",
                start: start,
                end: end,
                interval: "1d",
                threads: false,
                multi_level_index: true,
                client: client
            )
        }
        let financialTableCall = { () async throws -> YFTable in
            try await ticker.get_income_stmt_table()
        }
        let fundsModelCall = { () async throws -> YFFundsData in
            try await ticker.get_funds_data()
        }
        let fundsRawCall = { () async throws -> YFJSONValue in
            try await ticker.get_funds_data_raw()
        }
        let snakeFinancialAliasCall = { () async throws -> YFJSONValue in
            try await ticker.quarterly_cashflow()
        }
        let stringFreqCall = { () async throws -> YFJSONValue in
            try await ticker.get_income_stmt(freq: "quarterly")
        }
        let asDictAliasCall = { () async throws -> YFJSONValue in
            try await ticker.get_earnings_history(as_dict: true)
        }
        let optionChainAliasCall = { () async throws -> YFOptionsChain in
            try await ticker.option_chain("2026-01-16", tz: TimeZone(secondsFromGMT: 0))
        }
        let staticSearchSnakeCall = { () async throws -> YFSearchResult in
            try await YF.search("apple", max_results: 8, client: client)
        }
        let isinAliasCall = { () async throws -> String? in
            try await ticker.get_isin()
        }
        let tickerTimeZoneCall = { () async throws -> String? in
            try await ticker.tickerTimeZone()
        }
        let tickerTimeZoneSnakeCall = { () async throws -> String? in
            try await ticker.ticker_time_zone()
        }
        let setCacheLocationCall = { () async in
            await set_cache_location("/tmp/yfinance-cache")
        }
        _ = tickerTimeZoneCall
        _ = tickerTimeZoneSnakeCall
        _ = setCacheLocationCall
        let tickerSnakePropertyCall = { () async throws -> YFJSONValue in
            try await ticker.major_holders()
        }
        let searchResponseAliasCall = { () async throws -> YFJSONValue in
            try await search.get_response()
        }
        let searchObjectCall = { () async throws -> [YFSearchQuote] in
            try await searchObject.get_quotes()
        }
        let lookupSnakeAliasCall = { () async throws -> YFJSONValue in
            try await lookup.get_cryptocurrency(count: 5)
        }
        let lookupTableAliasCall = { () async throws -> YFTable in
            try await lookup.get_stock_table(count: 5)
        }
        let marketSnakeAliasCall = { () async throws -> YFJSONValue in
            try await market.get_summary()
        }
        let sectorSnakeAliasCall = { () async throws -> YFJSONValue in
            try await sector.get_top_etfs()
        }
        let sectorTableAliasCall = { () async throws -> YFTable in
            try await sector.get_industries_table()
        }
        let industrySnakeAliasCall = { () async throws -> YFJSONValue in
            try await industry.get_top_growth_companies()
        }
        let industryTableAliasCall = { () async throws -> YFTable in
            try await industry.get_top_growth_companies_table()
        }
        let calendarsSnakeAliasCall = { () async throws -> YFJSONValue in
            try await calendars.get_earnings_calendar(market_cap: nil, filter_most_active: true, start: nil, end: nil, limit: 12, offset: 0, force: false)
        }
        let optionsTableCall = { () async throws -> YFTable in
            let chain = try await ticker.option_chain(nil, tz: nil)
            return chain.callsTable()
        }
        let pythonDownloadSignatureCall = { () async throws -> YFTable in
            try await YF.download(
                "AAPL MSFT",
                start: start,
                end: end,
                actions: true,
                threads: false,
                ignore_tz: false,
                group_by: "column",
                auto_adjust: true,
                back_adjust: false,
                repair: false,
                keepna: false,
                progress: true,
                period: nil,
                interval: "1d",
                prepost: false,
                rounding: false,
                timeout: 10,
                multi_level_index: true,
                client: client
            )
        }
        let pythonDownloadDefaultGroupByCall = { () async throws -> YFTable in
            try await YF.download(
                "AAPL MSFT",
                start: start,
                end: end,
                actions: true,
                threads: false,
                ignore_tz: false,
                auto_adjust: true,
                back_adjust: false,
                repair: false,
                keepna: false,
                progress: true,
                period: nil,
                interval: "1d",
                prepost: false,
                rounding: false,
                timeout: 10,
                multi_level_index: true,
                client: client
            )
        }
        let tickersPythonHistorySignatureCall = { () async throws -> YFTable in
            try await tickers.history(
                start: start,
                end: end,
                prepost: false,
                actions: true,
                auto_adjust: true,
                back_adjust: false,
                repair: false,
                keepna: false,
                rounding: false,
                threads: false,
                group_by: "column",
                ignore_tz: false,
                progress: true,
                period: nil,
                interval: "1d",
                timeout: 10,
                multi_level_index: true
            )
        }
        let tickersPythonHistoryDefaultGroupByCall = { () async throws -> YFTable in
            try await tickers.history(
                start: start,
                end: end,
                prepost: false,
                actions: true,
                auto_adjust: true,
                back_adjust: false,
                repair: false,
                keepna: false,
                rounding: false,
                threads: false,
                progress: true,
                period: nil,
                interval: "1d",
                timeout: 10,
                multi_level_index: true
            )
        }
        let screenSnakeOverloadCall = { () async throws -> YFJSONValue in
            try await screen(
                screenQuery,
                quote_type: .equity,
                offset: 0,
                size: 25,
                count: nil,
                sortField: "percentchange",
                sortAsc: true,
                userId: "",
                userIdType: "guid",
                client: client
            )
        }
        let screenPredefinedCaseInsensitiveCall = { () async throws -> YFJSONValue in
            try await screen("MOST_ACTIVES", count: 10, sortAsc: false, client: client)
        }
        let liveWithoutHandlerCall = { () -> YFWebSocket in
            ticker.live(verbose: false)
        }
        let liveMessageHandlerLabelCall = { () -> YFWebSocket in
            ticker.live(message_handler: nil, verbose: false)
        }
        let isinLookupCall = { () async throws -> String in
            try await get_ticker_by_isin("US0378331005", client: client)
        }
        let isinTickerInitCall = { () async throws -> YFTicker in
            try await YFTicker(isin: "US0378331005", client: client)
        }

        _ = tickerCall
        _ = tickerPythonHistorySignatureCall
        _ = tickerAliasInit
        _ = micTickerInit
        _ = tickersCall
        _ = tickersAliasInit
        _ = micTickersInit
        _ = globalCall
        _ = globalStringDownloadCall
        _ = globalStringDownloadPeriodCall
        _ = globalStringDownloadStartEndCall
        _ = globalStringTablePeriodRangeCall
        _ = globalStringTablePeriodStringCall
        _ = globalStringTableStartEndCall
        _ = globalStringTableStartEndStringCall
        _ = globalStringSnakeTablePeriodRangeCall
        _ = globalStringSnakeTablePeriodStringCall
        _ = globalStringSnakeTableStartEndCall
        _ = globalStringSnakeTableStartEndStringCall
        _ = tickersStringCall
        _ = tickersTableCall
        _ = tickersStringGroupByTableCall
        _ = tickersPythonDownloadDefaultGroupByCall
        _ = globalTableCall
        _ = globalSnakeTableCall
        _ = globalSnakeTableDefaultGroupByCall
        _ = financialTableCall
        _ = fundsModelCall
        _ = fundsRawCall
        _ = snakeFinancialAliasCall
        _ = stringFreqCall
        _ = asDictAliasCall
        _ = optionChainAliasCall
        _ = staticSearchSnakeCall
        _ = isinAliasCall
        _ = tickerSnakePropertyCall
        _ = searchResponseAliasCall
        _ = searchAliasInit
        _ = searchObjectCall
        _ = lookupSnakeAliasCall
        _ = lookupAliasInit
        _ = lookupTableAliasCall
        _ = marketSnakeAliasCall
        _ = marketAliasInit
        _ = sectorSnakeAliasCall
        _ = sectorTableAliasCall
        _ = sectorAliasInit
        _ = industrySnakeAliasCall
        _ = industryTableAliasCall
        _ = industryAliasInit
        _ = calendarsSnakeAliasCall
        _ = optionsTableCall
        _ = pythonDownloadSignatureCall
        _ = pythonDownloadDefaultGroupByCall
        _ = tickersPythonHistorySignatureCall
        _ = tickersPythonHistoryDefaultGroupByCall
        _ = screenSnakeOverloadCall
        _ = screenPredefinedCaseInsensitiveCall
        _ = liveWithoutHandlerCall
        _ = liveMessageHandlerLabelCall
        _ = isinLookupCall
        _ = isinTickerInitCall

        XCTAssertEqual(String(describing: ticker), "yfinance.Ticker object <AAPL>")
        XCTAssertEqual(String(describing: tickers), "yfinance.Tickers object <AAPL,MSFT>")
        XCTAssertEqual(String(describing: sector), "yfinance.Sector object <technology>")
        XCTAssertEqual(String(describing: industry), "yfinance.Industry object <software>")
        XCTAssertEqual(__version__, "1.1.0")
        XCTAssertEqual(__author__, "Ran Aroussi")
        XCTAssertEqual(YFinanceKit.version, __version__)
    }

    func testFundsDataModelParsesExpectedShapes() throws {
        let raw: YFJSONValue = .object([
            "quoteType": .object([
                "quoteType": .string("MUTUALFUND"),
            ]),
            "summaryProfile": .object([
                "longBusinessSummary": .string("Example fund"),
            ]),
            "fundProfile": .object([
                "categoryName": .string("Large Blend"),
                "family": .string("Vanguard"),
                "legalType": .string("Open Ended Investment Company"),
                "feesExpensesInvestment": .object([
                    "annualReportExpenseRatio": .object(["raw": .number(0.03)]),
                    "annualHoldingsTurnover": .object(["raw": .number(0.05)]),
                    "totalNetAssets": .object(["raw": .number(12345)]),
                ]),
                "feesExpensesInvestmentCat": .object([
                    "annualReportExpenseRatio": .object(["raw": .number(0.11)]),
                    "annualHoldingsTurnover": .object(["raw": .number(0.09)]),
                    "totalNetAssets": .object(["raw": .number(98765)]),
                ]),
            ]),
            "topHoldings": .object([
                "cashPosition": .object(["raw": .number(0.02)]),
                "stockPosition": .object(["raw": .number(0.96)]),
                "holdings": .array([
                    .object([
                        "symbol": .string("AAPL"),
                        "holdingName": .string("Apple Inc."),
                        "holdingPercent": .object(["raw": .number(0.07)]),
                    ]),
                ]),
                "equityHoldings": .object([
                    "priceToEarnings": .object(["raw": .number(24.0)]),
                    "priceToEarningsCat": .object(["raw": .number(23.0)]),
                ]),
                "bondHoldings": .object([
                    "duration": .object(["raw": .number(5.0)]),
                    "durationCat": .object(["raw": .number(4.5)]),
                ]),
                "bondRatings": .array([
                    .object(["AAA": .number(0.6)]),
                ]),
                "sectorWeightings": .array([
                    .object(["technology": .number(0.3)]),
                ]),
            ]),
        ])

        let funds = YFFundsData(symbol: "VTI", raw: raw)
        XCTAssertEqual(funds.quoteType, "MUTUALFUND")
        XCTAssertEqual(funds.description, "Example fund")
        XCTAssertEqual(funds.fundOperations.rowCount, 3)
        XCTAssertEqual(funds.topHoldings.rows.first?["Symbol"]?.stringValue, "AAPL")
        XCTAssertEqual(funds.bondRatings["AAA"]?.doubleValue, 0.6)
        XCTAssertEqual(funds.sectorWeightings["technology"]?.doubleValue, 0.3)
    }

    private func encodeVarint(_ value: UInt64) -> [UInt8] {
        var value = value
        var out: [UInt8] = []
        while true {
            if value < 0x80 {
                out.append(UInt8(value))
                return out
            } else {
                out.append(UInt8(value & 0x7F) | 0x80)
                value >>= 7
            }
        }
    }
}
