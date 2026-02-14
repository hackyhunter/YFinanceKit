public typealias Ticker = YFTicker
public typealias Tickers = YFTickers
public typealias Lookup = YFLookup
public typealias Search = YFSearch
public typealias Market = YFMarket
public typealias Sector = YFSector
public typealias Industry = YFIndustry
public typealias Calendars = YFCalendars
public typealias CalendarQuery = YFCalendarQuery
public typealias WebSocket = YFWebSocket
public typealias AsyncWebSocket = YFAsyncWebSocket
public typealias EquityQuery = YFScreenerQuery
public typealias FundQuery = YFScreenerQuery

public let __version__ = "1.1.0"
public let __author__ = "Ran Aroussi"
public let version = __version__

public let config = YFConfigStore.shared

public func set_config(proxy: String? = nil, retries: Int? = nil) async {
    await YF.setConfig(proxy: proxy, retries: retries)
}

public func set_tz_cache_location(_ path: String) async {
    await YF.setTZCacheLocation(path)
}

public func set_cache_location(_ path: String) async {
    await YF.setTZCacheLocation(path)
}

public func enable_debug_mode(_ enabled: Bool = true) async {
    await YF.enableDebugMode(enabled)
}
