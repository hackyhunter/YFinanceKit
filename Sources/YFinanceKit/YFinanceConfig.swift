import Foundation

public struct YFNetworkConfig: Sendable {
    public var retries: Int
    public var proxy: String?

    public init(retries: Int = 0, proxy: String? = nil) {
        self.retries = retries
        self.proxy = proxy
    }
}

public struct YFDebugConfig: Sendable {
    public var enabled: Bool
    public var hideExceptions: Bool

    public init(enabled: Bool = false, hideExceptions: Bool = true) {
        self.enabled = enabled
        self.hideExceptions = hideExceptions
    }
}

public actor YFConfigStore {
    public static let shared = YFConfigStore()

    public var network = YFNetworkConfig()
    public var debug = YFDebugConfig()
    public var cacheDirectory: String?

    public func setConfig(proxy: String? = nil, retries: Int? = nil) {
        if let proxy {
            network.proxy = proxy
        }
        if let retries {
            network.retries = max(0, retries)
        }
    }

    public func enableDebugMode(_ enabled: Bool = true) {
        debug.enabled = enabled
        debug.hideExceptions = !enabled
    }

    public func setTZCacheLocation(_ path: String) {
        cacheDirectory = path
    }
}
