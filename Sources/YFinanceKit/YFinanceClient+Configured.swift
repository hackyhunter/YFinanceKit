import Foundation

extension YFinanceClient {
    public static func configured(
        sessionConfiguration: URLSessionConfiguration = .default,
        userAgent: String = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        query1BaseURL: URL = URL(string: "https://query1.finance.yahoo.com")!,
        query2BaseURL: URL = URL(string: "https://query2.finance.yahoo.com")!,
        rootBaseURL: URL = URL(string: "https://finance.yahoo.com")!
    ) async -> YFinanceClient {
        let network = await YFConfigStore.shared.network

        let configuration = sessionConfiguration
        if let proxy = network.proxy,
           let dictionary = YFProxyParser.connectionProxyDictionary(proxy) {
            configuration.connectionProxyDictionary = dictionary
        }

        let session = URLSession(configuration: configuration)
        return YFinanceClient(
            session: session,
            userAgent: userAgent,
            query1BaseURL: query1BaseURL,
            query2BaseURL: query2BaseURL,
            rootBaseURL: rootBaseURL
        )
    }
}

private enum YFProxyParser {
    private enum ProxyDictionaryKey {
        static let httpEnable = "HTTPEnable"
        static let httpProxy = "HTTPProxy"
        static let httpPort = "HTTPPort"
        static let httpsEnable = "HTTPSEnable"
        static let httpsProxy = "HTTPSProxy"
        static let httpsPort = "HTTPSPort"
        static let socksEnable = "SOCKSEnable"
        static let socksProxy = "SOCKSProxy"
        static let socksPort = "SOCKSPort"
    }

    static func connectionProxyDictionary(_ proxy: String) -> [AnyHashable: Any]? {
        let trimmed = proxy.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return nil
        }

        let candidate: String
        if trimmed.contains("://") {
            candidate = trimmed
        } else {
            candidate = "http://\(trimmed)"
        }

        guard let components = URLComponents(string: candidate),
              let host = components.host,
              !host.isEmpty else {
            return nil
        }

        let scheme = (components.scheme ?? "http").lowercased()
        let port: Int = components.port ?? defaultPort(forScheme: scheme)

        switch scheme {
        case "socks", "socks5", "socks5h":
            return [
                ProxyDictionaryKey.socksEnable: 1,
                ProxyDictionaryKey.socksProxy: host,
                ProxyDictionaryKey.socksPort: port,
            ]
        case "https":
            // URLSession's proxy settings are still an HTTP CONNECT proxy. Apply to both.
            fallthrough
        default:
            return [
                ProxyDictionaryKey.httpEnable: 1,
                ProxyDictionaryKey.httpProxy: host,
                ProxyDictionaryKey.httpPort: port,
                ProxyDictionaryKey.httpsEnable: 1,
                ProxyDictionaryKey.httpsProxy: host,
                ProxyDictionaryKey.httpsPort: port,
            ]
        }
    }

    private static func defaultPort(forScheme scheme: String) -> Int {
        switch scheme {
        case "https":
            return 443
        case "socks", "socks5", "socks5h":
            return 1080
        default:
            return 80
        }
    }
}
