import Foundation

public enum YFSubscriptionTier: String, Sendable {
    case gold
    case silver
    case bronze
    case premium
    case free
}

public struct YFYahooUser: Sendable, Equatable {
    public let guid: String

    public init(guid: String) {
        self.guid = guid
    }
}

/// Yahoo Finance login-cookie support matching yfinance 1.4+ `Auth`.
///
/// Yahoo exposes authenticated account state through its subscriptions endpoint.
/// Supply the `T` and `Y` cookies from a logged-in Yahoo Finance browser session;
/// this type keeps them inside a dedicated URLSession and shares that session with
/// the normal YFinanceClient cookie/crumb machinery.
public actor YFAuth {
    private let userAgent: String
    private var authenticatedClient: YFinanceClient?
    private var cookieT: String?
    private var cookieY: String?

    public init(
        userAgent: String = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    ) {
        self.userAgent = userAgent
    }

    /// Stores Yahoo's login cookies and verifies them against the live account
    /// entitlement endpoint. Cookies remain installed even when verification fails,
    /// matching upstream behavior during transient Yahoo outages.
    @discardableResult
    public func setLoginCookies(cookieT: String, cookieY: String) async -> Bool {
        let t = cookieT.trimmingCharacters(in: .whitespacesAndNewlines)
        let y = cookieY.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !t.isEmpty, !y.isEmpty else {
            return false
        }

        self.cookieT = t
        self.cookieY = y
        self.authenticatedClient = makeAuthenticatedClient(cookieT: t, cookieY: y)
        return await checkLogin()
    }

    /// Python-compatible spelling.
    @discardableResult
    public func set_login_cookies(_ cookie_t: String, _ cookie_y: String) async -> Bool {
        await setLoginCookies(cookieT: cookie_t, cookieY: cookie_y)
    }

    public func checkLogin() async -> Bool {
        await fetchEntitlement() != nil
    }

    /// Python-compatible spelling.
    public func check_login() async -> Bool {
        await checkLogin()
    }

    public func subscriptionTier() async -> YFSubscriptionTier? {
        guard let entitlement = await fetchEntitlement() else {
            return nil
        }

        let subscriptions = entitlement["subscriptionView"]?.arrayValue ?? []
        let active = subscriptions.first { item in
            item["action"]?.stringValue == "ACTIVE"
        }

        guard let active else {
            return .free
        }

        switch active["tier"]?.intValue {
        case 6: return .gold
        case 5: return .silver
        case 3: return .bronze
        default: return .premium
        }
    }

    /// Python-compatible spelling.
    public func subscription_tier() async -> String? {
        await subscriptionTier()?.rawValue
    }

    public func user() async -> YFYahooUser? {
        guard let entitlement = await fetchEntitlement(),
              let guid = entitlement["guid"]?.stringValue,
              !guid.isEmpty else {
            return nil
        }
        return YFYahooUser(guid: guid)
    }

    /// Returns a YFinanceClient backed by the authenticated Yahoo URLSession.
    /// Call `setLoginCookies` first.
    public func client() throws -> YFinanceClient {
        guard let authenticatedClient else {
            throw YFinanceError.invalidRequest("Set Yahoo T/Y login cookies before requesting an authenticated client")
        }
        return authenticatedClient
    }

    public func clearLoginCookies() {
        authenticatedClient = nil
        cookieT = nil
        cookieY = nil
    }

    private func fetchEntitlement() async -> YFJSONValue? {
        guard let authenticatedClient else {
            return nil
        }

        do {
            let raw = try await authenticatedClient.rawGet(
                host: .query1,
                path: "/ws/obi-integration/v1/subscriptions",
                requiresCrumb: true,
                timeout: 15
            )
            guard let result = raw["result"],
                  let guid = result["guid"]?.stringValue,
                  !guid.isEmpty else {
                return nil
            }
            return result
        } catch {
            // Upstream treats failed verification as "could not confirm login"
            // rather than destroying the supplied cookies.
            return nil
        }
    }

    private func makeAuthenticatedClient(cookieT: String, cookieY: String) -> YFinanceClient {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = true
        configuration.httpCookieAcceptPolicy = .always

        if let storage = configuration.httpCookieStorage {
            for (name, value) in [("T", cookieT), ("Y", cookieY)] {
                if let cookie = HTTPCookie(properties: [
                    .domain: ".yahoo.com",
                    .path: "/",
                    .name: name,
                    .value: value,
                    .secure: "TRUE",
                ]) {
                    storage.setCookie(cookie)
                }
            }
        } else {
            configuration.httpAdditionalHeaders = [
                "Cookie": "T=\(cookieT); Y=\(cookieY)"
            ]
        }

        return YFinanceClient(
            session: URLSession(configuration: configuration),
            userAgent: userAgent
        )
    }
}

public extension YF {
    static func auth() -> YFAuth {
        YFAuth()
    }
}
