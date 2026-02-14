import Foundation

actor YFCrumbStore {
    private let session: URLSession
    private let userAgent: String
    private let crumbTTL: TimeInterval = 60 * 60 * 6
    private let requestTimeout: TimeInterval = 10

    private var crumb: String?
    private var fetchedAt: Date?

    init(session: URLSession, userAgent: String) {
        self.session = session
        self.userAgent = userAgent
    }

    func currentCrumb(forceRefresh: Bool = false) async throws -> String {
        if !forceRefresh,
           let crumb,
           let fetchedAt,
           Date().timeIntervalSince(fetchedAt) < crumbTTL {
            return crumb
        }

        if !forceRefresh, let persisted = await loadPersistedCrumb(), let cachedAt = fetchedAt {
            if Date().timeIntervalSince(cachedAt) < crumbTTL {
                return persisted
            }
        }

        try await warmCookieJar()

        let crumbURLs = [
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            "https://query2.finance.yahoo.com/v1/test/getcrumb",
        ]

        for crumbURL in crumbURLs {
            guard let url = URL(string: crumbURL) else {
                continue
            }

            if let freshCrumb = try await fetchCrumb(from: url) {
                self.crumb = freshCrumb
                self.fetchedAt = Date()
                await persistCrumb(freshCrumb, fetchedAt: self.fetchedAt!)
                return freshCrumb
            }
        }

        throw YFinanceError.missingData("Could not fetch Yahoo crumb")
    }

    func invalidate() async {
        crumb = nil
        fetchedAt = nil
        await YFCacheStores.crumb.set(nil, for: "crumb")
        await YFCacheStores.crumb.set(nil, for: "fetchedAt")
    }

    private func warmCookieJar() async throws {
        guard let cookieURL = URL(string: "https://fc.yahoo.com") else {
            throw YFinanceError.invalidURL("https://fc.yahoo.com")
        }

        var request = URLRequest(url: cookieURL)
        request.httpShouldHandleCookies = true
        request.timeoutInterval = requestTimeout
        request.setValue(userAgent, forHTTPHeaderField: "User-Agent")
        request.setValue("application/json,text/html", forHTTPHeaderField: "Accept")

        do {
            _ = try await session.data(for: request)
        } catch {
            throw YFinanceError.transport(error)
        }
    }

    private func fetchCrumb(from url: URL) async throws -> String? {
        var request = URLRequest(url: url)
        request.httpShouldHandleCookies = true
        request.timeoutInterval = requestTimeout
        request.setValue(userAgent, forHTTPHeaderField: "User-Agent")
        request.setValue("text/plain,application/json", forHTTPHeaderField: "Accept")

        let data: Data
        do {
            (data, _) = try await session.data(for: request)
        } catch {
            throw YFinanceError.transport(error)
        }

        let crumbText = String(decoding: data, as: UTF8.self)
            .trimmingCharacters(in: .whitespacesAndNewlines)

        if crumbText.isEmpty ||
            crumbText.contains("<html") ||
            crumbText.contains("Too Many Requests") ||
            crumbText.contains("Edge:") {
            return nil
        }

        return crumbText
    }

    private func loadPersistedCrumb() async -> String? {
        guard let cachedCrumb = await YFCacheStores.crumb.lookup("crumb"),
              !cachedCrumb.isEmpty,
              let fetchedAtRaw = await YFCacheStores.crumb.lookup("fetchedAt"),
              let timestamp = Double(fetchedAtRaw) else {
            return nil
        }

        let cachedAt = Date(timeIntervalSince1970: timestamp)
        self.crumb = cachedCrumb
        self.fetchedAt = cachedAt
        return cachedCrumb
    }

    private func persistCrumb(_ crumb: String, fetchedAt: Date) async {
        await YFCacheStores.crumb.set(crumb, for: "crumb")
        await YFCacheStores.crumb.set(String(fetchedAt.timeIntervalSince1970), for: "fetchedAt")
    }
}
