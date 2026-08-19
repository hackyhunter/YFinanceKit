import Foundation

actor YFCrumbStore {
    private enum CookieStrategy: Sendable {
        case basic
        case csrf

        var alternate: CookieStrategy {
            self == .basic ? .csrf : .basic
        }
    }

    private struct CrumbFlight {
        let id: UUID
        let task: Task<String, Error>
    }

    private let session: URLSession
    private let userAgent: String
    private let crumbTTL: TimeInterval = 60 * 60 * 6
    private let requestTimeout: TimeInterval = 10

    private var crumb: String?
    private var fetchedAt: Date?
    private var cookieStrategy: CookieStrategy = .basic
    private var clearedLegacyCache = false
    private var crumbFlight: CrumbFlight?

    init(session: URLSession, userAgent: String) {
        self.session = session
        self.userAgent = userAgent
    }

    func currentCrumb(forceRefresh: Bool = false) async throws -> String {
        await clearLegacyPersistedCrumbIfNeeded()

        if forceRefresh {
            crumb = nil
            fetchedAt = nil
            crumbFlight?.task.cancel()
            crumbFlight = nil
        } else if let crumb,
                  let fetchedAt,
                  Date().timeIntervalSince(fetchedAt) < crumbTTL {
            return crumb
        }

        if let crumbFlight {
            return try await crumbFlight.task.value
        }

        let flightID = UUID()
        let task = Task<String, Error> {
            try await self.fetchFreshCrumb()
        }
        crumbFlight = CrumbFlight(id: flightID, task: task)

        do {
            let value = try await task.value
            clearCrumbFlight(id: flightID)
            return value
        } catch {
            clearCrumbFlight(id: flightID)
            throw error
        }
    }

    private func fetchFreshCrumb() async throws -> String {
        try Task.checkCancellation()

        // Match modern yfinance's behavior: try the active Yahoo cookie strategy,
        // then fall back to the alternate strategy if Yahoo rejects it.
        let strategies = [cookieStrategy, cookieStrategy.alternate]
        var lastError: Error?

        for strategy in strategies {
            do {
                if let freshCrumb = try await fetchCrumb(using: strategy) {
                    try Task.checkCancellation()
                    cookieStrategy = strategy
                    crumb = freshCrumb
                    fetchedAt = Date()
                    return freshCrumb
                }
            } catch YFinanceError.httpStatus(let status) where status == 429 {
                // Rate limiting is not a cookie-strategy problem. Surface it instead
                // of hammering Yahoo with another authentication bootstrap flow.
                throw YFinanceError.httpStatus(status)
            } catch {
                lastError = error
            }
        }

        if let lastError {
            throw lastError
        }
        throw YFinanceError.missingData("Could not fetch Yahoo crumb")
    }

    private func clearCrumbFlight(id: UUID) {
        guard crumbFlight?.id == id else { return }
        crumbFlight = nil
    }

    func invalidate() async {
        crumbFlight?.task.cancel()
        crumbFlight = nil
        crumb = nil
        fetchedAt = nil
        cookieStrategy = cookieStrategy.alternate
        await clearLegacyPersistedCrumbIfNeeded(force: true)
    }

    private func fetchCrumb(using strategy: CookieStrategy) async throws -> String? {
        switch strategy {
        case .basic:
            try await warmBasicCookieJar()
            guard let url = URL(string: "https://query1.finance.yahoo.com/v1/test/getcrumb") else {
                throw YFinanceError.invalidURL("https://query1.finance.yahoo.com/v1/test/getcrumb")
            }
            return try await fetchCrumb(from: url)

        case .csrf:
            try await warmCSRFCookieJar()
            guard let url = URL(string: "https://query2.finance.yahoo.com/v1/test/getcrumb") else {
                throw YFinanceError.invalidURL("https://query2.finance.yahoo.com/v1/test/getcrumb")
            }
            return try await fetchCrumb(from: url)
        }
    }

    private func warmBasicCookieJar() async throws {
        guard let cookieURL = URL(string: "https://fc.yahoo.com") else {
            throw YFinanceError.invalidURL("https://fc.yahoo.com")
        }

        var request = baseRequest(url: cookieURL)
        request.setValue("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", forHTTPHeaderField: "Accept")

        do {
            let (_, response) = try await session.data(for: request)
            if let httpResponse = response as? HTTPURLResponse,
               httpResponse.statusCode == 429 {
                throw YFinanceError.httpStatus(429)
            }
            // Yahoo's fc.yahoo.com bootstrap can return a non-2xx status while still
            // installing its A3 cookie. The cookie, not the page body, is the goal.
        } catch let error as YFinanceError {
            throw error
        } catch {
            throw YFinanceError.transport(error)
        }
    }

    private func warmCSRFCookieJar() async throws {
        guard let consentURL = URL(string: "https://guce.yahoo.com/consent") else {
            throw YFinanceError.invalidURL("https://guce.yahoo.com/consent")
        }

        var request = baseRequest(url: consentURL)
        request.setValue("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", forHTTPHeaderField: "Accept")

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw YFinanceError.transport(error)
        }

        guard let httpResponse = response as? HTTPURLResponse else {
            throw YFinanceError.missingData("Expected HTTPURLResponse while fetching Yahoo consent")
        }
        if httpResponse.statusCode == 429 {
            throw YFinanceError.httpStatus(429)
        }
        guard (200...399).contains(httpResponse.statusCode) else {
            throw YFinanceError.httpStatus(httpResponse.statusCode)
        }

        let html = String(decoding: data, as: UTF8.self)
        guard let csrfToken = inputValue(named: "csrfToken", in: html),
              let sessionId = inputValue(named: "sessionId", in: html) else {
            throw YFinanceError.missingData("Yahoo consent page did not contain csrfToken/sessionId")
        }

        let formItems = [
            URLQueryItem(name: "agree", value: "agree"),
            URLQueryItem(name: "agree", value: "agree"),
            URLQueryItem(name: "consentUUID", value: "default"),
            URLQueryItem(name: "sessionId", value: sessionId),
            URLQueryItem(name: "csrfToken", value: csrfToken),
            URLQueryItem(name: "originalDoneUrl", value: "https://finance.yahoo.com/"),
            URLQueryItem(name: "namespace", value: "yahoo"),
        ]

        var formComponents = URLComponents()
        formComponents.queryItems = formItems
        let formData = Data((formComponents.percentEncodedQuery ?? "").utf8)

        guard var collectComponents = URLComponents(string: "https://consent.yahoo.com/v2/collectConsent") else {
            throw YFinanceError.invalidURL("https://consent.yahoo.com/v2/collectConsent")
        }
        collectComponents.queryItems = [URLQueryItem(name: "sessionId", value: sessionId)]
        guard let collectURL = collectComponents.url else {
            throw YFinanceError.invalidURL("Yahoo collectConsent URL")
        }

        var post = baseRequest(url: collectURL)
        post.httpMethod = "POST"
        post.httpBody = formData
        post.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        post.setValue("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", forHTTPHeaderField: "Accept")

        do {
            let (_, postResponse) = try await session.data(for: post)
            if let postHTTP = postResponse as? HTTPURLResponse {
                if postHTTP.statusCode == 429 {
                    throw YFinanceError.httpStatus(429)
                }
                guard (200...399).contains(postHTTP.statusCode) else {
                    throw YFinanceError.httpStatus(postHTTP.statusCode)
                }
            }
        } catch let error as YFinanceError {
            throw error
        } catch {
            throw YFinanceError.transport(error)
        }

        guard var copyComponents = URLComponents(string: "https://guce.yahoo.com/copyConsent") else {
            throw YFinanceError.invalidURL("https://guce.yahoo.com/copyConsent")
        }
        copyComponents.queryItems = [URLQueryItem(name: "sessionId", value: sessionId)]
        guard let copyURL = copyComponents.url else {
            throw YFinanceError.invalidURL("Yahoo copyConsent URL")
        }

        var copy = baseRequest(url: copyURL)
        copy.setValue("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", forHTTPHeaderField: "Accept")

        do {
            let (_, copyResponse) = try await session.data(for: copy)
            if let copyHTTP = copyResponse as? HTTPURLResponse,
               copyHTTP.statusCode == 429 {
                throw YFinanceError.httpStatus(429)
            }
            // copyConsent has historically been tolerant of redirects/non-2xx
            // responses as long as it installs the final Yahoo cookie.
        } catch let error as YFinanceError {
            throw error
        } catch {
            throw YFinanceError.transport(error)
        }
    }

    private func fetchCrumb(from url: URL) async throws -> String? {
        var request = baseRequest(url: url)
        request.setValue("text/plain,application/json;q=0.9,*/*;q=0.8", forHTTPHeaderField: "Accept")

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw YFinanceError.transport(error)
        }

        guard let httpResponse = response as? HTTPURLResponse else {
            throw YFinanceError.missingData("Expected HTTPURLResponse while fetching Yahoo crumb")
        }
        guard (200...299).contains(httpResponse.statusCode) else {
            throw YFinanceError.httpStatus(httpResponse.statusCode)
        }

        let crumbText = String(decoding: data, as: UTF8.self)
            .trimmingCharacters(in: .whitespacesAndNewlines)

        guard isPlausibleCrumb(crumbText) else {
            return nil
        }
        return crumbText
    }

    private func baseRequest(url: URL) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpShouldHandleCookies = true
        request.timeoutInterval = requestTimeout
        request.setValue(userAgent, forHTTPHeaderField: "User-Agent")
        request.setValue("en-US,en;q=0.9", forHTTPHeaderField: "Accept-Language")
        return request
    }

    private func isPlausibleCrumb(_ value: String) -> Bool {
        guard !value.isEmpty, value.count <= 256 else {
            return false
        }

        let lowercased = value.lowercased()
        if lowercased.contains("<html") ||
            lowercased.contains("too many requests") ||
            lowercased.contains("edge:") ||
            lowercased.contains("unauthorized") ||
            lowercased.contains("invalid cookie") ||
            lowercased.contains("invalid crumb") ||
            value.hasPrefix("{") ||
            value.hasPrefix("[") {
            return false
        }

        return true
    }

    private func inputValue(named name: String, in html: String) -> String? {
        let inputPattern = #"<input\b[^>]*>"#
        guard let inputRegex = try? NSRegularExpression(pattern: inputPattern, options: [.caseInsensitive]) else {
            return nil
        }

        let fullRange = NSRange(html.startIndex..<html.endIndex, in: html)
        for match in inputRegex.matches(in: html, range: fullRange) {
            guard let range = Range(match.range, in: html) else { continue }
            let tag = String(html[range])
            guard attribute("name", in: tag)?.caseInsensitiveCompare(name) == .orderedSame else {
                continue
            }
            return attribute("value", in: tag).map(decodeHTMLEntities)
        }
        return nil
    }

    private func attribute(_ name: String, in tag: String) -> String? {
        let escaped = NSRegularExpression.escapedPattern(for: name)
        let pattern = #"\b"# + escaped + #"\s*=\s*([\"'])(.*?)\1"#
        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
            return nil
        }
        let range = NSRange(tag.startIndex..<tag.endIndex, in: tag)
        guard let match = regex.firstMatch(in: tag, range: range),
              match.numberOfRanges >= 3,
              let valueRange = Range(match.range(at: 2), in: tag) else {
            return nil
        }
        return String(tag[valueRange])
    }

    private func decodeHTMLEntities(_ value: String) -> String {
        value
            .replacingOccurrences(of: "&amp;", with: "&")
            .replacingOccurrences(of: "&quot;", with: "\"")
            .replacingOccurrences(of: "&#39;", with: "'")
            .replacingOccurrences(of: "&lt;", with: "<")
            .replacingOccurrences(of: "&gt;", with: ">")
    }

    private func clearLegacyPersistedCrumbIfNeeded(force: Bool = false) async {
        guard force || !clearedLegacyCache else {
            return
        }
        clearedLegacyCache = true

        // Older versions persisted a crumb independently from URLSession cookies.
        // Purge those keys so a stale session-bound crumb cannot be resurrected.
        await YFCacheStores.crumb.set(nil, for: "crumb")
        await YFCacheStores.crumb.set(nil, for: "fetchedAt")
    }
}
