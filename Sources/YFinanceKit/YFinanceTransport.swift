import Foundation

struct YFHTTPTransportResponse: Sendable {
    let data: Data
    let statusCode: Int
    private let headers: [String: String]

    init(data: Data, statusCode: Int, headers: [String: String]) {
        self.data = data
        self.statusCode = statusCode
        self.headers = headers
    }

    func header(_ name: String) -> String? {
        headers[name.lowercased()]
    }
}

protocol YFHTTPTransporting: Sendable {
    func send(_ request: URLRequest) async throws -> YFHTTPTransportResponse
}

/// Small transport boundary around URLSession. Endpoint construction, Yahoo
/// session/crumb policy and retry decisions intentionally remain above this
/// layer. The response crosses concurrency boundaries using only Sendable
/// values instead of carrying `HTTPURLResponse` around the package.
final class YFURLSessionTransport: YFHTTPTransporting, @unchecked Sendable {
    private let session: URLSession

    init(session: URLSession) {
        self.session = session
    }

    func send(_ request: URLRequest) async throws -> YFHTTPTransportResponse {
        let (data, response) = try await session.data(for: request)
        guard let response = response as? HTTPURLResponse else {
            throw YFinanceError.invalidRequest("Non-HTTP response")
        }

        var headers: [String: String] = [:]
        headers.reserveCapacity(response.allHeaderFields.count)
        for (rawKey, rawValue) in response.allHeaderFields {
            let key = String(describing: rawKey).lowercased()
            headers[key] = String(describing: rawValue)
        }

        return YFHTTPTransportResponse(
            data: data,
            statusCode: response.statusCode,
            headers: headers
        )
    }
}
