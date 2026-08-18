import Foundation

public enum YFinanceFailureKind: String, Sendable {
    case invalidRequest
    case unauthorized
    case forbidden
    case notFound
    case rateLimited
    case serverUnavailable
    case transport
    case decoding
    case missingData
    case yahooAPI
    case unknown
}

public extension YFinanceError {
    var failureKind: YFinanceFailureKind {
        switch self {
        case .invalidURL, .invalidRequest:
            return .invalidRequest
        case .transport:
            return .transport
        case .httpStatus(let status):
            switch status {
            case 401: return .unauthorized
            case 403: return .forbidden
            case 404: return .notFound
            case 429: return .rateLimited
            case 500...599: return .serverUnavailable
            default: return .unknown
            }
        case .serverError(let code, let description):
            let haystack = "\(code) \(description)".lowercased()
            if haystack.contains("too many requests") || haystack.contains("rate limit") || code == "429" {
                return .rateLimited
            }
            if haystack.contains("unauthorized") || code == "401" {
                return .unauthorized
            }
            if haystack.contains("forbidden") || code == "403" {
                return .forbidden
            }
            return .yahooAPI
        case .decoding:
            return .decoding
        case .missingData:
            return .missingData
        }
    }

    var httpStatusCode: Int? {
        switch self {
        case .httpStatus(let status):
            return status
        case .serverError(let code, _):
            return Int(code)
        default:
            return nil
        }
    }

    var isRateLimited: Bool { failureKind == .rateLimited }
    var isUnauthorized: Bool { failureKind == .unauthorized || failureKind == .forbidden }

    var isTransient: Bool {
        switch failureKind {
        case .transport, .rateLimited, .serverUnavailable:
            return true
        default:
            return false
        }
    }

    /// Yahoo-supplied explanation when available, without inventing a delisting reason.
    var yahooDescription: String? {
        switch self {
        case .serverError(_, let description):
            return description
        case .missingData(let description):
            return description
        default:
            return nil
        }
    }
}

public enum YFinanceErrorClassifier {
    public static func kind(of error: Error) -> YFinanceFailureKind {
        if let error = error as? YFinanceError {
            return error.failureKind
        }
        let nsError = error as NSError
        if nsError.domain == NSURLErrorDomain {
            return .transport
        }
        return .unknown
    }

    public static func isTransient(_ error: Error) -> Bool {
        if let error = error as? YFinanceError {
            return error.isTransient
        }
        let nsError = error as NSError
        guard nsError.domain == NSURLErrorDomain else { return false }
        switch nsError.code {
        case NSURLErrorTimedOut,
             NSURLErrorNetworkConnectionLost,
             NSURLErrorNotConnectedToInternet,
             NSURLErrorCannotFindHost,
             NSURLErrorCannotConnectToHost,
             NSURLErrorDNSLookupFailed,
             NSURLErrorSecureConnectionFailed,
             NSURLErrorCannotLoadFromNetwork:
            return true
        default:
            return false
        }
    }
}
