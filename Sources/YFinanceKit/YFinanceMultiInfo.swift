import Foundation

public struct YFMultiInfoFailure: Sendable {
    public let symbol: String
    public let kind: YFinanceFailureKind
    public let message: String

    public init(symbol: String, kind: YFinanceFailureKind, message: String) {
        self.symbol = symbol
        self.kind = kind
        self.message = message
    }
}

public struct YFMultiInfoResult: Sendable {
    public let values: [String: YFJSONValue]
    public let failures: [String: YFMultiInfoFailure]

    public init(values: [String: YFJSONValue], failures: [String: YFMultiInfoFailure]) {
        self.values = values
        self.failures = failures
    }

    public var isComplete: Bool { failures.isEmpty }
    public var succeededSymbols: [String] { values.keys.sorted() }
    public var failedSymbols: [String] { failures.keys.sorted() }
}

private struct YFMultiInfoItem: Sendable {
    let symbol: String
    let value: YFJSONValue?
    let failure: YFMultiInfoFailure?
}

public extension YFTickers {
    /// Fetches per-symbol `Ticker.info()` with bounded concurrency and isolated
    /// failures. This mirrors the direction of upstream yfinance multi-info work
    /// without introducing shared module globals or unbounded task fan-out.
    func infoResult(
        maxConcurrentRequests: Int = 4
    ) async -> YFMultiInfoResult {
        let policy = YFRequestPolicy(maxConcurrentRequests: maxConcurrentRequests)
        let coordinator = YFRequestCoordinator(policy: policy)
        let requestedSymbols = symbols

        return await withTaskGroup(of: YFMultiInfoItem.self) { group in
            for symbol in requestedSymbols {
                let ticker = ticker(symbol)
                group.addTask {
                    do {
                        let value = try await coordinator.execute(
                            endpoint: "multi-info",
                            resource: symbol
                        ) {
                            try await ticker.info()
                        }
                        return YFMultiInfoItem(symbol: symbol, value: value, failure: nil)
                    } catch {
                        let failure = YFMultiInfoFailure(
                            symbol: symbol,
                            kind: YFinanceErrorClassifier.kind(of: error),
                            message: error.localizedDescription
                        )
                        return YFMultiInfoItem(symbol: symbol, value: nil, failure: failure)
                    }
                }
            }

            var values: [String: YFJSONValue] = [:]
            var failures: [String: YFMultiInfoFailure] = [:]
            values.reserveCapacity(requestedSymbols.count)

            for await item in group {
                if let value = item.value {
                    values[item.symbol] = value
                }
                if let failure = item.failure {
                    failures[item.symbol] = failure
                }
            }

            return YFMultiInfoResult(values: values, failures: failures)
        }
    }

    /// Python-like best-effort convenience. Failed tickers are omitted from the
    /// returned dictionary, matching bulk-data ergonomics where one bad symbol
    /// should not discard all successful symbols.
    func getInfo(
        threads: Bool = true,
        progress _: Bool = false
    ) async throws -> [String: YFJSONValue] {
        let result = await infoResult(maxConcurrentRequests: threads ? 4 : 1)
        return result.values
    }

    func get_info(
        threads: Bool = true,
        progress: Bool = false
    ) async throws -> [String: YFJSONValue] {
        try await getInfo(threads: threads, progress: progress)
    }
}
