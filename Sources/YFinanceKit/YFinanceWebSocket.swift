import Foundation

public struct YFStreamingMessage: Sendable {
    public let raw: YFJSONValue
    public let encodedMessage: String?
    public let payloadData: Data?
    public let pricingData: YFPricingData?

    public var symbol: String? { pricingData?.symbol }
    public var quoteType: YFStreamQuoteType? { pricingData?.quoteTypeValue }
    public var marketHours: YFStreamMarketHours? { pricingData?.marketHoursValue }
}

public actor YFAsyncWebSocket {
    private let url: URL
    private let session: URLSession
    private let verbose: Bool
    private let subscriptionIntervalNanoseconds: UInt64
    private var task: URLSessionWebSocketTask?
    private var subscriptions: Set<String> = []
    private var heartbeatTask: Task<Void, Never>?

    public init(
        url: URL = URL(string: "wss://streamer.finance.yahoo.com/?version=2")!,
        session: URLSession = .shared,
        verbose: Bool = true,
        subscriptionIntervalSeconds: TimeInterval = 15
    ) {
        self.url = url
        self.session = session
        self.verbose = verbose
        self.subscriptionIntervalNanoseconds = UInt64(max(1, subscriptionIntervalSeconds) * 1_000_000_000)
    }

    public func connect() {
        guard task == nil else {
            return
        }
        let task = session.webSocketTask(with: url)
        task.resume()
        self.task = task
        if verbose {
            print("Connected to WebSocket.")
        }
        startHeartbeatIfNeeded()
    }

    public func close() {
        heartbeatTask?.cancel()
        heartbeatTask = nil
        task?.cancel(with: .normalClosure, reason: nil)
        task = nil
        if verbose {
            print("WebSocket connection closed.")
        }
    }

    public func subscribe(_ symbols: [String]) async throws {
        connect()
        let cleaned = symbols.map { $0.trimmingCharacters(in: .whitespacesAndNewlines).uppercased() }.filter { !$0.isEmpty }
        subscriptions.formUnion(cleaned)
        try await sendJSON(.object(["subscribe": .array(subscriptions.sorted().map { .string($0) })]))
        startHeartbeatIfNeeded()
        if verbose {
            print("Subscribed to symbols: \(cleaned)")
        }
    }

    public func subscribe(_ symbol: String) async throws {
        try await subscribe([symbol])
    }

    public func unsubscribe(_ symbols: [String]) async throws {
        connect()
        let cleaned = symbols.map { $0.trimmingCharacters(in: .whitespacesAndNewlines).uppercased() }.filter { !$0.isEmpty }
        subscriptions.subtract(cleaned)
        try await sendJSON(.object(["unsubscribe": .array(cleaned.map { .string($0) })]))
        if verbose {
            print("Unsubscribed from symbols: \(cleaned)")
        }
    }

    public func unsubscribe(_ symbol: String) async throws {
        try await unsubscribe([symbol])
    }

    public func messages() -> AsyncThrowingStream<YFStreamingMessage, Error> {
        connect()
        startHeartbeatIfNeeded()

        return AsyncThrowingStream { continuation in
            Task {
                do {
                    while !Task.isCancelled {
                        let message = try await receiveOne()
                        continuation.yield(message)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    public func listen(_ handler: (@Sendable (YFStreamingMessage) -> Void)? = nil) async throws {
        connect()
        startHeartbeatIfNeeded()
        if verbose {
            print("Listening for messages...")
        }
        while true {
            let message = try await receiveOne()
            if let handler {
                handler(message)
            } else {
                print(message.raw)
            }
        }
    }

    private func sendJSON(_ value: YFJSONValue) async throws {
        guard let task else {
            throw YFinanceError.missingData("WebSocket is not connected")
        }
        let data = try YFJSONValue.encode(value)
        guard let text = String(data: data, encoding: .utf8) else {
            throw YFinanceError.invalidRequest("Failed to encode websocket message")
        }
        try await task.send(.string(text))
    }

    private func startHeartbeatIfNeeded() {
        guard heartbeatTask == nil else {
            return
        }

        heartbeatTask = Task { [subscriptionIntervalNanoseconds] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: subscriptionIntervalNanoseconds)
                if Task.isCancelled {
                    break
                }
                await self.sendHeartbeatIfNeeded()
            }
        }
    }

    private func sendHeartbeatIfNeeded() async {
        guard !subscriptions.isEmpty else {
            return
        }
        do {
            try await sendJSON(.object(["subscribe": .array(subscriptions.sorted().map { .string($0) })]))
            if verbose {
                print("Heartbeat subscription sent for symbols: \(subscriptions.sorted())")
            }
        } catch {
            if verbose {
                print("Heartbeat subscription failed: \(error.localizedDescription)")
            }
        }
    }

    private func receiveOne() async throws -> YFStreamingMessage {
        guard let task else {
            throw YFinanceError.missingData("WebSocket is not connected")
        }

        let received = try await task.receive()
        switch received {
        case .string(let text):
            let data = Data(text.utf8)
            let raw = (try? YFJSONValue.decode(data: data)) ?? .object([:])
            let encoded = raw["message"]?.stringValue
            let decodedData = encoded.flatMap { Data(base64Encoded: $0) }
            let pricingData: YFPricingData?
            if let decodedData {
                do {
                    pricingData = try YFProtobufDecoder.decodePricingData(decodedData)
                } catch {
                    let hideExceptions = await YFConfigStore.shared.debug.hideExceptions
                    if !hideExceptions {
                        throw error
                    }
                    pricingData = nil
                }
            } else {
                pricingData = nil
            }
            return YFStreamingMessage(raw: raw, encodedMessage: encoded, payloadData: decodedData, pricingData: pricingData)
        case .data(let data):
            let raw = (try? YFJSONValue.decode(data: data)) ?? .object([:])
            let encoded = raw["message"]?.stringValue
            let decodedData = encoded.flatMap { Data(base64Encoded: $0) }
            let pricingData: YFPricingData?
            if let decodedData {
                do {
                    pricingData = try YFProtobufDecoder.decodePricingData(decodedData)
                } catch {
                    let hideExceptions = await YFConfigStore.shared.debug.hideExceptions
                    if !hideExceptions {
                        throw error
                    }
                    pricingData = nil
                }
            } else {
                pricingData = nil
            }
            return YFStreamingMessage(raw: raw, encodedMessage: encoded, payloadData: decodedData, pricingData: pricingData)
        @unknown default:
            throw YFinanceError.invalidRequest("Unknown websocket message type")
        }
    }
}

public final class YFWebSocket: @unchecked Sendable {
    private let asyncSocket: YFAsyncWebSocket
    private var listenTask: Task<Void, Never>?

    public init(
        url: URL = URL(string: "wss://streamer.finance.yahoo.com/?version=2")!,
        verbose: Bool = true
    ) {
        self.asyncSocket = YFAsyncWebSocket(url: url, verbose: verbose)
    }

    deinit {
        listenTask?.cancel()
        let socket = asyncSocket
        Task { await socket.close() }
    }

    public func subscribe(_ symbols: [String]) {
        Task { try? await asyncSocket.subscribe(symbols) }
    }

    public func subscribe(_ symbol: String) {
        Task { try? await asyncSocket.subscribe(symbol) }
    }

    public func unsubscribe(_ symbols: [String]) {
        Task { try? await asyncSocket.unsubscribe(symbols) }
    }

    public func unsubscribe(_ symbol: String) {
        Task { try? await asyncSocket.unsubscribe(symbol) }
    }

    public func listen(_ handler: (@Sendable (YFStreamingMessage) -> Void)? = nil) {
        listenTask?.cancel()
        listenTask = Task {
            try? await asyncSocket.listen(handler)
        }
    }

    public func close() {
        listenTask?.cancel()
        Task { await asyncSocket.close() }
    }
}
