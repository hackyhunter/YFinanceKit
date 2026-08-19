import Foundation

/// Thread-safe storage for URLProtocol handlers used by async package tests.
/// URLSession may read the handler on a worker thread while XCTest installs or
/// clears it on the test thread, so the shared state must be synchronized.
final class TestURLProtocolHandlerState: @unchecked Sendable {
    typealias Handler = (URLRequest) throws -> (HTTPURLResponse, Data)

    private let lock = NSLock()
    private var handler: Handler?

    func set(_ handler: Handler?) {
        lock.lock()
        self.handler = handler
        lock.unlock()
    }

    func snapshot() -> Handler? {
        lock.lock()
        defer { lock.unlock() }
        return handler
    }
}

/// Thread-safe integer state for URLProtocol request-count assertions.
final class TestURLProtocolCounterState: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    func increment() {
        lock.lock()
        value += 1
        lock.unlock()
    }

    func reset() {
        lock.lock()
        value = 0
        lock.unlock()
    }

    func snapshot() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}
