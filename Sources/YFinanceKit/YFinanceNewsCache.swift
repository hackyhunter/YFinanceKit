import Foundation

/// Bounded, short-lived results scoped to the originating client. Weak ownership
/// prevents retaining sessions and guards against ObjectIdentifier reuse.
actor YFNewsResponseCache {
    static let shared = YFNewsResponseCache()

    struct Key: Hashable, Sendable {
        var symbol: String
        var count: Int
        var tab: String
        var lang: String
        var region: String
    }

    private struct ScopedKey: Hashable {
        let owner: ObjectIdentifier
        let request: Key
    }
    private struct Entry {
        weak var owner: YFinanceClient?
        let value: YFJSONValue
        let expiresAt: TimeInterval
    }

    private let lifetime: TimeInterval
    private let capacity: Int
    private var entries: [ScopedKey: Entry] = [:]

    init(lifetime: TimeInterval = 60, capacity: Int = 128) {
        self.lifetime = lifetime.isFinite ? max(0, lifetime) : 0
        self.capacity = max(1, capacity)
    }

    func value(for key: Key, owner: YFinanceClient, now: TimeInterval = ProcessInfo.processInfo.systemUptime) -> YFJSONValue? {
        prune(now: now)
        let scoped = ScopedKey(owner: ObjectIdentifier(owner), request: key)
        guard let entry = entries[scoped], entry.owner === owner else { return nil }
        return entry.value
    }

    func insert(_ value: YFJSONValue, for key: Key, owner: YFinanceClient, now: TimeInterval = ProcessInfo.processInfo.systemUptime) {
        prune(now: now)
        guard lifetime > 0 else { return }
        let scoped = ScopedKey(owner: ObjectIdentifier(owner), request: key)
        if entries[scoped] == nil, entries.count >= capacity,
           let oldest = entries.min(by: { $0.value.expiresAt < $1.value.expiresAt })?.key {
            entries.removeValue(forKey: oldest)
        }
        entries[scoped] = Entry(owner: owner, value: value, expiresAt: now + lifetime)
    }

    func clear(owner: YFinanceClient) {
        let identity = ObjectIdentifier(owner)
        entries = entries.filter { $0.key.owner != identity && $0.value.owner != nil }
    }

    private func prune(now: TimeInterval) {
        entries = entries.filter { $0.value.owner != nil && $0.value.expiresAt > now }
    }

    nonisolated static func isSuccessfulPage(_ response: YFJSONValue) -> Bool {
        if let errors = response["errors"] {
            switch errors {
            case .null: break
            case .array(let values) where values.isEmpty: break
            default: return false
            }
        }
        let stream = response["data"]?["lightyearList"]?["main"]
        return stream?["edges"]?.arrayValue != nil
            && stream?["pageInfo"]?["hasNextPage"]?.boolValue != nil
    }
}

extension YFinanceClient {
    /// Invalidate this client's completed ticker-news results. News otherwise
    /// remains fresh for up to 60 seconds; failures and partial errors are not cached.
    public func clearNewsCache() async {
        await YFNewsResponseCache.shared.clear(owner: self)
    }
}
