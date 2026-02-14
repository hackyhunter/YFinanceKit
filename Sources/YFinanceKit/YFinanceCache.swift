import Foundation

// Lightweight persistent caches mirroring the intent of Python yfinance/cache.py.
// On iOS/macOS this uses a JSON file under the configured cache directory.

enum YFCachePaths {
    static func effectiveCacheDirectory() async -> URL {
        if let path = await YFConfigStore.shared.cacheDirectory,
           !path.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return URL(fileURLWithPath: path, isDirectory: true)
        }

        let fm = FileManager.default
        if let base = fm.urls(for: .cachesDirectory, in: .userDomainMask).first {
            return base.appendingPathComponent("YFinanceKit", isDirectory: true)
        }

        return fm.temporaryDirectory.appendingPathComponent("YFinanceKit", isDirectory: true)
    }
}

actor YFFileBackedStringKVCache {
    private let fileName: String
    private var loadedFrom: URL?
    private var store: [String: String] = [:]
    private var dummyMode = false

    init(fileName: String) {
        self.fileName = fileName
    }

    func lookup(_ key: String) async -> String? {
        await ensureLoaded()
        return store[key]
    }

    func set(_ value: String?, for key: String) async {
        await ensureLoaded()
        if let value {
            store[key] = value
        } else {
            store.removeValue(forKey: key)
        }
        await persist()
    }

    private func ensureLoaded() async {
        guard !dummyMode else { return }

        let directory = await YFCachePaths.effectiveCacheDirectory()
        let fileURL = directory.appendingPathComponent(fileName, isDirectory: false)

        if loadedFrom == fileURL {
            return
        }

        loadedFrom = fileURL
        store = [:]

        do {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        } catch {
            dummyMode = true
            return
        }

        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            return
        }

        do {
            let data = try Data(contentsOf: fileURL)
            let object = try JSONSerialization.jsonObject(with: data)
            if let dict = object as? [String: String] {
                store = dict
            }
        } catch {
            // Corrupted cache file should not break requests.
            store = [:]
        }
    }

    private func persist() async {
        guard !dummyMode, let fileURL = loadedFrom else { return }
        do {
            let data = try JSONSerialization.data(
                withJSONObject: store,
                options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
            )
            try data.write(to: fileURL, options: [.atomic])
        } catch {
            // Fall back to in-memory cache if persistence fails.
            dummyMode = true
        }
    }
}

enum YFCacheStores {
    static let tz = YFFileBackedStringKVCache(fileName: "tkr-tz.json")
    static let isin = YFFileBackedStringKVCache(fileName: "isin-tkr.json")
    static let crumb = YFFileBackedStringKVCache(fileName: "yahoo-crumb.json")
}
