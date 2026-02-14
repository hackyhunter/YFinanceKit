import Foundation

public struct YFScreener: Sendable {
    private let client: YFinanceClient

    public init(client: YFinanceClient = YFinanceClient()) {
        self.client = client
    }

    public func predefined(
        _ query: YFPredefinedScreenerQuery,
        offset: Int? = nil,
        count: Int? = nil,
        size: Int? = nil,
        sortField: String? = nil,
        sortAscending: Bool? = nil,
        sortAsc: Bool? = nil,
        userId: String? = nil,
        userIdType: String? = nil
    ) async throws -> YFJSONValue {
        try await predefined(
            query.rawValue,
            offset: offset,
            count: count,
            size: size,
            sortField: sortField,
            sortAscending: sortAscending,
            sortAsc: sortAsc,
            userId: userId,
            userIdType: userIdType
        )
    }

    public func predefined(
        _ queryId: String,
        offset: Int? = nil,
        count: Int? = nil,
        size: Int? = nil,
        sortField: String? = nil,
        sortAscending: Bool? = nil,
        sortAsc: Bool? = nil,
        userId: String? = nil,
        userIdType: String? = nil
    ) async throws -> YFJSONValue {
        let normalizedQueryId = queryId.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !normalizedQueryId.isEmpty else {
            throw YFinanceError.invalidRequest("queryId cannot be empty")
        }
        if let count, count > 250 {
            throw YFinanceError.invalidRequest("Yahoo limits screener count to 250")
        }
        if let size, size > 250 {
            throw YFinanceError.invalidRequest("Yahoo limits screener size to 250")
        }

        // Python yfinance switches to the regular screener endpoint when offset is specified,
        // because the predefined endpoint ignores offset.
        if let offset {
            guard let definition = _PREDEFINED_SCREENER_DEFINITIONS[normalizedQueryId] else {
                throw YFinanceError.invalidRequest("Unknown predefined query '\(normalizedQueryId)'")
            }

            var request = YFScreenerRequest(query: definition.query)
            request.quoteType = definition.quoteType
            request.offset = max(0, offset)
            request.count = max(1, count ?? definition.count ?? 25)
            request.size = size.map { max(1, $0) }
            request.sortField = sortField ?? definition.sortField

            let resolvedSortAscending = sortAsc ?? sortAscending ?? (definition.sortType.lowercased() == "asc")
            request.sortAscending = resolvedSortAscending
            request.userId = userId ?? ""
            request.userIdType = userIdType ?? "guid"

            return try await run(request)
        }

        let resolvedCount = size ?? count
        let resolvedSortAscending = sortAsc ?? sortAscending
        let raw = try await client.screenerPredefined(
            id: normalizedQueryId,
            offset: offset,
            count: resolvedCount,
            sortField: sortField,
            sortAsc: resolvedSortAscending,
            userId: userId,
            userIdType: userIdType
        )
        return raw["finance"]?["result"]?[0] ?? .object([:])
    }

    public func run(_ request: YFScreenerRequest) async throws -> YFJSONValue {
        if let quoteType = request.quoteType {
            try request.query.validate(for: quoteType)
        }
        let raw = try await client.screener(body: request.toJSONValue())
        return raw["finance"]?["result"]?[0] ?? .object([:])
    }

    public func run(
        query: YFScreenerQuery,
        quoteType: YFScreenerQuoteType? = nil,
        quote_type: YFScreenerQuoteType? = nil,
        count: Int? = 25,
        size: Int? = nil,
        offset: Int = 0,
        sortField: String = "ticker",
        sortAscending: Bool? = false,
        sortAsc: Bool? = nil,
        userId: String = "",
        userIdType: String = "guid"
    ) async throws -> YFJSONValue {
        if let count, count > 250 {
            throw YFinanceError.invalidRequest("Yahoo limits screener count to 250")
        }
        if let size, size > 250 {
            throw YFinanceError.invalidRequest("Yahoo limits screener size to 250")
        }

        var request = YFScreenerRequest(query: query)
        request.quoteType = quote_type ?? quoteType
        request.offset = max(0, offset)
        request.count = max(1, count ?? 25)
        request.size = size.map { max(1, $0) }
        request.sortField = sortField
        request.sortAscending = sortAsc ?? sortAscending ?? false
        request.userId = userId
        request.userIdType = userIdType
        return try await run(request)
    }
}

public enum YFQueryBuilder {
    public static func and(_ operands: [YFScreenerQuery]) -> YFScreenerQuery {
        YFScreenerQuery("and", operands: operands.map { .query($0) })
    }

    public static func or(_ operands: [YFScreenerQuery]) -> YFScreenerQuery {
        YFScreenerQuery("or", operands: operands.map { .query($0) })
    }

    public static func eq(_ field: String, _ value: YFScreenerOperand) -> YFScreenerQuery {
        YFScreenerQuery("eq", operands: [.string(field), value])
    }

    public static func gt(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFScreenerQuery("gt", operands: [.string(field), .double(value)])
    }

    public static func gte(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFScreenerQuery("gte", operands: [.string(field), .double(value)])
    }

    public static func lt(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFScreenerQuery("lt", operands: [.string(field), .double(value)])
    }

    public static func lte(_ field: String, _ value: Double) -> YFScreenerQuery {
        YFScreenerQuery("lte", operands: [.string(field), .double(value)])
    }

    public static func btwn(_ field: String, _ low: Double, _ high: Double) -> YFScreenerQuery {
        YFScreenerQuery("btwn", operands: [.string(field), .double(low), .double(high)])
    }

    public static func isIn(_ field: String, _ values: [YFScreenerOperand]) -> YFScreenerQuery {
        YFScreenerQuery("is-in", operands: [.string(field)] + values)
    }
}

public func screen(
    _ query: YFPredefinedScreenerQuery,
    offset: Int? = nil,
    count: Int? = nil,
    size: Int? = nil,
    sortField: String? = nil,
    sortAscending: Bool? = nil,
    sortAsc: Bool? = nil,
    userId: String? = nil,
    userIdType: String? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFJSONValue {
    try await YFScreener(client: client).predefined(
        query,
        offset: offset,
        count: count,
        size: size,
        sortField: sortField,
        sortAscending: sortAscending,
        sortAsc: sortAsc,
        userId: userId,
        userIdType: userIdType
    )
}

public func screen(
    _ queryId: String,
    offset: Int? = nil,
    count: Int? = nil,
    size: Int? = nil,
    sortField: String? = nil,
    sortAscending: Bool? = nil,
    sortAsc: Bool? = nil,
    userId: String? = nil,
    userIdType: String? = nil,
    client: YFinanceClient = YFinanceClient()
) async throws -> YFJSONValue {
    try await YFScreener(client: client).predefined(
        queryId,
        offset: offset,
        count: count,
        size: size,
        sortField: sortField,
        sortAscending: sortAscending,
        sortAsc: sortAsc,
        userId: userId,
        userIdType: userIdType
    )
}

public func screen(
    _ query: YFScreenerQuery,
    quoteType: YFScreenerQuoteType? = nil,
    quote_type: YFScreenerQuoteType? = nil,
    count: Int? = 25,
    size: Int? = nil,
    offset: Int = 0,
    sortField: String = "ticker",
    sortAscending: Bool? = false,
    sortAsc: Bool? = nil,
    userId: String = "",
    userIdType: String = "guid",
    client: YFinanceClient = YFinanceClient()
) async throws -> YFJSONValue {
    try await YFScreener(client: client).run(
        query: query,
        quoteType: quoteType,
        quote_type: quote_type,
        count: count,
        size: size,
        offset: offset,
        sortField: sortField,
        sortAscending: sortAscending,
        sortAsc: sortAsc,
        userId: userId,
        userIdType: userIdType
    )
}

public func screen(
    _ query: YFScreenerQuery,
    quoteType: YFScreenerQuoteType? = nil,
    quote_type: YFScreenerQuoteType? = nil,
    offset: Int = 0,
    size: Int? = nil,
    count: Int? = 25,
    sortField: String = "ticker",
    sortAscending: Bool? = false,
    sortAsc: Bool? = nil,
    userId: String = "",
    userIdType: String = "guid",
    client: YFinanceClient = YFinanceClient()
) async throws -> YFJSONValue {
    try await screen(
        query,
        quoteType: quoteType,
        quote_type: quote_type,
        count: count,
        size: size,
        offset: offset,
        sortField: sortField,
        sortAscending: sortAscending,
        sortAsc: sortAsc,
        userId: userId,
        userIdType: userIdType,
        client: client
    )
}
