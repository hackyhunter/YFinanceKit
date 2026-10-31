import Foundation

extension YFinanceClient {
    private static let newsGraphQLURL = URL(string: "https://nexus-finance-prod.media.yahoo.com/")!

    private static let newsGraphQLQuery = """
    query FinanceTickerListNews($listInput: LightyearListInput!, $clientContext: ClientContext!, $mlRecsInput: MLRecsInput!, $gqlContext: [GqlContext] = [], $imageResize: [ImageResizeInput!]! = [], $mlRecsFirst: Int, $after: String) {
      lightyearList: financeTickerList(list_input: $listInput, cc: $clientContext) {
        main: mlRecsStream(mlRecsInput: $mlRecsInput, first: $mlRecsFirst, after: $after) {
          edges { node { asset(gqlContext: $gqlContext) {
            __typename
            ... on Story { id: uuid title previewUrl(cc: $clientContext) isPremiumNews contentAttributes { ...Attrs } finance { ...Tickers } }
            ... on Video { id: uuid title previewUrl(cc: $clientContext) contentAttributes { ...Attrs } finance { ...Tickers } }
            ... on Outlink { id: uuid title: headline description displayTime url provider { displayName url } contentAttributes { thumbnail { ...Image } } }
          } } }
          pageInfo { hasNextPage endCursor }
        }
      }
    }
    fragment Image on Image { url height width resized(resizeInput: $imageResize) { url height width transformLabel } }
    fragment Attrs on ContentAttributes {
      description summary publishTime displayTime isHosted canonicalUrl clickthroughUrl(cc: $clientContext)
      provider { displayName url } thumbnail { ...Image }
    }
    fragment Tickers on Finance { stockTickers { symbol } }
    """

    func tickerNewsGraphQL(
        symbol: String,
        count: Int,
        tab: String,
        lang: String,
        region: String
    ) async throws -> YFJSONValue {
        let listNames = [
            "all": "all",
            "news": "news",
            "press releases": "pressrelease",
        ]
        guard let listName = listNames[tab.lowercased()] else {
            throw YFinanceError.invalidRequest("Invalid tab: \(tab)")
        }

        let requestedCount = max(count, 0)
        guard requestedCount > 0 else {
            return Self.newsEnvelope([])
        }

        let effectiveLang = lang.trimmingCharacters(in: .whitespacesAndNewlines)
        let effectiveRegion = region.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        guard !effectiveLang.isEmpty, !effectiveRegion.isEmpty else {
            throw YFinanceError.invalidRequest("News locale cannot be empty")
        }

        let slug = "list=finance-\(effectiveRegion)-\(effectiveLang)-ticker-\(listName)"
        let baseVariables: [String: YFJSONValue] = [
            "clientContext": .object([
                "device": .string("desktop"),
                "lang": .string(effectiveLang),
                "region": .string(effectiveRegion),
                "site": .string("finance"),
            ]),
            "gqlContext": .array([
                .object(["listAlias": .string(slug)]),
            ]),
            "imageResize": .array([
                .object([
                    "operations": .array([.string("resizefill_w170_h128")]),
                    "transformLabel": .string("170x128"),
                ]),
            ]),
            "listInput": .object([
                "disableDedupe": .bool(false),
                "enableBlockedContent": .bool(false),
                "enableQueryTimeLicenseCheck": .bool(true),
                "filterClientContext": .bool(false),
                "getFullList": .bool(true),
                "queryVariables": .object([
                    "tickerSymbol": .array([.string(symbol)]),
                ]),
                "slug": .string(slug),
            ]),
            "mlRecsInput": .object([
                "count": .number(200),
                "instance": .string("FINANCE"),
                "rankInput": .object([
                    "financeQuoteNewsV2": .object([
                        "aboutnessWeight": .number(0),
                        "boostDurationHours": .number(16),
                        "halfLifeHours": .number(24),
                        "recencyWeight": .number(1),
                        "tickerSymbol": .array([.string(symbol)]),
                    ]),
                ]),
            ]),
        ]

        var news: [YFJSONValue] = []
        var after: String?
        let maxPages = max(5, ((requestedCount + 9) / 10) * 2)
        var pagesFetched = 0

        while news.count < requestedCount, pagesFetched < maxPages {
            pagesFetched += 1
            var variables = baseVariables
            variables["mlRecsFirst"] = .number(Double(requestedCount - news.count))
            variables["after"] = after.map(YFJSONValue.string) ?? .null

            let body: YFJSONValue = .object([
                "operationName": .string("FinanceTickerListNews"),
                "query": .string(Self.newsGraphQLQuery),
                "variables": .object(variables),
            ])

            let response = try await rawPost(
                url: Self.newsGraphQLURL,
                body: body,
                headers: ["x-yahoo-cg-client-name": "finance"]
            )

            let stream = response["data"]?["lightyearList"]?["main"]
            let edges = stream?["edges"]?.arrayValue ?? []

            for edge in edges {
                if let article = Self.normalizedNewsAsset(edge["node"]?["asset"]) {
                    news.append(article)
                    if news.count >= requestedCount {
                        break
                    }
                }
            }

            let pageInfo = stream?["pageInfo"]
            let previousAfter = after
            after = pageInfo?["endCursor"]?.stringValue
            let hasNextPage = pageInfo?["hasNextPage"]?.boolValue ?? false

            if edges.isEmpty || !hasNextPage || after == nil || after == previousAfter {
                break
            }
        }

        return Self.newsEnvelope(Array(news.prefix(requestedCount)))
    }

    private static func newsEnvelope(_ articles: [YFJSONValue]) -> YFJSONValue {
        .object([
            "data": .object([
                "tickerStream": .object([
                    "stream": .array(articles),
                ]),
            ]),
        ])
    }

    private static func normalizedNewsAsset(_ asset: YFJSONValue?) -> YFJSONValue? {
        guard let asset,
              let type = asset["__typename"]?.stringValue,
              let contentType = ["Story": "STORY", "Video": "VIDEO", "Outlink": "OUTLINK"][type]
        else {
            return nil
        }

        let attributes = asset["contentAttributes"]
        let thumbnail = normalizedNewsThumbnail(attributes?["thumbnail"])

        let canonicalURL: String?
        let clickThroughURL: String?
        let provider: YFJSONValue
        if type == "Outlink" {
            canonicalURL = asset["url"]?.stringValue
            clickThroughURL = canonicalURL
            provider = asset["provider"] ?? .null
        } else {
            canonicalURL = attributes?["canonicalUrl"]?.stringValue
            clickThroughURL = attributes?["clickthroughUrl"]?.stringValue
            provider = attributes?["provider"] ?? .null
        }

        let identifier = asset["id"] ?? .null
        let content: YFJSONValue = .object([
            "id": identifier,
            "contentType": .string(contentType),
            "title": asset["title"] ?? .null,
            "description": preferredNewsValue(asset["description"], attributes?["description"]),
            "summary": attributes?["summary"] ?? .null,
            "pubDate": preferredNewsValue(attributes?["publishTime"], asset["displayTime"]),
            "displayTime": preferredNewsValue(attributes?["displayTime"], asset["displayTime"]),
            "isHosted": attributes?["isHosted"] ?? .null,
            "previewUrl": asset["previewUrl"] ?? .null,
            "thumbnail": thumbnail,
            "provider": provider,
            "canonicalUrl": newsURLObject(canonicalURL),
            "clickThroughUrl": newsURLObject(clickThroughURL),
            "finance": .object([
                "premiumFinance": .object([
                    "isPremiumNews": .bool(asset["isPremiumNews"]?.boolValue ?? false),
                ]),
                "stockTickers": asset["finance"]?["stockTickers"] ?? .array([]),
            ]),
        ])

        return .object([
            "id": identifier,
            "content": content,
        ])
    }

    private static func normalizedNewsThumbnail(_ value: YFJSONValue?) -> YFJSONValue {
        guard let value,
              let url = value["url"]?.stringValue,
              !url.isEmpty
        else {
            return .null
        }

        var resolutions: [YFJSONValue] = [
            .object([
                "url": .string(url),
                "width": value["width"] ?? .null,
                "height": value["height"] ?? .null,
                "tag": .string("original"),
            ]),
        ]

        for resized in value["resized"]?.arrayValue ?? [] {
            resolutions.append(
                .object([
                    "url": resized["url"] ?? .null,
                    "width": resized["width"] ?? .null,
                    "height": resized["height"] ?? .null,
                    "tag": resized["transformLabel"] ?? .null,
                ])
            )
        }

        return .object([
            "originalUrl": .string(url),
            "originalWidth": value["width"] ?? .null,
            "originalHeight": value["height"] ?? .null,
            "resolutions": .array(resolutions),
        ])
    }

    private static func newsURLObject(_ url: String?) -> YFJSONValue {
        guard let url, !url.isEmpty else { return .null }
        return .object(["url": .string(url)])
    }

    private static func preferredNewsValue(_ primary: YFJSONValue?, _ fallback: YFJSONValue?) -> YFJSONValue {
        if let primary, !isNullNewsValue(primary) {
            return primary
        }
        return fallback ?? .null
    }

    private static func isNullNewsValue(_ value: YFJSONValue) -> Bool {
        if case .null = value { return true }
        return false
    }
}
