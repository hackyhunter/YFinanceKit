import Foundation
import XCTest
@testable import YFinanceKit

final class YFinanceNewsTests: XCTestCase {
    override func setUp() {
        super.setUp()
        NewsURLProtocol.reset()
    }

    override func tearDown() {
        NewsURLProtocol.reset()
        super.tearDown()
    }

    func testTickerNewsUsesGraphQLAndNormalizesAssets() async throws {
        let session = makeSession()
        let client = YFinanceClient(session: session)

        NewsURLProtocol.handler = { request in
            guard let url = request.url else { throw NewsTestError.missingURL }
            guard url.host == "nexus-finance-prod.media.yahoo.com" else {
                throw NewsTestError.unexpectedURL(url.absoluteString)
            }
            guard request.value(forHTTPHeaderField: "x-yahoo-cg-client-name") == "finance" else {
                throw NewsTestError.missingHeader
            }
            guard let body = request.httpBody,
                  let json = try JSONSerialization.jsonObject(with: body) as? [String: Any],
                  json["operationName"] as? String == "FinanceTickerListNews",
                  let variables = json["variables"] as? [String: Any],
                  let listInput = variables["listInput"] as? [String: Any],
                  listInput["slug"] as? String == "list=finance-FR-fr-FR-ticker-news",
                  (variables["mlRecsFirst"] as? NSNumber)?.intValue == 3
            else {
                throw NewsTestError.invalidBody
            }

            NewsURLProtocol.incrementRequestCount()
            return Self.response(
                url: url,
                body: """
                {
                  "data": {
                    "lightyearList": {
                      "main": {
                        "edges": [
                          {"node":{"asset":{
                            "__typename":"Story",
                            "id":"abc",
                            "title":"Story title",
                            "previewUrl":"https://example.com/preview",
                            "isPremiumNews":false,
                            "contentAttributes":{
                              "description":"Description",
                              "summary":"Summary",
                              "publishTime":"2026-10-25T20:05:00Z",
                              "displayTime":"2026-10-25T20:05:00Z",
                              "isHosted":true,
                              "canonicalUrl":"https://example.com/abc",
                              "clickthroughUrl":"https://finance.yahoo.com/news/abc.html",
                              "provider":{"displayName":"Provider","url":"https://example.com"},
                              "thumbnail":{
                                "url":"https://media.example.com/abc.jpg",
                                "width":1400,
                                "height":933,
                                "resized":[
                                  {"url":"https://s.yimg.com/abc.jpg","width":170,"height":128,"transformLabel":"170x128"}
                                ]
                              }
                            },
                            "finance":{"stockTickers":[{"symbol":"MSFT"}]}
                          }}},
                          {"node":{"asset":{"__typename":"AssetError"}}},
                          {"node":{"asset":{
                            "__typename":"Video",
                            "id":"vid",
                            "title":"Video title",
                            "contentAttributes":{"publishTime":"2026-10-25T21:00:00Z"},
                            "finance":{"stockTickers":[{"symbol":"MSFT"}]}
                          }}}
                        ],
                        "pageInfo":{"hasNextPage":false,"endCursor":null}
                      }
                    }
                  }
                }
                """
            )
        }

        let raw = try await client.tickerNews(
            symbol: "MSFT",
            count: 3,
            tab: "news",
            lang: "fr-FR",
            region: "FR"
        )

        let items = raw["data"]?["tickerStream"]?["stream"]?.arrayValue ?? []
        XCTAssertEqual(items.count, 2)
        XCTAssertEqual(items[0]["id"]?.stringValue, "abc")
        XCTAssertEqual(items[0]["content"]?["contentType"]?.stringValue, "STORY")
        XCTAssertEqual(items[0]["content"]?["summary"]?.stringValue, "Summary")
        XCTAssertEqual(items[0]["content"]?["canonicalUrl"]?["url"]?.stringValue, "https://example.com/abc")
        XCTAssertEqual(
            items[0]["content"]?["thumbnail"]?["resolutions"]?.arrayValue?.map { $0["tag"]?.stringValue ?? "" },
            ["original", "170x128"]
        )
        XCTAssertEqual(items[1]["content"]?["contentType"]?.stringValue, "VIDEO")
        XCTAssertEqual(NewsURLProtocol.requestCount, 1)
    }

    func testTickerNewsPaginationStopsOnStalledCursor() async throws {
        let session = makeSession()
        let client = YFinanceClient(session: session)

        NewsURLProtocol.handler = { request in
            guard let url = request.url else { throw NewsTestError.missingURL }
            NewsURLProtocol.incrementRequestCount()
            switch NewsURLProtocol.requestCount {
            case 1:
                return Self.response(
                    url: url,
                    body: """
                    {"data":{"lightyearList":{"main":{
                      "edges":[{"node":{"asset":{
                        "__typename":"Story",
                        "id":"a",
                        "title":"A",
                        "contentAttributes":{"publishTime":"2026-10-25T20:05:00Z"},
                        "finance":{"stockTickers":[]}
                      }}}],
                      "pageInfo":{"hasNextPage":true,"endCursor":"c1"}
                    }}}}
                    """
                )
            case 2:
                guard let body = request.httpBody,
                      let json = try JSONSerialization.jsonObject(with: body) as? [String: Any],
                      let variables = json["variables"] as? [String: Any],
                      variables["after"] as? String == "c1"
                else {
                    throw NewsTestError.invalidBody
                }
                return Self.response(
                    url: url,
                    body: """
                    {"data":{"lightyearList":{"main":{
                      "edges":[{"node":{"asset":{"__typename":"AssetError"}}}],
                      "pageInfo":{"hasNextPage":true,"endCursor":"c1"}
                    }}}}
                    """
                )
            default:
                throw NewsTestError.tooManyRequests
            }
        }

        let raw = try await client.tickerNews(symbol: "MSFT", count: 5)
        let items = raw["data"]?["tickerStream"]?["stream"]?.arrayValue ?? []
        XCTAssertEqual(items.map { $0["id"]?.stringValue ?? "" }, ["a"])
        XCTAssertEqual(NewsURLProtocol.requestCount, 2)
    }

    func testUnsupportedLocaleGraphQLErrorProducesEmptyNews() async throws {
        let session = makeSession()
        let client = YFinanceClient(session: session)

        NewsURLProtocol.handler = { request in
            guard let url = request.url else { throw NewsTestError.missingURL }
            NewsURLProtocol.incrementRequestCount()
            return Self.response(
                url: url,
                body: """
                {
                  "data":{"lightyearList":null},
                  "errors":[{"message":"Error","extensions":{"code":"NOT_FOUND"}}]
                }
                """
            )
        }

        let raw = try await client.tickerNews(
            symbol: "MSFT",
            count: 10,
            lang: "ja-JP",
            region: "JP"
        )
        XCTAssertEqual(raw["data"]?["tickerStream"]?["stream"]?.arrayValue?.count, 0)
        XCTAssertEqual(NewsURLProtocol.requestCount, 1)
    }

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [NewsURLProtocol.self]
        return URLSession(configuration: configuration)
    }

    private static func response(url: URL, body: String) -> (HTTPURLResponse, Data) {
        let response = HTTPURLResponse(
            url: url,
            statusCode: 200,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": "application/json"]
        )!
        return (response, Data(body.utf8))
    }
}

private enum NewsTestError: Error {
    case missingURL
    case unexpectedURL(String)
    case missingHeader
    case invalidBody
    case tooManyRequests
}

private final class NewsURLProtocol: URLProtocol {
    private static let handlerState = TestURLProtocolHandlerState()
    private static let counter = TestURLProtocolCounterState()

    static var handler: ((URLRequest) throws -> (HTTPURLResponse, Data))? {
        get { handlerState.snapshot() }
        set { handlerState.set(newValue) }
    }

    static var requestCount: Int { counter.snapshot() }

    static func incrementRequestCount() {
        counter.increment()
    }

    static func reset() {
        counter.reset()
        handler = nil
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: NSError(domain: "NewsURLProtocol", code: 1))
            return
        }
        do {
            let (response, data) = try handler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}
