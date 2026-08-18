#!/usr/bin/env python3
"""Extract raw URLSession I/O from the giant compatibility client.

This is intentionally a local exact-source migration instead of a remote
whole-file rewrite. It leaves endpoint construction, Yahoo session policy,
retries and repair logic in `YFinanceClient`, but moves the actual URLSession
send/HTTP response normalization into `YFURLSessionTransport`.
"""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "Sources" / "YFinanceKit" / "YFinanceClient.swift"


class MigrationError(RuntimeError):
    pass


def replace_once(text: str, old: str, new: str, identity: str) -> tuple[str, bool]:
    if identity in text:
        return text, False
    if old not in text:
        raise MigrationError(f"Expected YFinanceClient pattern not found for {identity}")
    return text.replace(old, new, 1), True


def main() -> int:
    text = CLIENT.read_text(encoding="utf-8")
    changed = False

    old_property = """    private let session: URLSession\n    private let decoder: JSONDecoder\n"""
    new_property = """    private let session: URLSession\n    private let transport: any YFHTTPTransporting\n    private let decoder: JSONDecoder\n"""
    text, did = replace_once(
        text,
        old_property,
        new_property,
        "private let transport: any YFHTTPTransporting",
    )
    changed |= did

    old_init = """        self.session = session\n        self.userAgent = userAgent\n"""
    new_init = """        self.session = session\n        self.transport = YFURLSessionTransport(session: session)\n        self.userAgent = userAgent\n"""
    text, did = replace_once(
        text,
        old_init,
        new_init,
        "self.transport = YFURLSessionTransport(session: session)",
    )
    changed |= did

    old_io = """                let (data, response) = try await session.data(for: request)\n                guard let httpResponse = response as? HTTPURLResponse else {\n                    throw YFinanceError.missingData(\"Expected HTTPURLResponse\")\n                }\n                if debugEnabled {\n                    print(\"[YFinanceKit] \\(method) \\(redactedURLString(url)) -> \\(httpResponse.statusCode) (\\(data.count) bytes)\")\n                }\n                try validateResponse(data: data, response: httpResponse)\n                return data\n"""
    new_io = """                let response = try await transport.send(request)\n                if debugEnabled {\n                    print(\"[YFinanceKit] \\(method) \\(redactedURLString(url)) -> \\(response.statusCode) (\\(response.data.count) bytes)\")\n                }\n                try validateResponse(\n                    data: response.data,\n                    statusCode: response.statusCode,\n                    retryAfterHeader: response.header(\"Retry-After\")\n                )\n                return response.data\n"""

    if old_io in text:
        count = text.count(old_io)
        if count != 2:
            raise MigrationError(f"Expected exactly two raw URLSession request blocks, found {count}")
        text = text.replace(old_io, new_io)
        changed = True
    else:
        expected_count = text.count("let response = try await transport.send(request)")
        if expected_count != 2:
            raise MigrationError(
                "Could not find either the two legacy URLSession blocks or the two extracted transport blocks"
            )

    old_validate = """    private func validateResponse(data: Data, response: HTTPURLResponse) throws {\n        // Keep rate limits distinct so callers do not mistake a Yahoo edge\n        // throttle for a malformed finance payload.\n        if response.statusCode == 429 {\n            throw YFinanceError.httpStatus(429)\n        }\n\n        if let envelope = try? decoder.decode(YFFinanceErrorEnvelope.self, from: data),\n           let yahooError = envelope.finance?.error {\n            throw YFinanceError.serverError(\n                code: yahooError.code ?? \"unknown\",\n                description: yahooError.description ?? \"Unknown Yahoo error\"\n            )\n        }\n\n        guard (200...299).contains(response.statusCode) else {\n            throw YFinanceError.httpStatus(response.statusCode)\n        }\n    }\n"""
    new_validate = """    private func validateResponse(\n        data: Data,\n        statusCode: Int,\n        retryAfterHeader: String?\n    ) throws {\n        // Keep provider backpressure distinct from auth/session failures.\n        if statusCode == 429 {\n            let retryAfter = YFRetryAfterParser.parse(retryAfterHeader)\n            throw YFinanceError.rateLimited(retryAfter: retryAfter)\n        }\n\n        if let envelope = try? decoder.decode(YFFinanceErrorEnvelope.self, from: data),\n           let yahooError = envelope.finance?.error {\n            throw YFinanceError.serverError(\n                code: yahooError.code ?? \"unknown\",\n                description: yahooError.description ?? \"Unknown Yahoo error\"\n            )\n        }\n\n        guard (200...299).contains(statusCode) else {\n            throw YFinanceError.httpStatus(statusCode)\n        }\n    }\n"""
    text, did = replace_once(
        text,
        old_validate,
        new_validate,
        "retryAfterHeader: String?",
    )
    changed |= did

    required = (
        "private let transport: any YFHTTPTransporting",
        "self.transport = YFURLSessionTransport(session: session)",
        "let response = try await transport.send(request)",
        "retryAfterHeader: response.header(\"Retry-After\")",
        "throw YFinanceError.rateLimited(retryAfter: retryAfter)",
    )
    for fragment in required:
        if fragment not in text:
            raise MigrationError(f"Transport extraction missing expected fragment: {fragment}")

    if "session.data(for: request)" in text:
        raise MigrationError("YFinanceClient still contains direct URLSession request I/O")

    if changed:
        CLIENT.write_text(text, encoding="utf-8")
        print("Extracted YFinanceClient raw request I/O into YFURLSessionTransport.")
    else:
        print("YFinanceClient transport extraction already applied.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MigrationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
