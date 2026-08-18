# Security notes

YFinanceKit is primarily a client-side market-data library. It still handles security-sensitive Yahoo session state and exposes low-level networking helpers, so consumers should treat the following as hard invariants.

## Yahoo session secrets

Sensitive values include:

- Yahoo crumbs
- Yahoo cookies
- authenticated Yahoo `T` / `Y` login cookies
- any future authorization/session tokens

Do not:

- log them
- include them in diagnostics
- persist a crumb separately from the cookie session that produced it
- write login/session state into a shared App Group container
- include them in crash reports or analytics

The library's request diagnostics should record endpoint category, duration, attempts, cache behavior, and failure category only.

## Logging and URL redaction

Debug logging must redact crumb query values. New session/auth query parameters should be reviewed before they are ever included in logs.

Do not add request/response body logging around Yahoo auth, consent, crumb, or subscription calls without explicit redaction tests.

## Rate limiting

A Yahoo 429 is backpressure, not a normal retryable failure.

- do not aggressively retry it
- use the shared request coordinator/cooldown
- honor `Retry-After` when the transport eventually exposes it
- avoid unbounded multi-symbol task fan-out

## Cookie/crumb integrity

Yahoo crumbs are session-bound.

- never restore a cached crumb into a new unrelated URLSession cookie jar
- invalidating a Yahoo session should invalidate its crumb
- failed crumb responses must be status-checked and plausibility-checked
- HTML/login/error/rate-limit bodies must never be accepted as crumbs

## Authenticated Yahoo access

`YFAuth` accepts Yahoo login cookies supplied by the caller.

Consumers are responsible for obtaining those cookies legitimately and protecting them like credentials.

Do not send authenticated cookies to any host other than the intended Yahoo domains.

## Raw URL APIs

Low-level helpers such as raw URL/text fetching are intended for trusted application/library code.

If YFinanceKit is ever used in a server/service that accepts user-controlled URLs, do not pass those untrusted URLs directly into raw network helpers. That would create an SSRF-style trust boundary outside the intended iOS/client use case.

## Cached market data

Market-data caches may contain portfolio/watchlist symbols or user-selected instruments even when they contain no authentication material.

- keep default caches inside the app/container sandbox
- use app-group storage only when intentionally sharing with another app extension
- store only the minimum data needed for stale fallback
- never colocate Yahoo session secrets with shareable market snapshots

## Malformed Yahoo responses

Yahoo is an untrusted remote data source from a parser-safety perspective.

Parsing code must tolerate:

- null/missing objects
- missing array elements
- type shifts
- malformed JSON
- HTML/error bodies returned with HTTP 200
- non-finite or unsafe numeric values
- duplicate/non-monotonic timestamps
- structurally invalid OHLC data

Prefer structured errors or conservative empty/nil results over crashes.

## Dependency policy

The core Swift package currently has no external Swift package dependencies. Keep the dependency surface small and deliberate.

Do not add a dependency solely to mirror a Python yfinance implementation detail when a small native Swift implementation is sufficient.

## CI and automation

Automatic GitHub Actions and Dependabot are intentionally disabled for this repository.

Security-sensitive changes still require local build/test review. Use:

```sh
bash tools/verify.sh
bash tools/strict-concurrency.sh
```

Do not bypass local verification by silently turning hosted automation back on.
