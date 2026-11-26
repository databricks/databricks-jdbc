# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.
- Retry transient HTTP 408/429/502/503/504 errors when opening Thrift sessions, using jittered backoff when Retry-After is missing or invalid. During outages, connection failures may be delayed by the retry window (900 seconds for availability errors, 120 seconds for rate limits by default, plus HTTP attempt time); disable with TemporarilyUnavailableRetry=0 or RateLimitRetry=0, or lower the corresponding *RetryTimeout.
- Thrift OpenSession 503/429 retries with Retry-After now count elapsed HTTP-attempt time, not only sleeps, against their separate retry budgets; opt-in custom codes share one API retry window, and fallback backoff starts fresh per category. Exhaustion now reports the HTTP error (SQLState 08000, request ID and server message) instead of INVALID_STATE/"Retry failure" for 503/429 and custom retry codes; gateway 408/502/504 errors retain their existing HTTP format, and outer connection SQLStates are unchanged.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
