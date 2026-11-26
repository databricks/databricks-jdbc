# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.
- SEA connections now retry CreateSession HTTP 503 until `TemporarilyUnavailableRetryTimeout` (default 900 s) and HTTP 429 until `RateLimitRetryTimeout` (default 120 s) of elapsed time from the first response of each status, including responses without `Retry-After`, instead of stopping after four SDK attempts. During an outage, `getConnection` can block that long; `loginTimeout` does not bound this wait. Disable these retries with `TemporarilyUnavailableRetry=0` / `RateLimitRetry=0`, or choose smaller timeouts. Malformed timeouts for enabled retries fail validation before CreateSession is sent. Exhausted CreateSession retries now produce `DatabricksSQLException` (`CONNECTION_ERROR`) for 503 or `DatabricksRateLimitException` (`RATE_LIMIT_EXCEEDED`) for 429 in the connection failure's cause chain instead of the raw SDK `DatabricksException`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
