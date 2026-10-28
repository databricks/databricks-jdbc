# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- Removed automatic Thrift fallback and the 24-hour SEA circuit breaker after HTTP 429 session creation failures. Rate-limit errors now propagate to the caller without changing routing for subsequent connections.

- Removed the `UseQueryForMetadata` connection property. It no longer forces the Thrift client, so connections eligible for SEA now use SEA with SQL metadata queries. Thrift connections use native metadata RPCs.

- SQL Execution API sessions open with warehouse URLs. Real-time sessions use warehouse URLs for session-owned statement operations and session close.

### Fixed
- Fixed `NullPointerException` when a Thrift result declared as `URL_BASED_SET` (CloudFetch) has no result links, as the server does for small inline results such as `SHOW SCHEMAS LIKE 'x'`. Inline Arrow batches are now read directly. Fixes #1729.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
