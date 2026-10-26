# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- Removed automatic Thrift fallback and the 24-hour SEA circuit breaker after HTTP 429 session creation failures. Rate-limit errors now propagate to the caller without changing routing for subsequent connections.

- Removed the `UseQueryForMetadata` connection property. SEA connections use SQL metadata queries, while Thrift connections use native metadata RPCs.
### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
