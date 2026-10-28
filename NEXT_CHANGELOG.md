# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- Removed automatic Thrift fallback and the 24-hour SEA circuit breaker after HTTP 429 session creation failures. Rate-limit errors now propagate to the caller without changing routing for subsequent connections.

- Removed the `UseQueryForMetadata` connection property. It no longer forces the Thrift client, so connections eligible for SEA now use SEA with SQL metadata queries. Thrift connections use native metadata RPCs.

- SQL Execution API sessions open with warehouse URLs. Real-time sessions use warehouse URLs for session-owned statement operations and session close.

### Fixed

- Implemented `DatabricksArray.getResultSet()` (and its overloads) to return a
  `ResultSet` with one row per array element, where column 1 is the 1-based
  element index and column 2 is the element value, per the JDBC `Array` contract.
  Previously these methods threw `SQLFeatureNotSupportedException`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
