# NEXT CHANGELOG

## [Unreleased]

### Added

- Implemented `Array.getResultSet()` and its overloads, returning one row per element (1-based index, value).

### Updated

- Updated bundled Netty (`netty-common` and `netty-buffer`) from 4.2.15.Final to 4.2.18.Final.

- Upgraded the bundled Guava dependency to 33.7.2-jre.

- Removed automatic Thrift fallback and the 24-hour SEA circuit breaker after HTTP 429 session creation failures. Rate-limit errors now propagate to the caller without changing routing for subsequent connections.

- Removed the `UseQueryForMetadata` connection property. It no longer forces the Thrift client, so connections eligible for SEA now use SEA with SQL metadata queries. Thrift connections use native metadata RPCs.

- SQL Execution API sessions open with warehouse URLs. Real-time sessions use warehouse URLs for session-owned statement operations and session close.

### Fixed
- Fixed `DatabricksArray.getBaseTypeName()` and `getBaseType()` to describe the array's element type (e.g. `INT` / `Types.INTEGER` for `ARRAY<INT>`) per the `java.sql.Array` contract, instead of returning the full array type name and `Types.OTHER`. `ARRAY<MAP<...>>` reports `Types.OTHER`, since its elements are `DatabricksMap` objects.

- Reject statement creation on closed connections before stale connection state can disable server-controlled features for subsequent connections.
- Report `CONNECTION_CLOSED` with vendor code `1010` from `DatabricksConnection` closed-connection checks, including statement preparation and `setClientInfo`.
- Report only explicitly supplied client-info properties when bulk updates fail on a closed connection.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
