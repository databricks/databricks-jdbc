# NEXT CHANGELOG

## [Unreleased]

### Added
- Added support for geospatial data types.
- Added support for telemetry log levels, which can be controlled via the connection parameter `TelemetryLogLevel`. This allows users to configure the verbosity of telemetry logging from OFF to TRACE.
- Added full support for JDBC transaction control methods in Databricks. Transaction support in Databricks is currently available as a Private Preview. The `IgnoreTransactions` connection parameter can be set to `1` to disable or no-op transaction control methods.
- Added support for high-performance batched writes with parameter interpolation:
  - `supportManyParameters=1`: Enables parameter interpolation to bypass 256-parameter limit (default: 0)
  - `EnableBatchedInserts=1`: Enables multi-row INSERT batching (default: 0)
  - `BatchInsertSize=<SIZE>`: Maximum rows per batch (default: 1000)
  - Note: Large batches are chunked for execution. If a chunk fails, previous chunks remain committed (no transaction rollback). Consider using staging tables for critical workflows.
- Added a new config attribute `DisableOauthRefreshToken` to control whether refresh tokens are requested in OAuth exchanges. By default, the driver does not include the `offline_access` scope. If `offline_access` is explicitly provided by the user, it is preserved and not removed.
- Added Feature-flag integration for SQL Exec API rollout

### Updated
- Minimized OAuth requests by reducing calls in feature flags and telemetry.

### Fixed
- Fixed SQL syntax error when LIKE queries contain empty ESCAPE clauses.
- Fix: driver failing to authenticate on token update in U2M flow.
- Fix: driver failing to parse complex data types with nullable attributes.
- Fixed: Resolved SDK token-caching regression causing token refresh on every call. SDK is now configured once to avoid excessive token endpoint hits and rate limiting.
- Fixed: TimestampConverter.toString() returning ISO8601 format with timezone conversion instead of SQL standard format.
- Fixed: Driver not loading complete JSON result in the case of SEA Inline without Arrow
- Fix: SQLInterpolator failing to escape temporal fields and special characters.
- Fixed: Errors in table creation when using BIGINT, SMALLINT, TINYINT, or VOID types.
- Fixed: PreparedStatement.getMetaData() now correctly reports TINYINT columns as Types.TINYINT (java.lang.Byte) instead of Types.SMALLINT (java.lang.Integer).
- Fixed: TINYINT to String conversion to return numeric representation (e.g., "65") instead of character representation (e.g., "A").
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
