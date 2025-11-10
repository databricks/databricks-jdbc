# NEXT CHANGELOG

## [Unreleased]

### Added
- Added support for geospatial data types.
- Added support for telemetry log levels, which can be controlled via the connection parameter `TelemetryLogLevel`. This allows users to configure the verbosity of telemetry logging from OFF to TRACE.
- Added full support for JDBC transaction control methods in Databricks. Transaction support in Databricks is currently available as a Private Preview. The `IgnoreTransactions` connection parameter can be set to `1` to disable or no-op transaction control methods.

### Updated
- Updated sdk version from 0.65.0 to 0.67.3

### Fixed
- Fixed SQL syntax error when LIKE queries contain empty ESCAPE clauses.
- Fix: driver failing to authenticate on token update in U2M flow.
- Fix: driver failing to parse complex data types with nullable attributes.
- Fixed: Resolved SDK token-caching regression causing token refresh on every call. SDK is now configured once to avoid excessive token endpoint hits and rate limiting.
- Fixed: TimestampConverter.toString() returning ISO8601 format with timezone conversion instead of SQL standard format.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
