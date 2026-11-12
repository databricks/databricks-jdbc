# NEXT CHANGELOG

## [Unreleased]

### Added

- Added the connection property `EnableStrictGetObjectConversion`. When set to `1`, `ResultSet.getObject(column, Class)` throws a `SQLException` if the type is `null` or the value cannot be returned as an instance of the type (for example `UUID` on a `STRING` column, or `Calendar` on a `TIMESTAMP` column), as the JDBC specification requires. The default (`0`) is unchanged: such values are returned as a `String` (a `Timestamp` for `Calendar`). ([#1782](https://github.com/databricks/databricks-jdbc/issues/1782))

### Updated

### Fixed

- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: `TelemetrySocketTimeout` now also caps each final telemetry flush on close (authenticated and unauthenticated telemetry flush separately), including time queued behind other telemetry pushes and retries. After that many seconds the remaining telemetry batch is dropped (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))
- Fixed connection setup stalling under concurrency: per-connection auth setup and the first feature-flag fetch no longer block connections to other hosts; same-host connections wait for the first feature-flag initialization, including any required authentication.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
