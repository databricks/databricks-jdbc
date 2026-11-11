# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Affected-row APIs now classify CTE-prefixed and `FROM`-led multi-insert DML as updates. By default, `execute()` returns `false`, and `executeQuery()` throws `RESULT_SET_ERROR` after successful execution. Set `NonRowcountQueryPrefixes=WITH,FROM` to retain the previous counter ResultSet behavior for both forms. SQL classification and client-side placeholder detection now recognize backslash-escaped quotes, raw string literals, and backslash-continued line comments.

### Fixed

- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: `TelemetrySocketTimeout` now also caps each final telemetry flush on close (authenticated and unauthenticated telemetry flush separately), including time queued behind other telemetry pushes and retries. After that many seconds the remaining telemetry batch is dropped (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))
- Fixed connection setup stalling under concurrency: per-connection auth setup and the first feature-flag fetch no longer block connections to other hosts; same-host connections wait for the first feature-flag initialization, including any required authentication.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
