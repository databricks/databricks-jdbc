# NEXT CHANGELOG

## [Unreleased]

### Added

- `ResultSet.getObject(column, Class)` now supports the JDBC 4.2 `java.time` types: `LocalDate` and `LocalDateTime` for DATE, TIMESTAMP and TIMESTAMP_NTZ columns, and `Instant` and `OffsetDateTime` (UTC offset) for TIMESTAMP columns. With Arrow results (the default), these values are read straight from the server's epoch values, so BC and pre-1582 dates and timestamps are returned exactly. Previously, `Instant`, `LocalDateTime` and `OffsetDateTime` requests returned a `String`, and `LocalDate` went through `java.sql.Date` (for example, `DATE '1582-10-05'` came back as 1582-10-15). Requests that have no meaningful answer, such as `Instant` from a TIMESTAMP_NTZ or DATE column, or a `java.time` class on a non-temporal column, now throw an `SQLException` with `UNSUPPORTED_OPERATION`. `getDate`, `getTimestamp`, `getString` and `getObject(column)` are unchanged. ([#1779](https://github.com/databricks/databricks-jdbc/issues/1779))
- DATE, TIMESTAMP and TIMESTAMP_NTZ values nested in ARRAY and STRUCT can now be read exactly as `java.time` types through a type map, e.g. `array.getArray(Map.of("DATE", LocalDate.class))` or `struct.getAttributes(Map.of("TIMESTAMP", Instant.class))`, or through `array.getResultSet().getObject(2, LocalDate.class)`. `getArray()` and `getAttributes()` still return `java.sql` values. ([#1779](https://github.com/databricks/databricks-jdbc/issues/1779))

### Updated

### Fixed

- Fixed `getLong`, `getInt` and `getShort` (and `getObject(column, Long/Integer/Short/BigInteger)`) on DATE and TIMESTAMP columns returning shifted values for BC and pre-1582 dates; for example, `DATE '1582-10-05'` returned epoch day -141427 instead of -141437. Modern values are unchanged. ([#1779](https://github.com/databricks/databricks-jdbc/issues/1779))
- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: `TelemetrySocketTimeout` now also caps each final telemetry flush on close (authenticated and unauthenticated telemetry flush separately), including time queued behind other telemetry pushes and retries. After that many seconds the remaining telemetry batch is dropped (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))
- Fixed connection setup stalling under concurrency: per-connection auth setup and the first feature-flag fetch no longer block connections to other hosts; same-host connections wait for the first feature-flag initialization, including any required authentication.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
