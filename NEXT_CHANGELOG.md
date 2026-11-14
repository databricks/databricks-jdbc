# NEXT CHANGELOG

## [Unreleased]

### Added

- `ResultSet.getObject(column, Class)` now supports the JDBC 4.2 `java.time` types: `LocalDate` and `LocalDateTime` for DATE, TIMESTAMP and TIMESTAMP_NTZ columns, and `Instant` and `OffsetDateTime` (UTC offset) for TIMESTAMP columns. With Arrow results (the default), these values are read straight from the server's epoch values, so BC and pre-1582 dates and timestamps are returned exactly. Previously, `Instant`, `LocalDateTime` and `OffsetDateTime` requests returned a `String`, and `LocalDate` went through `java.sql.Date` (for example, `DATE '1582-10-05'` came back as 1582-10-15). String columns holding ISO date or timestamp text are parsed too, as JDBC 4.2 allows. Requests that have no meaningful answer, such as `Instant` from a TIMESTAMP_NTZ or DATE column, or a `java.time` class on a numeric column, now throw an `SQLException` with `UNSUPPORTED_OPERATION`. `getDate`, `getTimestamp`, `getString` and `getObject(column)` are unchanged. ([#1779](https://github.com/databricks/databricks-jdbc/issues/1779))
- DATE, TIMESTAMP and TIMESTAMP_NTZ values nested in ARRAY and STRUCT can now be read exactly as `java.time` types through a type map, e.g. `array.getArray(Map.of("DATE", LocalDate.class))`, `array.getResultSet(Map.of("DATE", LocalDate.class))` or `struct.getAttributes(Map.of("TIMESTAMP", Instant.class))`, or through `array.getResultSet().getObject(2, LocalDate.class)`. As for top-level columns, nested TIMESTAMP values are session-local for `LocalDate` and `LocalDateTime`. `getArray()` and `getAttributes()` still return `java.sql` values. ([#1779](https://github.com/databricks/databricks-jdbc/issues/1779))
- Added the connection property `EnableStrictGetObjectConversion`. When set to `1`, `ResultSet.getObject(column, Class)` on query results throws a `SQLException` if the type is `null` or the value cannot be returned as an instance of the type (for example `UUID` on a `STRING` column, or `Calendar` on a `TIMESTAMP` column), as the JDBC specification requires. The default (`0`) is unchanged: such values are returned as a `String` (a `Timestamp` for `Calendar`). `DatabaseMetaData` result sets always use the default. ([#1782](https://github.com/databricks/databricks-jdbc/issues/1782))

### Updated

- Reduced driver CPU when reading Arrow results: `ResultSet.next()` and the `getXxx`/`getObject` getters no longer re-read connection settings, re-parse the column type name, or record telemetry on every row or value. Reading narrow primitive columns (for example a single `BOOLEAN` or `DOUBLE` column) now takes several times less CPU per row.

### Fixed

- Fixed numeric getters returning shifted values for BC and pre-1582 dates: `getLong`, `getInt` and `getShort` (and `getObject(column, Long/Integer/Short/BigInteger)`) on DATE columns, and `getLong` (and `getObject(column, Long/BigInteger)`) on TIMESTAMP columns. For example, `DATE '1582-10-05'` returned epoch day -141427 instead of -141437. Values after the 1582 Gregorian cutover are unchanged. ([#1779](https://github.com/databricks/databricks-jdbc/issues/1779))
- Fixed reading ARRAY, STRUCT and MAP values that contain a TIMESTAMP_NTZ with zero seconds (for example `2023-10-05 15:20:00`), which failed with `Invalid TIMESTAMP_NTZ array representation`. ([#1779](https://github.com/databricks/databricks-jdbc/issues/1779))
- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Reduced lock contention when many threads read compressed inline results: the LZ4 decompressor and checksum are no longer looked up through JVM-wide synchronized factories for every result.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: `TelemetrySocketTimeout` now also caps each final telemetry flush on close (authenticated and unauthenticated telemetry flush separately), including time queued behind other telemetry pushes and retries. After that many seconds the remaining telemetry batch is dropped (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))
- Fixed connection setup stalling under concurrency: per-connection auth setup and the first feature-flag fetch no longer block connections to other hosts; same-host connections wait for the first feature-flag initialization, including any required authentication.
- Reduced connection setup time by caching successful host-metadata and OIDC discovery JSON documents for up to an hour, with a JVM-wide limit of 1,024 entries. Responses redirected to another origin or path, or whose bodies cannot be parsed as discovery documents, are excluded.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
