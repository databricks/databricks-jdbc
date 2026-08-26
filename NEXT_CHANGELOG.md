# NEXT CHANGELOG

## [Unreleased]

### Added
- Added `EnableThriftNativeMetadata` to request and consume supported Thrift-native SEA metadata results.
- Real-Time (Reyden) SQL warehouses now work over the default Thrift connection with no configuration change: when the gateway rejects a Thrift `OpenSession` with SQLSTATE `KP001`, the driver transparently re-opens the session on SEA and remembers the warehouse (per host, ~6h) so later connections skip Thrift. An explicit `UseThriftClient=1` is always honored and never auto-switched.
- Added session-version tracking from synchronous SQL Exec API responses for Lakehouse Real-Time.
  With asynchronous execution, subsequent statements may not observe session changes.

### Updated
- `UseBoundedSeaApi` and `EnableThriftNativeMetadata` now default to `1`; when unset, activation is controlled by the server-side `enableSqlExecForJdbc` rollout flag.
- `DatabaseMetaData.getColumns(...)` with a `null` catalog now issues a single `SHOW COLUMNS IN ALL CATALOGS` statement (consistent with `getSchemas`/`getTables`) instead of enumerating every catalog and issuing a per-catalog `SHOW COLUMNS`. Older DBR versions that do not support the syntax transparently fall back to the previous enumerate-and-fan-out behavior.
- Updated bundled Jackson, lz4-java, Netty, and Apache HttpComponents Client and Core dependencies to patched versions to address security findings.
- Updated bundled Apache Thrift to 0.24.0 to address CVE-2026-43871.

### Fixed
- Fixed `DatabaseMetaData.getTypeInfo()` returning the `INTERVAL` row out of `DATA_TYPE` order.

- Fixed later logging-enabled connections being unable to produce logs when an earlier connection
  used `LogLevel=OFF`. The first enabled connection now establishes the shared JUL handler, while a
  later `OFF` connection does not disable it.

- Invalid or incomplete Databricks JDBC URLs now fail with a descriptive `DatabricksSQLException`
  instead of leaking a `NullPointerException` when required connection parameters are missing.

- Fixed connections failing when the same parameter is provided in both the JDBC URL and the connection properties, with the JDBC URL taking precedence.
- Fixed long-running queries on the Thrift client path (`UseThriftClient=1`) intermittently failing with `Query has been timed out due to inactivity` under sustained concurrency. A transient transport-level failure (stale pooled connection, connection reset, load-balancer idle drop) on a `GetOperationStatus` poll previously abandoned the still-running server operation after a single blip, and the same failure class on `CloseOperation`/`CancelOperation` could leak completed operations until the server reaped them. These idempotent RPCs now transparently reconnect and retry on a fresh connection with bounded, jittered exponential backoff before surfacing the error. Only genuinely transient failures are retried — connection-level errors and transient gateway responses (408/500/502/504); permanent HTTP errors (401/403/404) surface immediately, and rate-limit/unavailable responses (429/503) are left to the existing HTTP-layer retry rather than retried again. The retry also honors the statement's `queryTimeout` so a failing poll cannot overshoot its deadline. Statement submission is deliberately excluded from the retry path to avoid double-execution. The SEA client path is unchanged.
- Fixed `IdleConnectionEvictor` thread leak in long-running applications. Driver-side resources (HTTP client, background threads) are now always released when `Connection.close()` is called, even if statement cleanup or server-side session termination fails.

- Throw `DatabricksSQLException` instead of an unchecked `ClassCastException` when a complex-type getter (`getArray`, `getStruct`, `getMap`) is called on a column of a different complex type.

- Fixed `NullPointerException` when reading collated string columns (e.g. `STRING COLLATE UTF8_LCASE`) over Arrow. Such columns report a `type_name` that does not map to a `ColumnInfoTypeName`, leaving it null; the value read now recovers `STRING` from the type text and the result set metadata reports `VARCHAR` instead of `OTHER`, while `getColumnTypeName()` still preserves the collated type text.
- Fixed `ResultSet.getObject(int)` on the Arrow result path leaking a raw `java.lang.IndexOutOfBoundsException` (with a null SQLState) for an out-of-range column index. It now throws a `DatabricksSQLException` (SQLState `INVALID_STATE`, `"Column index out of bounds: <n>"`), matching the JDBC contract and the Thrift/inline result implementations. Affects the Arrow/CloudFetch path used by SEA and by Thrift CloudFetch results.
- Fixed connecting with an unsupported `AuthMech` (e.g. `AuthMech=99`) intermittently failing with an internal `IllegalStateException: Recursive update` or `StackOverflowError` on both the SEA and Thrift paths. The value is now validated at connect time and rejected deterministically with a `SQLException` (`SQLState=INPUT_VALIDATION_ERROR`).

- Improved SEA connection-failure error messages.

- Fixed `NullPointerException` being thrown when materializing an array containing nested object types (other arrays, structs or maps) as `DatabricksArray` when some or all elements are literal `null`. 
---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
