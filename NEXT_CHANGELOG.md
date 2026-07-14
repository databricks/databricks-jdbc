# NEXT CHANGELOG

## [Unreleased]

### Added
- Added `UseBoundedSeaApi` connection property (default `0`/off). When enabled, the driver uses the bounded SEA API contract for CloudFetch: sends `row_offset` on GetResultData requests and uses `next_chunk_index` for chunk discovery instead of `total_chunk_count`. Requires server support.
- OAuth M2M (client credentials) connections can now supply the client secret via the JDBC `password`/`PWD` property and the client id via the JDBC `user`/`UID` property, instead of embedding `OAuth2Secret`/`OAuth2ClientId` in the connection URL. This lets BI tools (e.g. DBeaver) mask the OAuth secret in their password field rather than exposing it in the clear-text JDBC URL. Explicit `OAuth2ClientId`/`OAuth2Secret` still take precedence when present, so existing URLs are unaffected.

### Updated
- Bumped the Databricks SDK for Java dependency from `0.106.0` to `0.118.0`.

### Fixed
- Fixed telemetry misattribution when multiple connections (e.g. Thrift and SEA) are used on the same thread. Per-statement telemetry events could be tagged with another connection's context (e.g. transport mode); each connection's telemetry now uses its own context instead of a shared thread-local value.
- Hardened the OAuth U2M token cache at rest (encryption key derivation and file permissions).
- Fixed `DatabaseMetaData.getURL()` exposing credentials embedded in the connection URL; secret parameters are now masked (the URL is otherwise unchanged).
- Fixed presigned URL credentials not being fully redacted in logs.
- Fixed access token exposure in DEBUG logs.
- Fixed `StackOverflowError` / hang when closing a `ResultSet` or `Statement` with `closeOnCompletion()` enabled.
- Fixed SQL injection vulnerability in binary parameter handling.
- Fixed `setCatalog()` and `setSchema()` producing invalid SQL (e.g. `SET CATALOG ``name``) when the catalog or schema name was passed already wrapped in backticks. Backticks are now stripped before wrapping, and `getCatalog()`/`getSchema()` return the bare identifier name.
- Fixed metadata SQL generation for catalog, schema, and table identifiers containing backticks.
- Fixed SEA result truncation when direct results are disabled. Large, highly-compressible results that span multiple chunks were delivered inline via the old hybrid path and truncated to the first chunk. The SQL Execution path now uses an async (`0s`) wait timeout when direct results are disabled, so results are returned via external links and fetched in full.
- Fixed `getColumns()` flooding the `DriverManager` log writer with caught-and-recovered `Invalid column index` stack traces.
- Fixed timezone-shifted TIMESTAMP values when retrieving nested complex types (STRUCT/ARRAY/MAP) with `EnableComplexDatatypeSupport=1`.
- Fixed `MAP` columns whose values are themselves complex types (e.g. `MAP<INT,ARRAY<BIGINT>>`) rendering their values as empty (e.g. `SELECT MAP(0, ARRAY(34277,0))` returned `{0:}` instead of `{0:[34277,0]}`) when fetched via Arrow (`EnableArrow=1`) with `EnableComplexDatatypeSupport` disabled.
- Fixed `DatabricksDatabaseMetaData.supportsBatchUpdates()` always returning `false`, which caused batch-aware JDBC clients (e.g. Apache Hop) to skip `executeBatch()` and fall back to one INSERT per row. It now returns `true` when `EnableBatchedInserts=1`, so those clients use the optimized multi-row INSERT path.
- Fixed `Connection.setReadOnly(true)` throwing `DatabricksSQLFeatureNotSupportedException`, which broke clients (e.g. Trino/Starburst GenericJDBC, HikariCP, DBCP) that call it during connection initialization. Per the JDBC spec, `setReadOnly` is a hint the driver may ignore; it is now a no-op and `isReadOnly()` continues to return `false`.
- Fixed `ResultSetMetaData.getColumnTypeName()` returning `TIMESTAMP` for `TIMESTAMP_NTZ` columns (e.g. `SELECT MIN(ntz_col) ...`), a regression from 3.0.7. By default the driver now preserves the `TIMESTAMP_NTZ` type name across the SEA, Thrift, and describe-query metadata paths; `getColumnType()` continues to report `java.sql.Types.TIMESTAMP`. Set the new connection property `EnableTimestampNtzTypeName=0` to restore the previous behavior (report `TIMESTAMP`), which matches the legacy (v2.x.x) driver. ([#1495](https://github.com/databricks/databricks-jdbc/issues/1495))
- Fixed `SQLException.getErrorCode()` being inconsistent between transports for statement-execution failures: the Thrift path returned `0` while the SEA path returned `1003` for the same server error. Thrift statement errors now report the `EXECUTE_STATEMENT_FAILED` (`1003`) vendor code, matching SEA. The `SQLState` and error message are unchanged.
- Fixed `ResultSet.getObject(int)` on the Arrow result path leaking a raw `java.lang.IndexOutOfBoundsException` (with a null SQLState) for an out-of-range column index. It now throws a `DatabricksSQLException` (SQLState `INVALID_STATE`, `"Column index out of bounds: <n>"`), matching the JDBC contract and the Thrift/inline result implementations. Affects the Arrow/CloudFetch path used by SEA and by Thrift CloudFetch results.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*