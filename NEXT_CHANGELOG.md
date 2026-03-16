# NEXT CHANGELOG

## [Unreleased]

### Added
- Added connection property `OAuthWebServerTimeout` to configure the OAuth browser authentication timeout for U2M (user-to-machine) flows, and also updated hardcoded 1-hour timeout to default 120 seconds timeout.
- Added connection property `UseQueryForMetadata` to use SQL SHOW commands instead of Thrift RPCs for metadata operations (getCatalogs, getSchemas, getTables, getColumns, getFunctions). This fixes incorrect wildcard matching where `_` was treated as a single-character wildcard in Thrift metadata pattern filters.
- Added connection property `TreatMetadataCatalogNameAsPattern` to control whether catalog names are treated as patterns in Thrift metadata RPCs. When disabled (default), unescaped `_` in catalog names is escaped to prevent single-character wildcard matching. This aligns with JDBC spec which treats catalogName as identifier and not pattern.

### Updated
- Bumped `com.fasterxml.jackson.core:jackson-core` from 2.18.3 to 2.18.6.
- Fat jar now routes SDK and Apache HTTP client logs through Java Util Logging (JUL), removing the need for external logging libraries.
- PECOBLR-1121 Arrow patch to circumvent Arrow issues with JDK 16+.

### Fixed
- Fixed statement timeout when the server returns `TIMEDOUT_STATE` directly in the `ExecuteStatement` response (e.g. query queued under load), the driver now throws `SQLTimeoutException` instead of `DatabricksHttpException`.
- Fixed Thrift polling infinite loop when server restarts invalidate operation handles, and added configurable timeout (`MetadataOperationTimeout`, default 300s) with sleep between polls for metadata operations.
- Fixed `DatabricksParameterMetaData.countParameters` and `DatabricksStatement.trimCommentsAndWhitespaces` with a `SqlCommentParser` utility class.
- Fixed `rollback()` to throw `SQLException` when called in auto-commit mode (no active transaction), aligning with JDBC spec. Previously it silently sent a ROLLBACK command to the server.
- Fixed `fetchAutoCommitStateFromServer()` to accept both `"1"`/`"0"` and `"true"`/`"false"` responses from `SET AUTOCOMMIT` query, since different server implementations return different formats.
- Fixed socket leak in SDK HTTP client that prevented CRaC checkpointing. The SDK's connection pool was not shut down on `connection.close()`, leaving TCP sockets open.
- Fixed `IdleConnectionEvictor` thread leak in long-running services. The feature-flags context shared per host was ref-counted incorrectly and held a stale connection UUID after the owning connection closed; on the next 15-minute refresh it silently recreated an HTTP client (and its evictor thread) that was never cleaned up. Connection UUIDs are now tracked idempotently and the stored connection context is updated when the owning connection closes.
- Fixed Date fields within complex types (ARRAY, STRUCT, MAP) being returned as epoch day integers instead of proper date values.
- Fixed `DatabaseMetaData.getColumns()` returning the column type name in `COLUMN_DEF` for columns with no default value. `COLUMN_DEF` now correctly returns `null` per the JDBC specification.
- Coalesce concurrent expired cloud fetch link refreshes into a single batch FetchResults RPC to prevent thread pool exhaustion under high concurrency.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
