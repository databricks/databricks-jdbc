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
- Fixed `DatabricksParameterMetaData.countParameters` and `DatabricksStatement.trimCommentsAndWhitespaces` with a `SqlCommentParser` utility class.
- Fixed `rollback()` to throw `SQLException` when called in auto-commit mode (no active transaction), aligning with JDBC spec. Previously it silently sent a ROLLBACK command to the server.
- Fixed `fetchAutoCommitStateFromServer()` to accept both `"1"`/`"0"` and `"true"`/`"false"` responses from `SET AUTOCOMMIT` query, since different server implementations return different formats.
- Fixed socket leak in SDK HTTP client that prevented CRaC checkpointing. The SDK's connection pool was not shut down on `connection.close()`, leaving TCP sockets open.
- Fixed Date fields within complex types (ARRAY, STRUCT, MAP) being returned as epoch day integers instead of proper date values.
- Fixed `DatabaseMetaData.getColumns()` returning the column type name in `COLUMN_DEF` for columns with no default value. `COLUMN_DEF` now correctly returns `null` per the JDBC specification.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
