# NEXT CHANGELOG

## [Unreleased]

### Added
- Added connection property `OAuthWebServerTimeout` to configure the OAuth browser authentication timeout for U2M (user-to-machine) flows, and also updated hardcoded 1-hour timeout to default 120 seconds timeout.
- Added connection property `UseQueryForMetadata` to use SQL SHOW commands instead of Thrift RPCs for metadata operations (getCatalogs, getSchemas, getTables, getColumns, getFunctions). This fixes incorrect wildcard matching where `_` was treated as a single-character wildcard in Thrift metadata pattern filters.

### Updated
- Bumped `com.fasterxml.jackson.core:jackson-core` from 2.18.3 to 2.18.6.
- Fat jar now routes SDK and Apache HTTP client logs through Java Util Logging (JUL), removing the need for external logging libraries.
- PECOBLR-1121 Arrow patch to circumvent Arrow issues with JDK 16+.

### Fixed
- Fixed `rollback()` to throw `SQLException` when called in auto-commit mode (no active transaction), aligning with JDBC spec. Previously it silently sent a ROLLBACK command to the server.
- Fixed `fetchAutoCommitStateFromServer()` to accept both `"1"`/`"0"` and `"true"`/`"false"` responses from `SET AUTOCOMMIT` query, since different server implementations return different formats.
- Fixed socket leak in SDK HTTP client that prevented CRaC checkpointing. The SDK's connection pool was not shut down on `connection.close()`, leaving TCP sockets open.
- Fixed Date fields within complex types (ARRAY, STRUCT, MAP) being returned as epoch day integers instead of proper date values.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
