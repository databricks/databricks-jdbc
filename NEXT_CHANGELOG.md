# NEXT CHANGELOG

## [Unreleased]

### Added
- Added `CallableStatement` support with IN parameters. `Connection.prepareCall()` now returns a working `DatabricksCallableStatement` that supports positional parameter binding and execution via `{call proc(?)}` JDBC escape syntax. OUT/INOUT parameters and named parameters throw `SQLFeatureNotSupportedException`.
- Added AI coding agent detection to the User-Agent header. When the driver is invoked by a known AI coding agent (e.g. Claude Code, Cursor, Gemini CLI), `agent/<product>` is appended to the User-Agent string.

### Updated

### Fixed
- Fixed `IntervalConverter` crash (`IllegalArgumentException: Invalid interval metadata`) when INTERVAL columns are returned via CloudFetch. Arrow metadata from CloudFetch uses underscored format (`INTERVAL_YEAR_MONTH`, `INTERVAL_DAY_TIME`) which the driver's regex did not accept.
- Fixed primitive types within complex types (ARRAY, MAP, STRUCT) not being correctly parsed when Arrow serialization uses alternate formats: TIMESTAMP/TIMESTAMP_NTZ as epoch microseconds or component arrays, and BINARY as base64-encoded strings.
- Fixed `PARSE_SYNTAX_ERROR` for column names containing special characters (e.g., dots) when `EnableBatchedInserts` is enabled, by re-quoting column names with backticks in reconstructed multi-row INSERT statements.
- Fixed Volume ingestion for SEA mode, which was broken due to statement being closed prematurely.
- Fixed escaped pattern characters in catalogName for `getSchemas`, as returned catalogName should be unescaped.
- Fixed `getColumnClassName()` returning null for VARIANT columns in SEA mode by adding VARIANT to the type system.
- Fixed `getColumns()` returning `DATA_TYPE=0` (NULL) for GEOMETRY/GEOGRAPHY columns in Thrift mode. Now returns `Types.VARCHAR` (12) when geospatial is disabled and `Types.OTHER` (1111) when enabled, consistent with SEA mode.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
