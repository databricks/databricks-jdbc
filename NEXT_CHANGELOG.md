# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed primitive types within complex types (ARRAY, MAP, STRUCT) not being correctly parsed when Arrow serialization uses alternate formats: TIMESTAMP/TIMESTAMP_NTZ as epoch microseconds or component arrays, and BINARY as base64-encoded strings.
- Fixed `PARSE_SYNTAX_ERROR` for column names containing special characters (e.g., dots) when `EnableBatchedInserts` is enabled, by re-quoting column names with backticks in reconstructed multi-row INSERT statements.
- Fixed Volume ingestion for SEA mode, which was broken due to statement being closed prematurely.
- Fixed escaped pattern characters in catalogName for `getSchemas`, as returned catalogName should be unescaped.
- Fixed `getColumnClassName()` returning null for VARIANT columns in SEA mode by adding VARIANT to the type system.
- Fixed `getColumns()` returning `DATA_TYPE=0` (NULL) for GEOMETRY/GEOGRAPHY columns in Thrift mode. Now returns `Types.VARCHAR` (12) when geospatial is disabled and `Types.OTHER` (1111) when enabled, consistent with SEA mode.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
