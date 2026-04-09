# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `IntervalConverter` crash (`IllegalArgumentException: Invalid interval metadata`) when INTERVAL columns are returned via CloudFetch. Arrow metadata from CloudFetch uses underscored format (`INTERVAL_YEAR_MONTH`, `INTERVAL_DAY_TIME`) which the driver's regex did not accept.
- Fixed primitive types within complex types (ARRAY, MAP, STRUCT) not being correctly parsed when Arrow serialization uses alternate formats: TIMESTAMP/TIMESTAMP_NTZ as epoch microseconds or component arrays, and BINARY as base64-encoded strings.
- Fixed `PARSE_SYNTAX_ERROR` for column names containing special characters (e.g., dots) when `EnableBatchedInserts` is enabled, by re-quoting column names with backticks in reconstructed multi-row INSERT statements.
- Fixed Volume ingestion for SEA mode, which was broken due to statement being closed prematurely.
- Fixed escaped pattern characters in catalogName for `getSchemas`, as returned catalogName should be unescaped.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
