# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- Log timestamps now explicitly display timezone.
- **[Breaking Change]** `PreparedStatement.setTimestamp(int, Timestamp, Calendar)` now properly applies Calendar timezone conversion using LocalDateTime pattern (inline with `getTimestamp`). Previously Calendar parameter was ineffective.
- `DatabaseMetaData.getColumns()` with null catalog parameter now retrieves columns from all catalogs when using SQL Execution API, aligning the behaviour with thrift.
- `DatabaseMetaData.getFunctions()` with null catalog parameter now retrieves columns from the current catalog when using SQL Execution API, aligning the behaviour with thrift.

### Fixed
- Fix timeout exception handling to throw `SQLTimeoutException` instead of `DatabricksSQLException` when queries timeout.
- Removes dangerous global timezone modification that caused race conditions.
- Fixed `Statement.getLargeUpdateCount()` to return -1 instead of throwing Exception when there were no more results or result is not an update count.
- CVE-2025-66566. Updated lz4-java dependency to 1.10.1.
- Fix `INVALID_IDENTIFIER` error when using catalog/schema/table names for SQL Exec API with hyphens or special characters in metadata operations (`getSchemas()`, `getTables()`, `getColumns()`, etc.) and connection methods (`setCatalog()`, `setSchema()`). Per Databricks identifier rules, special characters are now properly enclosed in backticks.
- Fix Auth_Scope handling inconsistency in Azure U2M OAuth.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.*
