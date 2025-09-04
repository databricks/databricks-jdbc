# NEXT CHANGELOG

## [Unreleased]

### Added

- **Query Tags support**: Added ability to attach key-value tags to SQL queries for analytical purposes that would appear in `system.query.history` table. Example: `jdbc:databricks://host;QUERY_TAGS=team:marketing,dashboard:abc123`. 
- **SQL Scripting support**: Added support for [SQL Scripting](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting)
- Added a client property `enableVolumeOperations` to enable  GET/PUT/REMOVE volume operations on a stream. For backward compatibility, allowedVolumeIngestionPaths can also be used for REMOVE operation.
- Support for fetching schemas across all catalogs (when catalog is specified as null or a wildcard) in `DatabaseMetaData#getSchemas` API in SQL Execution mode.
- **Configurable SQL validation in isValid()**: Added `EnableSQLValidationForIsValid` connection property to control whether `isValid()` method executes an actual SQL query for server-side validation. Default value is 0.

### Updated
- Databricks SDK dependency upgraded to latest version 0.60.0

### Fixed
- Fixed `ResultSet.getString` for Boolean columns in Metadata result set.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
