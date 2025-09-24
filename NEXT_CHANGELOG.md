# NEXT CHANGELOG

## [Unreleased]

### Added

- **Query Tags support**: Added ability to attach key-value tags to SQL queries for analytical purposes that would appear in `system.query.history` table. Example: `jdbc:databricks://host;QUERY_TAGS=team:marketing,dashboard:abc123`. 
- **SQL Scripting support**: Added support for [SQL Scripting](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting)
- Added a client property `enableVolumeOperations` to enable  GET/PUT/REMOVE volume operations on a stream. For backward compatibility, allowedVolumeIngestionPaths can also be used for REMOVE operation.
- Support for fetching schemas across all catalogs (when catalog is specified as null or a wildcard) in `DatabaseMetaData#getSchemas` API in SQL Execution mode.
- **Configurable SQL validation in isValid()**: Added `EnableSQLValidationForIsValid` connection property to control whether `isValid()` method executes an actual SQL query for server-side validation. Default value is 0.
- Implement multi-row INSERT batching optimization for prepared statements to improve performance when executing large batches of INSERT operations.
- Implement lazy/incremental fetching for columnar results when using Databricks JDBC in Thrift mode without Arrow support. The change modifies the behavior from buffering entire result sets in memory to maintaining only a limited number of rows at a time, reducing peak heap memory usage and preventing OutOfMemory errors.
- Added new artifact `databricks-jdbc-thin` for thin jar with runtime dependency metadata.
- Introduce a memory-efficient columnar data access mechanism for JDBC result processing.

### Updated
- Databricks SDK dependency upgraded to latest version 0.60.0

### Fixed
- Integrated Azure U2M flow into driver for improved stability.
- Fixed `ResultSet.getString` for Boolean columns in Metadata result set.
- Fixed volume operations not completing unless the ResultSet is fully iterated.
- Fixed `connection.getMetadata().getColumns()` to return the correct SQL data type code for complex type columns.
- Fixed a bug in the JDBC driver's metadata parsing for nested decimal fields within struct types.
- Fixed case sensitive table search in `connection.getMetadata().getTables()`
- Fixed `connection.getMetadata().getColumns()` to return the correct scale.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
