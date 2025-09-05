# NEXT CHANGELOG

## [Unreleased]

### Added

- **Query Tags support**: Added ability to attach key-value tags to SQL queries for analytical purposes that would appear in `system.query.history` table. Example: `jdbc:databricks://host;QUERY_TAGS=team:marketing,dashboard:abc123`. 
- **SQL Scripting support**: Added support for [SQL Scripting](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting)
- Added a client property `enableVolumeOperations` to enable  GET/PUT/REMOVE volume operations on a stream. For backward compatibility, allowedVolumeIngestionPaths can also be used for REMOVE operation.
- Implement lazy/incremental fetching for columnar results when using Databricks JDBC in Thrift mode without Arrow support. The change modifies the behavior from buffering entire result sets in memory to maintaining only a limited number of rows at a time, reducing peak heap memory usage and preventing OutOfMemory errors.

### Updated
- Databricks SDK dependency upgraded to latest version 0.60.0

### Fixed
-
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
