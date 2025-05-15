# NEXT CHANGELOG

## [Unreleased]

### Added
- Support for fetching tables and views across all catalogs using SHOW TABLES FROM/IN ALL CATALOGS in the SQL Exec API.
- Support for Token Exchange in OAuth flows where in third party tokens are exchanged for InHouse tokens.
- Added support for polling of statementStatus and sqlState for async SQL execution.

### Updated
- 

### Fixed
- Fix: unsupported data types in `setObject(int,Object,int targetSqlType)` method in PreparedStatement
- Fix: Added explicit null check for Arrow value vector when the value is empty, and Arrow null checking is disabled.

---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 