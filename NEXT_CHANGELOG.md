# NEXT CHANGELOG

## [Unreleased]

### Added
- Added Feature-flag integration for SQL Exec API rollout

### Updated
- Minimized OAuth requests by reducing calls in feature flags and telemetry.

### Fixed
- Fixed: Errors in table creation when using BIGINT, SMALLINT, TINYINT, or VOID types.
- Fixed: PreparedStatement.getMetaData() now correctly reports TINYINT columns as Types.TINYINT (java.lang.Byte) instead of Types.SMALLINT (java.lang.Integer).
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
