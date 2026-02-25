# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `rollback()` to throw `SQLException` when called in auto-commit mode (no active transaction), aligning with JDBC spec. Previously it silently sent a ROLLBACK command to the server.
- Fixed `fetchAutoCommitStateFromServer()` to accept both `"1"`/`"0"` and `"true"`/`"false"` responses from `SET AUTOCOMMIT` query, since different server implementations return different formats.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
