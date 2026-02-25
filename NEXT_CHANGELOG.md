# NEXT CHANGELOG

## [Unreleased]

### Added
- Added connection property `OAuthWebServerTimeout` to configure the OAuth browser authentication timeout for U2M (user-to-machine) flows, and also updated hardcoded 1-hour timeout to default 120 seconds timeout.

### Updated

### Fixed
- Fixed `rollback()` to throw `SQLException` when called in auto-commit mode (no active transaction), aligning with JDBC spec. Previously it silently sent a ROLLBACK command to the server.
- Fixed `fetchAutoCommitStateFromServer()` to accept both `"1"`/`"0"` and `"true"`/`"false"` responses from `SET AUTOCOMMIT` query, since different server implementations return different formats.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
