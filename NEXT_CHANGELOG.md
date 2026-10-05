# NEXT CHANGELOG

## [Unreleased]

### Added

- Added typed getters for all six SAFE feature flag types using the existing cache.

### Updated

- Bumped Apache HttpClient 5 (`httpclient5`) from 5.6.3 to 5.6.4.

### Fixed

- Fixed `UserAgentEntry` attribution in Query History for SQL Execution API connections.
- Honor `Auth_Scope` for OAuth client-secret M2M; unify scope parsing with JWT-assertion M2M and U2M. (#1706)

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
