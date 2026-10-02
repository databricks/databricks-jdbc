# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Bumped Apache HttpClient 5 (`httpclient5`) from 5.6.3 to 5.6.4.
- Skipped host and OIDC discovery during initial PAT and OAuth access-token connection setup, while preserving SDK environment settings and token fallback.

### Fixed

- Fixed `UserAgentEntry` attribution in Query History for SQL Execution API connections.
- Rejected missing access tokens with an input-validation error after checking SDK credential fallback.
- Preserved original auth initialization errors and causes.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
