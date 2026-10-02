# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Bumped Apache HttpClient 5 (`httpclient5`) from 5.6.3 to 5.6.4.
- Reduced concurrent connection setup latency by initializing auth configurators outside the shared map lock.
- Skipped host and OIDC discovery during initial PAT and OAuth access-token connection setup, while preserving SDK environment settings and token fallback.

### Fixed

- Fixed `UserAgentEntry` attribution in Query History for SQL Execution API connections.
- Rejected missing access tokens with an input-validation error after checking SDK credential fallback.
- Prevented recursive auth initialization from hanging connection setup and preserved original initialization errors and causes.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
