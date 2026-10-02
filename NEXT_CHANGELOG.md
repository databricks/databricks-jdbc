# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Bumped Apache HttpClient 5 (`httpclient5`) from 5.6.3 to 5.6.4.

### Fixed

- Fixed `UserAgentEntry` attribution in Query History for SQL Execution API connections.
- Reduce cold connection latency by sharing SDK discovery and skipping unused setup for direct access tokens.
- Reduce concurrent connection tail latency by initializing configurators outside the shared map lock.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
