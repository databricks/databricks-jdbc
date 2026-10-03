# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Bumped Apache HttpClient 5 (`httpclient5`) from 5.6.3 to 5.6.4.

### Fixed

- Fixed `UserAgentEntry` attribution in Query History for SQL Execution API connections.
- Fixed request latency and lock contention that grew with every connection opened in a long-running JVM. The driver now registers each User-Agent entry once instead of on every connection.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
