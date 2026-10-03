# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Bumped Apache HttpClient 5 (`httpclient5`) from 5.6.3 to 5.6.4.
- Reduced concurrent connection setup latency by initializing auth configurators outside the shared map lock.

### Fixed

- Fixed `UserAgentEntry` attribution in Query History for SQL Execution API connections.
- Prevented recursive auth initialization from hanging connection setup and preserved original initialization errors and causes.
- Logged auth configurator close failures without interrupting connection cleanup.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
