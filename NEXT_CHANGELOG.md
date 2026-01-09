# NEXT CHANGELOG

## [Unreleased]

### Added
- Added token caching for all authentication providers to reduce token endpoint calls.

### Updated

### Fixed

- [PECOBLR-1131] Fix incorrect refetching of expired CloudFetch links when using Thrift protocol.
- Fixed logging to respect params when the driver is shaded.
- Fixed `isWildcard` to return true only when the value is `*`

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
