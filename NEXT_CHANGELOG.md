# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `NullPointerException` when a Thrift result declared as `URL_BASED_SET` (CloudFetch) has no result links, as the server does for small inline results such as `SHOW SCHEMAS LIKE 'x'`. Inline Arrow batches are now read directly. Fixes #1729.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
