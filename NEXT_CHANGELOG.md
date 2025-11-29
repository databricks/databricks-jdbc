# NEXT CHANGELOG

## [Unreleased]

### Added
- Added the EnableTokenFederation url param to enable or disable Token federation feature. By default it is set to 1

### Updated
- Added validation for positive integer configuration properties (RowsFetchedPerBlock, BatchInsertSize, etc.) to prevent hangs and errors when set to zero or negative values.
- Updated Circuit breaker to be triggered by 429 errors too.

### Fixed

- Fix driver crash when using `INTERVAL` types.

---
*Note: When making changes, please add your change under the appropriate section with a brief description.*
