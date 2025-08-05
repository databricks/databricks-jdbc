# NEXT CHANGELOG

## [Unreleased]

### Added
- Added support for providing custom HTTP options: `HttpMaxConnectionsPerRoute` and `HttpConnectionRequestTimeout`.

### Updated

### Fixed
- Fixed retry behaviour to not throw an exception when there is no retry-after header for 503 and 429 status codes.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
