# NEXT CHANGELOG

## [Unreleased]

### Added
- Added support for providing custom HTTP options: `HttpMaxConnectionsPerRoute` and `HttpConnectionRequestTimeout`.

### Updated

### Fixed
- If sever doesn't send retryAfter header, then the request won't be retried. It wouldn't throw an exception.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
