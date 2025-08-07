# NEXT CHANGELOG

## [Unreleased]

### Added
- Added support for providing custom HTTP options: `HttpMaxConnectionsPerRoute` and `HttpConnectionRequestTimeout`.
- Add V2 of chunk download using async http client with corresponding implementations of AbstractRemoteChunkProvider and 
AbstractArrowResultChunk

### Updated

### Fixed
- Fixed Statement.getUpdateCount to return -1 for non-DML queries.
- Fixed Statement.setMaxRows(0) to be interepeted as no limit.
- Fixed retry behaviour to not throw an exception when there is no retry-after header for 503 and 429 status codes.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
