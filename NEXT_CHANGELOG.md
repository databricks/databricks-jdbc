# NEXT CHANGELOG

## [Unreleased]

### Added
- Added streaming prefetch mode for Thrift inline results (columnar and Arrow) with background batch prefetching and configurable sliding window for improved throughput.
- Added `EnableInlineStreaming` connection parameter to enable/disable streaming mode (default: enabled).
- Added `ThriftMaxBatchesInMemory` connection parameter to control the sliding window size for streaming (default: 3).

### Updated
- Implemented lazy loading for inline Arrow results, fetching arrow batches on demand instead of all at once. This improves memory usage and initial response time for large result sets when using the Thrift protocol with Arrow format.

### Fixed
- Fixed complex data type metadata support when retrieving 0 rows in Arrow format
- Normalized TIMESTAMP_NTZ to TIMESTAMP in Thrift path for consistency with SEA behavior

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
