# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- Implemented lazy loading for inline Arrow results, fetching arrow batches on demand instead of all at once. This improves memory usage and initial response time for large result sets when using the Thrift protocol with Arrow format.

### Fixed
- Fixed complex data type metadata support when retrieving 0 rows in Arrow format

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
