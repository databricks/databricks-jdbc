# NEXT CHANGELOG

## [Unreleased]

### Added
- Added support for disabling CloudFetch via `EnableQueryResultDownload=0` to use inline Arrow results instead.

### Updated
- Geospatial column type names now include SRID information (e.g., `GEOMETRY(4326)` instead of `GEOMETRY`).
- Changed default value of `IgnoreTransactions` from `0` to `1` to disable multi-statement transactions by default. Preview participants can opt-in by setting `IgnoreTransactions=0`. Also updated `supportsTransactions()` to respect this flag.
- Implemented lazy loading for inline Arrow results, fetching arrow batches on demand instead of all at once. This improves memory usage and initial response time for large result sets when using the Thrift protocol with Arrow format.

### Fixed
- Fixed complex data type metadata support when retrieving 0 rows in Arrow format
- Normalized TIMESTAMP_NTZ to TIMESTAMP in Thrift path for consistency with SEA behavior
- Fixed complex types not being returned as objects in SEA Inline mode when `EnableComplexDatatypeSupport=true`.
- Fixed `StringIndexOutOfBoundsException` when parsing complex data types in Thrift CloudFetch mode. The issue occurred when metadata contained incomplete type information (e.g., "ARRAY" instead of "ARRAY<INT>"). Now retrieves complete type information from Arrow metadata.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
