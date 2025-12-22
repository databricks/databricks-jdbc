# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- Geospatial column type names now include SRID information (e.g., `GEOMETRY(4326)` instead of `GEOMETRY`).

### Fixed
- Fixed complex types not being returned as objects in SEA Inline mode when `EnableComplexDatatypeSupport=true`.
- Fixed errors with complex data types in Thrift CloudFetch mode.

---
*Note: When making changes, please add your change under the appropriate section with a brief description.*
