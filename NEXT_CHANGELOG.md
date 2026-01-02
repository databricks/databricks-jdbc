# NEXT CHANGELOG

## [Unreleased]

### Added
- Added token caching for all authentication providers to reduce token endpoint calls.

### Updated
- Geospatial column type names now include SRID information (e.g., `GEOMETRY(4326)` instead of `GEOMETRY`).

### Fixed
- Fixed complex types not being returned as objects in SEA Inline mode when `EnableComplexDatatypeSupport=true`.
- Fixed errors with complex data types in Thrift CloudFetch mode.

- [PECOBLR-1131] Fix incorrect refetching of expired CloudFetch links when using Thrift protocol.
- Fixed logging to respect params when the driver is shaded.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
