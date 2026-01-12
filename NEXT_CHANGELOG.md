# NEXT CHANGELOG

## [Unreleased]

### Added
- Added token caching for all authentication providers to reduce token endpoint calls.

### Updated
- Geospatial column type names now include SRID information (e.g., `GEOMETRY(4326)` instead of `GEOMETRY`).
- Changed default value of `IgnoreTransactions` from `0` to `1` to disable multi-statement transactions by default. Preview participants can opt-in by setting `IgnoreTransactions=0`. Also updated `supportsTransactions()` to respect this flag.

### Fixed
- Fixed complex types not being returned as objects in SEA Inline mode when `EnableComplexDatatypeSupport=true`.
- Fixed errors with complex data types in Thrift CloudFetch mode.

- [PECOBLR-1131] Fix incorrect refetching of expired CloudFetch links when using Thrift protocol.
- Fixed logging to respect params when the driver is shaded.
- Fixed `isWildcard` to return true only when the value is `*`

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
