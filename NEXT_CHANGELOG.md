# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Bumped Apache HttpClient 5 (`httpclient5`) from 5.6.3 to 5.6.4.

### Fixed

- Decode Reyden's native Arrow `struct<srid,wkb>` results for GEOMETRY and GEOGRAPHY, including
  nested values in arrays, map values, and structs in both native-object and EWKT string modes.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
