# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed primitive types within complex types (ARRAY, MAP, STRUCT) not being correctly parsed when Arrow serialization uses alternate formats: TIMESTAMP/TIMESTAMP_NTZ as epoch microseconds or component arrays, and BINARY as base64-encoded strings.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
