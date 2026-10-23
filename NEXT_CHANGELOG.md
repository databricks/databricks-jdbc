# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `DatabricksArray.getBaseTypeName()` and `getBaseType()` to describe the array's element type (e.g. `INT` / `Types.INTEGER` for `ARRAY<INT>`) per the `java.sql.Array` contract, instead of returning the full array type name and `Types.OTHER`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
