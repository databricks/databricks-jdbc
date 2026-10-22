# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Implemented `DatabricksArray.getResultSet()` (and its overloads) to return a
  `ResultSet` with one row per array element, where column 1 is the 1-based
  element index and column 2 is the element value, per the JDBC `Array` contract.
  Previously these methods threw `SQLFeatureNotSupportedException`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
