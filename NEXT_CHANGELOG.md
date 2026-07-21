# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `IdleConnectionEvictor` thread leak in long-running applications. Driver-side resources (HTTP client, background threads) are now always released when `Connection.close()` is called, even if statement cleanup or server-side session termination fails.

- Throw `DatabricksSQLException` instead of an unchecked `ClassCastException` when a complex-type getter (`getArray`, `getStruct`, `getMap`) is called on a column of a different complex type.

- Fixed `NullPointerException` when reading collated string columns (e.g. `STRING COLLATE UTF8_LCASE`) over Arrow. Such columns report a `type_name` that does not map to a `ColumnInfoTypeName`, leaving it null; the value read now recovers `STRING` from the type text and the result set metadata reports `VARCHAR` instead of `OTHER`, while `getColumnTypeName()` still preserves the collated type text.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
