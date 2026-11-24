# NEXT CHANGELOG

## [Unreleased]

### Added

- Support binding `OffsetDateTime` values as TIMESTAMP parameters, preserving their instant at microsecond precision.

### Updated

- Quote `OffsetDateTime` leaves in the text views of `DatabricksArray`, `DatabricksStruct`, and `DatabricksMap`.

### Fixed

- Preserve signed years when binding BC, year-zero, and extended-year `java.sql.Date` and `Timestamp` values with native parameters or SQL interpolation, excluding Calendar overloads.
- Bind region-based `ZonedDateTime` values using their offset without the unsupported region suffix.
- Escape quotes in non-String values interpolated as DATE or TIMESTAMP parameters.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
