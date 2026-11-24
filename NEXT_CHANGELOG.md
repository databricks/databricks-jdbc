# NEXT CHANGELOG

## [Unreleased]

### Added

- Support binding `OffsetDateTime` values as TIMESTAMP parameters, preserving their instant at microsecond precision.

### Updated

- Quote `OffsetDateTime` leaves in the text views of `DatabricksArray`, `DatabricksStruct`, and `DatabricksMap`.

### Fixed

- Preserve signed years in `java.sql.Date` and `Timestamp` parameter text for every native target type and DATE/TIMESTAMP interpolation, excluding `setTimestamp(int, Timestamp, Calendar)`.
- Bind region-based `ZonedDateTime` values as TIMESTAMP using their offset without the unsupported region suffix; other target types retain their original text.
- Include zero seconds in whole-minute `OffsetDateTime` and `ZonedDateTime` TIMESTAMP binds so midnight and truncated values round-trip.
- Escape quotes in non-String values interpolated as DATE or TIMESTAMP parameters.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
