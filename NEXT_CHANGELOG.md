# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Decode TIME columns and nested TIME values on Thrift and SEA with exact clock text and JDBC TIME metadata. Thrift TIME columns now report Types.TIME and clock values instead of strings containing raw nanosecond counts, so numeric getters no longer return those counts.
- SEA JSON-inline TIME columns now return `java.sql.Time` instead of `String` from `getObject` and report `Types.TIME` instead of `Types.OTHER`; use `getObject(column, LocalTime.class)` for exact fractions.
- Preserve fractional milliseconds in `getTime(column, Calendar)` in JVM time zones ahead of UTC, including when reading TIMESTAMP columns.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
