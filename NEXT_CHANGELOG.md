# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- `absolute()` on forward-only result sets now throws `SQLFeatureNotSupportedException` instead of skipping forward (or returning `false` for empty results); call `next()` instead. The `relative()` and `previous()` messages now say "<method> is not supported on a forward-only ResultSet"; the empty result returned by `Statement.getGeneratedKeys()` now rejects cursor moves and reports `TYPE_FORWARD_ONLY`, `CONCUR_READ_ONLY`, `FETCH_FORWARD`, and `CLOSE_CURSORS_AT_COMMIT`.

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
