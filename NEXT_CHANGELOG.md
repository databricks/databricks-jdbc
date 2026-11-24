# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Forward-only result sets now reject `absolute()`, `relative()`, and `previous()` with consistent unsupported-operation messages; applications relying on `absolute()` to skip forward should call `next()` instead. Empty result sets now report `TYPE_FORWARD_ONLY` and reject these operations rather than returning `false`.

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
