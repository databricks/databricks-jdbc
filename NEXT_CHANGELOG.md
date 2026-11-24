# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- MAP values returned by `getString()` and `getObject()` when complex datatype support is disabled (the default) are now valid JSON: quotes, backslashes and control characters in string keys and values come back as JSON escapes (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
