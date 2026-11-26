# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.
- Render DECIMAL values as plain, scale-preserving text instead of scientific notation (#1613).

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
