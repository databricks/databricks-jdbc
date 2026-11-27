# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- `getBigDecimal(String, int)` now applies its scale argument like `getBigDecimal(int, int)`: values are rounded HALF_UP or zero-padded to the requested scale. Previously it returned the column's declared scale whatever scale was passed.
- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
