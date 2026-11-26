# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Preserve the exact value and declared scale of DECIMAL values nested in ARRAY, STRUCT and MAP results, returning BigDecimal objects and JSON numbers instead of rounded strings.
- Nested DECIMAL JSON numbers and DECIMAL MAP keys now use plain fixed-point spelling at the declared scale; string-key lookups must use that exact spelling.

- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
