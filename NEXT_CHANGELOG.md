# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- DECIMAL `getString()`, `getCharacterStream()`, `getAsciiStream()`/`getUnicodeStream()`, `getObject(..., String.class)` and String type-map conversions now return plain text (`0.0000001`/`0.0000000` instead of `1E-7`/`0E-7`), as does `getObject`'s String fallback for unsupported target classes (#1613). This also affects DECIMAL VALUE columns from `Array.getResultSet()`; exact nested scale, including Arrow's double-derived scale, remains tracked in #1854.
- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
