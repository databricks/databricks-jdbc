# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- **Compatibility:** Metadata search patterns now treat a raw backslash as the JDBC escape character on Thrift as well as SEA. Escape a literal backslash as `\\`: for example, the pattern `a\\b` matches the name `a\b` (`"a\\\\b"` in Java). Patterns without backslashes or single quotes are unchanged.

### Fixed

- Match literal backslashes and single quotes consistently in metadata schema, table, column and function search patterns on Thrift and SEA (#1598).

- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
