# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.
- `Statement.setQueryTimeout` and `MetadataOperationTimeout` now bound held status polls to the timeout plus a five-second in-flight grace (previously up to `socketTimeout`), without whole-second rounding. Cancellation is awaited for up to two seconds before reporting the timeout; slower cleanup continues in the background within a 30-second budget, with inline fallback if worker submission fails.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
