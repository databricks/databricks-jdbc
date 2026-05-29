# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `setCatalog()` and `setSchema()` producing invalid SQL (e.g. `SET CATALOG ``name``) when the catalog or schema name was passed already wrapped in backticks. Backticks are now stripped before wrapping, and `getCatalog()`/`getSchema()` return the bare identifier name.
- Fixed metadata SQL generation for catalog, schema, and table identifiers containing backticks.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*