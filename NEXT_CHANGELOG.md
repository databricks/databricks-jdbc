# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `MAP` columns whose values are complex types (`ARRAY`/`STRUCT`/`MAP`) returning empty values (e.g. `{0:}` instead of `{0:[34277,0]}`) with `EnableArrow=1` when complex datatype support is disabled. The string formatter now reproduces nested values correctly; null nested values inside maps and arrays are also handled (#1505).
- Fixed `setCatalog()` and `setSchema()` producing invalid SQL (e.g. `SET CATALOG ``name``) when the catalog or schema name was passed already wrapped in backticks. Backticks are now stripped before wrapping, and `getCatalog()`/`getSchema()` return the bare identifier name.
- Fixed metadata SQL generation for catalog, schema, and table identifiers containing backticks.
- Fixed SEA result truncation when direct results are disabled. Large, highly-compressible results that span multiple chunks were delivered inline via the old hybrid path and truncated to the first chunk. The SQL Execution path now uses an async (`0s`) wait timeout when direct results are disabled, so results are returned via external links and fetched in full.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*