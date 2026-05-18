# NEXT CHANGELOG

## [Unreleased]

### BREAKING CHANGES in 3.4.1

1. **`getTables()`: Percent sign (`%`) in catalog argument is now treated as a literal character, not a wildcard.** Previously returned all tables; now returns zero rows unless a catalog named "%" exists. JDBC spec: catalog is an exact-match parameter, not a pattern. Migration: Pass `null` to search all catalogs.

2. **`getColumnTypeName()`: DECIMAL columns now return `"DECIMAL"` without precision/scale** (e.g., `"DECIMAL"` not `"DECIMAL(10,2)"`). Use `getPrecision()` and `getScale()` for numeric constraints. JDBC spec: `getColumnTypeName()` returns the base type name only.

3. **For DBSQL warehouses, metadata operations are now powered by SHOW SQL commands.** SQL Exec API mode already was powered by SHOW commands, now the same is true for Thrift server mode as well. To revert to native Thrift metadata RPCs, set `UseQueryForMetadata` to `0`.

4. **Native geospatial type support (`GEOMETRY` and `GEOGRAPHY`) is now enabled by default.** `getObject()` now returns `IGeometry`/`IGeography` instances instead of EWKT strings. Set `EnableGeoSpatialSupport=0` to restore the previous behavior.

### Added

### Updated
- `EnableGeoSpatialSupport` no longer requires `EnableComplexDatatypeSupport=1`. Geospatial types (GEOMETRY, GEOGRAPHY) can now be enabled independently of complex type support (ARRAY, MAP, STRUCT).
- Arrow schema deserialization failures (Thrift metadata path) now surface a dedicated driver error code `ARROW_SCHEMA_PARSING_ERROR` (vendor code `22000`) and a proper SQLSTATE `22000` (Data Exception) on the thrown `SQLException`, instead of the generic `RESULT_SET_ERROR` (1004) and the enum name as SQLSTATE. The exception message is unchanged.
- When a Statement is re-executed, the previous server-side operation is now explicitly closed before starting the new execution, preventing orphaned server-side operations when Statements are reused.

### Fixed
- Fixed `DatabaseMetaData.getTables()` in Thrift mode returning rows when called with an empty `types` array. Per JDBC spec, empty types means "no types selected" and now correctly returns zero rows (matching SEA mode).
- Fixed `?` characters inside SQL comments, string literals, and quoted identifiers being incorrectly counted as parameter placeholders when `supportManyParameters=1`. `SQLInterpolator` now uses `SqlCommentParser` to locate only real placeholders. Fixes #1331.
- Fixed `MetadataOperationTimeout` not being applied when metadata operations use SHOW commands. Operations like `getTables`, `getSchemas`, and `getColumns` now respect the `MetadataOperationTimeout` connection property instead of hanging indefinitely with no timeout.
- Reclassify transient server errors to standard SQL states (08S01, 40001) across all Thrift error sites. This ensures UC unavailability and concurrent modification errors surface consistently for better retry handling. Note: Dashboards and branching logic keyed on legacy XXUCC or 42000 must be updated.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
