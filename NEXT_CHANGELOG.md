# NEXT CHANGELOG

## [Unreleased]

### BREAKING CHANGES in 3.4.1 — Metadata JDBC Spec Compliance

This release unifies metadata behavior across Thrift and SQL Exec API backends
using SQL SHOW commands for all metadata operations on SQL warehouses. Several
non-spec-compliant behaviors have been corrected. Review the changes below before
upgrading. These changes do not affect metadata on All-Purpose Clusters.

* **`getTables`/`getColumns`/`getSchemas`: Catalog parameter is now treated as
  an exact-match identifier per JDBC spec.** Passing `%` or wildcard patterns as
  catalog previously returned results across all catalogs.
  Use `null` to search all catalogs.

* **`getTables` with empty types array: Now returns zero rows per JDBC spec.**
  Use `null` to return all types.

* **`getSchemas`: Now includes `information_schema` in results.** Excludes
  `global_temp` schema (previously returned by Thrift for all catalogs).

* **`getPrimaryKeys`/`getImportedKeys`/`getCrossReference` with non-existent
  catalog, schema, or table: Now returns empty `ResultSet` instead of throwing
  `SQLException`.**

* **`getImportedKeys` `UPDATE_RULE`/`DELETE_RULE`: Now returns `3` (`NO_ACTION`)
  instead of `0` (`CASCADE`) for Thrift, and `3` instead of `null` for SEA.**
  This reflects that Unity Catalog foreign keys are informational and non-enforced.

* **Native geospatial type support (`GEOMETRY` and `GEOGRAPHY`) is now enabled
  by default.** `getObject()` now returns `IGeometry`/`IGeography` instances
  instead of EWKT strings. Set `EnableGeoSpatialSupport=0` to restore the
  previous behavior.

### Added
- Metadata operations now use SQL SHOW commands for both Thrift and SEA backends,
  ensuring consistent behavior for SQL warehouses regardless of underlying
  protocol. To revert to native Thrift metadata RPCs, set `UseQueryForMetadata=0`.

### Updated
- `getColumnTypeName()` for DECIMAL columns now preserves precision/scale suffix (e.g., `"DECIMAL(10,2)"`) consistently across both Thrift and SEA backends.
- `EnableGeoSpatialSupport` no longer requires `EnableComplexDatatypeSupport=1`. Geospatial types (GEOMETRY, GEOGRAPHY) can now be enabled independently of complex type support (ARRAY, MAP, STRUCT).
- Arrow schema deserialization failures (Thrift metadata path) now surface a dedicated driver error code `ARROW_SCHEMA_PARSING_ERROR` (vendor code `22000`) and a proper SQLSTATE `22000` (Data Exception) on the thrown `SQLException`, instead of the generic `RESULT_SET_ERROR` (1004) and the enum name as SQLSTATE. The exception message is unchanged.
- When a Statement is re-executed, the previous server-side operation is now explicitly closed before starting the new execution, preventing orphaned server-side operations when Statements are reused.

### Fixed
- Fixed `DatabaseMetaData.getTables()` in Thrift mode returning rows when called with an empty `types` array. Per JDBC spec, empty types means "no types selected" and now correctly returns zero rows (matching SEA mode).
- Fixed `?` characters inside SQL comments, string literals, and quoted identifiers being incorrectly counted as parameter placeholders when `supportManyParameters=1`. `SQLInterpolator` now uses `SqlCommentParser` to locate only real placeholders. Fixes #1331.
- Fixed `MetadataOperationTimeout` not being applied when metadata operations use SHOW commands. Operations like `getTables`, `getSchemas`, and `getColumns` now respect the `MetadataOperationTimeout` connection property instead of hanging indefinitely with no timeout.
- Reclassify transient server errors to standard SQL states (08S01, 40001) across all Thrift error sites. This ensures UC unavailability and concurrent modification errors surface consistently for better retry handling. Note: Dashboards and branching logic keyed on legacy XXUCC or 42000 must be updated.
- Fixed `setCatalog()` and `setSchema()` producing invalid SQL (e.g. `SET CATALOG ``name``) when the catalog or schema name was passed already wrapped in backticks. Backticks are now stripped before wrapping, and `getCatalog()`/`getSchema()` return the bare identifier name.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*