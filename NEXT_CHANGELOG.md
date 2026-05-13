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

### Fixed
- Fixed `MetadataOperationTimeout` not being applied when metadata operations use SHOW commands. Operations like `getTables`, `getSchemas`, and `getColumns` now respect the `MetadataOperationTimeout` connection property instead of hanging indefinitely with no timeout.

- Reclassify transient/mis-categorized server errors so callers can identify
  retryable failures. The remap is applied at all Thrift error sites
  (`checkResponseForErrors`, `executeAsync`, `verifySuccessStatus`, and the
  polling status handler) so the same server failure surfaces with the same
  SQL state regardless of which response carries it.
  - Unity Catalog unavailability (`[UC_CLIENT_EXCEPTION]`, previously `XXUCC`)
    and parquet read / connection-acquisition deadlines
    (`[PARQUET_FAILED_READ_FOOTER]`, `DEADLINE_EXCEEDED: acquiring connection`)
    are now reported with SQL state `08S01` (communication link failure).
  - Server-side `java.util.ConcurrentModificationException` is now reported
    with SQL state `40001` (serialization failure) instead of the misleading
    `42000`. The remap only applies when the original SQL state is `42000` so
    unrelated `42xxx` states (e.g. `42501` insufficient privilege) are
    preserved.
  Notes for callers and operators:
  - Callers branching on the legacy `XXUCC`/`42000` states for these failures
    must update to `08S01`/`40001`. The driver logs the original→remapped
    state at `INFO` level for traceability.
  - The driver's failure telemetry uses `sqlState` as the error-name field,
    so dashboards/alerts keyed on `XXUCC` or `42000` for these specific
    failure modes will need to be updated to the new states.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
