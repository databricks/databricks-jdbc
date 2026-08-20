# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- `DatabaseMetaData.getColumns(...)` with a `null` catalog now issues a single `SHOW COLUMNS IN ALL CATALOGS` statement (consistent with `getSchemas`/`getTables`) instead of enumerating every catalog and issuing a per-catalog `SHOW COLUMNS`. Older DBR versions that do not support the syntax transparently fall back to the previous enumerate-and-fan-out behavior.

### Fixed
- Fixed connections failing when the same parameter is provided in both the JDBC URL and the connection properties, with the JDBC URL taking precedence.
- Fixed Arrow chunk download failures being emitted under the internal `DOWNLOAD_FAILED` lifecycle
  state instead of the canonical `CHUNK_DOWNLOAD_ERROR` telemetry code.

- Fixed `IdleConnectionEvictor` thread leak in long-running applications. Driver-side resources (HTTP client, background threads) are now always released when `Connection.close()` is called, even if statement cleanup or server-side session termination fails.

- Throw `DatabricksSQLException` instead of an unchecked `ClassCastException` when a complex-type getter (`getArray`, `getStruct`, `getMap`) is called on a column of a different complex type.

- Fixed `NullPointerException` when reading collated string columns (e.g. `STRING COLLATE UTF8_LCASE`) over Arrow. Such columns report a `type_name` that does not map to a `ColumnInfoTypeName`, leaving it null; the value read now recovers `STRING` from the type text and the result set metadata reports `VARCHAR` instead of `OTHER`, while `getColumnTypeName()` still preserves the collated type text.
- Fixed `ResultSet.getObject(int)` on the Arrow result path leaking a raw `java.lang.IndexOutOfBoundsException` (with a null SQLState) for an out-of-range column index. It now throws a `DatabricksSQLException` (SQLState `INVALID_STATE`, `"Column index out of bounds: <n>"`), matching the JDBC contract and the Thrift/inline result implementations. Affects the Arrow/CloudFetch path used by SEA and by Thrift CloudFetch results.
- Fixed connecting with an unsupported `AuthMech` (e.g. `AuthMech=99`) intermittently failing with an internal `IllegalStateException: Recursive update` or `StackOverflowError` on both the SEA and Thrift paths. The value is now validated at connect time and rejected deterministically with a `SQLException` (`SQLState=INPUT_VALIDATION_ERROR`).

- Improved SEA connection-failure error messages.
---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
