# NEXT CHANGELOG

## [Unreleased]

### Added
- Added `EnableThriftNativeMetadata` to request and consume supported Thrift-native SEA metadata results.

### Updated
- `UseBoundedSeaApi` and `EnableThriftNativeMetadata` now default to `1`; when unset, activation is controlled by the server-side `enableSqlExecForJdbc` rollout flag.
- `DatabaseMetaData.getColumns(...)` with a `null` catalog now issues a single `SHOW COLUMNS IN ALL CATALOGS` statement (consistent with `getSchemas`/`getTables`) instead of enumerating every catalog and issuing a per-catalog `SHOW COLUMNS`. Older DBR versions that do not support the syntax transparently fall back to the previous enumerate-and-fan-out behavior.
- Updated bundled Jackson, lz4-java, Netty, and Apache HttpComponents Client and Core dependencies to patched versions to address security findings.

### Fixed
- Invalid or incomplete Databricks JDBC URLs now fail with a descriptive `DatabricksSQLException`
  instead of leaking a `NullPointerException` when required connection parameters are missing.

- Fixed connections failing when the same parameter is provided in both the JDBC URL and the connection properties, with the JDBC URL taking precedence.
- Fixed `IdleConnectionEvictor` thread leak in long-running applications. Driver-side resources (HTTP client, background threads) are now always released when `Connection.close()` is called, even if statement cleanup or server-side session termination fails.

- Throw `DatabricksSQLException` instead of an unchecked `ClassCastException` when a complex-type getter (`getArray`, `getStruct`, `getMap`) is called on a column of a different complex type.

- Fixed `NullPointerException` when reading collated string columns (e.g. `STRING COLLATE UTF8_LCASE`) over Arrow. Such columns report a `type_name` that does not map to a `ColumnInfoTypeName`, leaving it null; the value read now recovers `STRING` from the type text and the result set metadata reports `VARCHAR` instead of `OTHER`, while `getColumnTypeName()` still preserves the collated type text.
- Fixed `ResultSet.getObject(int)` on the Arrow result path leaking a raw `java.lang.IndexOutOfBoundsException` (with a null SQLState) for an out-of-range column index. It now throws a `DatabricksSQLException` (SQLState `INVALID_STATE`, `"Column index out of bounds: <n>"`), matching the JDBC contract and the Thrift/inline result implementations. Affects the Arrow/CloudFetch path used by SEA and by Thrift CloudFetch results.
- Fixed connecting with an unsupported `AuthMech` (e.g. `AuthMech=99`) intermittently failing with an internal `IllegalStateException: Recursive update` or `StackOverflowError` on both the SEA and Thrift paths. The value is now validated at connect time and rejected deterministically with a `SQLException` (`SQLState=INPUT_VALIDATION_ERROR`).

- Improved SEA connection-failure error messages.

- Fixed string parameters silently losing apostrophes when `supportManyParameters=1` interpolates them into SQL.
  `SQLInterpolator` escaped `'` as the SQL-standard `''`, which Databricks does not implement: `'O''Brien'` is read
  as the adjacent literals `'O'` and `'Brien'` and concatenated, so `O'Brien` was stored as `OBrien`. Quotes are now
  escaped as `\'`, consistent with the backslash escaping already applied to `\`, `\n`, `\r` and `\t`.
  Data written through this path before the upgrade still holds the stripped values, so lookups that now send the
  apostrophe (`WHERE name = ?` bound to `O'Brien`) no longer match those rows. Only values written with
  `supportManyParameters=1` are affected; correcting them requires a data fix.

- Fixed `NullPointerException` being thrown when materializing an array containing nested object types (other arrays, structs or maps) as `DatabricksArray` when some or all elements are literal `null`. 
---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
