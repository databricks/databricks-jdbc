# NEXT CHANGELOG

## [Unreleased]

### Added

- Implemented `Array.getResultSet()` and its overloads, returning one row per element (1-based index, value).

### Updated
- Removed automatic Thrift fallback and the 24-hour SEA circuit breaker after HTTP 429 session creation failures. Rate-limit errors now propagate to the caller without changing routing for subsequent connections.

- Removed the `UseQueryForMetadata` connection property. It no longer forces the Thrift client, so connections eligible for SEA now use SEA with SQL metadata queries. Thrift connections use native metadata RPCs.

- SQL Execution API sessions open with warehouse URLs. Real-time sessions use warehouse URLs for session-owned statement operations and session close.

- **Behavior change:** `DatabricksArray.getArray()` now returns elements whose Java type matches `getBaseType()`. Elements of `ARRAY<TINYINT>` are returned as `Byte` and elements of parameterized `ARRAY<DECIMAL(p,s)>` as `BigDecimal`; previously both were returned as `String`, so code that casts these elements to `String` will throw `ClassCastException` and must be updated. Nested complex types are also now matched case-insensitively in `ARRAY` elements, `STRUCT` fields and `MAP` values. Lowercase complex type metadata (e.g. `array<array<int>>`, `array<struct<...>>`, `struct<a:array<int>>`, `map<string,struct<...>>`) now produces nested `DatabricksArray` / `DatabricksStruct` / `DatabricksMap` objects instead of strings. If the value does not match the declared complex type, a conversion exception is now thrown instead of falling back to `toString()`. Code that casts these nested values to `String` must be updated. The `TINYINT` / `DECIMAL(p,s)` element-type change applies only to `ARRAY` elements: `TINYINT` and parameterized `DECIMAL(p,s)` fields of a `STRUCT` and values of a `MAP` are still returned as `String` (for example, an element of `ARRAY<DECIMAL(10,2)>` is a `BigDecimal`, but field `a` of `STRUCT<a:DECIMAL(10,2)>` is a `String`).

### Fixed
- Fixed `DatabricksArray.getBaseTypeName()` and `getBaseType()` to describe the array's element type (e.g. `INT` / `Types.INTEGER` for `ARRAY<INT>`) per the `java.sql.Array` contract, instead of returning the full array type name and `Types.OTHER`. `getBaseType()` reports the JDBC type of the objects actually stored in the array, so it intentionally differs from the column-level `ResultSetMetaData.getColumnType()` mapping for `MAP`: `ARRAY<MAP<...>>` reports `Types.JAVA_OBJECT` (elements are `DatabricksMap` objects), while a top-level `MAP` column reports `Types.VARCHAR`; likewise `ARRAY<FLOAT>` / `ARRAY<REAL>` report `Types.REAL` (elements are `java.lang.Float`). See the related `getArray()` element-type behavior change under **Updated**.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
