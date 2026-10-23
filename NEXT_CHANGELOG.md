# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `DatabricksArray.getBaseTypeName()` and `getBaseType()` to describe the array's element type (e.g. `INT` / `Types.INTEGER` for `ARRAY<INT>`) per the `java.sql.Array` contract, instead of returning the full array type name and `Types.OTHER`. As part of this fix, `getArray()` now returns elements whose Java type matches `getBaseType()`: elements of `ARRAY<TINYINT>` are returned as `Byte` and elements of parameterized `ARRAY<DECIMAL(p,s)>` as `BigDecimal`. Previously both were returned as `String`, so code that casts these elements to `String` must be updated. This change applies only to `ARRAY` elements: `TINYINT` and parameterized `DECIMAL(p,s)` fields of a `STRUCT` and values of a `MAP` are still returned as `String` (for example, an element of `ARRAY<DECIMAL(10,2)>` is a `BigDecimal`, but field `a` of `STRUCT<a:DECIMAL(10,2)>` is a `String`).

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
