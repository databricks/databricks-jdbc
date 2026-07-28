# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- `DatabaseMetaData.getColumns(...)` with a `null` catalog now issues a single `SHOW COLUMNS IN ALL CATALOGS` statement (consistent with `getSchemas`/`getTables`) instead of enumerating every catalog and issuing a per-catalog `SHOW COLUMNS`. Older DBR versions that do not support the syntax transparently fall back to the previous enumerate-and-fan-out behavior.
- Bumped `jackson-databind` (and `jackson-core`/`jackson-annotations`) from 2.18.8 to 2.18.9 to resolve CVE-2026-54515, CVE-2026-59889, and GHSA-mhm7-754m-9p8w (`@JsonView`/`@JsonIgnoreProperties` deserialization bypasses).
- Bumped `lz4-java` from 1.10.1 to 1.11.1 to resolve CVE-2026-59949 (native XXHash JVM crash on invalid byte-array ranges).
- Bumped shaded `netty-buffer`/`netty-common` from 4.2.13.Final to 4.2.15.Final (Netty security release). Addresses issue #1584.
- Bumped Apache `httpcore5` from 5.3.6 to 5.4.3 and pinned the transitive `httpcore5-h2` (HTTP/2 HPACK decoder) to 5.4.3 to resolve CVE-2026-54399 (HTTP/1.1 parser DoS) and CVE-2026-54428 (HPACK header-list-size enforcement). `httpclient5` stays at 5.5.2, which is compatible with the httpcore5 5.4.x branch. Addresses issue #1584.

### Fixed
- Fixed `IdleConnectionEvictor` thread leak in long-running applications. Driver-side resources (HTTP client, background threads) are now always released when `Connection.close()` is called, even if statement cleanup or server-side session termination fails.

- Throw `DatabricksSQLException` instead of an unchecked `ClassCastException` when a complex-type getter (`getArray`, `getStruct`, `getMap`) is called on a column of a different complex type.

- Fixed `NullPointerException` when reading collated string columns (e.g. `STRING COLLATE UTF8_LCASE`) over Arrow. Such columns report a `type_name` that does not map to a `ColumnInfoTypeName`, leaving it null; the value read now recovers `STRING` from the type text and the result set metadata reports `VARCHAR` instead of `OTHER`, while `getColumnTypeName()` still preserves the collated type text.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
