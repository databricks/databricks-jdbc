# NEXT CHANGELOG

## [Unreleased]

### BREAKING CHANGES in 3.4.1

#### Metadata JDBC Spec Compliance

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

#### Default Behavior Changes

* **Native geospatial type support (`GEOMETRY` and `GEOGRAPHY`) is now enabled
  by default.** `getObject()` now returns `IGeometry`/`IGeography` instances
  instead of EWKT strings. Set `EnableGeoSpatialSupport=0` to restore the
  previous behavior.

* **`EnableArrow` connection property is deprecated and ignored.** Arrow
  serialization is now always enabled. Setting `EnableArrow=0` previously
  disabled Arrow and forced columnar/JSON inline results; this value is now
  ignored and a deprecation warning is logged. For JSON inline results with
  SEA, disable CloudFetch via `EnableQueryResultDownload=0`. Exception: on AIX
  platforms and PowerPC architectures (`os.arch` contains `ppc`), `EnableArrow`
  is still honoured and defaults to disabled due to known Arrow native library
  compatibility issues.

### Added
- Metadata operations now use SQL SHOW commands for both Thrift and SEA backends,
  ensuring consistent behavior for SQL warehouses regardless of underlying
  protocol. To revert to native Thrift metadata RPCs, set `UseQueryForMetadata=0`.

### Updated
- `getColumnTypeName()` for DECIMAL columns now preserves precision/scale suffix (e.g., `"DECIMAL(10,2)"`) consistently across both Thrift and SEA backends.
- `EnableGeoSpatialSupport` no longer requires `EnableComplexDatatypeSupport=1`. Geospatial types (GEOMETRY, GEOGRAPHY) can now be enabled independently of complex type support (ARRAY, MAP, STRUCT).
- Arrow schema deserialization failures (Thrift metadata path) now surface a dedicated driver error code `ARROW_SCHEMA_PARSING_ERROR` (vendor code `22000`) and a proper SQLSTATE `22000` (Data Exception) on the thrown `SQLException`, instead of the generic `RESULT_SET_ERROR` (1004) and the enum name as SQLSTATE. The exception message is unchanged.
- When a Statement is re-executed, the previous server-side operation is now explicitly closed before starting the new execution, preventing orphaned server-side operations when Statements are reused.
- Server-side operations are now closed proactively when `ResultSet.close()` is called, improving resource utilization. The client-side Statement remains open and reusable for re-execution. As a result, `getExecutionResult()` after result consumption returns the cached ResultSet instead of making a server RPC.

### Fixed
- Bump shaded `jackson-core` from 2.18.6 to 2.18.7 to address [SNYK-JAVA-COMFASTERXMLJACKSONCORE-15907551](https://security.snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-15907551) (DoS via oversized JSON documents bypassing size limits). Fixes #1436.
- Bump shaded `httpclient5`/`httpcore5`/`httpcore5-h2` from 5.3.1 to 5.5.2 to address [CVE-2025-8671](https://security.snyk.io/vuln/SNYK-JAVA-ORGAPACHEHTTPCOMPONENTSCORE5-15857052) (HTTP/2 stream-reset DoS in `httpcore5-h2`). Fixes #1436.
- Bump shaded `netty-buffer`/`netty-common` from 4.2.12.Final to 4.2.13.Final to clear OWASP scanner reports for the May 2026 batch of netty codec CVEs (CVE-2026-42577/42579/42580/42581/42582/42583/42584/42585/42586/42587, CVE-2026-44248, CVE-2026-41417, CVE-2026-42578). The driver does not use any netty HTTP/codec components — these vulnerabilities are not exploitable in this usage — but the bump silences the false-positive CPE matches.
- Bump shaded `commons-configuration2` from 2.10.1 to 2.15.0 to address [CVE-2026-45205](https://nvd.nist.gov/vuln/detail/CVE-2026-45205) (uncontrolled recursion when parsing untrusted YAML configurations). The driver does not parse untrusted YAML, so the practical risk is negligible.
- Bump `lz4-java` from `org.lz4:lz4-java:1.8.1` to `at.yawk.lz4:lz4-java:1.10.1` to address [CVE-2025-66566](https://nvd.nist.gov/vuln/detail/CVE-2025-66566) (information leak via uncleared output buffers in the safe/unsafe Java decompressors). `org.lz4:lz4-java:1.8.1` is a relocation-only POM that resolves to `at.yawk.lz4:lz4-java:1.8.1`, so the published `databricks-jdbc-thin` artifact previously pulled the vulnerable fork transitively. The upstream `org.lz4` GA is no longer maintained; `at.yawk.lz4` is the fork that received the fix. Fixes #1455.
- Fixed `DatabaseMetaData.getTables()` in Thrift mode returning rows when called with an empty `types` array. Per JDBC spec, empty types means "no types selected" and now correctly returns zero rows (matching SEA mode).
- Fixed `?` characters inside SQL comments, string literals, and quoted identifiers being incorrectly counted as parameter placeholders when `supportManyParameters=1`. `SQLInterpolator` now uses `SqlCommentParser` to locate only real placeholders. Fixes #1331.
- Fixed `MetadataOperationTimeout` not being applied when metadata operations use SHOW commands. Operations like `getTables`, `getSchemas`, and `getColumns` now respect the `MetadataOperationTimeout` connection property instead of hanging indefinitely with no timeout.
- Reclassify transient server errors to standard SQL states (08S01, 40001) across all Thrift error sites. This ensures UC unavailability and concurrent modification errors surface consistently for better retry handling. Note: Dashboards and branching logic keyed on legacy XXUCC or 42000 must be updated.
- Fixed telemetry HTTP client socket leak that prevented CRaC checkpoint. After `Connection.close()`, delayed telemetry flush tasks could re-create HTTP clients that were never closed, leaking TCP sockets. Fixes #1325.
- Fixed client-side enforcement of `maxRows` limit. When `statement.setMaxRows()` is set, `ResultSet.next()` now returns false once the row limit is reached, even if the server returns more rows. Applies to all result types (Thrift, SEA, inline, CloudFetch).
- Bump shaded `bouncycastle` (`bcprov-jdk18on`, `bcpkix-jdk18on`) from 1.79 to 1.84 to address [CVE-2026-5598](https://github.com/advisories/GHSA-p93r-85wp-75v3) (covert timing channel, severity 8.9) and two related MEDIUM CVEs (GHSA-wg6q-6289-32hp, GHSA-c3fc-8qff-9hwx). All three are unsurfaced by NVD-CPE scanners but visible to GHSA-backed scanners like OSV.
- Bump shaded `libthrift` from 0.19.0 to 0.23.0 to clear the May 2026 Apache Thrift advisory batch (GHSA-7pwc-h2j2-rjgj covering CVE-2026-41603/41604/41605/43869). The libthrift 0.21 release changed `ProcessFunction`'s generic signatures, which required regenerating the project's checked-in Thrift-generated Java sources with the matching compiler.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
