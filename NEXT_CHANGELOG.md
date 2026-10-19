# NEXT CHANGELOG

## [Unreleased]

### Added

- Added typed getters for all six SAFE feature flag types using the existing cache.

### Updated

- Bumped Apache HttpClient 5 (`httpclient5`) from 5.6.3 to 5.6.4.
- SQL Execution API requests now skip HTTP response compression by default to avoid gzipping inline LZ4-compressed Arrow results. Set `EnableSeaResponseCompression=1` to restore compression on bandwidth-constrained connections.

### Fixed

- Decode Reyden's native Arrow `struct<srid,wkb>` results for GEOMETRY and GEOGRAPHY, including
  nested values in arrays, map values, and structs in both native-object and EWKT string modes.
- Fixed `UserAgentEntry` attribution in Query History for SQL Execution API connections.
- Honor `Auth_Scope` for OAuth client-secret M2M; unify scope parsing with JWT-assertion M2M and U2M. (#1706)
- Fixed request latency and lock contention that grew with every connection opened in a long-running JVM. The driver now registers each User-Agent entry once instead of on every connection. At most 64 distinct `UserAgentEntry` values are added to the shared User-Agent per JVM; SQL Execution API requests still send their own entry.
- Fixed queries failing with `Multiple entries with same key` when result column names differ only in case (e.g. `policyid` and `policyId`). Lookups by column label now return the first matching column, as the JDBC spec requires. (#1727)

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
