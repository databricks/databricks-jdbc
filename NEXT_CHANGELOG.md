# NEXT CHANGELOG

## [Unreleased]

### Added

- All-purpose cluster sessions can now use the SQL Execution API when enabled by the dedicated APC rollout flag; `UseThriftClient=1` keeps Thrift, and `UseThriftClient=0` lets the rollout flag apply even when `TreatMetadataCatalogNameAsPattern=1` is set. Native `ARROW_BATCH` responses support inline single-batch and paginated results, plus external-link results when result download is enabled with `EnableQueryResultDownload`.

### Updated

- Paginated inline APC `ARROW_BATCH` results now use bounded background prefetching to reduce wait time while limiting batches held in memory.

### Fixed

- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.
- String keys and values in MAP values returned by `getString()`/`getObject()` when complex datatype support is disabled (the default) are now JSON-escaped (for example `\"` and `\n`), and collated strings (`STRING COLLATE ...`) are quoted like `STRING`.
- `getConnectionId()` for Thrift sessions on SQL warehouses now returns a Thrift (`t|...`) connection ID instead of a SQL Execution API (`s|...`) ID, so `Driver.closeConnection` closes the session through the Thrift client that created it.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
