# NEXT CHANGELOG

## [Unreleased]

### Added

- All-purpose cluster sessions can now use the SQL Execution API when enabled by the dedicated APC rollout flag; `UseThriftClient=1` keeps Thrift, and `UseThriftClient=0` lets the rollout flag apply even when `TreatMetadataCatalogNameAsPattern=1` is set. Native `ARROW_BATCH` responses support inline single-batch and paginated results, plus external-link results when result download is enabled with `EnableQueryResultDownload`.

### Updated

- Paginated inline APC `ARROW_BATCH` results now use bounded background prefetching to reduce wait time while limiting batches held in memory.

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
