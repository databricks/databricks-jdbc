# NEXT CHANGELOG

## [Unreleased]

### Added

- All-purpose cluster sessions can now use the SQL Execution API when enabled by the dedicated APC rollout flag or explicitly selected with `UseThriftClient=0`. Inline native `ARROW_BATCH` responses support both single-batch and paginated results.

### Updated

- Paginated inline APC `ARROW_BATCH` results now use bounded background prefetching to reduce wait time while limiting batches held in memory.

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
