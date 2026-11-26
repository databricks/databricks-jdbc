# NEXT CHANGELOG

## [Unreleased]

### Added

- All-purpose cluster sessions can now use the SQL Execution API when enabled by the dedicated APC rollout flag or explicitly selected with `UseThriftClient=0`. Query responses support inline and externally linked native `ARROW_BATCH` results when result download is enabled, and metadata responses support inline or externally linked `ARROW_STREAM` results.

### Updated

- Paginated inline APC `ARROW_BATCH` results now use bounded background prefetching to reduce wait time while limiting batches held in memory.
- SQL Execution API columnar results now expose consistent row-count metadata and JDBC cursor
  state across paginated responses.

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
