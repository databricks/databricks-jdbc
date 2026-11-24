# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- `Statement.setQueryTimeout` and `MetadataOperationTimeout` now bound held status polls to the timeout plus a five-second in-flight grace (previously up to `socketTimeout`), without whole-second rounding. Cancellation is awaited for up to two seconds before reporting the timeout; slower cleanup continues in the background within a 30-second budget, with inline fallback if worker submission fails.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
