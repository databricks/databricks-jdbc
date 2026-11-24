# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- `Statement.setQueryTimeout` and `MetadataOperationTimeout` now bound held status polls to the timeout plus a five-second in-flight grace (previously up to `socketTimeout`), without whole-second rounding. Timed-out operations are cancelled in the background, with bounded caller-side cancellation if cleanup capacity is exhausted.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
