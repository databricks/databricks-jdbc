# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Server-side close failures no longer escape `Statement.close()`, `ResultSet.close()` with `closeOnCompletion`, or `Connection.close()`; failures other than already-closed operations are logged at WARN, while local cleanup errors can still throw `SQLException`. `Connection.close()` now closes every statement instead of stopping at the first server-close failure.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
