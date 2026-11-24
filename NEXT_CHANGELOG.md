# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Server-side close failures no longer escape `Statement.close()`, `ResultSet.close()` with `closeOnCompletion`, or `Connection.close()`; failures other than already-closed operations are logged at WARN, while local cleanup errors can still throw `SQLException`. Each statement close makes at most one server-close attempt; after the first non-404 failure, `Connection.close()` closes remaining statements locally before closing the session, bounding teardown waits during outages.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
