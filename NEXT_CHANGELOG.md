# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Server cleanup failures are recorded without escaping `Statement.close()`, `ResultSet.close()` with `closeOnCompletion`, or `Connection.close()`, allowing remaining local resources and the session to be released. Local cleanup errors can still throw `SQLException`.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
