# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `PARSE_SYNTAX_ERROR` for column names containing special characters (e.g., dots) when `EnableBatchedInserts` is enabled, by re-quoting column names with backticks in reconstructed multi-row INSERT statements.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
