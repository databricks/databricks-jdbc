# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed OAuth U2M failing with "No token cache passphrase configured" under the default `EnableTokenCache=1`. Without a `TokenCachePassPhrase` (or with a blank one), U2M now caches tokens on disk under a key derived from the OS user name, as the SQL kernel and ODBC driver do, and a failed cache write is logged instead of failing authentication. ([#1612](https://github.com/databricks/databricks-jdbc/issues/1612))

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
