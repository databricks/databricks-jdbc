# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed OAuth U2M failing with "No token cache passphrase configured" under the default `EnableTokenCache=1`. Without a `TokenCachePassPhrase`, U2M now caches tokens on disk under a key derived from the OS user name, as the SQL kernel and ODBC driver do; a blank passphrase is treated as unset, so a cache written with one needs one new browser login. A failed cache write is now logged instead of failing authentication. ([#1612](https://github.com/databricks/databricks-jdbc/issues/1612))

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
