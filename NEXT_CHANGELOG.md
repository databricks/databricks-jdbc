# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Fix unresolvable Maven Central POM for the uber JAR. The published POM no longer
  declares a transitive dependency on the internal `databricks-jdbc-core` coordinate
  (which is not published to Maven Central), restoring resolution for downstream
  consumers (#1431).

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
