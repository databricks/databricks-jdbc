# NEXT CHANGELOG

## [Unreleased]

### Added

- Added `enableMultipleCatalogSupport` connection parameter to control catalog metadata behavior.

### Updated

### Fixed
- Fixed complex data type conversion issues by improving StringConverter to handle Databricks complex objects (arrays/maps/structs), JDBC arrays/structs, and generic collections.
- Fixed ComplexDataTypeParser to correctly parse ISO timestamps with T separators and timezone offsets, preventing Arrow ingestion failures.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
