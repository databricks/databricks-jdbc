# Version Changelog

## [v1.0.2-oss] - 2025-03-19

### Fixed
- Fixed columnType conversion for Variant and Timestamp_NTZ types
- Fix minor issue for string dealing with whitespaces

---

## [v1.0.1-oss] - 2025-03-11

### Added
- Support for complex data types, including MAP, ARRAY, and STRUCT.
- Support for TIMESTAMP_NTZ and VARIANT data types.
- Extended support for prepared statement when using thrift DBSQL/all-purpose clusters.
- Improved backward compatibility with the latest Databricks driver.
- Improved driver performance for large queries by optimizing chunk handling.
- Configurable HTTP connection pool size for better resource management.
- Support for Azure Active Directory (AAD) Service Principal in M2M OAuth.
- Implemented java.sql.Driver#getPropertyInfo to fetch driver properties.

### Updated
- Set Thrift mode as the default for the driver.
- Improved driver telemetry (opt-in feature) for better monitoring and debugging.
- Enhanced test infrastructure to improve accuracy and reliability.
- Added SQL state support in SEA mode.
- Changes to JDBC URL parameters (to ensure compatibility with the latest Databricks driver):
  1. Removed catalog in favour of ConnCatalog
  2. Removed schema in favour of ConnSchema
  3. Renamed OAuthDiscoveryURL to OIDCDiscoveryEndpoint
  4. Renamed OAuth2TokenEndpoint to OAuth2ConnAuthTokenEndpoint
  5. Renamed OAuth2AuthorizationEndPoint to OAuth2ConnAuthAuthorizeEndpoint
  6. Renamed OAuthDiscoveryMode to EnableOIDCDiscovery
  7. Renamed OAuthRefreshToken to Auth_RefreshToken

### Fixed
- Ensured TIMESTAMP columns are returned in local time.
- Resolved inconsistencies in schema and catalog retrieval from the Connection class.
- Fixed minor issues with metadata fetching in Thrift mode.
- Addressed incorrect handling of access tokens provided via client info.
- Corrected the driver version reported by DatabaseMetaData.
- Fixed case-sensitive behaviour while fetching client info.

---

## [v0.9.9-oss] - 2025-01-03

### Added
- Telemetry support in OSS JDBC.
- Support for fetching connection ID and closing connections by connection ID.
- Stream support implementation in the UC Volume DBFS Client.
- Hybrid result support added to the driver (for both metadata and executed queries).
- Support for complex data types.
- Apache Async HTTP Client 5.3 added for parallel query result downloads, optimizing query fetching and resource cleanup.

### Updated
- Enhanced end-to-end testing for M2M and DBFS UCVolume operations, including improved logging and proxy handling.
- Removed the version check SQL call when connection is established.

### Fixed
- Fixed statement ID extraction from Thrift GUID.
- Made volume operations flag backward-compatible with the existing Databricks driver.
- Improved backward compatibility of ResultSetMetadata with the legacy driver.
- Fix schema in connection string

---

## [v0.9.8-oss] - 2024-12-13

### Added
* Run queries in async mode in the thrift client.
* Added GET and DELETE operations for the DBFS client, enabling full UC Volume operations (PUT, GET, DELETE) without spinning up DB compute.

### Updated
* Do not send repeated DBSQL version queries.
* Skip SEA compatibility check if null or empty DBSQL version is returned by the workspace.
* Skips SEA check when DBSQL version string is blank space.
* Updated SDK version to resolve CVEs.

### Fixed
* Eliminated the statement execution thread pool.
* Fixed UC volume GET operation.
* Fixed async execution in SEA mode.
* Fixed and updated the SDK version to resolve CVEs.
---

## [v0.9.7-oss] - 2024-11-20
### Added
* Added GCP OAuth support: Use Google ID (service account email) with a custom JWT or Google Credentials.
* SQL state added in thrift flow
* Add readable statement-Id for thrift
* Added Client to perform UC Volume operations without the need of spinning up your DB compute
* Add compression for SEA flow
### Updated
* Updated support for large queries in thrift flow
* Throw exceptions in case unsupported old DBSQL versions are used (i.e., before DBR V15.2)
* Deploy reduced POM during release
* Improve executor service management
### Fixed
* Certificate revocation properties only apply when provided
* Create a new HTTP client for each connection
* Accept customer userAgent without errors

---

## [v0.9.6-oss] - 2024-10-24
### Added
* Added compression in the Thrift protocol flow.
* Added support for asynchronous query execution.
* Implemented `executeBatch` for batch operations.
* Added a method to extract disposition from result set metadata.
### Updated
* Optimised memory allocation for type converters.
* Enhanced logging for better traceability.
* Improved performance in the Thrift protocol flow.
* Upgraded `commons-io` to address security vulnerability (CVE mitigation).
* Ensured thread safety in `DatabricksPooledConnection`.
* Set UBER jar as the default jar for distribution.
* Refined result chunk management for better efficiency.
* Enhanced integration tests for broader coverage.
* Increased unit test coverage threshold to 85%.
* Improved interaction with Thrift-server client.
### Fixed
* Fixed compatibility issue with other drivers in the driver manager.

---

## [v0.9.5-oss] - 2024-09-25
### Added
- Support proxy ignore list.
- OSS Readiness improvements.
- Improve Logging.
- Add SSL Truststore URL params to allow configuring custom SSL truststore.
- Accept Pass-through access token as part of JDBC connector parameter.

### Updated
- `getTables` Thrift call to align with JDBC standards.
- Improved metadata functions.

### Fixed
- Fixed memory leaks and made chunk download thread-safe.
- Fixed issues with prepared statements in Thrift and set default timestamps.
- Fixed issues with empty table types, null pointer in `IS_GENERATEDCOLUMN`, and ordinal position.
- Increased retry attempts for chunk downloads to enhance resilience.
- Fixed exceptions being thrown for statement timeouts and cancel futures.
- Improved UC Volume code.
- Remove cyclic dependencies in package

---

## [v0.9.4-oss] - 2024-09-13
### Added
- Fallback mechanism for smoother token refresh flow.
- Retry logic to improve chunk download reliability.
- Improved logging for timeouts and statement execution for better issue tracking.
- Timestamp logging in UTC to avoid skew caused by local timezones.
- Passthrough token handling with backward compatibility for the existing driver.
- Continued improvements towards OSS readiness.

### Updated
- `getTables` Thrift call to align with JDBC standards.
- Improved accuracy of column metadata, fixing issues with empty table types, null pointer in `IS_GENERATEDCOLUMN`, and ordinal position.
- Passthrough token handling for backward compatibility.

### Fixed
- Memory leaks and made chunk download thread-safe.
- Issues with prepared statements in Thrift and set default timestamps.
- Increased retry attempts for chunk downloads to enhance resilience.
- Exceptions are now thrown for statement timeouts and cancel futures.

---

## [v0.9.3-oss] - 2024-09-01
### Added
- OSS readiness changes.
- M2M JWT support.
- Credential provider OAuthRefresh.

### Updated
- Commands to run benchmarking tests.
- Compiling logic for benchmarking workflows.
- Fixed metadata and TableType issues.

---

## [v0.9.2-oss] - 2024-08-24
### Added
- Fixed precision and scale for certain dataTypes.

### Fixed
- Minor bug for UC Volume in Thrift mode.
- SLF4j support for default SDK mode.
- Deprecated username handling.
- Catalog and schema not set by default.

---

## [v0.9.1-oss] - 2024-08-08
### Added
- Support for Input Stream in UC Volume Operations.
- Metadata fixes.
- Redacted passwords from logging.

---

## [v0.9.0-oss] - 2024-07-24
### Added
- Release OSS JDBC driver for Public Preview.

---

## [v0.9.0-beta] - 2024-07-22
### Added
- Initial beta release of Databricks JDBC OSS Driver for Public Preview.

---

## [v0.7.0] - 2024-07-09
### Added
- Stable release before Public Preview.

---

## [v0.1.0] - 2024-06-02
### Added
- All-purpose cluster support and logging support.

---

## [v0.0.1] - 2024-02-29
### Added
- First stable release with support for SQL warehouses.
