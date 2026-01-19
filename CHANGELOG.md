# Version Changelog
## [v3.1.1] - 2026-01-07

### Added
- Added token caching for all authentication providers to reduce token endpoint calls.
- We will be rolling out the use of Databricks SQL Execution API by default for queries submitted on DBSQL. To continue using Databricks Thrift Server backend for execution, set `UseThriftClient` to `1`.

### Updated
- Changed default value of `IgnoreTransactions` from `0` to `1` to disable multi-statement transactions by default. Preview participants can opt-in by setting `IgnoreTransactions=0`. Also updated `supportsTransactions()` to respect this flag.

### Fixed
- [PECOBLR-1131] Fix incorrect refetching of expired CloudFetch links when using Thrift protocol.
- Fixed logging to respect params when the driver is shaded.
- Fixed `isWildcard` to return true only when the value is `*`

## [v3.0.7] - 2025-12-18

### Updated
- Log timestamps now explicitly display timezone.
- **[Breaking Change]** `PreparedStatement.setTimestamp(int, Timestamp, Calendar)` now properly applies Calendar timezone conversion using LocalDateTime pattern (inline with `getTimestamp`). Previously Calendar parameter was ineffective.
- `DatabaseMetaData.getColumns()` with null catalog parameter now retrieves columns from all catalogs when using SQL Execution API, aligning the behaviour with thrift.
- `DatabaseMetaData.getFunctions()` with null catalog parameter now retrieves columns from the current catalog when using SQL Execution API, aligning the behaviour with thrift.

### Fixed
- Fix timeout exception handling to throw `SQLTimeoutException` instead of `DatabricksSQLException` when queries timeout.
- Removes dangerous global timezone modification that caused race conditions.
- Fixed `Statement.getLargeUpdateCount()` to return -1 instead of throwing Exception when there were no more results or result is not an update count.
- CVE-2025-66566. Updated lz4-java dependency to 1.10.1.
- Fix `INVALID_IDENTIFIER` error when using catalog/schema/table names for SQL Exec API with hyphens or special characters in metadata operations (`getSchemas()`, `getTables()`, `getColumns()`, etc.) and connection methods (`setCatalog()`, `setSchema()`). Per Databricks identifier rules, special characters are now properly enclosed in backticks.
- Fix Auth_Scope handling inconsistency in Azure U2M OAuth.

---

## [v3.0.6] - 2025-12-11

### Added
- Added the EnableTokenFederation url param to enable or disable Token federation feature. By default it is set to 1
- Added the ApiRetriableHttpCodes, ApiRetryTimeout url params to enable retries for specific HTTP codes irrespective of Retry-After header. By default the HTTP codes list is empty.

### Updated
- Added validation for positive integer configuration properties (RowsFetchedPerBlock, BatchInsertSize, etc.) to prevent hangs and errors when set to zero or negative values.
- Updated Circuit breaker to be triggered by 429 errors too.
- Refactored chunk download to keep a sliding window of chunk links. The window advances as the main thread consumes chunks. These changes can be enabled using the connection property EnableStreamingChunkProvider=1. The changes are expected to make chunk download faster and robust.
- Added separate circuit breaker to handle 429 from SQL Exec API connection creation calls, and fall back to Thrift.

### Fixed
- Fix driver crash when using `INTERVAL` types.
- Fix connection failure in restricted environments when `LogLevel.OFF` is used.
- Fix U2M by including SDK OAuth HTML callback resources.
- Fix microsecond precision loss in `PreparedStatement.setTimestamp(int,Timestamp, Calendar)` and address thread-safety issues with global timezone modification.
- Fix metadata methods (`getColumns`, `getFunctions`, `getPrimaryKeys`, `getImportedKeys`) to return empty ResultSets instead of throwing exceptions when catalog parameter is NULL, for SQL Exec API.

---

## [v3.0.5] - 2025-11-20

### Added
- Added support for high-performance batched writes with parameter interpolation:
  - `supportManyParameters=1`: Enables parameter interpolation to bypass 256-parameter limit (default: 0)
  - `EnableBatchedInserts=1`: Enables multi-row INSERT batching (default: 0)
  - `BatchInsertSize=<SIZE>`: Maximum rows per batch (default: 1000)
  - Note: Large batches are chunked for execution. If a chunk fails, previous chunks remain committed (no transaction rollback). Consider using staging tables for critical workflows.
- Added Feature-flag integration for SQL Exec API rollout
- Call statements will return result sets in response
- Add a gating flag for enabling GeoSpatial support: `EnableGeoSpatialSupport`. By default, it will be disabled

### Updated
- Minimized OAuth requests by reducing calls in feature flags and telemetry.
- Geospatial `getWKB()` now returns OGC-compliant WKB values.

### Fixed
- Fix: SQLInterpolator failing to escape temporal fields and special characters.
- Fixed: Errors in table creation when using BIGINT, SMALLINT, TINYINT, or VOID types.
- Fixed: PreparedStatement.getMetaData() now correctly reports TINYINT columns as Types.TINYINT (java.lang.Byte) instead of Types.SMALLINT (java.lang.Integer).
- Fixed: TINYINT to String conversion to return numeric representation (e.g., "65") instead of character representation (e.g., "A").
- Fixed: Complex types (Structs, arrays, maps) now show detailed type information in metadata calls in Thrift mode
- Fixed: incorrect chunk download/processing status codes.
- Shade SLF4J to avoid conflicts with user applications.

---

## [v3.0.4] - 2025-11-12: DEPRECATED, Use v3.0.5 instead

### Added
- Added support for geospatial data types. (Use v3.0.5+ for OGC compliant WKB support)
- Added support for telemetry log levels, which can be controlled via the connection parameter `TelemetryLogLevel`. This allows users to configure the verbosity of telemetry logging from OFF to TRACE.
- Added full support for JDBC transaction control methods in Databricks. Transaction support in Databricks is currently available as a Private Preview. The `IgnoreTransactions` connection parameter can be set to `1` to disable or no-op transaction control methods.
- Added a new config attribute `DisableOauthRefreshToken` to control whether refresh tokens are requested in OAuth exchanges. By default, the driver does not include the `offline_access` scope. If `offline_access` is explicitly provided by the user, it is preserved and not removed.

### Updated
- Updated sdk version from 0.65.0 to 0.69.0

### Fixed
- Fixed SQL syntax error when LIKE queries contain empty ESCAPE clauses.
- Fix: driver failing to authenticate on token update in U2M flow.
- Fix: driver failing to parse complex data types with nullable attributes.
- Fixed: Resolved SDK token-caching regression causing token refresh on every call. SDK is now configured once to avoid excessive token endpoint hits and rate limiting.
- Fixed: TimestampConverter.toString() returning ISO8601 format with timezone conversion instead of SQL standard format.
- Fixed: Driver not loading complete JSON result in the case of SEA Inline without Arrow
---

## [v3.0.3] - 2025-10-30
### Added

### Updated

### Fixed
- Fixed token endpoint regression, which caused excessive token refresh calls

## [v3.0.1] - 2025-10-13: DEPRECATED, Use v3.0.3 instead 
### Added
- Added `enableMultipleCatalogSupport` connection parameter to control catalog metadata behavior.

### Updated

### Fixed
- Fixed complex data type conversion issues by improving StringConverter to handle Databricks complex objects (arrays/maps/structs), JDBC arrays/structs, and generic collections.
- Fixed ComplexDataTypeParser to correctly parse ISO timestamps with T separators and timezone offsets, preventing Arrow ingestion failures.
---

## [v1.0.11-oss] - 2025-10-06: DEPRECATED, Use v3.0.3 instead

### Added
- Enabled direct results by default in SEA mode to improve latency for short and small queries.

### Updated
- Telemetry data is now captured more efficiently and consistently due to enhancements in the log and connection close flush logic.
- Updated Databricks SDK version to v0.65.0 (This is to fix OAuthClient to properly encode complex query parameters.)
- Added IgnoreTransactions connection parameter to silently ignore transaction method calls.

### Fixed
- Fixed state leaking issue in thrift client.
- Fixed timestamp values returning only milliseconds instead of the full nanosecond precision.
- Fixed Statement.getUpdateCount() for DML queries.
---

## [v1.0.10-oss] - 2025-09-18: DEPRECATED, Use v3.0.3 instead
### Added
- **Query Tags support**: Added ability to attach key-value tags to SQL queries for analytical purposes that would appear in `system.query.history` table. Example: `jdbc:databricks://host;QUERY_TAGS=team:marketing,dashboard:abc123`. (This feature is in [private preview](https://docs.databricks.com/aws/en/release-notes/release-types#:~:text=Private%20Preview-,Invite%20only,-No))
- **SQL Scripting support**: Added support for [SQL Scripting](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting)
- Added a client property `enableVolumeOperations` to enable  GET/PUT/REMOVE volume operations on a stream. For backward compatibility, allowedVolumeIngestionPaths can also be used for REMOVE operation.
- Support for fetching schemas across all catalogs (when catalog is specified as null or a wildcard) in `DatabaseMetaData#getSchemas` API in SQL Execution mode.
- **Configurable SQL validation in isValid()**: Added `EnableSQLValidationForIsValid` connection property to control whether `isValid()` method executes an actual SQL query for server-side validation. Default value is 0.
- Implement multi-row INSERT batching optimization for prepared statements to improve performance when executing large batches of INSERT operations.
- Implement lazy/incremental fetching for columnar results when using Databricks JDBC in Thrift mode without Arrow support. The change modifies the behavior from buffering entire result sets in memory to maintaining only a limited number of rows at a time, reducing peak heap memory usage and preventing OutOfMemory errors.
- Added new artifact `databricks-jdbc-thin` for thin jar with runtime dependency metadata.
- Introduce a memory-efficient columnar data access mechanism for JDBC result processing.
- Added support for using a custom Discovery URL in U2M flows on AWS and GCP.

### Updated
- Databricks SDK dependency upgraded to latest version 0.64.0

### Fixed
- Integrated Azure U2M flow into driver for improved stability.
- Fixed `ResultSet.getString` for Boolean columns in Metadata result set.
- Fixed volume operations not completing unless the ResultSet is fully iterated.
- Fixed `connection.getMetadata().getColumns()` to return the correct SQL data type code for complex type columns.
- Fixed a bug in the JDBC driver's metadata parsing for nested decimal fields within struct types.
- Fixed case sensitive table search in `connection.getMetadata().getTables()`
- Fixed `connection.getMetadata().getColumns()` to return the correct scale.
---

## [v1.0.9-oss] - 2025-08-19
### Added
- Added support for providing custom HTTP options: `HttpMaxConnectionsPerRoute` and `HttpConnectionRequestTimeout`.
- Add V2 of chunk download using async http client with corresponding implementations of AbstractRemoteChunkProvider and
  AbstractArrowResultChunk
- Telemetry is enabled by default, subject to server-side rollout.

### Updated

### Fixed
- Fixed Statement.getUpdateCount to return -1 for non-DML queries.
- Fixed Statement.setMaxRows(0) to be interepeted as no limit.
- Fixed retry behaviour to not throw an exception when there is no retry-after header for 503 and 429 status codes.
- Fixed encoded UserAgent parsing in BI tools.
- Fixed setting empty schema as the default schema in the spark session.
---

## [v1.0.8-oss] - 2025-07-25

### Added
- Added DCO (Developer Certificate of Origin) check workflow for pull requests to ensure all commits are properly signed-off
- Added support for SSL client certificate authentication via parameter: SSLTrustStoreProvider
- Provide an option to push telemetry logs (using the flag `ForceEnableTelemetry=1`). For more details see [documentation](https://docs.databricks.com/aws/en/integrations/jdbc-oss/properties#-telemetry-collection)
- Added putFiles methods in DBFSVolumeClient for async multi-file upload.
- Added validation on UID param to ensure it is either not set or set to 'token'.
- Added CloudFetch download speed logging at INFO level
- Added vendor error codes to SQLExceptions raised for incorrect UID, host or token.

### Updated
- Column name support for JDBC ResultSet operations is now case-insensitive
- Updated arrow to 17.0.0 to resolve CVE-2024-52338
- Updated commons-lang3 to 3.18.0 to resolve CVE-2025-48924
- Enhanced SSL certificate path validation error messages to provide actionable troubleshooting steps.

### Fixed
- Fixed Bouncy Castle registration conflicts by using local provider instance instead of global security registration.
- Fixed Azure U2M authentication issue.
- Fixed unchecked exception thrown in delete session
- Fixed ParameterMetaData.getParameterCount() to return total parameter count from SQL parsing instead of bound parameter count, aligning with JDBC standards

---

## [v1.0.7-oss] - 2025-05-26

### Added
- Added support for DoD (.mil) domains
- Enables fetching of metadata for SELECT queries using PreparedStatement prior to setting parameters or executing the query.
- Added support for SSL client certificate authentication via keystore configuration parameters: SSLKeyStore, SSLKeyStorePwd, SSLKeyStoreType, and SSLKeyStoreProvider.

### Fixed
- Updated JDBC URL regex to accept valid connection strings that were incorrectly rejected.
- Updated decimal conversion logic to fix numeric values missing decimal precision.

---

## [v1.0.6-oss] - 2025-05-29

### Added
- Support for fetching tables and views across all catalogs using SHOW TABLES FROM/IN ALL CATALOGS in the SQL Exec API.
- Support for Token Exchange in OAuth flows where in third party tokens are exchanged for InHouse tokens.
- Support for polling of statementStatus and sqlState for async SQL execution.
- Support for REAL, NUMERIC, CHAR, and BIGINT JDBC types in `PreparedStatement.setObject` method
- Support for INTERVAL data type.

### Fixed
- Added explicit null check for Arrow value vector when the value is not set and Arrow null checking is disabled.

---

## [v1.0.5-oss] - 2025-04-28

### Added
- Support for token cache in OAuth U2M Flow using the configuration parameters: `EnableTokenCache` and `TokenCachePassPhrase`.
- Support for additional SSL functionality including use of System trust stores (`UseSystemTruststore`) and allowing self signed certificates (via `AllowSelfSignedCerts`)
- Added support for `getImportedKeys` and `getCrossReferences` in SQL Exec API mode

### Updated
- Modified E2E tests to validate driver behavior under multi-threaded access patterns.
- Improved error handling through telemetry by throwing custom exceptions across the repository.

### Fixed
- Fixed bug where batch prepared statements could lead to backward-incompatible error scenarios.
- Corrected setting of decimal types in prepared statement executions.
- Resolved NullPointerException (NPE) that occurred during ResultSet and Connection operations in multithreaded environment.

---

## [v1.0.4-oss] - 2025-04-14

### Added
- Support for connection parameter SocketTimeout.
- Handle server returned Thrift version as part of open session response gracefully
- Added OWASP security check in the repository.

### Updated
- Updated SDK to the latest version (0.44.0).
- Add descriptive messages in thrift error scenario

### Fixed
- BigDecimal is now set correctly to NULL if null value is provided.
- Fixed issue with JDBC URL not being parsed correctly when compute path is provided via properties.
- Addressed CVE vulnerabilities (CVE-2024-47535, CVE-2025-25193, CVE-2023-33953)
- Fix bug in preparedStatement decimal parameter in thrift flow.

---

## [v1.0.3-oss] - 2025-04-08

### Added
- Introduces a centralized timeout check and automatic cancellation for statements
- Allows specifying a default size for STRING columns (set to 255 by default) via `defaultStringColumnLength` connection parameter
- Implements a custom retry strategy to handle long-running tasks and connection attempts
- Added support for Azure Managed Identity based authentication
- Adds existence checks for volumes, objects, and prefixes to improve operational coverage
- Allows adjusting the number of rows retrieved in each fetch operation for better performance via `RowsFetchedPerBlock` parameter
- Allows overriding the default OAuth redirect port (8020) with a single port or comma-separated list of ports using `OAuth2RedirectUrlPort`
- Support for custom headers in the JDBC URL via `http.header.<key>=<value>` connection parameter

### Updated
- Removes the hard-coded default poll interval configuration in favor of a user-defined parameter for greater flexibility
- Adjusts the handling of NULL and non-NULL boolean values

### Fixed
- Ensures the driver respects the configured limit on the number of rows returned
- Improves retry behaviour to cover all operations, relying solely on the total retry time specified via the driver URL parameter
- Returns an exception instead of -1 when a column is not found

---

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
