# Databricks JDBC Driver v1.0.3 Release Notes

We're excited to announce the release of Databricks JDBC Driver v1.0.3. This release introduces significant new features and improvements, including centralized timeout management, custom retry strategies, and Azure Managed Identity authentication.

## What's New in v1.0.3

### Added Features
- **Timeout Management**
  - Introduces a centralized timeout check and automatic cancellation for statements

- **Connection Parameters**
  - Allows specifying a default size for `STRING` columns (set to 255 by default) via `defaultStringColumnLength` connection parameter
  - Allows adjusting the number of rows retrieved in each fetch operation for better performance via `RowsFetchedPerBlock` parameter
  - Allows overriding the default OAuth redirect port (8020) with a single port or comma-separated list of ports using `OAuth2RedirectUrlPort`
  - Support for custom headers in the JDBC URL via `http.header.<key>=<value>` connection parameter

- **Retry and Performance**
  - Implements a custom retry strategy to handle long-running tasks and connection attempts
  - Adds existence checks for volumes, objects, and prefixes to improve operational coverage

- **Authentication**
  - Added support for Azure Managed Identity based authentication

### Updated
- Removes the hard-coded default poll interval configuration in favor of a user-defined parameter for greater flexibility
- Adjusts the handling of `NULL` and non-`NULL` boolean values

### Fixed Issues
- Ensures the driver respects the configured limit on the number of rows returned
- Improves retry behaviour to cover all operations, relying solely on the total retry time specified via the driver URL parameter
- Returns an exception instead of `-1` when a column is not found

## Previous Release: v1.0.2

### Fixed Issues
- Fixed `columnType` conversion for `Variant` and `Timestamp_NTZ` types
- Fixed minor issue for string handling with whitespaces

## Previous Release: v1.0.1

### Added Features
- **Enhanced Data Type Support**
  - Support for complex data types, including `MAP`, `ARRAY`, and `STRUCT`
  - Support for `TIMESTAMP_NTZ` and `VARIANT` data types

- **Extended Prepared Statement Support**
  - Improved support for prepared statements when using Thrift DBSQL/all-purpose clusters

- **Performance Optimizations**
  - Improved driver performance for large queries by optimizing chunk handling
  - Configurable HTTP connection pool size for better resource management

- **Authentication Enhancements**
  - Support for Azure Active Directory (AAD) Service Principal in M2M OAuth
  - Implemented `java.sql.Driver#getPropertyInfo` to fetch driver properties

### Updated
- Set Thrift mode as the default for the driver
- Improved driver telemetry (opt-in feature) for better monitoring and debugging
- Enhanced test infrastructure to improve accuracy and reliability
- Added SQL state support in SEA mode
- Changes to JDBC URL parameters (to ensure compatibility with the latest Databricks driver):
  1. Removed `catalog` in favour of `ConnCatalog`
  2. Removed `schema` in favour of `ConnSchema`
  3. Renamed `OAuthDiscoveryURL` to `OIDCDiscoveryEndpoint`
  4. Renamed `OAuth2TokenEndpoint` to `OAuth2ConnAuthTokenEndpoint`
  5. Renamed `OAuth2AuthorizationEndPoint` to `OAuth2ConnAuthAuthorizeEndpoint`
  6. Renamed `OAuthDiscoveryMode` to `EnableOIDCDiscovery`
  7. Renamed `OAuthRefreshToken` to `Auth_RefreshToken`

### Fixed Issues
- Ensured `TIMESTAMP` columns are returned in local time
- Resolved inconsistencies in schema and catalog retrieval from the `Connection` class
- Fixed minor issues with metadata fetching in Thrift mode
- Addressed incorrect handling of access tokens provided via client info
- Corrected the driver version reported by `DatabaseMetaData`
- Fixed case-sensitive behaviour while fetching client info

## Documentation

For detailed information about the Databricks JDBC Driver, please refer to our [official documentation](https://docs.databricks.com/sql/jdbc-odbc-drivers.html).

## Feedback

Your feedback is important to us. If you encounter any issues or have suggestions for improvement, please [submit an issue](https://github.com/databricks/databricks-jdbc/issues) on our GitHub repository. 