# NEXT CHANGELOG

## [Unreleased]

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
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
