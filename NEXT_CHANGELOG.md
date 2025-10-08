# NEXT CHANGELOG

## [Unreleased]

### Added
- Enabled direct results by default in SEA mode to improve latency for short and small queries.
### Updated
- Telemetry data is now captured more efficiently and consistently due to enhancements in the log and connection close flush logic.
- Updated Databricks SDK version to v0.65.0 (This is to fix OAuthClient to properly encode complex query parameters.)

### Fixed
- Fixed state leaking issue in thrift client.
- Fixed timestamp values returning only milliseconds instead of the full nanosecond precision.
- Fixed Statement.getUpdateCount() for DML queries.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
