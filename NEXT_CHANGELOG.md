# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: after `TelemetrySocketTimeout` seconds it drops the remaining telemetry batch (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
