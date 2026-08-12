# NEXT CHANGELOG

## [Unreleased]

### Added
- Added native server-side batching (supported on DBR 18.2 and later) for parameterized `INSERT INTO ... VALUES (?, ...)` statements executed with `PreparedStatement`. The driver sends all parameter sets in one request instead of rewriting INSERT statements on the client. Each batch is limited to 10,000 parameters or 1 MB of parameter data, whichever limit is reached first. Native batching is enabled by default; set `EnableNativeBatching=0` to use legacy batching. `EnableBatchedInserts` now defaults to `1`, enabling client-side batching for compute running DBR versions earlier than 18.2. The `BINARY` data type is not supported in native batching; to batch binary values, set `EnableNativeBatching=0` and `supportManyParameters=1` to use legacy client-side batching.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
