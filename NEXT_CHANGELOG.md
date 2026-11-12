# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

- Large Thrift CloudFetch results on SQL warehouses can start downloading while the remaining result links are still being fetched, as SEA results already do. A server-side flag turns this on; setting `EnableStreamingChunkProvider` to `1` or `0` still forces either behavior.

### Fixed

- With `EnableStreamingChunkProvider=1`, Thrift CloudFetch results no longer report the first batch of result links as the total row count, which could make `ResultSet.isLast()`, `isAfterLast()` and `hasUpdateCount()` return wrong values.
- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: `TelemetrySocketTimeout` now also caps each final telemetry flush on close (authenticated and unauthenticated telemetry flush separately), including time queued behind other telemetry pushes and retries. After that many seconds the remaining telemetry batch is dropped (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))
- Fixed connection setup stalling under concurrency: per-connection auth setup and the first feature-flag fetch no longer block connections to other hosts; same-host connections wait for the first feature-flag initialization, including any required authentication.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
