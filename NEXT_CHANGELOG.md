# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: `TelemetrySocketTimeout` now also caps each final telemetry flush on close (authenticated and unauthenticated telemetry flush separately), including time queued behind other telemetry pushes and retries. After that many seconds the remaining telemetry batch is dropped (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))
- Fixed connection setup stalling under concurrency: per-connection auth setup and the first feature-flag fetch no longer block connections to other hosts; same-host connections wait for the first feature-flag initialization, including any required authentication.
- Reduced connection setup time by caching successful host-metadata and OIDC discovery JSON documents for up to an hour, with a JVM-wide limit of 1,024 entries. Responses redirected to another origin or path, or whose bodies cannot be parsed as discovery documents, are excluded.
- Reduced heap usage when reading large CloudFetch results: chunks are now streamed from the HTTP response through decompression into Arrow instead of first being buffered on-heap. When Arrow uses off-heap memory, this cuts peak heap by about 40% and GC pause time by about half, so large results fit in smaller heaps.
- Fixed CloudFetch rows being silently skipped when a chunk failed to parse and its retried download then succeeded: the chunk stayed in a failed state and its rows were not returned.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
