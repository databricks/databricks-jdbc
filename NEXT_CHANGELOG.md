# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Fixed many concurrent CloudFetch queries in one JVM failing with `OutOfMemoryError` ("Failed to ready chunk"): the number of chunks being downloaded at once is now bounded for each workspace host, scaled to the maximum heap (one download per 128 MiB, at least 16), instead of growing by 16 per query. A JVM that queries several workspaces gets this bound once per host. Single-query download parallelism is unchanged: a query with a larger `cloudFetchThreadPoolSize` raises its host's bound to that size, up to 256, while it is open. A user-set `ChunkReadyTimeoutSeconds` now includes time spent waiting for a download slot.
- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: `TelemetrySocketTimeout` now also caps each final telemetry flush on close (authenticated and unauthenticated telemetry flush separately), including time queued behind other telemetry pushes and retries. After that many seconds the remaining telemetry batch is dropped (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))
- Fixed connection setup stalling under concurrency: per-connection auth setup and the first feature-flag fetch no longer block connections to other hosts; same-host connections wait for the first feature-flag initialization, including any required authentication.
- Reduced connection setup time by caching successful host-metadata and OIDC discovery JSON documents for up to an hour, with a JVM-wide limit of 1,024 entries. Responses redirected to another origin or path, or whose bodies cannot be parsed as discovery documents, are excluded.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
