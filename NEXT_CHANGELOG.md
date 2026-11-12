# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Removed Thrift handle secrets from timeout, operation, metadata, and session diagnostics.
- Fixed connection opens and other telemetry-emitting calls stalling for tens of seconds while another connection to the same host closed and its final telemetry flush was slow. `Connection.close()` also no longer waits indefinitely for that final flush: `TelemetrySocketTimeout` now also caps each final telemetry flush on close (authenticated and unauthenticated telemetry flush separately), including time queued behind other telemetry pushes and retries. After that many seconds the remaining telemetry batch is dropped (a value of `0` keeps the unbounded wait). ([#1442](https://github.com/databricks/databricks-jdbc/issues/1442))
- Fixed connection setup stalling under concurrency: per-connection auth setup and the first feature-flag fetch no longer block connections to other hosts; same-host connections wait for the first feature-flag initialization, including any required authentication.
- Reduced connection setup time by caching successful host-metadata and OIDC discovery JSON documents for up to an hour, with a JVM-wide limit of 1,024 entries. Responses redirected to another origin or path, or whose bodies cannot be parsed as discovery documents, are excluded.
- Fixed `Connection.close()` waiting for the final telemetry flush when it closes the last connection to a host (about 100 ms per close, up to `TelemetrySocketTimeout` when the telemetry endpoint is slow or throttling). The flush now finishes on a background thread; the session is still closed before `close()` returns.
- Fixed telemetry from a host's open connections being dropped after the connection that created the host's telemetry client closed, and one telemetry collector leaking per closed connection.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
