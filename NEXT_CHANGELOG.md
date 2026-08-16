# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- Removed automatic Thrift fallback and the 24-hour SEA circuit breaker after HTTP 429 session creation failures. Rate-limit errors now propagate to the caller without changing routing for subsequent connections.

- Removed the `UseQueryForMetadata` connection property. It no longer forces the Thrift client, so connections eligible for SEA now use SEA with SQL metadata queries. Thrift connections use native metadata RPCs.

- SQL Execution API sessions open with warehouse URLs. Real-time sessions use warehouse URLs for session-owned statement operations and session close.

### Fixed

- Fixed long-running queries on the Thrift client path (`UseThriftClient=1`) intermittently failing with `Query has been timed out due to inactivity` under sustained concurrency. A transient transport-level failure (stale pooled connection, connection reset, load-balancer idle drop) on a `GetOperationStatus` poll previously abandoned the still-running server operation after a single blip, and the same failure class on `CloseOperation`/`CancelOperation` could leak completed operations until the server reaped them. These idempotent RPCs now transparently reconnect and retry on a fresh connection with bounded, jittered exponential backoff before surfacing the error. Statement submission is deliberately excluded from the retry path to avoid double-execution. The SEA client path is unchanged.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
