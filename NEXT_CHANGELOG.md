# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Retry transient HTTP 408/429/502/503/504 errors when opening Thrift sessions, using jittered backoff when Retry-After is missing or invalid. During outages, connection failures may be delayed by the retry window (900 seconds for availability errors, 120 seconds for rate limits by default, plus HTTP attempt time); disable with TemporarilyUnavailableRetry=0 or RateLimitRetry=0, or lower the corresponding *RetryTimeout.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
