# NEXT CHANGELOG

## [Unreleased]

### Added
- Added the EnableTokenFederation url param to enable or disable Token federation feature. By default it is set to 1
- Added the ApiRetriableHttpCodes, ApiRetryTimeout url params to enable retries for specific HTTP codes irrespective of Retry-After header. By default the HTTP codes list is empty.

### Updated
- Added validation for positive integer configuration properties (RowsFetchedPerBlock, BatchInsertSize, etc.) to prevent hangs and errors when set to zero or negative values.
- Updated Circuit breaker to be triggered by 429 errors too.
- Refactored chunk download to keep a sliding window of chunk links. The window advances as the main thread consumes chunks. These changes can be enabled using the connection property EnableStreamingChunkProvider=1. The changes are expected to make chunk download faster and robust. 
- Added separate circuit breaker to handle 429 from SEA connection creation calls, and fall back to Thrift.

### Fixed

- Fix driver crash when using `INTERVAL` types.
- Fix connection failure in restricted environments when `LogLevel.OFF` is used.
- Fix U2M by including SDK OAuth HTML callback resources.
- Fix microsecond precision loss in `PreparedStatement.setTimestamp(int,Timestamp, Calendar)` and address thread-safety issues with global timezone modification.
- Fix metadata methods (`getColumns`, `getFunctions`, `getPrimaryKeys`, `getImportedKeys`) to return empty ResultSets instead of throwing exceptions when catalog parameter is NULL, for SEA.

---
*Note: When making changes, please add your change under the appropriate section with a brief description.*
