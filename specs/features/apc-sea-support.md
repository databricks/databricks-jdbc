---
service: databricks-jdbc
created: 2026-10-28
---

# Add All-Purpose Cluster (APC) support over SEA in the JDBC driver

## Intent

Let the OSS JDBC driver run all-purpose cluster (APC) workloads through the SQL Execution API
(SEA), which now routes APC requests to Reyden server-side. This brings APC connections the same
Arrow/CloudFetch result path, telemetry, and future feature work as SQL warehouses, and unblocks
retiring the direct-Thrift APC path. Routing is controlled by a dedicated APC server flag so APC
can ramp and roll back independently of warehouses, with an explicit client setting taking
precedence so customers can force either protocol.

## Context

- In universe `cmdexec/sql-exec-api`, `SqlExecBackendRouter` routes APC
  `backend_compute` to Reyden behind `databricks.sqlexecapi.enable_apc_reyder_routing`.
  Reyder is the SEA service; Reyden is its runtime backend. APC lacks
  `GetStatementStatus`, so polling uses `GetStatementResult`.
- SC-256188/SC-256186 gate live metadata and termination; SC-254478 covers
  per-statement config, not T8's CreateSession override. T2–T7 establish the
  remaining Reyder command and native SHOW contract before JDBC exposure.
- Connector-service returns sub-flags at
  `/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`;
  `FeatureFlagsHandler.scala` may allowlist JDBC partners, so Q9 requires an
  APC-wide exclusion.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
  (API contract, Phase II, metadata). AC87 verifies the deployed SQL-size
  boundary; server rollout begins opt-in and rejects non-OSS-JDBC clients.
- Driver baseline: `origin/main` @ `840f3a407` (spec #1809; bounded SEA default
  #1827). APC routes to Thrift. SEA uses warehouse paths, `FAST` sessions, and
  `/status` heartbeats; its statement classifier treats every `SET` as a ResultSet.
  `DatabricksConnection.getConnectionId()` calls `SessionId.create()`, which
  assumes an APC session has a Thrift handle. Saved session IDs do not encode
  an APC SEA target; raw public statement IDs encode no target. Reyder's APC
  response has no session version (R6).
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the
  prototype baseline; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756) is unchanged.
- Parity baseline for review and release uses the same driver build on all routes. A
  difference needs its cited limit or warehouse precedent, measured caller impact,
  and approval under the shared gate below.

  | Behavior | APC Thrift | Warehouse SEA on `origin/main` | APC SEA; reason and caller impact | Disposition |
  | --- | --- | --- | --- | --- |
  | Routing | APC always Thrift | Explicit `UseThriftClient=0` selects SEA before Arrow check | Arrow prerequisite precedes explicit setting; dedicated APC flag otherwise decides (R1). | APC Arrow exception; AC1. |
  | Paging, close, IDs | Thrift target and saved ID | SEA warehouse target and saved ID | APC targeting persists through continuation/close; saved close gains its original target at T44. | Interim ID error then restoration; AC3/25/152. |
  | Results and tuning | Thrift streaming and fetch-block settings | SEA paging, bounded/legacy providers | APC uses bounded linked delivery at either API setting, counts inline chunks rather than batches, and follows the actual LZ4 manifest. | Server/warehouse precedents; AC31/39/101/109. |
  | Download modes | Thrift inline/linked delivery | Hybrid `0` external-only; bounded setting changes inline format | APC hybrid `0` still requests hybrid; download-off uses inline `ARROW_BATCH` and rejects typed complex getters. | Server-format exceptions; AC112/145/156 measures caller impact. |
  | Pools, retries, timeouts | Thrift HTTP controls | SDK API and separate link controls | APC follows the warehouse split; accepted write retries can leave commit uncertain. In-budget calls must survive shorter request timeouts. | Warehouse precedent; AC30/46/51/60/63/84. |
  | Idle cursor | Thrift heartbeat | SEA `/status` heartbeat | APC disables heartbeat because status RPC is unsupported. | Server exception; AC136 lifetime gate. |
  | Native SHOW | Native RPC | Native-shaped SHOW, Arrow/HIVE | APC uses seven inline SHOW operations; null-catalog tables and exported keys may be typed empty. | Warehouse precedent, with row-impact gates AC16/54/105 and load gate AC61. |
  | Partial metadata fan-out | Compare live APC Thrift rows and errors | Per-catalog schema/column `SQLException` warns and omits that catalog | APC follows warehouse SEA; caller loses only failed-catalog rows and the connection remains usable. | AC83 records omitted row counts and types against both routes; timeout/interruption still fails. |
  | Procedure/function metadata | Query failures propagate; function switch chooses RPC/SHOW | Query failures propagate; SHOW at either switch value | APC propagates procedure failures, uses SHOW for functions, and defaults to require-native on supported operations. | Warehouse precedent; AC65/75/82. |
  | Batches and commands | Per-set counts; `SET` classified as ResultSet | Scalar/native binding; same `SET` classifier | APC V10 gives `SUCCESS_NO_INFO`; no-result assignment returns false. | Server/wire exceptions; AC17/52/102. |
  | Session, size, telemetry | Thrift session and errors | `FAST`/version, SEA telemetry | APC uses `DEFAULT`/no version and the deployed UTF-8 SQL-size limit; preserve error identity and redaction. | Server exceptions; AC10/23/85/87/89/106. |

## Requirements

- R1: APC routing to SEA is controlled by a dedicated APC client-config flag (Q8), separate from
the warehouse flag `enableSqlExecForJdbc`, with this precedence: Arrow unavailable → Thrift,
including when `UseThriftClient=0`; otherwise an explicit `UseThriftClient` value wins (`0` → SEA,
`1` → Thrift); when `UseThriftClient` is unset, explicit
`TreatMetadataCatalogNameAsPattern=1` → Thrift; otherwise the APC flag decides (ON → SEA, OFF,
absent, or lookup failure → Thrift). The explicit setting's precedence over the metadata parameter
matches warehouse behavior, but Arrow-first is APC-specific: warehouse routing remains unchanged
and still lets explicit `UseThriftClient=0` select SEA before its Arrow check.
`enableSqlExecForJdbc` has no effect on APC routing. When explicit `UseThriftClient=0` selects SEA
alongside `TreatMetadataCatalogNameAsPattern=1`, the Thrift-only property is ignored and metadata uses
normal SEA SHOW-command semantics (AC148). This applies to HTTP paths with a concrete
`/o/<org>/<cluster>` target; the legacy `cliservice` path has only placeholder `default` ids and
continues on Thrift rather than sending an invalid APC SEA target.
- R2: APC SEA uses the standard REST bindings below, never a warehouse-scoped binding. Each
request targets `backend_compute.all_purpose_cluster_compute.cluster_id` exactly once and omits
`warehouse_id`. Paths are relative to the workspace host.

  | Operation | Method and APC URL | Target placement |
  | --- | --- | --- |
  | CreateSession | `POST /api/2.0/sql/sessions` | JSON body |
  | ExecuteStatement | `POST /api/2.0/sql/statements` | JSON body |
  | GetStatementResult (poll) | `GET /api/2.0/sql/statements/{statement_id}` | URL-encoded dotted query field |
  | GetResultData | `GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}` | URL-encoded dotted query field and `row_offset` query field |
  | CancelStatement | `POST /api/2.0/sql/statements/{statement_id}/cancel` | JSON body |
  | CloseStatement | `DELETE /api/2.0/sql/statements/{statement_id}` | URL-encoded dotted query field |
  | TerminateSession | `DELETE /api/2.0/sql/sessions/{session_id}` | JSON body |

The dotted field is `backend_compute.all_purpose_cluster_compute.cluster_id`.
GET and CloseStatement DELETE have no body; polls omit `chunk_index` and chunk
fetches require it. Connection close surfaces TerminateSession rejection.
Presigned links and standard/Real-time warehouse serialization stay unchanged;
dogfood termination requires the tracked server binding fix.

- R3: APC execution polls with `GetStatementResult`. Result heartbeats are disabled even with
`EnableHeartbeat=1`: APC has no `GetStatementStatus`, and a post-completion result poll can
re-download payloads. No APC `/status` request is sent. An idle, partly read APC SEA ResultSet
must remain readable for the same observed interval as its paired APC Thrift cursor with
heartbeats enabled; a shorter lifetime is a documented server-limit exception requiring
measured caller impact and approval before release (AC136).
- R4: APC non-metadata execution sends `format=ARROW_BATCH`,
`disposition=INLINE_OR_EXTERNAL_LINKS`, and `session_id`, never `statements`.
Decode all bare batches in `chunk.arrow_batches` and complete Arrow IPC streams
at external links, retaining link headers and every row. `EnableQueryResultDownload=0`
keeps SEA and sets string-valued CreateSession
`session_confs["can_cloud_download"]="false"`; live results must remain inline
once T8/AC119 proves it overrides the APC per-statement capability; until then
inline-only gates remain open.
Initial inline-size failure prevents ResultSet success; later fetch failure
interrupts iteration without retracting delivered rows. Hybrid `0` stays gated
by AC145: APC still requests hybrid because Reyder lacks external-only delivery.
`UseBoundedSeaApi` changes no APC request/session setting. Positive
`setMaxRows` sends `row_limit` across pages/links; zero is unlimited.

Successful no-result DDL, `SET AUTOCOMMIT = TRUE/FALSE`, `COMMIT`, `ROLLBACK`,
and no-result `CALL` have no terminal schema, chunks, links, continuation, or
data (the manifest may be absent), distinct from a zero-column schema. Direct
and polled completion expose no ResultSet, count zero then `-1`
after `getMoreResults()`, and issue no fetch. APC assignment `execute` returns
`false`, unlike the existing-route `SET` classifier; preserve connection side
effects and measure caller impact under AC52.
Result-producing `SELECT`, `SHOW`, `VALUES`, unassigned `SET AUTOCOMMIT`, and
`NonRowcountQueryPrefixes` matches require a schema. Explicit
`total_row_count=0` without payload or continuation is a typed empty ResultSet.
Absent count requires first-chunk fetch even with an empty list; missing terminal
evidence or declared rows is an error. Counted `INSERT`, `UPDATE`, `DELETE`,
and `MERGE` require a `num_affected_rows` column and value, including zero. A matching
`NonRowcountQueryPrefixes` command instead exposes the count ResultSet and update
count `-1`, and stops a Statement batch with prior counts and the existing error.

APC advertises LZ4 decompression. `QueryResultCompressionType` and its
higher-precedence `EnableQueryResultLZ4Compression` alias request numeric `0`
(`NONE`) or `1` (`LZ4_FRAME`), but Reyder ignores the request. Decode the
manifest's actual codec; unmarked means uncompressed. `NONE` cannot force
uncompressed APC results, and invalid settings retain the warehouse LZ4 default
(AC101). For failed SQL,
keep SQLException class, SQLState, and vendor code where APC Thrift and warehouse
SEA agree; otherwise follow warehouse SEA and document caller impact (AC89).
- R5: Seven APC native metadata SHOW operations (`GET_CATALOGS`, `GET_SCHEMAS`,
`GET_TABLES`, `GET_COLUMNS`, `GET_FUNCTIONS`, `GET_PRIMARY_KEYS`,
`GET_CROSS_REFERENCE`) send `X-Databricks-Metadata-Operation-Type` with the
corresponding case-sensitive `GetCatalogs`, `GetSchemas`, `GetTables`,
`GetColumns`, `GetFunctions`, `GetPrimaryKeys`, or `GetCrossReference` value.
They request `ARROW_STREAM` and `INLINE_OR_EXTERNAL_LINKS` at either hybrid
setting. SEA forwards the operation and fully-sync headers to Reyder, which
honors fully-sync only for synchronous execution; APC require-native stays at
the JDBC-to-SEA hop. These operations are inline/link-free, including at hybrid
`0`, but may span R12 continuation pages (AC16/61).
On APC SEA, unset or explicit `EnableThriftNativeMetadata=1` sends
`X-Databricks-Require-Thrift-Native-Metadata: true` for supported SHOW; `0`
omits it. Unset warehouse SEA depends on its server flag, even when an explicit
override selects SEA.
Reyder still returns native-shaped metadata, so conversion follows
`manifest.is_native_metadata_result`, not the request setting. Synchronous
SHOW sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only at
`EnableSeaSyncMetadata=1`; async SHOW omits it. Decode each response as Arrow
IPC `chunk.attachment` or HIVE `chunk.columnar_data`/`COLUMN_BASED_SET`.
`getImportedKeys` and `getCrossReference` share `GET_CROSS_REFERENCE` but remain
distinct: the first queries a table's foreign keys; the second filters the
foreign table's keys by parent. For cross-reference, a null foreign table
returns typed empty before request; null/empty parent table or empty-string
foreign table is rejected.
Error outcomes follow the warehouse method: catalog syntax errors propagate;
null-catalog schema and column syntax errors trigger catalog fan-out;
null-catalog table syntax errors return typed empty without fan-out;
function and primary-key syntax errors propagate; imported-key and
cross-reference syntax errors return typed empty. Object-not-found returns
typed empty for schemas, tables, columns, functions, primary keys, imported
keys, and cross-reference; catalog errors propagate. Other SQL failures
propagate, except existing table/column empty-pattern handling and the
documented fan-out omission rule below (AC16/83/105/114).

`enableMultipleCatalogSupport=0` retains current-catalog filtering; `1`
permits eligible catalogs. `EnableMetricViewMetadata=1` adds
`spark.sql.thriftserver.metadata.metricview.enabled=true` to session config
and exposes `METRIC_VIEW` table types and eligible rows; `0` omits both.
`getExportedKeys` is outside native SHOW. APC Thrift's parent-only
`GetCrossReference` returns no rows on the documented DBR service; the SEA
contract has no parent-only operation. APC SEA returns warehouse SEA's typed
empty result without an exported-key SHOW request, subject to AC54's live parity
check. With `enableMultipleCatalogSupport=0`, current-catalog filtering may
issue `SELECT CURRENT_CATALOG()` first; with `1`, the empty result needs no
catalog lookup. A null
table raises `SQLException` before request with SQLState `42000` and `EXECUTE_STATEMENT_FAILED`
(1003).
Session-reachable async metadata has typed ResultSet handles for seven SHOW
operations, with separate imported-key and cross-reference calls. It submits
at `wait_timeout=0s`, returns while RUNNING, and preserves synchronous await,
timeout, cancel, and conversion outcomes. Version and saved IDs do not gate it;
public JDBC metadata stays synchronous.

Internal catalog/schema SELECTs and parameterized `getProcedures` and
`getProcedureColumns` SELECTs are ordinary hybrid `ARROW_BATCH` queries without
metadata headers. Reyder rejects the procedure headers sent by warehouse SEA;
APC procedure queries use R6 scalar binding and cannot use async SHOW. T28
establishes the shared version-aware binding path before T19 exposes either method.
The public `getProcedures` and `getProcedureColumns` methods propagate query,
fetch, and decode failures, matching APC Thrift and warehouse SEA. A typed
empty result is reserved for the existing catalog-access filter, not a failed
query or result conversion (AC75).
`EnableShowCommandForGetFunctions` still selects native RPC or SHOW
on APC Thrift, whose SHOW path returns typed empty for null catalog; APC SEA,
like warehouse SEA, uses SHOW at either setting (AC82).

Async null-catalog schema/column `42601` triggers per-catalog SHOW without
sync headers. A per-catalog `SQLException` warns and omits that catalog;
timeout/interruption or initial/enumeration error fails the call, except
established typed empties. Partial results keep the connection usable (AC83).
Null-catalog `getTables` instead returns typed empty without fan-out (AC105).
Each fan-out future retains its independent 90-second wait. Positive
`MetadataOperationTimeout` bounds the whole call/await; the first deadline wins.
`0` disables only the overall deadline (AC83/124).

- R6: APC CreateSession explicitly sends `execution_mode=DEFAULT` and leaves
`session_version` absent on later ExecuteStatement requests, matching Reyder's
absent version. `FAST` is reserved for sessions that return an initial version
and can echo the maximum observed version; the current warehouse FAST behavior
is unchanged. AC23 captures session mode, response, and later requests; APC `FAST` without
a version fails the gate. CreateSession `negotiated_protocol_version` alone grants native scalar
parameters at V8 (42248) and opt-in `EnableNativeBatching=1` INSERT batches
at V10 (42250). It does not gate R5 async metadata. Reyder leaves `session_version` unset; neither missing versions nor saved IDs
grant capability. Retain negotiated protocol version. Reject complex bindings
before request with a tested driver error; `toString()` is not lossless SQL. At `supportManyParameters=0`,
V8+ uses native scalar `parameters` and older/versionless sessions interpolate
supported scalars without unresolved markers; V8–V9 legacy batch sets may use
native scalars. At `supportManyParameters=1`, single and legacy batch sets
interpolate at every version and non-null `setBytes` is permitted; at `0`,
non-null `setBytes` remains rejected. Keep missing-version telemetry distinct.

Opt-in native V10 INSERT batches send ordered nested `parameter_sets` and no
top-level `parameters`.
Its aggregate count yields `Statement.SUCCESS_NO_INFO` for each successful
input set; unlike warehouse native SEA, APC V10 cannot return per-set counts.
If execution fails, a `BatchUpdateException` marks every set
`Statement.EXECUTE_FAILED`; commit status remains uncertain, with no atomic
rollback claim. If execution succeeds but JDBC counts cannot be read, raise
the distinct result error without failed-write counts, state that rows may
already be committed, and clear the batch so retry cannot duplicate them.
Below V10 or native-off,
`EnableBatchedInserts=1` combines eligible INSERT sets. For interpolated sets
(`supportManyParameters=1` at every version, or `0` before V8), `BatchInsertSize`
bounds rows per chunk; for native-scalar legacy sets (`supportManyParameters=0`
at V8–V9 or V10 with native batching off), the existing 256-parameter limit
bounds rows per chunk and `BatchInsertSize` has no effect. Ineligible sets
execute individually. Preserve ordered counts, stop-on-error, and the existing
uncertain-commit behavior when a prior chunk may have succeeded.

APC SEA validates SQL text at the deployed Reyder limit (default 16,777,216
UTF-8 bytes) for single statements and legacy generated chunks, without silent
split or fallback; an oversized request propagates the SEA size error. AC87
must prove the byte boundary before dependent batch gates close. No warehouse
validator change is required.
Pre-execution `PreparedStatement.getMetaData()` uses binding-free
`DESCRIBE QUERY` without losing bindings. Warehouse behavior stays unchanged.
- R7: R1 selects the protocol before connecting; `UseBoundedSeaApi` does not affect that
decision. After SEA is selected, server errors, including unsupported regions, propagate
without Thrift fallback. Warehouse-only KP001 Thrift-to-SEA recovery does not change APC routing.
- R8: When telemetry export is enabled, APC and warehouse SEA/Thrift connection attempts
report compute type, selected protocol, routing reason, and success/failure, including failed
opens. New events obey `TelemetryLogLevel` (including `OFF`), `EnableTelemetry`, the
server `enableTelemetryForJdbc` flag, and `ForceEnableTelemetry` exactly as existing
`TelemetryHelper` export does; `OFF` suppresses export even when forced. Log when explicit
SEA selection ignores `TreatMetadataCatalogNameAsPattern=1`. Preserve the existing
operation-latency, result-format, total-chunk, chunk-download-latency, and error telemetry
families for APC SEA execution, metadata, linked downloads, batches, and failures, with the
selected protocol and compute identity available for correlation. Compare event presence,
operation type, format, counts, error name/code, SQLState, and vendor code at each public
slice with APC Thrift and warehouse SEA (AC85). APC diagnostics and telemetry redact
org/cluster ids and compound session/statement ids, including execute and session logs;
no event leaks a raw target through a message or serialized identity.
- R9: The connection-only slice terminates its live APC SEA session; the first
SELECT slice also closes its open statement before session termination.
Until the saved-session close slice, public `getConnectionId()` on APC SEA raises
`DatabricksSQLFeatureNotSupportedException` with SQLState `0A000`, vendor code
`UNSUPPORTED_OPERATION` (1005), and the same telemetry error name, without
dereferencing a Thrift handle or logging the session ID.
Other routes
retain their existing IDs. The saved-session close slice makes public
`getConnectionId()` return an ID encoding the selected protocol and
target without requiring a Thrift handle for an APC SEA session. Given the original endpoint and credentials, saved IDs let
`Driver.closeConnection` close the original session after flag/target changes.
IDs encode neither endpoint nor credentials; changes to those are outside the guarantee. Public statement IDs remain raw.
Internal identities retain the compute target and reject malformed IDs without logging identifiers.
Decode legacy warehouse SEA/Thrift IDs. Legacy warehouse Thrift IDs encoded
as SEA on `origin/main` lack a secret and cannot support saved close. Recovered
statements target the current connection; only a live CreateSession version
grants capabilities.
- R10: APC ExecuteStatement keeps SEA `wait_timeout` unset for synchronous direct results and
uses `0s` for async or `EnableDirectResults=0` (alias `EnableSQLExecDirectResults=0`). Public SQL `executeAsync` and
`getExecutionResult` use APC targeting, polling, and decoding separately from async metadata.
Measure server request timeouts separately from JDBC waits, including the extra
`GetStatementResult` round trip (AC60, AC63).
- R11: Decode inline `ARROW_BATCH` from `manifest.schema` (`type_text`/`type_json`, including
nested types), without requiring `manifest.arrow_schema`; external links remain full Arrow IPC
streams. A deferred logical type fails before false ResultSet success. Cover
primitive, decimal, array, map, struct, interval, timestamp, timestamp_ntz, geography,
geometry, and variant values. Preserve
`EnableComplexDatatypeSupport` and `EnableGeoSpatialSupport` effects on JDBC metadata and
`getObject`: disabled support returns established strings and rejects typed complex getters;
enabled support permits linked typed getters and native geospatial values where applicable.
Inline typed complex getters still reject, so callers needing them require linked results.
Download-off APC `ARROW_BATCH` and these getter outcomes differ from warehouse SEA's
bounded-dependent `JSON_ARRAY`/`ARROW_STREAM` requests; AC156 measures caller impact
and the R17 load bounds at both settings.
- R12: Page APC results by `next_chunk_index` and `row_offset`, without requiring totals or
inline data in the initial response. When the initial manifest has no payload and
no explicit zero-row count, fetch chunk zero even if its chunk list is empty.
An empty fetched chunk is terminal zero-row evidence only with no continuation.
The next inline offset includes every batch in a chunk;
for linked results it follows the final link's end offset and continuation. Missing final-link
`next_chunk_index` ends paging.
- R13: Decode APC SEA `columnar_data` physical vectors and null masks using
`manifest.schema`; JDBC columns, values, and nulls match APC Thrift metadata.
Masks use least-significant-bit-first order and `1` for null. Null placeholders
consume positions; short/extra mask bits do not change row count. Unknown
vectors fail without partial success (AC157).
- R14: APC query timeout and cancellation match warehouse SEA, including targeted
CancelStatement. A valid query or metadata call within its configured client wait must not
fail early from a Reyder/SEA request timeout (AC60, AC63).
- R15: `Statement.addBatch(String)` / `executeBatch` on APC executes SQL commands sequentially,
one ExecuteStatement request per SQL string, preserving the existing stop-on-first-failure and
update-count behavior. A command configured by `NonRowcountQueryPrefixes` to return a
ResultSet stops the batch with the existing batch error and preceding counts. `MaxBatchSize`
rejects an excess `addBatch` before execution with the same validation error as APC Thrift
and warehouse SEA. The batch never sends the unsupported `statements` field.
- R16: SEA result formats include `ARROW_BATCH` for APC queries and `COLUMN_BASED_SET`
(wire value 6) for APC metadata while preserving warehouse wire values. Unknown formats
deserialize safely and fail clearly if decoded, without corrupting later responses
on the connection (AC158).
- R17: APC SEA follows warehouse SEA's retry, pool, and linked-download behavior
at either `UseBoundedSeaApi` value. APC lacks reliable continuation totals, so
linked results retain the bounded provider even at `0`; the warehouse provider
switch and APC Thrift inline switch do not change APC SEA rows, paging, or
prefetch. AC39 gates caller impact.
`ThriftMaxBatchesInMemory` bounds queued APC inline response chunks (minimum two),
whereas APC Thrift bounds batches. A chunk can contain several batches, so the
same setting can use more memory; paging must avoid materializing the full result
(AC109).
Case-insensitive `cloudFetchThreadPoolSize` (default 16) bounds per-result
linked workers and buffered chunks; `LinkPrefetchWindow` (default 128) bounds
fetched-ahead links only. `ChunkReadyTimeoutSeconds` bounds linked-chunk wait;
`RowsFetchedPerBlock` does not tune SEA paging.

Presigned links use the Thrift HTTP pool: ordinary 429/503 retries controlled by
`RateLimitRetry`/`TemporarilyUnavailableRetry` require `Retry-After`, while
`ApiRetriableHttpCodes` uses `ApiRetryTimeout` without that requirement.
`IdleHttpConnectionExpiry` applies to link and SDK API pools. API calls use
warehouse SEA's SDK retry policy, independent of link-handler controls; AC51
gates attempts, delay, final error, and caller impact per request class.

SDK 0.118.0 may retry an accepted 429/503 write POST. Surface warehouse SEA's
ambiguous-response SQLException without APC replay or at-most-once claims; AC151
warns callers to reconcile before resubmitting. No APC-only cap or full-result
buffer is allowed (AC30/46/51/84).
- R18: Each JDBC package exposes only caller workflows whose live gates passed.
Incomplete methods or settings fail before request when known; unsupported responses
fail before false success. This includes pages, links, hybrid `0`, download-off,
SQL async, timeout/cancel, SQL-backed `isValid`, prepared writes, and CALL until
their owning slice. Single prepared writes preserve counted-DML `executeUpdate`
and `execute` outcomes, including zero (AC146). A result-producing CALL exposes
its ResultSet; no-result CALL returns `execute=false`, no ResultSet, and count zero.
Unsupported OUT/named parameters and callable `executeUpdate` reject before request;
`executeQuery` on no-result CALL raises the existing result-set-expected error
(AC147). Guard other nondefault settings until their gate passes. No fallback after
SEA selection; flag-off APC stays on Thrift. Production assignment awaits the
assembled T-val gate and post-merge smoke.
- R19: `Connection.setAutoCommit`, `getAutoCommit`, `commit`, and `rollback` retain their
APC Thrift and warehouse SEA contracts on APC SEA. With default `IgnoreTransactions=1`,
set/commit/rollback are no-ops; `getAutoCommit` returns cached state unless
`FetchAutoCommitFromServer=1`, which executes `SET AUTOCOMMIT` and requires one state row.
With `IgnoreTransactions=0`, changed `setAutoCommit` executes `SET AUTOCOMMIT = TRUE/FALSE`,
`commit` executes `COMMIT`, and `rollback` executes `ROLLBACK` except for its local
auto-commit-mode error. Successful DML must commit or roll back as requested; missing result
data or server errors must not become success. Preserve `DatabricksTransactionException`
wrapping and cause SQLState/vendor code, including driver fallback error names when absent.

## Clarifications

- Q1: Reuse `UseThriftClient` or add `EnableApcSea`?
A1: Reuse `UseThriftClient` (`0` SEA, `1` Thrift), after APC's Arrow prerequisite. It precedes
catalog-pattern preference and the APC flag; warehouse routing is unchanged (R1).
- Q2: Fall back to Thrift after an APC SEA routing error?
A2: No. Propagate SEA errors as warehouse SEA does.
- Q3: How should Arrow IPC and HIVE `columnar_data` metadata coexist?
A3: Decode Arrow IPC or HIVE `chunk.columnar_data` per response, including DD2/DD4, not by
requested format or connection lifetime (R5, R13).
- Q4: Emulate batches below V10 or reject them?
A4: Emulate through the existing legacy executor, including eligible multi-row INSERTs;
`BatchInsertSize` bounds interpolated chunks and the parameter limit bounds native-scalar
chunks. Only opt-in V10 uses native `parameter_sets`. Statement SQL batches stay
sequential without `statements` (R6, R15).
- Q5: Add a client denylist for govcloud/non-Reyden regions?
A5: No. Keep the flag off there and propagate any SEA rejection without fallback.
- Q6: Add a special User-Agent or capability header?
A6: No. Preserve the ordinary User-Agent; CreateSession path and `backend_compute` identify APC.
- Q7: Is manifest `type_text`/`type_json` sufficient for all Arrow types?
A7: AC13 must prove lossless reconstruction; otherwise block release for a Reyder
`arrow_schema` contract and driver support. Never coerce silently or switch to Thrift.
- Q8: Share `enableSqlExecForJdbc` or add a dedicated APC flag?
A8: Add default-off sub-flag
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` for independent
ramp/rollback. Older drivers ignore it; no connector-service version filter is needed.
- Q9: Force the APC flag false for partner User-Agents despite Reyder's OSS prefix allowlist?
A9: Yes, including JDBC partners allowed by `enableSqlExecForJdbc`. Deploy T1 before APC
assignment; partner enablement needs a separate rollout decision (R1, T1).
- Q10: How is repeated transfer avoided during `GetStatementResult` polling?
A10: At least three RUNNING polls of a large query transfer no result payload; terminal
response and fetch return all rows once (AC27).
- Q11: Should APC SEA procedure metadata suppress query, fetch, and decode errors?
A11: No. Propagate them with their SQLState and vendor code, matching APC Thrift
and warehouse SEA. This supersedes the earlier typed-empty-on-error wording.

## Acceptance Criteria

At first exposure, each public slice proves its completed call on scoped flag-ON APC staging
against APC Thrift and warehouse SEA where relevant. AC3/11/40/51/84/85 apply to
new request classes, AC30 to new pools, AC10 to new DEBUG paths, and conditional AC106
to changed driver-owned errors. T37/AC144 precedes read-fault AC51 checks;
T2/AC113 and AC154 precede write-fault checks. Fixtures cannot replace required live checks. Retired AC IDs
(AC6/8/33/41/47/48/86/103/104/110/111/115/116/120/121/123/125/126/127/128/129/130/131/133/134/135/137/143/149/150/153) are not reused.

The shared paired-load gate (AC25/31/32/46/52/61/75/107/117/118) uses the same
driver build, staging region, warmed cluster, data shape, and one/32 callers on
APC SEA and Thrift. At 32 callers, aggregate SEA/Thrift throughput is at least
0.90; median latency is at most 1.10, p95 latency at most 1.20, slowest completed
caller at most 1.50 times its paired Thrift bound, and peak heap at most 1.20.
Every caller must complete or return its expected bounded error; a stalled caller
fails the gate. AC31/32 also bound retained heap growth across two data sizes to
max(8 MiB, 5% of added bytes). Keep per-caller durations, aggregate throughput,
fixture sizes, and errors; the test plan owns sampling. For a proposed non-functional
exception, also measure warehouse SEA/Thrift on the same build and comparable
workload. Record both deltas, precedent or server limit, caller impact, and parity
matrix disposition. PECO/Perf Eng must approve measured exceptions before release.

For each native SHOW operation, the shared metadata gate is a live direct and
polled run in repeatable Arrow and HIVE conditions, with R5 headers, native mark,
inline/link-free delivery, full continuation, paired JDBC rows and types, and
the AC61 load comparison at both hybrid settings. A live HIVE `GET_TABLES` result with a null
optional cell must also match paired values and `wasNull()`; AC157 retains
malformed-mask fixtures. AC138–142 establish server wire readiness; AC16 closes
each synchronous caller gate and AC50 closes typed async handles. The test plan
records fixtures, DBR settings, and per-method runs.

- AC1: GIVEN scoped flag-ON/OFF APC staging WHEN R1 overrides and metadata preference vary THEN SELECT route, flag value, partner exclusion, rows, and no-fallback match R1 and paired warehouse behavior. Fixtures cover Arrow absence, legacy `cliservice`, and lookup failure.
- AC2: GIVEN flag-on APC staging at bounded `1` and hybrid `1` WHEN single-chunk, RUNNING, manifest-only, and explicit zero-row SELECTs run THEN APC-targeted hybrid Arrow returns paired rows/types/empties and closes; polling and first fetch follow R4/R12 without false success. Fixtures cover absent `arrow_schema`; AC158 covers unknown formats; AC145 gates alternate settings.
- AC3: GIVEN a live APC statement owned by its connection WHEN it polls, fetches, and closes THEN captures show R2 target placement, exactly-once close, connection usability, and unchanged warehouse serialization. AC18 covers cancel; AC152 covers recovered statements.
- AC9: GIVEN live APC SEA CreateSession rejection WHEN connecting THEN the error and failed
route event propagate without Thrift fallback. GIVEN APC Thrift OpenSession returns KP001 WHEN
connecting THEN warehouse-only recovery does not select SEA.
- AC10: GIVEN successful and failed opens on APC SEA, APC Thrift, warehouse SEA, and warehouse Thrift WHEN DEBUG logs and enabled telemetry are captured THEN each attempted route emits compute type, selected protocol, routing reason, success/failure, and error identity when failed, without credentials, presigned secrets, or raw target/session/statement IDs. Compare event presence and suppression under R8; primitive statement diagnostics retain the same redaction.
- AC11: GIVEN standard and Real-time warehouse connections WHEN equivalent R2 operations run THEN JDBC outcomes and URL/body/header serialization, including `realtimeSession`, match pre-change `origin/main`; each slice checks affected classes.
- AC12: GIVEN APC synchronous SELECT with `EnableDirectResults=1` and `0` WHEN paired callers drain rows THEN `wait_timeout` is absent and `0s` respectively, no `statements` field is sent, rows/errors match, and each setting passes AC31's load gate. Record first-row latency and request counts; disposition protocol deltas under the shared exception rule.
- AC13: GIVEN R11's primitive and logical types WHEN inline and linked getters run
with complex/geospatial settings at 0/1 THEN types and values match both baselines;
inline typed complex getters reject, enabled linked getters work, and disabled modes
return established strings. Unavailable live types remain pending despite fixtures.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN a deployed native SHOW operation WHEN its synchronous JDBC method runs THEN rows, types, syntax errors, and typed empties pass the shared metadata gate against warehouse SEA and applicable APC Thrift behavior; AC105 and AC114 govern documented differences.
- AC17: GIVEN live V10 APC and paired routes WHEN native INSERT batches succeed or fail THEN ordered `parameter_sets` yield R6 counts and committed-row evidence. A post-write count fault raises a distinct result error, clears the batch, and reports uncertain commit. Apply AC154 and the shared load gate.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. Compare elapsed time, SQLState/vendor code, target, and
post-timeout connection usability on flag-ON staging.
- AC22: GIVEN a live Statement batch with a failing middle command or a `NonRowcountQueryPrefixes` match after prior commands WHEN APC executes it THEN requests stay sequential without `statements`, preceding counts retain order, and execution stops with the paired batch error. AC32/64/66 cover load and settings.
- AC23: GIVEN live versionless APC CreateSession WHEN its next statement executes THEN the session sends `DEFAULT`, later requests omit `session_version`, and only negotiated protocol version grants R6 capabilities. Warehouse SEA retains `FAST` and version echo; record missing-version diagnostics.
- AC25: GIVEN new APC and warehouse session IDs and the original endpoint/credentials WHEN saved close follows flag or target changes under paired load THEN only the original session closes. Legacy IDs decode, malformed IDs fail without leaks, and R9’s irreparable legacy Thrift IDs are recorded.
- AC27: GIVEN the first public SELECT slice and a live single-chunk APC query with three or more RUNNING polls WHEN transfer
is measured on explicit-override and flag-on routes THEN polls carry no result payload and
the terminal fetch delivers every row once; compare APC Thrift and warehouse SEA traffic.
- AC28: GIVEN the T1 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN flag-on APC staging and paired routes WHEN a complex binding is submitted to a single statement or an exposed prepared batch THEN APC SEA raises its named/numeric driver error before any request, keeps the connection usable, and records its identity against both baselines. The owning PR records an owner-verified taxonomy/dashboard update or maintainer handoff.
- AC30: GIVEN flag-on APC staging and paired APC Thrift/warehouse SEA under pool saturation WHEN `HttpConnectionPoolSize`, `HttpMaxConnectionsPerRoute`, and `HttpConnectionRequestTimeout` vary for each new API or linked-download class THEN measured in-flight count, wait, error identity, and per-caller completion pass the shared load gate for the applicable pool; SDK API acquisition ignores the Thrift HTTP wait setting.
- AC31: GIVEN each SELECT mode at its first public exposure WHEN one and 32 paired APC SEA/Thrift callers drain primitive, multi-page, or linked results as applicable THEN rows/errors match, SEA paging ignores `RowsFetchedPerBlock`, and the shared load gate passes. The release gate repeats the combined mode mix.
- AC32: GIVEN each live Statement, legacy prepared, or V10 batch mode WHEN one and 32 paired callers use disjoint keys THEN ordered counts, rows, errors, version, and shared load bounds pass. Repeat the combined mix at release.
- AC34: GIVEN scoped flag-ON staging and default-OFF configuration WHEN connector-service looks up the APC sub-flag for ordinary, partner, and allowlisted JDBC partner User-Agents THEN only the ordinary scoped agent receives true; default lookup and both partner lookups return false. Record flag version and values.
- AC36: GIVEN APC chunks with multiple Arrow batches and inline continuations WHEN JDBC drains them THEN rows appear once and `row_offset` includes all batch rows; a failed page cannot report success. If live staging lacks multiple batches, retain a fixture and leave live proof pending.
- AC39: GIVEN each exposed result mode WHEN bounded and streaming switches vary on paired callers THEN APC requests and provider choice follow R17, rows match, and first-row latency, heap, and link expiry pass AC31 or have an approved exception.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF or scoped ON THEN flag-OFF APC stays on Thrift, completed flag-ON calls pass their live gate, and incomplete calls fail before request or false success without fallback. Production assignment waits for T-val.
- AC45: GIVEN linked or inline APC results WHEN `EnableSeaResponseCompression` varies under paired load THEN only `0` sends API `Accept-Encoding: identity`; links remain unchanged, and bytes, requests, rows, and load pass the shared gate.
- AC46: GIVEN paired linked results WHEN `cloudFetchThreadPoolSize`, `LinkPrefetchWindow`, chunk wait, and slow-link settings vary THEN workers/chunks remain within the per-result worker bound and fetched-ahead links within the window. All 32 callers finish or return their expected bounded error under the shared throughput and tail-latency gate; inline pages stay unaffected.
- AC49: GIVEN live SELECT modes WHEN `setMaxRows` is zero, one, or a page boundary THEN positive limits send `row_limit` and cap all pages, while zero remains unlimited. Before this gate, nondefault limits stay guarded.
- AC50: GIVEN each typed native SHOW handle WHEN delayed live SHOW submits with `wait_timeout=0s` THEN it returns while RUNNING, permits another call before await, and passes the shared metadata, timeout, cancel, cleanup, and header gates.
- AC51: GIVEN AC144 WHEN scoped 429/503 faults with/without `Retry-After` affect each new SDK or link request class THEN attempts, delay, errors, and caller outcome follow R17 and paired routes. Writes also require AC154 before exposure.
- AC52: GIVEN deployed AC113 WHEN live direct/polled no-result commands and counted DML including zero execute THEN R4 ResultSet/count/fetch shape, rows, and errors match paired routes, except the documented assignment boolean. Missing schema or count fails without breaking the connection.
- AC53: GIVEN live prepared scalars or an exposed fallback batch plus older/versionless fixtures WHEN `supportManyParameters` is 0 or 1 THEN R6's native/interpolated request choice has no unresolved markers, `setBytes` follows R6, and rows/counts match paired routes. Record fixture-only branches.
- AC54: GIVEN the documented DBR service WHEN live `getExportedKeys` runs at both catalog settings THEN APC sends no exported-key SHOW and returns paired empty JDBC columns; capture any current-catalog lookup at `0`. A nonempty Thrift result needs impact disposition; a null table fails before request with SQLState `42000` and code 1003.
- AC55: GIVEN public APC SEA SQL async SELECT WHEN primitive and linked results complete
THEN `wait_timeout=0s`, targeting, rows/errors, cancel, close, and diagnostics follow R2/R10.
Apply AC31's one/32-caller load bounds to this public path before unguarding it.
- AC60: GIVEN a valid live query within its configured client wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Early timeout is a
correctness failure; only a measured latency difference may receive a documented exception.
- AC61: GIVEN each nonempty metadata method or typed handle WHEN one and 32 paired callers run THEN rows/errors and the shared load gate pass, null-catalog requests number at most N+2, and decode CPU per 1,000 cells and peak RSS are at most 1.20 times APC Thrift absent an approved exception.
- AC63: GIVEN a live delayed synchronous SHOW within its configured positive metadata wait WHEN it exceeds one SEA request timeout THEN it completes through polling without premature failure and leaves the connection usable. At overall timeout `0`, only the overall bound is disabled; AC83's per-future wait remains. An early timeout fails the gate.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching INSERT runs through public Statement THEN `execute` returns true, its ResultSet contains the affected-row value, and `getUpdateCount` is `-1`.
- AC65: GIVEN each synchronous APC SHOW at first exposure WHEN native metadata is unset/0/1 and sync, hybrid, and bounded settings vary THEN R5 headers and AC16 rows match paired routes, including Arrow/HIVE equality; warehouse unset-header behavior and AC148’s metadata-pattern override are recorded.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized procedure metadata WHEN queries read pages/links or encounter query, fetch, or decode failure THEN header-free rows pass AC61 or errors retain agreeing paired SQLState/vendor code (otherwise warehouse identity with caller impact); failures never become typed empty. Older/versionless bindings use fixtures.
- AC82: GIVEN live `getFunctions` with explicit or null catalog WHEN `EnableShowCommandForGetFunctions` is 0 or 1 on paired routes THEN APC SEA uses SHOW at both values and follows warehouse SEA rows/errors; record APC Thrift's native/SHOW and null-catalog differences with caller impact.
- AC83: GIVEN live synchronous or typed async null-catalog schema/column `42601` fan-out WHEN one catalog raises `SQLException` THEN APC and warehouse SEA warn, omit only that catalog, preserve other rows and usability, and record omitted counts/types against APC Thrift. Future timeout/interruption fails the call; 90-second per-future waits remain at overall timeout `0`.
- AC84: GIVEN each newly exposed live SDK API or linked-download request class WHEN socket, poll, and idle settings vary THEN `socketTimeout` bounds one request, `asyncexecpollinterval` changes later-poll delay, and `IdleHttpConnectionExpiry` evicts idle connections in each applicable pool. In-budget calls complete with paired timing/error identity and usable connections.
- AC85: GIVEN each newly exposed path WHEN telemetry toggles vary THEN R8 event families and error identity match paired routes or have an approved exception; disabled export suppresses them. DEBUG and events redact credentials, presigned secrets, raw targets, and compound IDs.
- AC87: GIVEN Reyder’s default 16 MiB SQL limit WHEN APC sends ASCII and multibyte SQL at 16,777,216 and 16,777,217 UTF-8 bytes THEN the first is accepted and the second gets the SEA size error without fallback; a different boundary leaves the gate open.
- AC88: GIVEN two accessible catalogs and a metric view WHEN `enableMultipleCatalogSupport` and `EnableMetricViewMetadata` vary on all three live routes THEN catalog access, discovery, table types/rows, and APC session setting match R5; record any server-limited row difference and caller impact.
- AC89: GIVEN live invalid SQL, missing-object, or permission failure WHEN exposed execution methods surface it THEN SQLException class, SQLState, vendor code, and message category match agreeing baselines or warehouse SEA mapping with caller impact; no failed command reports success.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN live inline/linked results WHEN codec property or alias is `0`, `1`, or invalid THEN decoding follows the manifest, including unmarked and LZ4 despite `0`; invalid retains warehouse default. Rows and load pass; record actual codec, bytes, CPU, and impact.
- AC102: GIVEN an isolated live table WHEN transaction and auto-commit settings vary THEN R19 state, committed/rolled-back rows, counts, and errors match paired routes. Public no-result commands have no fetch, count zero then `-1`; missing state or failed commands never succeed.
- AC105: GIVEN live synchronous or typed async null-catalog `getTables` receiving `42601` WHEN APC SEA, APC Thrift, and warehouse SEA are compared THEN APC returns warehouse SEA’s typed empty result without fan-out; record omitted Thrift row counts/types and caller impact.
- AC106: GIVEN a changed driver-owned APC SEA error WHEN it reaches JDBC THEN a test asserts its `DatabricksDriverErrorCode` name and unique number, and the PR records owner-verified telemetry taxonomy/dashboard updates or maintainer handoff and completes the telemetry-error template. Server errors retain server identity.
- AC107: GIVEN paired one/32-caller staging WHEN SELECT repeatedly opens, leaves an operation open, and closes THEN statement close precedes session termination, resources return to baseline, retained heap grows at most 8 MiB, and the shared cycle-latency gate passes. Repeat at release.
- AC108: GIVEN AC87 passed and live legacy APC INSERT batches WHEN batching and binding settings vary THEN R6 chunk limits, per-set counts, committed rows, errors, and AC32 paired load pass for each binding mode/version. Oversized chunks follow AC132.
- AC109: GIVEN concurrent live multi-page inline SELECTs WHEN `ThriftMaxBatchesInMemory` varies around two THEN queued chunks obey R17, all rows arrive once, and AC31 memory/load bounds pass; record chunks, batches, bytes, and heap against APC Thrift.
- AC112: GIVEN AC119 WHEN download-off APC SELECTs drain at either bounded value THEN CreateSession sends `can_cloud_download=false` and every page stays inline and lossless; initial size failure prevents ResultSet success and later-page failure interrupts iteration.
- AC113: GIVEN deployed T2 on scoped staging WHEN direct/polled no-result DDL, transaction commands, and counted DML including zero complete THEN wire captures show R4 terminal shapes and errors. T14 must prove AC52 before caller exposure; fixtures cannot close this wire gate.
- AC114: GIVEN a child with two parents and a second foreign table WHEN live imported-key and cross-reference methods run THEN each filters `GET_CROSS_REFERENCE` to its own paired JDBC rows; null foreign table is typed empty, invalid inputs fail before request, and syntax/object-not-found is typed empty.
- AC119: GIVEN deployed T8 WHEN CreateSession disables cloud download while ExecuteStatement advertises download capability THEN live single- and multi-page APC results remain inline only; capture both wire values. A link or unproved override keeps AC112 open.
- AC117: GIVEN paired bound SELECT callers WHEN `PreparedStatement.getMetaData()` runs before execution THEN metadata matches paired routes, bindings survive execution, and request count, latency, throughput, and heap pass the shared load gate.
- AC118: GIVEN paired live callers with `EnableSQLValidationForIsValid=1` WHEN `isValid(timeout)` repeatedly runs `SELECT VERSION()` THEN healthy/failed booleans, timeout, connection usability, SELECT counts, median call latency, calls per second, and peak heap pass the shared load gate. Fixtures cannot close it.
- AC122: GIVEN live two-parent and second-foreign-table keys WHEN typed async imported-key and cross-reference handles complete THEN each independently passes AC114 and the per-handle AC50/61 gates.
- AC124: GIVEN exposed synchronous SHOW or typed async metadata WHEN `MetadataOperationTimeout` varies THEN positive values bound the full call/await and target cancel, while `0` disables only the overall bound; AC83’s 90-second futures remain. In-budget calls survive shorter SEA request timeouts and timeout leaves the connection usable.
- AC132: GIVEN AC87 passed and a live legacy INSERT chunk at the UTF-8 SQL-size boundary WHEN an interpolated ASCII or multibyte chunk, including one oversized value, exceeds 16,777,216 bytes THEN APC SEA raises AC87's size error without splitting or fallback; accepted chunks retain committed rows and per-set counts.
- AC136: GIVEN first-slice APC SELECT with heartbeat enabled WHEN a partly read ResultSet idles beyond two heartbeat intervals and the configured expiry window beside APC Thrift THEN no APC `/status` request occurs and both cursors drain identically. Record survival time; a shorter APC lifetime requires R3 exception approval.
- AC138: GIVEN deployed T3 WHEN direct and polled `GET_CATALOGS` run on scoped APC staging THEN Reyder meets the shared metadata wire gate with nonempty Arrow IPC.
- AC139: GIVEN deployed T4 after AC138/142 WHEN `GET_SCHEMAS` and `GET_TABLES` run THEN each meets the shared Arrow/HIVE server gate before T17 exposure.
- AC140: GIVEN deployed T5 after AC139 WHEN `GET_COLUMNS` and `GET_FUNCTIONS` run THEN each meets the shared Arrow/HIVE server gate before T18 exposure.
- AC141: GIVEN deployed T6 after AC140 WHEN `GET_PRIMARY_KEYS` and `GET_CROSS_REFERENCE` run THEN each meets the shared Arrow/HIVE server gate before T35 exposure.
- AC142: GIVEN deployed T7 and T4 after AC138 WHEN `GET_CATALOGS` and a `GET_TABLES` result with a null-valued optional cell run under repeatable HIVE conditions THEN native-marked `columnar_data` includes the expected values and null mask. Record DBR version/configuration; AC16 verifies live JDBC `wasNull()` against paired routes.
- AC144: GIVEN deployed T37's staging-only fault hook WHEN scoped 429/503 responses with and without `Retry-After` are injected before APC SEA and warehouse SEA session-open or read responses THEN each original request reaches its intended target, the mutation is recorded, and unrelated callers receive no fault. This live proof precedes connection and SELECT retry exposure.
- AC145: GIVEN paired cloud download WHEN `EnableSQLExecHybridResults` and `UseBoundedSeaApi` each vary over `0`/`1` for linked AC2 shapes THEN all APC combinations stay targeted/hybrid with paired rows/types/close while warehouse hybrid `0` requests external-only; compare inline/link bytes, first-row latency, throughput, and heap before exposing `0`.
- AC146: GIVEN deployed AC113 and isolated live table WHEN single prepared INSERT/UPDATE/DELETE/MERGE run with scalar bindings, including zero rows THEN one counted request per call yields paired counts, `execute=false`, no ResultSet, committed rows, and error identity. Apply AC29/51/53/85/89 and shared load before exposure.
- AC147: GIVEN live IN-only procedures with rows, no result, and server failure WHEN APC `CallableStatement.execute`/`executeQuery` runs THEN targeting, rows/errors, ResultSet boolean, zero count, and result-set-expected error follow R18 and paired routes. Unsupported OUT/named parameters and callable `executeUpdate` reject before request. Apply AC51/85 and shared load.
- AC148: GIVEN flag-on APC with `UseThriftClient=0` and `TreatMetadataCatalogNameAsPattern=1` WHEN each exposed `getCatalogs` or catalog-sensitive `getTables` call runs THEN captures prove APC SEA SHOW rather than Thrift RPC, paired rows/shapes, and an ignored-pattern diagnostic. Prove each method before its own exposure and repeat both at release.
- AC151: GIVEN the JDBC stack is ready for one release WHEN the final package updates `NEXT_CHANGELOG.md` and docs THEN entries describe only delivered, live-gated behavior and warn that accepted writes may have uncertain commit status. Earlier packages use `NO_CHANGELOG=true` and cannot enter a release independently.
- AC152: GIVEN a saved APC SEA session ID and a raw public statement ID WHEN the supplied connection flag or target changes THEN saved close terminates only the session's original target, while a recovered statement targets the current connection; captures show each target, closure outcome, and no identifier leak under R9.
- AC154: GIVEN deployed T2/AC113 and T37 WHEN a scoped post-acceptance write or count-response fault runs on APC and warehouse SEA THEN captures prove target, committed rows, mutation, and isolation. Before each write exposure, missing counts fail and accepted writes never auto-replay.
- AC155: GIVEN scoped flag-ON/OFF APC staging WHEN connections open and close without SQL THEN flag-ON CreateSession/TerminateSession target the same APC, flag-OFF stays Thrift, rejected close surfaces, and warehouse wire stays unchanged. Before T44, APC ID access raises R9’s error without leaks.
- AC156: GIVEN live download-off APC and warehouse SEA at both bounded settings WHEN primitive and complex SELECTs drain at one/32 callers THEN APC uses `ARROW_BATCH`, warehouse uses `JSON_ARRAY`/`ARROW_STREAM`, values match, and APC inline typed complex getters reject; record paired getter and shared load impact.
- AC157: GIVEN HIVE fixtures with null placeholders, short/extra null-mask bits, and an unknown vector WHEN decoded THEN least-significant-bit-first masks preserve positions and row count, uncovered rows are non-null, and unknown vectors raise a named error without partial success. A later valid result works; AC16 still requires live HIVE parity.
- AC158: GIVEN a fixture with an unknown numeric SEA result format WHEN the wire payload deserializes and its result is decoded THEN deserialization succeeds, decoding raises a named driver error without partial ResultSet success, and a later valid SELECT on the connection still works.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The final release package alone edits
  `docs/` and `NEXT_CHANGELOG.md`, after all visible slices pass their live gates.
  Earlier stacked PRs use `NO_CHANGELOG=true` and cannot be released separately.
  No production APC assignment occurs before the assembled release gate.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T1–T8 and staging-only T37; known gaps SC-256188 under SC-256186
  remain external prerequisites for affected dogfood checks. T8 is limited to
  proving or fixing CreateSession's inline-only override of the APC per-statement capability.
- Ramping the APC flag in SAFE (an operational step after the driver ships, owned by PECO).
- Benchmark suites (`MultiJdbcDriverBenchmark` APC variant, Quicksilver `dust-cluster-benchmark`
parameter sets).
- ODBC, Python, Go, and Node.js connectors.
- Govcloud / non-Reyden region enablement beyond keeping the APC flag off there; server errors
propagate without fallback.
- Removing the direct-Thrift APC code path.

---

## Plan

### Approach

`plan.json` assigns each active task once. Seven JDBC packages form a stack;
each closes the listed live and telemetry gates before exposing its methods.
The first native catalog metadata workflow follows result delivery, ahead of
writes. T28's scalar binding precedes procedure metadata. Server packages
deploy T1/T37/T8, command shapes, and Arrow/HIVE SHOW before the corresponding
JDBC live gates; cross-repository `parent` links express deployment prerequisites,
while same-repository links are branch ancestry. The implementation stack has
one release boundary: no intermediate JDBC package enters an independent
release. Its final package owns docs and `NEXT_CHANGELOG.md`, describing only
delivered behavior and uncertain writes (AC151). T-val repeats assembled live,
load, telemetry, and rollback gates before production assignment. Each JDBC
PR targets fewer than 1,500 changed lines; re-plan if implementation exceeds that bound.

### Affected Components

JDBC connection, statement, prepared-statement, metadata, SEA transport, and
telemetry; the named Reyder and connector-service contracts.

### Data Model Changes

Session state carries the APC target and negotiated capability version.

### API Changes

Existing JDBC methods keep their signatures, with APC no-result boolean
exceptions in R4/R18. The session-reachable async metadata API covers seven
SHOW operations; public connection IDs may encode APC SEA.

### Risks

Server deployment gates metadata and termination; AC13/Q7 block lost logical
types, AC27 blocks repeated poll payloads, and AC11/14 protect warehouse and
presigned-link boundaries.

---

## Tasks

`plan.json` assigns each active task ID to one PR; T40 remains retired.

- Server: T1 flag; T37 fault hook; T8 inline override; T2 commands;
  T3/T7 catalog Arrow/HIVE; T4 schemas/tables; T5 columns/functions;
  T6 keys.
- JDBC result path: T9 route/close/interim ID error; T10 SELECT;
  T44 saved IDs; T11 inline pages; T12 links;
  T43 modes; T41 codecs; T13 pools; T42 retries; T32 download-off.
- JDBC SQL: T14 commands; T30 transactions; T31 Statement batches;
  T29/T33 async and cancel; T34 `isValid`; T28 prepared SELECT; T39 writes;
  T15 legacy batches; T36 native batches; T38 CALL.
- JDBC metadata: T16 catalogs; T17 schemas/tables; T18 columns/functions;
  T19 procedures; T35 keys; T20 catalog; T21 schema; T22 table; T23 column;
  T24 function; T25 primary key; T26 imported/cross-reference async handles.
- T27/T-val: final release notes, assembled live, load, telemetry, rollback,
  and release gates.
