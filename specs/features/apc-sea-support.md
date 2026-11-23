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
  Warehouse SEA requests `ARROW_STREAM` or `JSON_ARRAY` and decodes only those
  formats; `ResultData` has no `columnar_data` field. HIVE decoding is new APC work.
  `DatabricksConnection.getConnectionId()` calls `SessionId.create()`, which
  assumes an APC session has a Thrift handle. Saved session IDs do not encode
  an APC SEA target; raw public statement IDs encode no target. Reyder's APC
  response has no session version (R6). `Connection.close()` reaches
  `DatabricksSession.close()`, which logs and suppresses session-delete failures;
  `Driver.closeConnection()` propagates them. SEA's
  `ExecuteStatementOperation.validateStatements` currently compares Scala
  string length to the 16 MiB SQL limit, despite its byte-limit error text (R6).
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the
  prototype baseline; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756) is unchanged.
- Parity uses one driver build on all routes. The shared gate below governs
  differences and exception approval.

  | Behavior | APC Thrift | Warehouse SEA on `origin/main` | APC SEA; reason and caller impact | Disposition |
  | --- | --- | --- | --- | --- |
  | Route with Arrow disabled | Thrift works | Explicit `UseThriftClient=0` can select SEA before Arrow check | APC selects Thrift even with explicit SEA: APC `ARROW_BATCH` needs Arrow, so a forced SEA choice cannot be honored. | APC format exception; caller observes Thrift, AC1. |
  | Close and saved IDs | `Connection.close()` suppresses delete failure; saved close propagates | Same close behavior, warehouse target in saved ID | APC uses the original cluster target for both close APIs; ordinary close remains best effort, saved close propagates rejection after T44. | Match close semantics; interim ID error until saved-close slice, AC25/152/155. |
  | Result paging | Thrift fetch blocks | SEA continuation | APC pages by server continuation. | Follow SEA; AC14/31. |
  | Compression preference | Thrift honors requested codec | SEA forwards `QueryResultCompressionType`/`EnableQueryResultLZ4Compression` | APC forwards the preference, but Reyder always sends LZ4 even for `NONE`; decode the manifest codec. | Server exception; `NONE` cannot disable compression, AC101. |
  | `RowsFetchedPerBlock` | Tunes Thrift fetch | No SEA paging effect | No APC SEA paging effect because SEA continuation controls pages. | Warehouse precedent; caller's setting is ignored, AC31. |
  | `ThriftMaxBatchesInMemory` | Limits batches | Limits inline response chunks | APC limit counts chunks, which can each contain multiple Arrow batches. | Warehouse precedent; memory may rise at the same setting, AC109. |
  | Download modes and complex getters | Inline/linked; enabled complex support permits representable typed getters | Hybrid `0` requests external-only; download-off selects JSON inline or Arrow by bounded setting, and Arrow results permit enabled typed getters | APC hybrid `0` still requests hybrid because Reyder lacks external-only delivery; download-off uses inline `ARROW_BATCH`, with representable typed getters following Arrow parity. | Server-format transfer exception; no Arrow getter exception, AC13/112/145/156. |
  | `EnableBatchedInserts` | Combines eligible legacy INSERT sets when enabled | Same legacy optimization and opt-in V10 native sets | APC uses the legacy optimizer below V10 or with native batching off; enabling it does not change V10 native `parameter_sets`. | Match legacy behavior; native aggregate counts are the documented R6 exception, AC17/108. |
  | `BatchInsertSize` | Limits eligible interpolated INSERT rows per chunk | Limits interpolated legacy chunks; native scalar chunks use the parameter limit | APC uses the same split: the setting has no effect on native-scalar legacy chunks, whose 256-parameter cap controls size. | Warehouse precedent; caller may need a different tuning path, AC108. |
  | Pools and API retries | Thrift HTTP settings | SDK API pool/retry and link HTTP controls | APC uses SEA split; link retry properties do not change SDK API retries. | Warehouse precedent; callers may see different attempts/waits, AC30/46/51/84. |
  | Idle cursor | Thrift heartbeat | SEA `/status` heartbeat | APC disables heartbeat because status RPC is unsupported. | Server exception; possible shorter idle lifetime requires AC136 approval. |
  | Native metadata | Native RPC | SEA requests Arrow/JSON; HIVE `columnar_data` is not decoded | APC SHOW decodes Arrow IPC or HIVE; null-catalog table syntax and exported-key requests return typed empty where Thrift may have rows. | New APC HIVE decoding plus warehouse method precedent; caller may lose those rows, AC16/54/105. |
  | Unset `EnableThriftNativeMetadata` | Thrift native RPC/SHOW selection | SEA request depends on the warehouse server flag | APC sends require-native on supported SHOW even when unset, to obtain Reyder's native shape. | APC contract exception; metadata response format may differ, AC65. |
  | Partial metadata fan-out | Compare live rows/errors | Failed schema/column catalog is omitted | APC omits the failed catalog; connection remains usable. | Warehouse precedent; caller loses those rows, AC83. |
  | Procedure/function metadata | Query errors; RPC/SHOW switch | Query errors; SHOW | APC propagates query errors and uses SHOW. | Warehouse precedent; AC75/82. |
  | Assignment execute | `SET` classifier exposes ResultSet | `SET` classifier exposes ResultSet | APC no-result assignment has no schema and `execute` returns false. | APC terminal-wire exception; caller boolean changes, AC52. |
  | V10 batch counts | Per-set counts | Native scalar/batch binding | APC V10 returns `SUCCESS_NO_INFO` per successful set because Reyder supplies only aggregate count. | Server exception; caller loses exact per-set counts, AC17. |
  | Session, size, telemetry | Thrift session/errors | `FAST`/version, SEA events; current shared SQL validator counts characters | APC uses `DEFAULT` without version; T2 adds an APC-targeted UTF-8 byte limit while leaving warehouse validation unchanged. | Server exceptions; AC10/23/85/87/89. |
  | Download concurrency and resource use | Thrift fetch pools/batch buffering | SDK API pool plus linked workers/prefetch; bounded SEA limits inline chunks | APC follows SEA pool/retry separation and bounds workers, chunks, heap, RSS, and decode CPU at each exposed mode. | Warehouse precedent for knobs; measured APC Thrift impact and exception process in shared load gate, AC31/46/61/84/109. |
  | Timeouts and retry timing | Thrift request and cursor waits | SDK API retry/request timeout plus JDBC waits and link wait | APC follows SEA timeout classes without premature client-wait failure; write response ambiguity follows SEA. | Warehouse precedent; caller may see different attempts or uncertain commit, AC18/51/60/63/84/154. |
  | Error identity and telemetry | Thrift SQLState/vendor code and telemetry | SEA mapping, event and error families | APC preserves agreeing mappings, otherwise follows SEA; every new path records route and error identity with redaction. | Warehouse precedent where mappings differ; record client impact, AC10/85/89/106. |

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
fetches require it. JDBC `Connection.close()` attempts TerminateSession and
remains best effort on rejection, with local cleanup and a redacted warning;
after T44, `Driver.closeConnection()` propagates a rejected TerminateSession
for the saved ID. Presigned links and standard/Real-time warehouse serialization
stay unchanged; live dogfood termination requires the tracked server binding fix.

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
setting. SEA's forward allowlist must include `X-Databricks-Metadata-Operation-Type`
and `X-Databricks-Sea-Can-Run-Fully-Sync`: forward each present value unchanged
to Reyder and omit it when absent. Reyder honors fully-sync only for synchronous
execution; APC require-native stays at the JDBC-to-SEA hop. These operations
are inline/link-free. If a response provides `next_chunk_index`, consume it through
R12; the current metadata contract does not promise paginated SHOW results (AC16/61).
On APC SEA, unset or explicit `EnableThriftNativeMetadata=1` sends
`X-Databricks-Require-Thrift-Native-Metadata: true` for supported SHOW; `0`
omits it. Unset warehouse SEA depends on its server flag, even when an explicit
override selects SEA.
Reyder still returns native-shaped metadata, so conversion follows
`manifest.is_native_metadata_result`, not the request setting. Synchronous
SHOW sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only at
`EnableSeaSyncMetadata=1`; async SHOW omits it. Decode each response as Arrow
IPC `chunk.attachment` or HIVE `chunk.columnar_data`/`COLUMN_BASED_SET`.
`getImportedKeys` and `getCrossReference` share `GET_CROSS_REFERENCE`; the latter
filters a foreign table's keys by parent. A null foreign table returns typed
empty before request; null/empty parent or empty foreign table is rejected.
Error outcomes follow the warehouse method: catalog syntax errors propagate;
null-catalog schema and column syntax errors trigger catalog fan-out;
null-catalog table syntax errors return typed empty without fan-out;
function and primary-key syntax errors propagate; imported-key and
cross-reference syntax errors return typed empty. Object-not-found returns
typed empty for schemas, tables, columns, functions, primary keys, imported
keys, and cross-reference; catalog errors propagate. Other SQL failures
propagate except existing table/column empty-pattern and fan-out behavior
(AC16/83/105/114).

`enableMultipleCatalogSupport=0` retains current-catalog filtering; `1`
permits eligible catalogs. `EnableMetricViewMetadata=1` adds
`spark.sql.thriftserver.metadata.metricview.enabled=true` to session config
and exposes `METRIC_VIEW` table types and eligible rows; `0` omits both.
`getExportedKeys` is outside native SHOW: the SEA contract lacks parent-only
lookup and documented APC Thrift returns no rows. APC SEA follows warehouse
SEA's typed empty result without SHOW, subject to AC54 live parity. Catalog
filtering at `enableMultipleCatalogSupport=0` may first run
`SELECT CURRENT_CATALOG()`; at `1` it needs no lookup. A null table fails before
request with SQLState `42000` and `EXECUTE_STATEMENT_FAILED` (1003).
Session-reachable async metadata has typed ResultSet handles for seven SHOW
operations, with separate imported-key and cross-reference calls. It submits
at `wait_timeout=0s`, returns while RUNNING, and preserves synchronous await,
timeout, cancel, and conversion outcomes. Version and saved IDs do not gate it;
public JDBC metadata stays synchronous.

Internal catalog/schema and parameterized procedure SELECTs use hybrid
`ARROW_BATCH` without metadata headers; Reyder rejects warehouse procedure
headers. Procedure queries use R6 binding, not async SHOW, and propagate query,
fetch, and decode errors. Only the existing catalog-access filter returns typed
empty (AC75).
`EnableShowCommandForGetFunctions` still selects native RPC or SHOW
on APC Thrift, whose SHOW path returns typed empty for null catalog; APC SEA,
like warehouse SEA, uses SHOW at either setting (AC82).

Async null-catalog schema/column `42601` fans out without sync headers.
Per-catalog `SQLException` warns and omits that catalog; timeout,
interruption, or initial/enumeration error fails the call. Partial results
leave the connection usable (AC83).
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

T2 changes SEA's APC-targeted SQL validation from Scala string length to UTF-8
byte length at the configured 16 MiB limit (default 16,777,216 bytes) for
single statements and legacy generated chunks. Warehouse-targeted validation
retains its current behavior. The APC driver sends the text without silent split
or fallback and propagates the SEA size error. AC87 must prove the deployed
byte boundary before dependent batch gates close.
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
families on each public APC path, with protocol and compute identity for correlation.
AC85 compares event fields and errors at exposure. Diagnostics and telemetry redact
credentials, org/cluster IDs, and compound session/statement IDs, including those
inside messages; raw targets never appear.
- R9: The connection-only slice attempts live APC SEA session termination and
verifies server termination once the tracked binding fix is deployed; its
`Connection.close()` remains best effort on rejection. The first SELECT slice
also closes its open statement before session termination.
Until the saved-session close slice, public `getConnectionId()` on APC SEA raises
`DatabricksSQLFeatureNotSupportedException` with SQLState `0A000`, vendor code
`UNSUPPORTED_OPERATION` (1005), and the same telemetry error name, without
dereferencing a Thrift handle or logging the session ID.
Other routes retain their IDs. The saved-close slice returns an APC SEA ID with
protocol and target, allowing `Driver.closeConnection` to terminate the original
session after flag/target changes when endpoint and credentials stay the same.
IDs contain neither endpoint nor credentials. Public statement IDs stay raw;
recovered statements use the current connection target. Internal IDs retain the
target and reject malformed values without logging them. Legacy warehouse
SEA/Thrift IDs still decode, except `origin/main` warehouse Thrift IDs wrongly
encoded as SEA cannot support saved close because they lack a secret. Only a
live CreateSession version grants capabilities.
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
For inline `ARROW_BATCH`, enabled typed complex getters work when the value is
representable by the reconstructed logical type, as they do for warehouse Arrow
results; disabled support retains the established rejection. A getter exception
requires a demonstrated wire or type limit, the warehouse Arrow comparison, and
measured caller impact before release (AC13/156).
Download-off APC `ARROW_BATCH` and its transfer mode differ from warehouse SEA's
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
at either `UseBoundedSeaApi` value. APC links lack reliable continuation totals,
so both settings must return every row with bounded memory and prefetch (AC39).
`ThriftMaxBatchesInMemory` limits APC inline chunks (minimum two), while APC
Thrift limits batches. Since one chunk may contain several batches, compare
memory at the same setting and avoid whole-result buffering (AC109).
Case-insensitive `cloudFetchThreadPoolSize` (default 16) bounds per-result
linked workers and buffered chunks; `LinkPrefetchWindow` (default 128) bounds
fetched-ahead links only. `ChunkReadyTimeoutSeconds` bounds linked-chunk wait;
`RowsFetchedPerBlock` has no SEA paging effect.

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
- R18: Incomplete APC SEA methods or settings fail before request when known;
unsupported responses fail before false success. Counted prepared DML preserves
`executeUpdate`/`execute` outcomes, including zero (AC146).
CALL with rows exposes a ResultSet; no-result CALL returns `false`, no ResultSet,
and count zero. OUT/named parameters and callable `executeUpdate` reject before
request; `executeQuery` on a no-result CALL raises the existing result-set-expected
error (AC147). No fallback follows SEA selection; flag-off APC stays on Thrift.
Production assignment awaits T-val and post-merge smoke.
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
against APC Thrift and warehouse SEA where relevant. Common routing, transport,
telemetry, error, and load gates apply at each new request class; fixtures do not
replace feasible live checks. AC144 gates session and API-read faults, AC160
gates presigned-link faults, and AC113/154 gate writes.
When one AC names several callers, a package closes only its newly exposed
caller subset; T-val repeats the assembled check after all callers exist.

The shared paired-load gate applies to each newly exposed public workflow. It uses one
driver build, warmed staging cluster, identical data, and one/32 callers on APC
SEA and Thrift. At 32 callers, SEA/Thrift aggregate operation throughput is at
least 0.90; median latency at most 1.10, p95 at most 1.20, slowest caller at most
1.50, and peak heap at most 1.20. Every caller completes or returns its expected
bounded error. AC31 drains at least 8 MiB per caller across repeated
single-chunk SELECTs in the first inline slice and at least 64 MiB per caller
once linked/multi-page modes are exposed, with
sustained payload bytes/second at least 0.90 of APC Thrift in each mode. AC32
inserts at least 1,024 rows per caller over at least two Statement requests,
legacy chunks, or V10 parameter sets as applicable, with committed rows/second
at least 0.90 of APC Thrift. Both bound retained heap growth across two data
sizes to max(8 MiB, 5% of added bytes). Record bytes, rows, durations, errors,
and resource use. A proposed exception needs same-build warehouse SEA/Thrift evidence, server limit
or precedent, caller impact, and PECO/Perf Eng approval before release.

For each native SHOW operation, the server-readiness gate uses raw scoped APC
staging requests at both hybrid settings: direct and `wait_timeout=0s` polled
Arrow IPC and HIVE `columnar_data`, native mark, exact forwarded headers,
inline/link-free delivery, schema and row values, and continuation if supplied.
AC138–142 close it without JDBC callers; AC142 also requires a repeatable null
cell with its vector value and mask. At exposure, AC16/50 add paired JDBC
rows/types, `wasNull()`, errors, and AC61 load for each method or typed handle.

- AC1: GIVEN scoped flag-ON/OFF APC staging WHEN R1 overrides and metadata preference vary at connection open THEN selected protocol, target, flag value, partner exclusion, and no-fallback match R1 and paired warehouse routing. Fixtures cover Arrow absence, legacy `cliservice`, and lookup failure; AC2 proves SELECT rows after exposure.
- AC2: GIVEN flag-on APC staging at bounded/hybrid `1` WHEN single-chunk, RUNNING, manifest-only, and zero-row SELECTs run THEN targeted Arrow yields paired rows/types/empties and closes without false success; AC145 gates alternate settings.
- AC3: GIVEN a live APC statement WHEN it polls, fetches, and closes THEN R2 targeting, exactly-once close, connection usability, and unchanged warehouse serialization are captured.
- AC9: GIVEN live APC SEA CreateSession rejection or APC Thrift OpenSession KP001
WHEN connecting on the affected route THEN the SEA error and failed-route event
propagate without fallback, and warehouse-only KP001 recovery never selects SEA for APC.
- AC10: GIVEN successful and failed opens on four APC/warehouse SEA/Thrift routes WHEN DEBUG logs and telemetry are captured THEN compute, protocol, reason, outcome, and error identity appear where enabled; R8 toggles suppress export and secrets/targets/IDs are redacted.
- AC11: GIVEN standard and Real-time warehouse connections WHEN equivalent R2 operations run THEN JDBC outcomes and URL/body/header serialization, including `realtimeSession`, match `origin/main`.
- AC12: GIVEN live synchronous SELECT at both direct-result settings WHEN paired callers drain rows THEN `wait_timeout` is absent or `0s` respectively, no `statements` field appears, rows/errors and AC31 load pass, and first-row latency/request count are recorded.
- AC13: GIVEN R11 primitive/logical values WHEN inline and linked getters run at both complex/geospatial settings THEN types and values match paired routes; enabled typed getters work for representable inline Arrow and linked values, while disabled support retains established rejection. A remaining inline exception requires the R11 evidence. Unavailable live types stay pending.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN a deployed native SHOW operation WHEN its synchronous JDBC method runs THEN it passes the shared JDBC metadata gate against warehouse SEA and applicable APC Thrift behavior; AC105 and AC114 govern documented differences.
- AC17: GIVEN live V10 batches on paired routes WHEN success or failure occurs THEN ordered `parameter_sets`, R6 counts, and committed rows are verified; a post-write count fault raises the distinct result error, clears the batch, and reports uncertain commit under AC154/load.
- AC18: GIVEN an APC statement past `setQueryTimeout(n)` WHEN it times out THEN targeted CancelStatement raises the paired timeout SQLException within the expected elapsed time and leaves the connection usable.
- AC22: GIVEN a live Statement batch with middle failure or non-rowcount-prefix match WHEN APC executes THEN requests remain sequential without `statements`, prior counts retain order, and the paired batch error stops execution.
- AC23: GIVEN live versionless APC CreateSession WHEN the next statement runs THEN `DEFAULT`, absent `session_version`, negotiated capability, and missing-version diagnostics match R6 while warehouse FAST/version behavior stays intact.
- AC25: GIVEN APC/warehouse saved IDs WHEN flag or target changes before live `Driver.closeConnection()` THEN only the original session is targeted, a TerminateSession rejection propagates, legacy IDs decode, malformed IDs fail without leakage, and R9’s irreparable legacy Thrift IDs are recorded.
- AC27: GIVEN a live single-chunk APC query with at least three RUNNING polls WHEN transfer is captured on flag-on and explicit SEA routes THEN polls transfer no payload and terminal fetch returns each row once.
- AC28: GIVEN the T1 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN flag-on APC and paired routes WHEN a complex binding enters a single bound statement, legacy prepared batch, or V10 prepared batch THEN each exposed form raises a named/numeric driver error before ExecuteStatement, leaves the connection usable, and records taxonomy/dashboard ownership or maintainer handoff.
- AC30: GIVEN paired routes under API or link-pool saturation WHEN HTTP pool sizes and acquisition wait vary THEN in-flight count, wait, errors, and per-caller completion pass the shared load gate; SDK API acquisition ignores Thrift HTTP wait.
- AC31: GIVEN each SELECT mode at first exposure WHEN paired callers drain the shared download workload THEN rows/errors, SEA's ignored `RowsFetchedPerBlock`, and sustained bytes/second pass the shared paired-load gate. Repeat the mode mix at release.
- AC32: GIVEN each Statement, legacy prepared, or V10 batch mode WHEN paired callers insert disjoint keys using the shared batch workload THEN ordered counts, committed rows, and sustained rows/second pass the shared paired-load gate. Repeat the mode mix at release.
- AC34: GIVEN scoped ON and default OFF flag states WHEN connector-service serves ordinary, partner, and allowlisted JDBC partner agents THEN only the ordinary scoped agent gets true; record the flag version.
- AC36: GIVEN multi-batch Arrow chunks and inline continuation WHEN JDBC drains them THEN rows occur once, `row_offset` counts every batch row, and failed pages never succeed; if staging lacks multi-batch chunks, live proof stays pending.
- AC39: GIVEN each exposed result mode WHEN bounded and streaming switches vary on paired callers THEN APC requests follow R17, rows match, and first-row latency, heap, link expiry, and sustained download rate pass AC31 or have an approved exception.
- AC40: GIVEN any JDBC slice WHEN APC flag is OFF or scoped ON THEN OFF stays Thrift, completed ON calls pass their live gate, and incomplete calls fail without fallback or false success before production assignment.
- AC45: GIVEN live linked or inline results WHEN `EnableSeaResponseCompression` varies THEN only `0` sends API `Accept-Encoding: identity`; links remain unchanged and paired bytes/rows/load pass.
- AC46: GIVEN linked results at one/32 callers WHEN worker, prefetch, and chunk-wait settings vary THEN workers/buffered chunks and fetched-ahead links respect R17 bounds; all callers finish or return bounded errors under the shared load gate.
- AC49: GIVEN live SELECT pages WHEN `setMaxRows` is zero, one, or a page boundary THEN positive limits send `row_limit` and cap all pages; zero is unlimited.
- AC50: GIVEN each typed native SHOW handle WHEN delayed live SHOW submits at `wait_timeout=0s` THEN it returns while RUNNING and passes shared metadata, await, timeout, cancel, cleanup, and header gates.
- AC51: GIVEN AC144's SDK fault path or AC160's presigned-link path WHEN scoped 429/503 faults with/without `Retry-After` affect an exposed request class THEN attempts, delay, errors, and caller outcome follow R17 and paired routes. Writes also require AC154.
- AC52: GIVEN AC113 WHEN direct/polled no-result and counted commands, including zero, run live THEN R4 JDBC shapes and paired rows/errors hold except the documented assignment boolean; missing terminal evidence fails.
- AC53: GIVEN live scalar binding and older/versionless fixtures WHEN `supportManyParameters` varies THEN native/interpolated requests, `setBytes`, rows, and counts follow R6 without unresolved markers.
- AC54: GIVEN live `getExportedKeys` at both catalog settings WHEN APC and Thrift run THEN APC issues no exported-key SHOW and returns paired typed empty columns, with current-catalog lookup only if needed; a nonempty Thrift result needs disposition and null table fails before request with SQLState `42000`/code 1003.
- AC55: GIVEN live async SQL SELECT with primitive and linked results WHEN it completes or cancels THEN `wait_timeout=0s`, target, rows/errors, close, diagnostics, and AC31 load pass.
- AC60: GIVEN a valid live query within its client wait WHEN it exceeds one SEA request timeout THEN polling completes without premature failure; only measured latency may receive an exception.
- AC61: GIVEN each nonempty metadata method or typed handle WHEN paired callers run THEN the shared JDBC metadata and paired-load gates pass, null-catalog requests stay at most N+2, and decode CPU per 1,000 cells and peak RSS stay at most 1.20 times Thrift absent approved exception.
- AC63: GIVEN delayed live synchronous SHOW within its metadata deadline WHEN it exceeds one SEA request timeout THEN polling succeeds and the connection remains usable; deadline `0` disables only the overall bound, retaining AC83 future waits.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching INSERT runs through public Statement THEN `execute` returns true, its ResultSet contains the affected-row value, and `getUpdateCount` is `-1`.
- AC65: GIVEN each live synchronous SHOW WHEN native, sync, hybrid, and bounded settings vary THEN R5 headers and AC16 Arrow/HIVE rows pass, including warehouse default and AC148 override differences.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized procedure metadata WHEN pages/links or query/fetch/decode errors occur THEN header-free rows pass AC61 or errors propagate with paired identity; failure never becomes typed empty.
- AC82: GIVEN live `getFunctions` at both SHOW-switch settings and catalog forms WHEN paired routes run THEN APC SEA uses SHOW with warehouse rows/errors; record Thrift native/SHOW and null-catalog impact.
- AC83: GIVEN synchronous or async null-catalog schema/column `42601` fan-out WHEN one live catalog raises `SQLException` THEN APC and warehouse SEA omit only its rows, warn, and remain usable; record Thrift delta, while timeout/interruption fails the call.
- AC84: GIVEN each new live SDK or link request class WHEN socket, poll, and idle settings vary THEN one-request timeout, later-poll delay, and applicable idle eviction follow R17, with paired timing/errors and usable connections.
- AC85: GIVEN each new path WHEN telemetry toggles vary THEN R8 event/error families match paired routes or approved exceptions, disabled export suppresses them, and logs/events redact secrets, targets, and IDs.
- AC87: GIVEN deployed T2 and the default 16 MiB SQL limit WHEN syntactically valid APC ASCII and multibyte statements cross 16,777,216/16,777,217 UTF-8 bytes THEN SEA's size validator admits the first and rejects the second with the size error; a raw scoped request and JDBC call prove the boundary without fallback. The same warehouse probes retain their pre-T2 validator outcome.
- AC88: GIVEN two catalogs and a metric view WHEN catalog and metric-view settings vary on three live routes THEN discovery, table types/rows, and APC session config follow R5; record server-limited impact.
- AC89: GIVEN live invalid SQL, missing object, or permission failure WHEN exposed methods run THEN SQLException type/SQLState/vendor code match agreeing baselines or documented warehouse mapping; failure never succeeds.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN live inline/linked results WHEN codec setting or alias is `0`, `1`, or invalid THEN manifest codec governs lossless decode, invalid keeps warehouse default, and paired bytes/CPU/load are recorded.
- AC102: GIVEN an isolated live table WHEN transaction settings vary THEN R19 state, commit/rollback rows, counts, and errors match paired routes; no-result commands fetch nothing and fail on missing state.
- AC105: GIVEN live synchronous or typed async null-catalog `getTables` receiving `42601` WHEN APC SEA, APC Thrift, and warehouse SEA are compared THEN APC returns warehouse SEA’s typed empty result without fan-out; record omitted Thrift row counts/types and caller impact.
- AC106: GIVEN a changed driver-owned error WHEN emitted THEN a test asserts its code name/number, the PR records taxonomy/dashboard ownership or handoff, and the telemetry-error template is complete; server errors retain server identity.
- AC107: GIVEN one/32 paired callers WHEN SELECT opens, leaves an operation open, and closes repeatedly THEN close order, baseline resource recovery, at most 8 MiB retained heap growth, and cycle-latency gate pass.
- AC108: GIVEN AC87 WHEN live legacy INSERT batching settings vary THEN R6 chunk limits, counts, committed rows, errors, AC32 load, and AC132 oversize behavior pass each binding mode.
- AC109: GIVEN concurrent live multi-page inline SELECTs WHEN `ThriftMaxBatchesInMemory` varies around two THEN all rows arrive once without whole-result buffering, AC31 memory/load bounds pass, and observed chunks, batches, bytes, and heap explain the APC Thrift difference.
- AC112: GIVEN AC119 WHEN download-off SELECT drains at both bounded settings THEN all pages remain inline and lossless; initial size failure blocks ResultSet success and later failure interrupts iteration.
- AC113: GIVEN deployed T2 WHEN direct/polled no-result and counted commands run on scoped staging THEN captures prove R4 terminal shapes/errors, including zero, before T14 caller exposure.
- AC114: GIVEN two-parent and second-foreign-table keys WHEN live imported/cross-reference methods run THEN each returns its filtered paired JDBC rows; null foreign table is typed empty and invalid input fails before request.
- AC119: GIVEN deployed T8 WHEN session disables cloud download but statement advertises it THEN live single/multi-page results remain inline; capture both wire values before AC112.
- AC117: GIVEN paired bound SELECT callers WHEN pre-execution `PreparedStatement.getMetaData()` runs THEN metadata matches, bindings survive, and request/latency/throughput/heap load passes.
- AC118: GIVEN live `EnableSQLValidationForIsValid=1` callers WHEN `isValid(timeout)` runs `SELECT VERSION()` repeatedly THEN booleans, timeout, usability, request count, latency, call-rate, and heap pass paired load.
- AC122: GIVEN live two-parent and second-foreign-table keys WHEN typed async imported-key and cross-reference handles complete THEN each independently passes AC114 and the per-handle AC50/61 gates.
- AC124: GIVEN synchronous SHOW or typed async metadata WHEN `MetadataOperationTimeout` varies THEN positive values bound full call/await and cancel; `0` removes only the overall bound, in-budget calls survive shorter request timeouts, and timed-out connections remain usable.
- AC132: GIVEN AC87 and a live legacy INSERT chunk near the UTF-8 limit WHEN ASCII or multibyte content crosses it THEN SEA accepts an in-limit chunk and rejects an over-limit chunk with the size error, without client split/fallback; committed rows and counts match captured requests.
- AC136: GIVEN a live APC result with unread server continuation or linked data and heartbeat enabled WHEN one row is read, the cursor idles beyond two heartbeat intervals and the configured expiry window beside APC Thrift, and both cursors drain THEN no APC `/status` request occurs, all remaining rows match, and survival time is recorded. A shorter APC lifetime requires R3 exception approval.
- AC138: GIVEN deployed T3 WHEN raw synchronous and async `GET_CATALOGS` and an ordinary SELECT run on scoped APC staging THEN captures at SEA ingress and Reyder ingress retain the exact operation value on SHOW, forward fully-sync only for synchronous SHOW, omit both headers on SELECT, and keep require-native at SEA ingress only; SHOW returns nonempty native Arrow IPC.
- AC139: GIVEN deployed T4 after AC138/142 WHEN raw `GET_SCHEMAS` Arrow/HIVE and `GET_TABLES` Arrow requests run THEN each meets the server-readiness gate before T17 exposure; AC142 already owns the `GET_TABLES` HIVE contract.
- AC140: GIVEN deployed T5 after AC139 WHEN raw `GET_COLUMNS` and `GET_FUNCTIONS` Arrow/HIVE requests run THEN each meets the server-readiness gate before T18 exposure.
- AC141: GIVEN deployed T6 after AC140 WHEN raw `GET_PRIMARY_KEYS` and `GET_CROSS_REFERENCE` Arrow/HIVE requests run THEN each meets the server-readiness gate before T35 exposure.
- AC142: GIVEN deployed T7 after AC138 WHEN `GET_CATALOGS` and a `GET_TABLES` result with a null-valued optional cell run under repeatable HIVE conditions THEN native-marked `columnar_data` includes the expected values and null mask. Record DBR version/configuration; AC16 verifies live JDBC `wasNull()` against paired routes.
- AC144: GIVEN T37's staging-only scoped hook WHEN 429/503 responses with and without `Retry-After` are injected for APC/warehouse SEA CreateSession, TerminateSession, SELECT ExecuteStatement, GetStatementResult, GetResultData, CancelStatement, and CloseStatement THEN captures prove original target, fault point, mutation, and unrelated-caller isolation before each corresponding JDBC AC51 gate.
- AC145: GIVEN linked SELECT with hybrid and bounded settings at `0`/`1` WHEN paired routes run THEN APC stays targeted/hybrid with paired rows while warehouse hybrid `0` is external-only; bytes, latency, throughput, and heap pass before exposure.
- AC146: GIVEN AC113 and isolated table WHEN live prepared INSERT/UPDATE/DELETE/MERGE run, including zero count THEN each yields one counted request, paired counts/rows/errors, `execute=false`, and no ResultSet under AC154/load.
- AC147: GIVEN live IN-only CALL with rows, no result, or failure WHEN execute/executeQuery runs THEN R18 JDBC shapes and paired rows/errors hold; unsupported OUT/named/update forms reject before request.
- AC148: GIVEN flag-on APC with explicit SEA and catalog-pattern preference WHEN live `getCatalogs` and `getTables` run THEN each captures SEA SHOW, paired rows, and ignored-pattern diagnostic at first exposure and release.
- AC151: GIVEN all JDBC slices are live-gated WHEN final docs and `NEXT_CHANGELOG.md` are updated THEN they describe delivered behavior and accepted-write uncertainty; earlier PRs use `NO_CHANGELOG=true`.
- AC152: GIVEN saved APC SEA session and raw statement IDs WHEN a connection flag or target changes THEN saved close uses the original session target and recovered statement uses the current connection; captures prove both outcomes without ID leakage.
- AC154: GIVEN deployed T2/AC113 and T46's scoped post-acceptance write/count fault hook WHEN APC and warehouse SEA writes commit before their response is faulted THEN captures prove target, committed rows, mutation, unrelated-caller isolation, and no APC-only replay; missing counts fail and accepted writes report uncertain commit before each write exposure.
- AC155: GIVEN scoped ON/OFF APC and warehouse connections without SQL, with the tracked termination binding fix deployed, WHEN paired callers repeatedly open and close THEN ON creates and terminates the same server session, OFF stays Thrift, and the shared paired-load gate passes. A rejected TerminateSession leaves `Connection.close()` successful and locally closed with a redacted warning, matching existing routes; before T44, APC SEA `getConnectionId()` raises `DatabricksSQLFeatureNotSupportedException`, SQLState `0A000`, name `UNSUPPORTED_OPERATION`, and code 1005 without ID leakage. APC Thrift and warehouse IDs retain their existing behavior.
- AC156: GIVEN one/32 live download-off callers at both bounded settings WHEN primitive/complex results drain THEN APC `ARROW_BATCH` and warehouse JSON/Arrow modes match values, enabled typed complex getters work where Arrow values are representable, and transfer/getter differences and load impact are recorded.
- AC157: GIVEN HIVE null-mask and unknown-vector fixtures WHEN decoded THEN null positions and row count follow R13, unknown vectors fail before partial success, and a later valid result works; AC16 proves live HIVE parity.
- AC158: GIVEN unknown numeric SEA format fixture WHEN deserialized and decoded THEN decoding raises a named driver error before ResultSet success and a later valid SELECT still works.
- AC159: GIVEN first-slice single-chunk APC SELECT with no unread server page and heartbeat enabled WHEN a partly read ResultSet idles beyond two heartbeat intervals THEN no APC `/status` request is sent; cursor survival remains gated by AC136 once unread server data exists.
- AC160: GIVEN T45's staging-only scoped link hook WHEN 429/503 responses with and without `Retry-After` fault a direct presigned-link fetch on APC and warehouse SEA THEN captures identify the external URL fetch, original target, fault point, mutation, and unrelated-caller isolation before linked JDBC AC51 exposure.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- The final release package alone edits `docs/` and `NEXT_CHANGELOG.md`;
  intermediate stacked PRs use `NO_CHANGELOG=true`.
- Each JDBC package closes a live paired workflow gate before its flag-scoped
  callers are exposed; keep each PR below roughly 1,500 changed lines and split
  an implementation package if that review limit would be exceeded.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T1–T8 and staging-only T37/T45/T46; known gaps SC-256188 under SC-256186
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

`plan.json` assigns each task once. The JDBC stack has seven packages: a live
connection-only gate, then a live single-chunk SELECT gate, followed by result,
bound SQL, synchronous metadata, async metadata, and batch/release gates. A
package closes its own live paired workflows before exposing them, and its
notes name deployed universe prerequisites. Bound SELECT precedes parameterized
procedure metadata. The final package alone owns docs and `NEXT_CHANGELOG.md`
and repeats the assembled release gate before production assignment.

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

Server deployment gates metadata and termination; T2's APC byte validator
gates AC87 and dependent batches. AC13/Q7 block lost logical types, AC27 blocks
repeated poll payloads, and AC11/14 protect warehouse and presigned-link
boundaries.

---

## Tasks

`plan.json` is the task and package inventory. T37 provides scoped session and
read-API faults before the first JDBC gates; T45 provides scoped presigned-link
faults before linked-result exposure; T46 provides scoped post-acceptance
write/count faults before writes. AC144, AC160, and AC154 respectively close
those server gates. T-val repeats the assembled live checks after all slices.
