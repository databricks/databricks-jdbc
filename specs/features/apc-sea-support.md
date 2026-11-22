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

- The server routing path exists: `SqlExecBackendRouter` (universe `cmdexec/sql-exec-api`) routes a
request to Reyden when it carries `backend_compute.all_purpose_cluster_compute.cluster_id`; this
route is guarded by `databricks.sqlexecapi.enable_apc_reyder_routing`. APC does not support
`GetStatementStatus` — clients must poll with `GetStatementResult`. In this document, Reyder is
the SEA service and Reyden is the runtime backend.
- Known server gaps are tracked outside this client plan (SC-256188 under SC-256186).
  Affected live staging metadata and session-termination gates remain pending until the fixes
  deploy. `SC-254478` concerns per-statement `ExecuteStatement.session_confs`, not R4's
  CreateSession setting, and is not an inline-only release gate.
  The TerminateSession REST binding declares `body: "*"` (R2).
- Reyder's APC terminal translation currently requires an Arrow schema when metadata is
  present. T32 must prove no-result DDL and counted DML on both direct `ExecuteStatement`
  and terminal `GetStatementResult` before the JDBC live gates pass (AC52). Live
  manifests populate `arrow_schema`; absent-field decoding has a fixture gate.
- Reyder currently discards APC `result_compression`, advertises LZ4 decompression capability
  to DBR, and reports DBR's actual codec in the result manifest. Uncompressed results are valid;
  the JDBC codec setting cannot select APC's effective wire codec (R4, AC101).
- Connector-service returns sub-flags of
  `databricks.partnerplatform.clientConfigsFeatureFlags` at
  `/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`
  (`FeatureFlagsHandler.scala`). It maps `enableSqlExecForJdbc` to
  `enableSqlExecForJdbc_v3.4.3` for ≥3.4.3 and may leave that flag on for an
  allowlisted JDBC partner. Other sub-flags pass through; Q9 adds a blanket APC override.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata tabs). The server limits SQL text to 16 MiB
(16,777,216 UTF-8 bytes), rejects non-OSS-JDBC clients, and starts rollout opt-in
before defaulting APC to Reyder through the dedicated flag.
- Driver baseline: `origin/main` @ `840f3a407` (spec #1809; bounded SEA default
  #1827). APC routes to Thrift before warehouse flag logic. `DatabricksSdkClient`
  assumes `Warehouse`; its result heartbeat uses `/status`. `SessionId.create`,
  `getConnectionId()`, and `Driver.closeConnection` can misroute saved-ID close after
  flag or target changes. SEA lacks APC `ARROW_BATCH`, async SHOW, and SEA
  `columnar_data` decoding. Procedure metadata methods catch failures into typed
  empty results; public SQL `executeAsync` is separate from metadata SHOW async.
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the prototype
baseline, not the design; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756)
is unchanged.
- Parity baseline for review and release ("warehouse" uses the same driver build).
  R1–R19 define behavior; this matrix records the disposition and caller impact of
  differences. AC gates provide evidence. For each non-functional exception, add the
  measured warehouse Thrift/SEA delta required by the shared paired-load gate.

  | Behavior | APC Thrift today | Warehouse SEA today | APC SEA | Disposition and caller impact |
  | --- | --- | --- | --- | --- |
  | Route/heartbeat | Thrift; heartbeat | Flag/override; `/status` | Dedicated flag; no `/status` | APC status RPC is unavailable. SEA errors do not fall back (R1, R3, R7). |
  | Explicit SEA without Arrow | Thrift | Explicit `UseThriftClient=0` selects SEA first | Thrift | APC needs Arrow batches; caller override loses to this prerequisite. Route telemetry explains why (R1; AC1). |
  | SELECT/results | Inline and links | Inline, links, `row_limit` | Same rows/limit through APC pages/links | Match rows; extra pages may raise latency (R4, R10–R12; AC31). |
  | Download/codec | Thrift result path | SEA requested codec | Hybrid; manifest codec | APC cannot select external-only or force uncompressed output. Callers may get inline/LZ4 (R4; AC8, AC101). |
  | `UseBoundedSeaApi` | No effect on Thrift | Changes paging and links; defaults to `1` | Bounded at either value | APC lacks legacy path totals. At `0`, rows match but fetch timing/buffering may differ; measure both protocol deltas (R4, R17; AC39). |
  | `EnableInlineStreaming` | Changes inline latency/buffering | Does not choose SEA inline mode | No effect on rows or chunk prefetch policy | Follow warehouse SEA; first-row latency and retained heap may differ from APC Thrift, especially at `0` (R17; AC111). |
  | `EnableStreamingChunkProvider` | Chooses Thrift provider | Changes legacy linked mode; bounded SEA forces streaming | Bounded at either value | APC legacy totals are unreliable. Rows match; memory/latency can differ (R17; AC39). |
  | Fetch tuning | `RowsFetchedPerBlock` tunes Thrift; HTTP settings tune links | SEA paging ignores row-block setting; SDK and links have separate pools | Same SEA scopes | Callers may see different latency/memory; measure worker, pool, and page bounds (R17; AC30, AC31, AC46). |
  | Public SQL async | Thrift operation lifecycle | SEA submit/poll | SEA submit/poll without status heartbeat | Follow warehouse SEA; APC polling can change network use and latency (R3, R10; AC55). |
  | DDL/DML/transactions | Counts and transaction SQL | Same public outcomes | Same outcomes | Match counts, committed rows, and errors; missing count is failure (R4, R19; AC33, AC48, AC102). |
  | Metadata/procedures | Native RPC; procedure errors empty | SHOW; procedure errors empty | SHOW; no procedure headers | APC server rejects those headers. JDBC rows/empty behavior match (R5; AC65, AC75). |
  | Unset `EnableThriftNativeMetadata` | Native RPC where selected | Property default `1`, gated by warehouse SEA flag | Acts as `0`; no header | Reyder still returns native-shaped rows. JDBC rows match; request observers see a different header (R5; AC65). |
  | Null-catalog schemas/columns | Native RPC | `42601` fan-out can omit catalogs | Same fan-out | Warehouse precedent; warnings and missing rows can reach callers (R5; AC83). |
  | Null-catalog tables/exported keys | Native RPC; valid exported keys empty | `42601` tables empty; valid exported keys empty | Same | Warehouse precedent can hide APC Thrift rows. Exported keys make no cross-reference request; `enableMultipleCatalogSupport=0` queries current catalog (R5; AC54, AC105). |
  | `getFunctions` setting | Native RPC or SHOW | SHOW at both values | SHOW at both values | Warehouse precedent; null-catalog rows, errors, or latency may differ (R5; AC82). |
  | Async metadata | JDBC waits | JDBC waits | Typed handles overlap when server accepts async SHOW | Approved additional API; synchronous JDBC remains. No V9 version gate (R5; AC50). |
  | Bindings/batches/SQL size | Complex types and ordered batches | Scalars, per-set counts, 16 MiB SQL cap | V8 scalars; V10 aggregate batches; same cap | Complex binding fails early; V10 successes use `SUCCESS_NO_INFO`. Chunk/SQL limits may shorten writes (R6, R15; AC17, AC29, AC87, AC108). |
  | Logical getters | Thrift setting behavior | Inline typed complex getters reject | Same SEA getters | Warehouse precedent; callers needing typed complex values use links (R11; AC13). |
  | Saved close | Thrift ID can misroute after flag change | Legacy IDs may misidentify Thrift | New ID retains protocol/target | Original endpoint/credentials are required; old IDs may require reopen (R9; AC25). |
  | Errors/retries/telemetry | Thrift transport/events | SEA SDK/events | SEA scopes/events | Warehouse precedent may change error identity. No write replay; measure resource and event parity (R8, R17; AC30, AC51, AC85, AC89). |

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
normal SEA SHOW-command semantics. This applies to HTTP paths with a concrete
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

The dotted field is `backend_compute.all_purpose_cluster_compute.cluster_id`. GET and
CloseStatement DELETE have no body. Polling omits `chunk_index`, including zero; chunk fetch
requires it in the path. Connection close in the first public slice sends TerminateSession;
a server rejection is diagnosed rather than called successful cleanup. Presigned-link URLs,
headers, and standard/Real-time warehouse serialization remain unchanged. Dogfood
TerminateSession target extraction depends on the tracked server binding fix.

- R3: APC execution polls with `GetStatementResult`. Result heartbeats are disabled even with
`EnableHeartbeat=1`: APC has no `GetStatementStatus`, and a post-completion result poll can
re-download payloads. No APC `/status` request is sent.
- R4: APC non-metadata execution sends `format=ARROW_BATCH`,
`disposition=INLINE_OR_EXTERNAL_LINKS`, and `session_id`, never `statements`.
Decode all bare batches in `chunk.arrow_batches` and complete Arrow IPC streams
at external links, retaining link headers and every row. `EnableQueryResultDownload=0`
keeps SEA and sets string-valued CreateSession
`session_confs["can_cloud_download"]="false"`; live results must remain inline.
An initial inline-size failure yields no successful ResultSet. A later fetch failure
propagates during iteration; rows already read remain delivered, and the cursor
must not report normal completion.
`EnableSQLExecHybridResults=0` cannot force external-only APC results, and
`UseBoundedSeaApi` changes neither APC request nor session behavior. Positive
`Statement.setMaxRows` sends `row_limit` and caps rows across pages/links;
zero is unlimited.

Successful no-result DDL has no ResultSet and update count zero, then `-1`
after `getMoreResults()`. Result-producing SQL, including `SELECT`, `SHOW`,
`VALUES`, `SET AUTOCOMMIT` state queries and `NonRowcountQueryPrefixes` matches,
requires schema and data before public success. Counted `INSERT`, `UPDATE`,
`DELETE`, and `MERGE` require a `num_affected_rows` schema column and value,
including zero; missing count data is a contract error. A matching
`NonRowcountQueryPrefixes` command instead exposes the count ResultSet and update
count `-1`, and stops a Statement batch with prior counts and the existing error.
Direct and polled DDL/DML live gates depend on T32.

APC advertises LZ4 decompression capability. `QueryResultCompressionType` and
its higher-precedence `EnableQueryResultLZ4Compression` alias may set the request
codec, but Reyder discards it; decode inline and linked data from the manifest's
actual codec, treating an unmarked response as uncompressed. `NONE` cannot
force uncompressed APC results without a server change; invalid text codec
settings retain the warehouse default of `LZ4_FRAME` (AC101). For failed SQL,
keep SQLException class, SQLState, and vendor code where APC Thrift and warehouse
SEA agree; otherwise follow warehouse SEA and document caller impact (AC89).
- R5: Seven APC metadata SHOW operations (`GET_CATALOGS`, `GET_SCHEMAS`,
`GET_TABLES`, `GET_COLUMNS`, `GET_FUNCTIONS`, `GET_PRIMARY_KEYS`,
`GET_CROSS_REFERENCE`) send `X-Databricks-Metadata-Operation-Type` with their
case-sensitive `GetCatalogs`, `GetSchemas`, `GetTables`, `GetColumns`,
`GetFunctions`, `GetPrimaryKeys`, and `GetCrossReference` values. They request
`ARROW_STREAM` and `INLINE_OR_EXTERNAL_LINKS` at either hybrid setting.
On APC, unset `EnableThriftNativeMetadata` acts as false; only explicit `1`
sends `X-Databricks-Require-Thrift-Native-Metadata: true` for supported SHOW.
Reyder still returns native-shaped metadata, so conversion follows
`manifest.is_native_metadata_result`, not the request setting. Synchronous
SHOW sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only at
`EnableSeaSyncMetadata=1`; async SHOW omits it. Decode each response as Arrow
IPC `chunk.attachment` or HIVE `chunk.columnar_data`/`COLUMN_BASED_SET`.

`enableMultipleCatalogSupport=0` retains current-catalog filtering; `1`
permits eligible catalogs. `EnableMetricViewMetadata=1` adds
`spark.sql.thriftserver.metadata.metricview.enabled=true` to session config
and exposes `METRIC_VIEW` table types and eligible rows; `0` omits both.
`getExportedKeys` is outside the seven SHOW operations: the DBR parent-only
`GetCrossReference` contract and warehouse SEA return typed empty for a valid
table, so APC SEA returns typed empty without a cross-reference or metadata SHOW
request. With `enableMultipleCatalogSupport=0`, the existing current-catalog
filter issues `SELECT CURRENT_CATALOG()` first, as warehouse SEA does; with
`1`, a valid table needs no request. A null table retains warehouse SEA's
`SQLException`, SQLState `42000`, and `EXECUTE_STATEMENT_FAILED` (1003) vendor
code before any request (AC54).

The session-reachable async metadata API accepts those seven operations and
JDBC inputs and returns typed ResultSet handles. Async SHOW uses the server's
`wait_timeout=0s` submission path, then awaits within the metadata timeout with
synchronous conversion, empty, error, and cancel behavior. No negotiated V9
capability is defined for this API: first exposure requires live proof that a
delayed APC SHOW returns a handle while RUNNING, including with an older or
versionless negotiated response if such a response is available. A versionless
fixture must not infer a capability from a saved ID. APC Thrift and warehouse
SEA retain existing synchronous JDBC results.
Public `getColumns` remains synchronous, and SQL async is separate.

Internal catalog/schema SELECTs and parameterized `getProcedures` and
`getProcedureColumns` SELECTs are ordinary hybrid `ARROW_BATCH` queries without
metadata headers. Reyder rejects the procedure headers sent by warehouse SEA;
APC procedure queries use R6 scalar binding and cannot use async SHOW.
The public `getProcedures` and `getProcedureColumns` methods retain their
typed-empty-on-error behavior for query, fetch, and decode failures, as on
APC Thrift and warehouse SEA; diagnostics record the failure (AC75).
`EnableShowCommandForGetFunctions` still selects native RPC or SHOW
on APC Thrift, whose SHOW path returns typed empty for null catalog; APC SEA,
like warehouse SEA, uses SHOW at either setting (AC82).

For async null-catalog `GET_SCHEMAS`/`GET_COLUMNS`, initial SQLState `42601`
triggers catalog enumeration and per-catalog SHOW with the operation header
and no sync header. As on synchronous warehouse SEA, a per-catalog
`SQLException` warns and omits that catalog; initial/enumeration/overall errors
propagate except established object-not-found or empty-pattern typed-empty
cases. Overall timeout/cancel fails; partial rows and a usable connection remain
(AC83). Null-catalog `getTables` instead returns typed empty on `42601`
without fan-out, as warehouse SEA does, which may hide APC Thrift rows (AC105).

- R6: CreateSession `negotiated_protocol_version` alone grants native scalar
parameters at V8 (42248) and opt-in `EnableNativeBatching=1` INSERT batches
at V10 (42250). It does not gate R5 async metadata. Reyder leaves
`session_version` unset; serialized IDs and missing versions grant no capability.
Retain the negotiated version for later requests. Reject complex bindings
before any single or batch request with a tested driver-owned error name/code;
`toString()` interpolation is not lossless SQL. At `supportManyParameters=0`,
V8+ uses native scalar `parameters` and older/versionless sessions interpolate
supported scalars without unresolved markers; V8–V9 legacy batch sets may use
native scalars. At `supportManyParameters=1`, single and legacy batch sets
interpolate at every version and non-null `setBytes` is permitted; at `0`,
non-null `setBytes` remains rejected. Keep missing-version telemetry distinct.

Native V10 sends ordered nested `parameter_sets`, no top-level `parameters`.
Its aggregate count yields `Statement.SUCCESS_NO_INFO` for each successful
input set. If execution fails, a `BatchUpdateException` marks every set
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

SEA's 16,777,216 UTF-8 byte SQL-text limit applies to single statements and
legacy generated chunks without silent split or fallback; an oversized request
propagates the warehouse SEA error. `BatchInsertSize` can reduce interpolated
legacy chunk size; native-scalar chunking follows the parameter limit.
Pre-execution `PreparedStatement.getMetaData()` uses binding-free
`DESCRIBE QUERY` without losing bindings. Warehouse behavior stays unchanged.
- R7: R1 selects the protocol before connecting; `UseBoundedSeaApi` does not affect that
decision. After SEA is selected, server errors, including unsupported regions, propagate
without Thrift fallback. Warehouse-only KP001 Thrift-to-SEA recovery does not change APC routing.
- R8: Every APC and warehouse SEA/Thrift connection attempt emits compute type, selected
protocol, routing reason, and success/failure, including failed opens. Log when explicit
SEA selection ignores `TreatMetadataCatalogNameAsPattern=1`. Preserve the existing
operation-latency, result-format, total-chunk, chunk-download-latency, and error telemetry
families for APC SEA execution, metadata, linked downloads, batches, and failures, with the
selected protocol and compute identity available for correlation. Compare event presence,
operation type, format, counts, error name/code, SQLState, and vendor code at each public
slice with APC Thrift and warehouse SEA (AC85). APC diagnostics and telemetry redact
org/cluster ids and compound session/statement ids, including execute and session logs;
no event leaks a raw target through a message or serialized identity.
- R9: From the first flag-on connection slice, public `getConnectionId()` returns a usable
ID encoding the selected protocol and target; `SessionId.create()` must not require a
Thrift handle for an APC SEA session. Given the original workspace endpoint (scheme, host, port)
and usable credentials,
new saved IDs let `Driver.closeConnection` close the original session after a flag or compute-target
change in the supplied URL or properties. The ID does not encode the endpoint or credentials;
endpoint and credential changes are outside this guarantee. Public statement IDs remain raw.
Internal identities retain the compute target and reject malformed IDs without logging identifiers.
Preserve decoding of legacy warehouse SEA and Thrift IDs;
legacy warehouse Thrift IDs encoded as SEA by `origin/main` lack the Thrift secret and cannot
be repaired for saved close, so callers holding them may need to reopen. Recovered statements
use the current connection target. Only a live CreateSession version grants capabilities.
- R10: APC ExecuteStatement keeps SEA `wait_timeout` unset for synchronous direct results and
uses `0s` for async or `EnableDirectResults=0` (alias `EnableSQLExecDirectResults=0`). Public SQL `executeAsync` and
`getExecutionResult` use APC targeting, polling, and decoding separately from async metadata.
Measure server request timeouts separately from JDBC waits, including the extra
`GetStatementResult` round trip (AC60, AC63).
- R11: Decode inline `ARROW_BATCH` from `manifest.schema` (`type_text`/`type_json`, including
nested types), without requiring `manifest.arrow_schema`; external links remain full Arrow IPC
streams. A deferred logical type fails before false ResultSet success. Preserve
`EnableComplexDatatypeSupport` and `EnableGeoSpatialSupport` effects on JDBC metadata and
`getObject`: disabled support returns established strings and rejects typed complex getters;
enabled support permits linked typed getters and native geospatial values where applicable.
Inline typed complex getters still reject, so callers needing them require linked results.
- R12: Page APC results by `next_chunk_index` and `row_offset`, without requiring totals or
inline data in the initial response. The next inline offset includes every batch in a chunk;
for linked results it follows the final link's end offset and continuation. Missing final-link
`next_chunk_index` ends paging.
- R13: Decode APC SEA `columnar_data` physical vectors and null masks using
`manifest.schema`; JDBC columns, values, and nulls match APC Thrift metadata.
The server sets a mask for every vector. Within each mask byte, bit 0 is the
first row (least-significant-bit first);
`1` means null. A null row still consumes its placeholder vector value. Rows
beyond a short mask are non-null; bits beyond the row count are ignored.
An unrecognized vector fails clearly. Test-plan fixtures cover emitted vectors,
short and long masks, and byte boundaries.
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
deserialize safely and fail clearly if decoded.
- R17: APC SEA follows warehouse SEA's SDK retry/pool and streaming CloudFetch behavior
at either `UseBoundedSeaApi` value. `EnableInlineStreaming` and
`EnableStreamingChunkProvider` cannot change APC SEA rows, paging, or chunk prefetch
policy; APC
continuation lacks reliable totals for the legacy linked behavior.
Unlike APC Thrift, which changes between streaming and lazy inline providers with
`EnableInlineStreaming`, APC SEA follows warehouse SEA's separate inline provider:
the switch does not select a provider or change APC SEA buffering. Callers may
observe different first-row latency and retained heap when migrating at `0`; AC111
measures and dispositions that difference.
`ThriftMaxBatchesInMemory` bounds queued inline response chunks (minimum two), not
the number of Arrow batches already present inside one response. The driver
may also hold the current chunk and one in-flight fetch response. It must consume
each chunk incrementally, avoid materializing the full result, and
record both queued chunks and their contained batch/byte counts (AC109).
Case-insensitive `cloudFetchThreadPoolSize` (default 16) bounds per-result linked
download workers and buffered chunks; `LinkPrefetchWindow` (default 128) bounds
fetched-ahead links, not workers, chunks, or inline pages. `ChunkReadyTimeoutSeconds`
bounds linked-chunk wait. `RowsFetchedPerBlock` does not tune SEA paging.
Thrift HTTP retry and connection-acquisition settings tune presigned-link downloads,
not SEA SDK API requests. `IdleHttpConnectionExpiry` applies to idle connections
in both the SDK API pool and the linked-download HTTP pool. Match warehouse SEA
429/503 and `Retry-After` handling
by request class without new write retries. No APC-only concurrency cap or
full-result buffer is allowed (AC30/46/51/84).
- R18: Every approved JDBC slice exposes its completed public behavior when R1's default-off
APC flag is enabled or `UseThriftClient=0` is explicit. Incomplete public calls, including
`executeAsync`/`getExecutionResult` before T39, fail before network execution when known;
unsupported response shapes fail before false success. They never
silently switch to Thrift after SEA selection. After T17 and T35 deploy, a scoped staging
assignment may enable flag-on validation; production assignment or ramp waits for T18/T-val
on the assembled approved stack and a post-merge smoke check. Remove each guard when its
slice passes live staging, and keep ordinary flag-off APC connections on Thrift.
The first public SELECT slice includes T58's CreateSession download setting,
single-chunk inline behavior, and statement cancel/close. Until the paging
slice passes AC112, a download-disabled multi-page result fails before false
success. Nondefault row-limit or query-timeout settings, `EnableDirectResults=0`,
and SQL-backed `isValid` are guarded before request until their controls slice passes.
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
A9: Yes, including JDBC partners allowed by `enableSqlExecForJdbc`. Deploy T35 before APC
assignment; partner enablement needs a separate rollout decision (R1, T17).
- Q10: How is repeated transfer avoided during `GetStatementResult` polling?
A10: At least three RUNNING polls of a large query transfer no result payload; terminal
response and fetch return all rows once (AC27).

## Acceptance Criteria

At first exposure, each public slice proves its completed call on scoped flag-ON APC staging
against APC Thrift and warehouse SEA where relevant. AC3/11/40/51/84/85 apply to each new
request class; AC30 applies to a new pool and AC10 to a new DEBUG path. For induced
failures, use a controlled staging fault proxy or service hook that preserves the real
request, then replaces only the specified response and records the mutation. If no such
hook is available, deterministic fake-service fixtures may close only that induced
failure branch; record the unavailable hook and retain live ordinary flag-on and paired
parity gates. Fixtures cannot close an ordinary live gate. Server-dependent gates stay
pending until deployment and live proof. Remaining AC IDs stay stable; retired duplicates
(AC41/47/103/104/110) are not reused.

For the shared paired-load gate (AC31/32/33/46/48/61/75/107), compare the same
driver build, staging region, warmed cluster, data shape, and one/32-caller workload
on APC SEA and APC Thrift. Median SEA/Thrift throughput must be at least 0.90,
latency at most 1.10, and peak heap at most 1.20. AC31/32 additionally bound
retained heap growth across two data sizes to max(8 MiB, 5% of added data bytes).
Keep raw measurements, fixture sizes, and error counts as release evidence.
The test plan owns rounds, sampling, and fixture matrices. For every proposed non-functional
exception in this spec, also measure warehouse SEA versus warehouse Thrift on the same driver build and
comparable workload, then record the APC and warehouse protocol deltas, server limitation or
warehouse precedent, caller impact, and disposition in the parity matrix. A measured exception
requires PECO/Perf Eng approval; an unmeasured or undispositioned difference leaves the gate open.

- AC1: GIVEN scoped flag-ON and flag-OFF APC staging WHEN `UseThriftClient` is unset, `0`, or `1` and `TreatMetadataCatalogNameAsPattern=1` is varied THEN live routing proves flag default, forced Thrift with flag ON, forced SEA with flag OFF, and explicit SEA precedence over catalog-pattern Thrift. With the scoped APC flag ON and client override unset, repeat a public JDBC SELECT using ordinary, partner, and JDBC-allowlisted partner User-Agents: only the ordinary User-Agent takes SEA; both partners stay on APC Thrift and return the paired APC Thrift rows. Capture the selected route and connector-service flag value for each. Compare APC Thrift and warehouse SEA; fixtures cover Arrow-unavailable, legacy `cliservice`, and lookup failure. Selected SEA errors never fall back.
- AC2: GIVEN flag-on APC staging WHEN a cluster-path connection opens, runs a primitive single-chunk SELECT, and closes THEN CreateSession carries that cluster ID without `warehouse_id`, rows/order/types match APC Thrift and warehouse SEA, and targeted TerminateSession succeeds after its server fix. Fixtures cover absent `arrow_schema` and unknown R16 wire formats without false success.
- AC3: GIVEN each new APC R2 operation WHEN it runs, including fetch/cancel/close after a flag change THEN R2 target/body placement holds, the original statement target is used, running statements close once, and the connection remains usable; warehouse serialization is unchanged. For a live flag-on SELECT, set `EnableHeartbeat=1` and a short heartbeat interval, hold its ResultSet open across at least two intervals, and capture zero APC `/status` requests while rows and close still work.
- AC6: GIVEN live APC prepared SQL with bound scalars WHEN `getMetaData()` runs before
execution THEN it returns JDBC metadata without losing bindings; subsequent execution
matches APC Thrift and warehouse SEA. AC53 covers the version and interpolation matrix.
- AC8: GIVEN a live single-chunk APC SELECT with `EnableQueryResultDownload=0` at either `UseBoundedSeaApi` value WHEN the first SELECT slice runs THEN CreateSession sends `session_confs["can_cloud_download"]="false"`, its result stays inline, and an inline-size error fails before partial success. Capture the CreateSession request and result path.
- AC9: GIVEN live APC SEA CreateSession rejection WHEN connecting THEN the error and failed
route event propagate without Thrift fallback. GIVEN APC Thrift OpenSession returns KP001 WHEN
connecting THEN warehouse-only recovery does not select SEA.
- AC10: GIVEN successful and failed opens or statements WHEN DEBUG logs and telemetry are captured THEN route/outcome/compute/protocol appear without credentials or raw APC IDs; recheck linked and recovered paths when exposed.
- AC11: GIVEN standard and Real-time warehouse connections WHEN equivalent R2 operations run THEN JDBC outcomes and URL/body/header serialization, including `realtimeSession`, match pre-change `origin/main`; each slice checks affected classes.
- AC12: GIVEN APC synchronous SELECT WHEN `EnableDirectResults` is 1 or 0 THEN `wait_timeout` is respectively absent or `0s`, `statements` is absent, and rows/errors match both baselines; record first-row latency and requests.
- AC13: GIVEN primitive, decimal, array, map, struct, interval, timestamp,
timestamp_ntz, geography, geometry, and variant values WHEN inline and linked getters
run with complex/geospatial settings at 0/1 THEN types and values match both baselines;
inline typed complex getters reject, enabled linked getters work, and disabled modes
return established strings. Unavailable live types remain pending despite fixtures.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN each synchronous APC SHOW when introduced in emitted Arrow IPC or columnar format WHEN read on live staging THEN JDBC columns, values, nulls, and errors match the applicable baseline and R5. HIVE fixtures verify R13's least-significant-bit mask order, placeholder consumption, short-mask non-null rows, and ignored trailing bits; fixture-only formats remain pending live proof.
- AC17: GIVEN live V10 APC and paired routes WHEN native prepared INSERT batches succeed or fail through either batch API THEN ordered `parameter_sets` return `SUCCESS_NO_INFO` per successful input; execution failure gives `BatchUpdateException`, `EXECUTE_FAILED` for every set, and uncertain commit status. Mutate only a successful write's terminal count response through a staging fault hook: committed rows remain, JDBC raises the distinct result error without failed-write counts, warns that rows may be committed, and clears the batch. Record mutation boundary, rows, and error. If mutation is unavailable, a fixture closes only that induced branch; live success, failure, and load remain required.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. Compare elapsed time, SQLState/vendor code, target, and
post-timeout connection usability on flag-ON staging.
- AC22: GIVEN a live Statement INSERT batch with a failing middle command WHEN APC SEA
executes it THEN requests are sequential without `statements`, counts retain order, execution
stops on failure, and errors match APC Thrift. AC32/64/66 cover load and settings.
- AC23: GIVEN live and captured versionless CreateSession responses WHEN APC connects THEN
only `negotiated_protocol_version` grants R6 capabilities; neither `session_version` nor a
saved ID does. Record the live version and missing-version diagnostic.
- AC25: GIVEN new APC and warehouse SEA/Thrift IDs with the original endpoint and credentials WHEN `Driver.closeConnection` uses `getConnectionId()` after a flag or target change THEN only the original session closes, without an APC SEA Thrift handle. Legacy IDs decode, malformed IDs fail without logging IDs, statement IDs stay raw, and lifecycle/errors match baselines. Prechange warehouse Thrift IDs may require reopen; changed endpoint or credentials are outside the guarantee.
- AC27: GIVEN a live single-chunk APC query with three or more RUNNING polls WHEN transfer
is measured on explicit-override and flag-on routes THEN polls carry no result payload and
the terminal fetch delivers every row once; compare APC Thrift and warehouse SEA traffic.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN flag-on APC staging and the paired APC Thrift/warehouse SEA routes WHEN complex scalar or batch bindings are submitted THEN APC SEA raises its named/numeric driver error before any request, keeps the connection usable, and records the route's error identity against both baselines. Record the owner-verified taxonomy/dashboard update or maintainer handoff in the PR.
- AC30: GIVEN flag-on APC staging and paired APC Thrift/warehouse SEA under pool saturation WHEN `HttpConnectionPoolSize`, `HttpMaxConnectionsPerRoute`, and `HttpConnectionRequestTimeout` vary separately for each newly exposed API or linked-download class THEN measured in-flight count, wait, transport error identity, and throughput follow the applicable transport scope; SDK API acquisition ignores the Thrift HTTP wait setting.
- AC31: GIVEN each SELECT mode at its first public exposure WHEN one and 32 paired APC SEA/Thrift callers drain primitive, multi-page, or linked results as applicable THEN rows/errors match, SEA paging ignores `RowsFetchedPerBlock`, and the shared load gate passes. The release gate repeats the combined mode mix.
- AC32: GIVEN flag-on live APC SEA and paired APC Thrift/warehouse SEA legacy, V10 native prepared, and Statement INSERT batches WHEN one and 32 callers use disjoint keys and R6 settings THEN committed rows, counts, errors, and the shared load gate pass for each exposed mode. Record negotiated version; captured V10 responses cannot close native correctness or load gates.
- AC33: GIVEN successful live APC DDL with absent or zero-column manifest WHEN public JDBC executes it
THEN no ResultSet/fetch appears, execute is false, count is zero, and getMoreResults makes it -1.
Repeated close is safe; failed DDL preserves its error, while result-producing SQL without schema
fails before success. Paired one/32-caller create/drop on isolated schemas passes the shared command load gate.
- AC34: GIVEN the deployed APC flag with a scoped staging assignment ON for a non-partner User-Agent WHEN the same connector-service flag lookup runs for that User-Agent, a partner User-Agent, and an allowlisted JDBC partner User-Agent THEN the APC sub-flag is true only for the non-partner and false for both partners. A separate default-off lookup proves the production default; record the returned values and version.
- AC36: GIVEN APC chunks with multiple Arrow batches and inline continuations WHEN JDBC drains them THEN rows appear once and `row_offset` includes all batch rows; a failed page cannot report success. If live staging lacks multiple batches, retain a fixture and leave live proof pending.
- AC39: GIVEN each inline or linked mode at its first flag-on live exposure WHEN hybrid, bounded-SEA, and streaming switches vary at one and 32 callers THEN R4/R17 rows, requests, and AC31 load bounds hold against APC Thrift and warehouse SEA; switches that cannot select an APC wire mode do not alter returned rows or bypass buffering limits.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF THEN APC uses Thrift; on scoped
flag-ON staging completed calls pass their live parity gate. Known incomplete calls fail
before SEA requests without Thrift fallback; public SQL async stays guarded until T39 and
production assignment waits for T18/T-val.
- AC45: GIVEN APC results WHEN `EnableSeaResponseCompression` changes THEN `0` sends `Accept-Encoding: identity`, `1` permits API compression, and neither alters presigned links; compare API bytes and latency with both baselines.
- AC46: GIVEN paired linked results at one and 32 callers WHEN `cloudFetchThreadPoolSize`, `LinkPrefetchWindow`, chunk wait, and slow-link settings vary independently THEN active downloads and buffered chunks stay within the per-result worker limit, fetched-ahead links within the window, and the aggregate limits scale by caller count. Rows, queue depth, timeouts, warnings, duration, and AC31 load bounds match the baselines; inline pages are unaffected.
- AC48: GIVEN live APC counted `INSERT`, `UPDATE`, `DELETE`, or `MERGE` WHEN public
update methods run THEN counts, including zero, match APC Thrift after T32. A missing
`num_affected_rows` column, row, or value produces a contract error, not successful zero.
Paired one/32-caller writes on disjoint keys pass the shared command load gate.
- AC49: GIVEN each live SELECT mode after T59 exposes row limits WHEN
`setMaxRows(0)`, `1`, and boundaries applicable to that mode run THEN positive limits send SEA
`row_limit`, zero is unlimited, and rows never exceed the limit. Compare rows and request
count with APC Thrift and warehouse SEA; fixtures cover unavailable boundaries.
- AC50: GIVEN each typed SHOW handle when introduced on live APC WHEN it submits a delayed operation through `wait_timeout=0s` THEN it returns while RUNNING and another call completes before await. Awaited rows/errors, timeout, cancel, and cleanup match synchronous APC and both baselines for that operation; no V9 gate is inferred from the version. An older/versionless response fixture verifies the same request and completion contract. Procedure SELECTs reject async before request.
- AC51: GIVEN isolated staging 429/503 responses with and without `Retry-After` for each new SEA API or link-download class WHEN R17 settings apply THEN attempts, delay, exception identity, SQLState, and vendor code match warehouse SEA without write replay. Include exhausted CreateSession; Thrift HTTP settings affect links only. If a class cannot be faulted live, a fixture closes only its injected retry branch. Ordinary flag-on execution, paired load, and transport scope still need live proof; record the hook or fixture.
- AC52: GIVEN scoped flag-on staging and a paired APC Thrift or warehouse SEA route WHEN public JDBC runs no-result DDL and counted DML through both direct `ExecuteStatement` completion and polled `GetStatementResult` completion THEN live captures show successful DDL without a result schema and successful DML with the affected-row schema and value, including zero; public AC33/AC48 outcomes match the paired route and failed commands retain errors. Fixtures alone cannot close either terminal path.
- AC53: GIVEN live APC prepared scalars or fallback batch mode when introduced, plus older/versionless fixtures WHEN `supportManyParameters` is 0 or 1 THEN V8+ at 0 sends native scalars, while older/versionless at 0 and every version at 1 interpolate without unresolved markers; `setBytes` follows R6. Rows/counts and request shape match both baselines for the exposed mode; identify fixture-only branches.
- AC54: GIVEN a valid table with a referencing foreign key WHEN `getExportedKeys` runs live on all three routes with `enableMultipleCatalogSupport=1` and `0` THEN each returns typed empty, APC SEA makes no cross-reference or metadata SHOW request, and APC Thrift sends parent-only GetCrossReference. At `1`, APC SEA makes no request; at `0`, it issues `SELECT CURRENT_CATALOG()` with warehouse SEA's request count and filtering. A null table at either setting fails before request with warehouse SEA's `SQLException`, SQLState `42000`, and `EXECUTE_STATEMENT_FAILED` (1003). Thrift gaining rows reopens parity.
- AC55: GIVEN public APC SEA SQL async SELECT WHEN primitive and linked results complete
THEN `wait_timeout=0s`, targeting, rows/errors, cancel, close, and diagnostics follow R2/R10.
Apply AC31's one/32-caller load bounds to this public path before unguarding it.
- AC60: GIVEN a valid live query within its configured client wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Record timing,
error identity, and connection usability against both baselines; an exception needs caller
impact and PECO approval.
- AC61: GIVEN each nonempty metadata operation or typed handle at first exposure WHEN one and 32 callers run it THEN rows/errors and shared load bounds pass; null-catalog fan-out makes at most N+2 requests for N eligible catalogs. For the resource comparator, issue the same APC-targeted SEA SHOW or procedure SELECT directly on the same staging workspace and driver build's SDK transport, with identical headers, catalog inputs, pages/links, and caller count; decode the response into the same Java scalar values and count row-column cells as JDBC does. Measure process CPU per 1,000 materialized cells and peak RSS above the same idle baseline over repeated warmed runs, excluding client setup on both paths. JDBC CPU is at most 110% and RSS at most 120% of that paired direct SEA route. Record requests, cell counts, data bytes, and both measurements. The final slice repeats the combined SHOW/handle/procedure mix; typed empty cannot replace nonempty samples.
- AC63: GIVEN valid delayed SHOW within its configured metadata wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Record timing,
error identity, and usability against both baselines; an exception needs caller impact and
PECO approval.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching
INSERT runs THEN `execute` returns true, its ResultSet contains the affected-row value,
and `getUpdateCount` returns `-1`. Once Statement batches are exposed, that command
stops the batch with preceding counts and the baseline error identity.
- AC65: GIVEN each exposed live APC SHOW with `EnableThriftNativeMetadata` unset, `0`, and `1` and
`EnableSeaSyncMetadata` at `0` and `1` WHEN a supported synchronous method runs THEN the
native request header appears only at native setting `1`, the sync header only at sync
setting `1`, and native-shaped JDBC columns/values match APC Thrift and warehouse SEA at
all settings. Async SHOW omits the sync header when typed handles are exposed. A changed Reyder response shape leaves
this gate pending for contract review.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized `getProcedures` and `getProcedureColumns` WHEN each runs across pages/links THEN header-free R5/R6 queries return ordered JDBC rows matching baselines and pass AC61. On query/fetch/decode failure, public JDBC returns the same typed empty result as both baselines and logs the failure; zero matches are also typed empty. Older/versionless interpolation needs fixtures.
- AC82: GIVEN live `getFunctions` with an explicit catalog and with null catalog WHEN
`EnableShowCommandForGetFunctions` is `0` and `1` on APC SEA, APC Thrift, and warehouse SEA
THEN APC SEA sends the R5 SHOW request at both values, rows and errors follow the documented
warehouse SEA precedent, and the APC Thrift native/SHOW choice and null-catalog difference are
recorded with client impact.
- AC83: GIVEN live null-catalog getSchemas/getColumns with 42601 fan-out WHEN one catalog errors,
times out, or cancels THEN APC and warehouse SEA warn, omit only that catalog, and keep the connection
usable; record lost rows against APC Thrift. Initial object-not-found/empty-pattern yields typed
empty, while other initial or overall failures propagate.
- AC84: GIVEN each new live request class WHEN its applicable timeout, poll, and idle settings vary THEN `socketTimeout` bounds one SDK API or linked request, and `asyncexecpollinterval` changes delay after the first poll. At their first exposure, verify `IdleHttpConnectionExpiry` evicts idle SDK API and linked-download HTTP connections independently. For SHOW, `MetadataOperationTimeout` bounds the overall wait and targets cancel; `0` disables it. In-budget calls complete; timing, error identity, request counts, and usability match the applicable baselines without fallback.
- AC85: GIVEN each live execution, metadata, download, batch, and failure path WHEN
telemetry exports THEN R8's redacted event families, operation/format/counts, and error
identity match both baselines or document a measured exception.
- AC86: GIVEN live APC with `EnableSQLValidationForIsValid=1` WHEN
`isValid(timeout)` runs its `SELECT VERSION()` check THEN healthy/failed results, timeout,
and connection usability match APC Thrift and warehouse SEA.
- AC87: GIVEN SQL at 16,777,215/16,777,216/16,777,217 UTF-8 bytes WHEN an exposed
single statement or legacy INSERT chunk executes THEN APC SEA accepts up to the SEA limit
and propagates the warehouse SEA oversize error above it without fallback. Record bytes,
counts, committed rows, and APC Thrift differences; `BatchInsertSize` can reduce
interpolated legacy chunks, while native-scalar chunks follow the parameter limit.
- AC88: GIVEN two accessible catalogs and a metric view WHEN `enableMultipleCatalogSupport` and `EnableMetricViewMetadata` vary on all three live routes THEN catalog access, discovery, table types/rows, and APC session setting match R5; record any server-limited row difference and caller impact.
- AC89: GIVEN live invalid SQL, missing-object, and permission failures WHEN applicable execute
methods surface them THEN record SQLException class, SQLState, vendor code, and message category
across APC SEA/Thrift and warehouse SEA. APC SEA matches agreement between baselines, otherwise
measured warehouse SEA mapping with the caller impact; no failed command reports success.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN each APC inline or linked mode when introduced WHEN result-codec settings select `NONE` or `LZ4_FRAME` THEN APC decodes the manifest's actual codec, including unmarked data and LZ4 despite requested `NONE`, and returns baseline rows; invalid text retains R4's default. Record requested/actual codecs and the shared load-gate impact; a selectable APC codec requires a server change.
- AC102: GIVEN an isolated write table WHEN transaction and auto-commit settings vary on all three live routes THEN state, rows, no-ops, and errors match R19, including missing state-row and failed-command behavior; record cause, SQLState/vendor code, and usability.
- AC105: GIVEN live null-catalog `getTables` that receives `42601` WHEN each exposed
synchronous call or typed async handle is compared across APC SEA, APC Thrift, and
warehouse SEA THEN APC SEA and warehouse SEA return the same typed empty table result
without catalog fan-out; record any APC Thrift rows omitted and the caller impact.
- AC106: GIVEN a changed driver-owned APC SEA error WHEN it reaches JDBC THEN a test asserts its `DatabricksDriverErrorCode` name and unique number, and the PR records owner-verified telemetry taxonomy/dashboard updates or maintainer handoff and completes the telemetry-error template. Server errors retain server identity.
- AC107: GIVEN one and 32 concurrent callers on paired APC SEA and APC Thrift staging
WHEN each repeatedly opens, executes, cancels, and closes a statement/session, including
saved-ID close after a flag change, THEN no live session or statement remains; server
active counts and client handles/threads return to baseline, retained heap growth is at
most 8 MiB, and median SEA/Thrift cycle latency is at most 1.10. Record cleanup
delay or exceptions with caller impact and PECO/Perf Eng approval.
- AC108: GIVEN flag-on live APC legacy prepared INSERT batches with `EnableBatchedInserts=1` and paired APC Thrift/warehouse SEA WHEN `BatchInsertSize` and the 256-parameter limit vary THEN interpolated chunks change only with `BatchInsertSize`, while native-scalar chunks change only at the parameter limit; eligible sets combine into bounded request groups and ineligible sets remain individual. Record binding mode, negotiated version, request groups, committed rows, ordered counts, errors, and throughput against each baseline. With batching disabled, eligible sets no longer combine. No generated chunk exceeds the SEA SQL-text limit.
- AC109: GIVEN concurrent flag-on live APC multi-page inline SELECTs and paired APC Thrift/warehouse SEA WHEN `ThriftMaxBatchesInMemory` varies below two and above two THEN at most max(2, configured value) response chunks are queued per result, plus the chunk currently being consumed and one in-flight fetch response. Count the Arrow batches and bytes in every queued/current/in-flight chunk, including the initial response; a single response may exceed the configured value in batches. Peak resident result data stays within those observed chunks rather than growing with total result size, all rows arrive once, and AC31 heap/load bounds hold. Record the chunk count, per-chunk batch/byte totals, and peak heap at both settings.
- AC111: GIVEN paired one- and 32-caller APC SEA/Thrift and warehouse SEA/Thrift inline SELECTs WHEN `EnableInlineStreaming` varies at `0` and `1` at each applicable SEA mode THEN rows and page counts are unchanged, each SEA route's provider selection is unchanged by this switch, and APC SEA stays within the R17 queued-chunk bound. Measure APC Thrift's lazy/streaming effect; record first-row latency, peak and retained heap, protocol deltas, caller impact at `0`, and the shared AC31 load disposition. A non-functional exception requires PECO/Perf Eng approval.
- AC112: GIVEN a live multi-page APC SELECT with `EnableQueryResultDownload=0` at either `UseBoundedSeaApi` value WHEN the inline paging slice drains it THEN every page remains inline and rows appear once. An initial inline-size error prevents successful ResultSet creation; a later-page error propagates from iteration without a normal end-of-rows signal, although earlier rows may already have been read. Capture CreateSession and page requests; compare APC Thrift and warehouse SEA rows and error timing.
- AC113: GIVEN the T32 Reyder change deployed on scoped APC staging WHEN direct `ExecuteStatement` and terminal `GetStatementResult` are called through SEA for no-result DDL and counted DML, including a zero count, THEN wire captures show success without a DDL result schema, a DML `num_affected_rows` schema/value, and unchanged failed-command errors. This server-only gate precedes public JDBC AC52; a JDBC fixture cannot close it.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The last JDBC package alone owns `docs/`
  and `NEXT_CHANGELOG.md` and records the complete gated feature. Earlier stacked PRs use
  `NO_CHANGELOG=true` only as internal implementation slices of that same unreleased feature:
  no driver release or production APC assignment may occur between slices. This basis must be
  stated in each PR; if a release is cut early, its visible slice needs a changelog update
  before release. Completed slices remain shippable behind the flag, with no partial ramp.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T32 and T35; known gaps SC-256188 under SC-256186 remain
  external prerequisites for affected dogfood checks. SC-254478 is outside this
  project's CreateSession-only download contract.
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

`plan.json` assigns every task once. Twelve JDBC packages each expose a bounded public
outcome behind the default-off APC flag, with a target of fewer than about 1,500 changed
lines per PR. This exceeds the original 4–7-package aim because the live command, batch,
SHOW, and typed-handle outcomes need separate review and staging gates. The packages form
one dependency chain: server flag and terminal-command wire proof precede the JDBC stack;
connection, inline, linked, and scalar SELECT behavior precede metadata procedures; the
write modes and synchronous SHOW groups precede the final typed handles and assembled
validation. Each JDBC child branches from its JDBC parent. A cross-repository `parent`
in the plan denotes a deployed prerequisite, not a Git branch across repositories.
The chain avoids an undeclared sibling merge before the final gate; independent code
preparation can proceed in parallel, but each package is validated on its declared parent.
Each package checks newly exposed request classes against AC3/11/40/51/84/85, AC30 for
a new pool, and AC10 for a new DEBUG path. The first SELECT package proves
`EnableQueryResultDownload=0` for a single inline chunk; its multi-page behavior is
gated with the inline paging package. Public controls that await a later slice fail
before request. T32's server-only AC113 gates public JDBC AC52 and dependent batches; the tracked metadata and
TerminateSession fixes gate their affected live checks. No package is approved with
an open live or performance gate. The last behavior package owns docs/changelog and
the assembled test gate; there is no release-only PR.

### Affected Components

- Public JDBC connection, statement, prepared-statement, and metadata APIs.
- SEA request targeting, session and statement identity, result decoding and paging.
- Driver diagnostics and error taxonomy; Reyder and connector-service contracts in Universe.

### Data Model Changes

- APC target and live capability version in session state.

### API Changes

Existing public JDBC methods retain their behavior. The approved session-reachable
asynchronous metadata API covers seven SHOW operations without a V9 capability gate.
Public connection IDs may encode APC SEA; statement IDs stay raw.

### Risks

- Tracked server fixes gate live metadata and termination checks; fixtures
  protect unavailable wire shapes before deployment.
- AC13/Q7 block release on lost logical types; AC27 blocks repeated poll payload transfer.
- AC11/AC14 protect warehouse request and presigned-link boundaries.

---

## Tasks

R/AC define each task's observable contract; `plan.json` gives its sole package owner.

| Outcome | Task IDs |
| --- | --- |
| Server APC flag and partner exclusion | T17, T35 |
| Server terminal command wire results | T32 |
| APC connection and saved-ID close | T1, T2, T3, T4, T6, T19, T26, T29, T44 |
| Primitive inline SELECT, including download disabled | T7, T8, T10, T27, T45, T47, T57, T58, T61 |
| Paged inline SELECT and public controls | T11, T12, T25, T34, T59, T62, T63 |
| Linked and public async SELECT | T15, T20, T33, T36, T39, T46 |
| Prepared scalar SELECT and versioned binding | T5, T9, T21 |
| DDL/DML, counts, errors and transactions | T22, T28, T38 |
| Legacy prepared and Statement batches | T30, T60 |
| V10 native prepared batches | T31, T64 |
| Synchronous catalog/schema/table discovery | T13, T14, T37, T42, T43 |
| Synchronous columns/functions/keys/procedures | T48, T49, T54, T55, T56 |
| Async catalog/schema/table handles | T23, T24, T40, T41, T50 |
| Remaining async handles, docs and assembled validation | T16, T18, T51, T52, T53, T-val |
