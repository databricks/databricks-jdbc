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
- Known server gaps are tracked under SC-256188/SC-256186. Affected live metadata and
  session-termination gates remain pending until fixes deploy. `SC-254478` concerns
  per-statement `ExecuteStatement.session_confs`; it does not prove that the
  CreateSession `can_cloud_download=false` setting overrides APC's per-statement
  `can_download_result=true`. T66/AC119 resolve that separate inline-only prerequisite.
  The TerminateSession REST binding declares `body: "*"` (R2).
- Reyder's APC terminal translation currently requires an Arrow schema when metadata is
  present. T32 must prove no-result DDL and counted DML on both direct `ExecuteStatement`
  and terminal `GetStatementResult` before the JDBC live gates pass (AC52). Live
  manifests populate `arrow_schema`; absent-field decoding has a fixture gate.
- Reyder currently discards APC `result_compression`, advertises LZ4 decompression capability
  to DBR, and reports DBR's actual codec in the result manifest. Uncompressed results are valid;
  the JDBC codec setting cannot select APC's effective wire codec (R4, AC101).
- Current Reyder APC request normalization accepts `ARROW_BATCH` but rejects the design
  contract's `ARROW_STREAM` metadata request. Its production APC route also lacks
  native metadata dispatch for SHOW headers and rejects HIVE columnar results.
  T65 must close all three gaps and prove the response contract before APC metadata
  live gates pass (AC115); JDBC must not change SHOW to `ARROW_BATCH` to work around them.
- Connector-service returns sub-flags of
  `databricks.partnerplatform.clientConfigsFeatureFlags` at
  `/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`
  (`FeatureFlagsHandler.scala`). It maps `enableSqlExecForJdbc` to
  `enableSqlExecForJdbc_v3.4.3` for ≥3.4.3 and may leave that flag on for an
  allowlisted JDBC partner. Other sub-flags pass through; Q9 adds a blanket APC override.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata tabs). The intended SQL-text limit is 16 MiB
(16,777,216 UTF-8 bytes). Current Reyder `ExecuteStatementOperation` compares Scala
string `.length` to that limit while its error says bytes; T67/AC123 correct and gate
this contract before JDBC SQL-size acceptance. The server rejects non-OSS-JDBC clients
and starts rollout opt-in through the dedicated flag.
- Driver baseline: `origin/main` @ `840f3a407` (spec #1809; bounded SEA default
  #1827). APC routes to Thrift before warehouse flag logic. `DatabricksSdkClient`
  assumes `Warehouse`; its result heartbeat uses `/status`. `SessionId.create`,
  `getConnectionId()`, and `Driver.closeConnection` can misroute saved-ID close after
  flag or target changes. SEA lacks APC `ARROW_BATCH`, async SHOW, and SEA
  `columnar_data` decoding. Procedure metadata methods catch failures into typed
  empty results; public SQL `executeAsync` is separate from metadata SHOW async.
  `EnableThriftNativeMetadata` has property default `1`, but on selected warehouse
  SEA its unset value resolves true only when `enableSqlExecForJdbc` is on. Explicit
  `UseThriftClient=0` can select warehouse SEA with that flag off and no native
  header. Its compute-type gate resolves unset APC to false today; R5 deliberately
  sets unset selected APC SEA to true, independent of the warehouse flag.
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the prototype
baseline, not the design; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756)
is unchanged.
- Parity baseline for review and release ("warehouse" uses the same driver build).
  R1–R19 define behavior; this matrix records the disposition and caller impact of
  differences. AC gates provide evidence. For each non-functional exception, add the
  measured warehouse Thrift/SEA delta required by the shared paired-load gate.

  | Behavior | APC Thrift today | Warehouse SEA today | APC SEA disposition and caller impact |
  | --- | --- | --- | --- |
  | Route and heartbeat | Thrift with status heartbeat | Flag/override; SEA status | Dedicated flag; APC status RPC is unavailable, so poll results without heartbeat or fallback (R1–R3). |
  | Explicit SEA without Arrow | Thrift | Explicit `UseThriftClient=0` selects SEA first | APC needs Arrow batches; selects Thrift despite override and reports the reason (R1). |
  | Results and download | Inline or links | Inline or links; legacy download-off can use JSON | APC requests hybrid Arrow batches at either bounded setting. Download-off must stay inline after T66; at bounded `0` it cannot adopt warehouse JSON or external-only mode. Rows match; measure first-row latency and heap (R4, AC8/39/101). |
  | Paging and buffering | `RowsFetchedPerBlock` and `ThriftMaxBatchesInMemory` tune Thrift | SEA pages ignore row-block size; inline queue counts response chunks | APC follows SEA paging and chunk queue, minimum two. A chunk may contain multiple Arrow batches, so retained bytes can exceed the Thrift batch bound (R12/R17, AC31/109). |
  | Streaming switches | `EnableInlineStreaming` selects Thrift provider | SEA inline provider is independent; bounded linked results stream | APC follows SEA. Switches do not alter rows or APC prefetch; first-row latency and heap may differ at `0` (AC39/111). |
  | Direct, async, and timeouts | Thrift operation lifecycle | Direct/polled SEA; targeted cancel | APC follows SEA without status heartbeat. Extra polls may change latency and request counts; `EnableDirectResults=0` and configured waits have paired gates (R10/R14, AC12/55/84). |
  | Linked transport and compression | Thrift HTTP link controls | SDK API and link pools have separate settings; API compression does not cover links | Same scopes. `ApiRetriableHttpCodes` and `ApiRetryTimeout` govern configured retryable codes on the link HTTP handler, not SDK API requests; capture retries, delay, and exhaustion (R17, AC45/46/51). |
  | Metadata and procedures | Native RPC; procedure failures return empty | SHOW; null-catalog methods have established exceptions | APC uses native SHOW responses and warehouse SEA conversion; procedure requests omit APC-rejected headers. Null-catalog omissions may hide APC Thrift rows; async handles are an added API (R5, AC65/83/105/114). |
  | Bindings | Complex types and scalar bindings | SEA scalar binding and pre-execution metadata | V8 scalars; complex values fail before request. Describing a prepared query adds latency but preserves bindings (R6, AC29/117). |
  | Batches | Ordered counts; `EnableBatchedInserts` and `BatchInsertSize` tune eligible INSERTs | Legacy sets retain per-set counts; 256 native-parameter chunk cap | Legacy mode follows the binding-specific chunk limit and preserves counts; V10 returns only an aggregate server count, so successful sets report `SUCCESS_NO_INFO`. Callers lose per-set counts and may see changed throughput; compare modes, tuning, committed rows, and load (R6/R15, AC17/32/108). |
  | SQL size | Thrift path | SEA advertises a 16 MiB SQL-text limit | T67 makes the server enforce UTF-8 bytes, including non-ASCII text, before AC87/108; oversized SQL fails without fallback (R6). |
  | Transactions and getters | DDL/DML counts; native result getters | SEA counts and typed-getter restrictions | Match SEA outcomes; inline typed complex getters reject and missing DML counts fail (R4/R11/R19). |
  | Saved close | Legacy Thrift ID can misroute after flag change | Legacy ID may misidentify Thrift | New ID retains protocol/target; callers need original endpoint and credentials. Old IDs may need reopen (R9, AC25). |
  | Errors and telemetry | Thrift transport/events | SEA errors and gated telemetry | Match SEA error/retry mapping without write replay. Events obey telemetry opt-outs and redact targets; enabled routes compare event families (R8/R17, AC51/85/89). |

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
`session_confs["can_cloud_download"]="false"`; live results must remain inline
after T66/AC119 proves that this overrides the APC per-statement download
capability, or a server fix makes it so. Until then inline-only gates cannot pass.
An initial inline-size failure yields no successful ResultSet. A later fetch failure
propagates during iteration; rows already read remain delivered, and the cursor
must not report normal completion.
`EnableSQLExecHybridResults=0` cannot force external-only APC results, and
`UseBoundedSeaApi` changes neither APC request nor session behavior. Positive
`Statement.setMaxRows` sends `row_limit` and caps rows across pages/links;
zero is unlimited.

Successful no-result DDL, `SET AUTOCOMMIT = TRUE/FALSE`, `COMMIT`, and `ROLLBACK`
have either no terminal manifest or a manifest without a schema, chunks, links,
continuation, or result data. A zero-column schema is not a no-result shape.
They expose no ResultSet
and update count zero, then `-1` after `getMoreResults()`. This holds for both direct
`ExecuteStatement` and polled terminal `GetStatementResult` completion; no fetch is issued.
Result-producing SQL, including `SELECT`, `SHOW`, `VALUES`, the unassigned
`SET AUTOCOMMIT` state query, and `NonRowcountQueryPrefixes` matches,
requires a result schema before public success. A terminal manifest with an
explicit `total_row_count=0`, no chunks or continuation, and no result payload
is a successful empty ResultSet with the declared columns. An absent count is
not zero-row evidence: a manifest-only initial response must fetch its first
chunk, even when its chunk list is empty (R12). If that fetch supplies neither
data nor a terminal empty indication, or fewer rows than an explicit count
arrive, fail rather than report empty.
Counted `INSERT`, `UPDATE`,
`DELETE`, and `MERGE` require a `num_affected_rows` schema column and value,
including zero; missing count data is a contract error. A matching
`NonRowcountQueryPrefixes` command instead exposes the count ResultSet and update
count `-1`, and stops a Statement batch with prior counts and the existing error.
Direct and polled command live gates depend on T32.

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
T65 is a server prerequisite: Reyder must accept `ARROW_STREAM`, dispatch
all seven APC SHOW headers to native metadata RPCs on its production APC route,
set `manifest.is_native_metadata_result=true` for each native response, and return
decodable Arrow IPC or HIVE `columnar_data` rather than rejecting it;
until then, the metadata gates remain pending. On selected APC SEA, unset
`EnableThriftNativeMetadata` uses the property default `1`; unset and explicit
`1` send `X-Databricks-Require-Thrift-Native-Metadata: true` for supported SHOW,
and explicit `0` omits it. Unset warehouse SEA sends this header only when
its warehouse server flag is enabled; an explicit warehouse SEA override can
select SEA without it.
Reyder still returns native-shaped metadata, so conversion follows
`manifest.is_native_metadata_result`, not the request setting. Synchronous
SHOW sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only at
`EnableSeaSyncMetadata=1`; async SHOW omits it. Decode each response as Arrow
IPC `chunk.attachment` or HIVE `chunk.columnar_data`/`COLUMN_BASED_SET`.
Both `getImportedKeys(catalog, schema, table)` and
`getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog,
foreignSchema, foreignTable)` use `GET_CROSS_REFERENCE`, but they remain distinct
JDBC calls. Imported keys query the specified table's foreign keys; cross-reference
queries the foreign table's keys and filters them by the supplied parent. A null
foreign table returns a typed empty cross-reference result before request. Public
`getCrossReference` rejects a null or empty parent table and an empty-string
foreign table before request. For either method, syntax and object-not-found
responses return the established typed empty result; other SQL failures propagate.

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

The session-reachable async metadata API accepts the seven wire operations and
each corresponding JDBC method, including separate imported-keys and
cross-reference handles with the inputs, rows, filtering, and typed-empty behavior
above. It returns typed ResultSet handles. Async SHOW uses the server's
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

After the T67/AC123 server prerequisite, SEA enforces a 16,777,216 UTF-8 byte
SQL-text limit for single statements and legacy generated chunks without silent
split or fallback; an oversized request propagates the warehouse SEA error. `BatchInsertSize` can reduce interpolated
legacy chunk size; native-scalar chunking follows the parameter limit.
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
- R9: The first flag-on SELECT slice closes its live APC SEA session directly. The saved-ID
close slice makes public `getConnectionId()` return an ID encoding the selected protocol and
target; `SessionId.create()` must not require a Thrift handle for an APC SEA session. Given
the original workspace endpoint (scheme, host, port) and usable credentials, new saved IDs
let `Driver.closeConnection` close the original session after a flag or compute-target change
in the supplied URL or properties. The ID does not encode the endpoint or credentials;
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
streams. A deferred logical type fails before false ResultSet success. Cover
primitive, decimal, array, map, struct, interval, timestamp, timestamp_ntz, geography,
geometry, and variant values. Preserve
`EnableComplexDatatypeSupport` and `EnableGeoSpatialSupport` effects on JDBC metadata and
`getObject`: disabled support returns established strings and rejects typed complex getters;
enabled support permits linked typed getters and native geospatial values where applicable.
Inline typed complex getters still reject, so callers needing them require linked results.
- R12: Page APC results by `next_chunk_index` and `row_offset`, without requiring totals or
inline data in the initial response. When the initial manifest has no payload and
no explicit zero-row count, fetch chunk zero even if its chunk list is empty.
An empty fetched chunk is terminal zero-row evidence only with no continuation.
The next inline offset includes every batch in a chunk;
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
in both the SDK API pool and the linked-download HTTP pool. Match warehouse SEA 429/503 and `Retry-After` handling by request class without
new write retries. `ApiRetriableHttpCodes` adds response codes to the existing
linked-download HTTP retry handler, including retries without `Retry-After`;
`ApiRetryTimeout` caps accumulated delay for those configured codes, including
429/503 when listed instead of using their ordinary retry budgets. The SEA SDK
API transport does not use these two link-handler settings. Preserve caller-visible
delay and exhausted-retry error identity (AC51). No APC-only concurrency cap or
full-result buffer is allowed (AC30/46/51/84).
- R18: Every approved JDBC slice exposes its completed public behavior when R1's default-off
APC flag is enabled or `UseThriftClient=0` is explicit. Incomplete public calls, including
`executeAsync`/`getExecutionResult` before T39, fail before network execution when known;
unsupported response shapes fail before false success. They never
silently switch to Thrift after SEA selection. After T17 and T35 deploy, a scoped staging
assignment may enable flag-on validation; production assignment or ramp waits for T18/T-val
on the assembled approved stack and a post-merge smoke check. Remove each guard when its
slice passes live staging, and keep ordinary flag-off APC connections on Thrift.
The first JDBC slice exposes open, direct close, and primitive SELECT together;
the next identity slice adds saved-ID close. Both guard incomplete statement workflows
before network execution.
This slice includes T7's hybrid Arrow-batch request and single-chunk inline behavior.
The paging slice adds T58's download-disabled session setting after AC119; before
that slice, download-disabled APC SEA is guarded. Nondefault row-limit or query-timeout settings, `EnableDirectResults=0`,
`EnableSeaResponseCompression=1`, and SQL-backed `isValid` are guarded before
the affected connection or statement request until their owning slices pass
their live gates.
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

For the shared paired-load gate (AC31/32/33/46/48/61/75/107/117/118), compare the same
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

- AC1: GIVEN scoped flag-ON/OFF APC staging WHEN `UseThriftClient` is unset, `0`, or `1` and `TreatMetadataCatalogNameAsPattern=1` varies THEN a live SELECT proves R1 precedence and no SEA fallback. At flag ON with no override, ordinary User-Agent uses SEA while partner and JDBC-allowlisted partner agents use APC Thrift and return the same rows; capture route and returned flag. Compare warehouse SEA routing. Fixtures cover Arrow-unavailable, legacy `cliservice`, and flag-lookup failure.
- AC2: GIVEN flag-on APC staging WHEN a cluster-path connection runs single-chunk and zero-row SELECTs and closes THEN live captures show APC-only CreateSession targeting, R4's hybrid Arrow request at both bounded values, paired rows/types and typed empties, and successful targeted TerminateSession. An explicit terminal zero count returns empty; absent count fetches chunk zero even with no manifest chunk list. Missing expected rows fail before false success; fixtures cover absent `arrow_schema` and unknown formats.
- AC3: GIVEN each exposed APC R2 operation WHEN it runs after a flag or target change THEN captures show R2 target placement, original statement target, exactly-once close, connection usability, and unchanged warehouse serialization.
- AC6: GIVEN live APC prepared SQL with bound scalars WHEN `getMetaData()` runs before
execution THEN it returns JDBC metadata without losing bindings; subsequent execution
matches APC Thrift and warehouse SEA. AC53 covers the version and interpolation matrix;
AC117 gates paired metadata load at first exposure.
- AC8: GIVEN AC119 passed and a live single-chunk APC SELECT with `EnableQueryResultDownload=0` at either `UseBoundedSeaApi` value WHEN the inline paging slice runs THEN CreateSession sends `session_confs["can_cloud_download"]="false"`, its result stays inline, and an inline-size error fails before partial success.
- AC9: GIVEN live APC SEA CreateSession rejection WHEN connecting THEN the error and failed
route event propagate without Thrift fallback. GIVEN APC Thrift OpenSession returns KP001 WHEN
connecting THEN warehouse-only recovery does not select SEA.
- AC10: GIVEN successful and failed APC opens or primitive statements WHEN DEBUG logs and enabled telemetry are captured THEN route/outcome/compute/protocol appear without credentials or raw APC IDs.
- AC11: GIVEN standard and Real-time warehouse connections WHEN equivalent R2 operations run THEN JDBC outcomes and URL/body/header serialization, including `realtimeSession`, match pre-change `origin/main`; each slice checks affected classes.
- AC12: GIVEN APC synchronous SELECT with `EnableDirectResults=1` and `0` WHEN paired callers drain rows THEN `wait_timeout` is absent and `0s` respectively, no `statements` field is sent, rows/errors match, and each setting passes AC31's load gate. Record first-row latency and request counts; disposition protocol deltas under the shared exception rule.
- AC13: GIVEN R11's primitive and logical types WHEN inline and linked getters run
with complex/geospatial settings at 0/1 THEN types and values match both baselines;
inline typed complex getters reject, enabled linked getters work, and disabled modes
return established strings. Unavailable live types remain pending despite fixtures.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN scoped APC staging after AC115 WHEN synchronous `getCatalogs` and `getSchemas` read nonempty Arrow IPC and HIVE responses THEN JDBC columns, values, nulls, and errors match R5 and paired routes. A format that cannot be induced live remains open; R13 mask details use fixtures.
- AC17: GIVEN live V10 APC and paired routes WHEN native prepared INSERT batches succeed or fail THEN ordered `parameter_sets` yield `SUCCESS_NO_INFO` per success or `EXECUTE_FAILED` for every set in a `BatchUpdateException` on execution failure; commit status is uncertain. A post-write count-response fault raises a distinct result error, clears the batch, and warns that rows may be committed. Apply the shared fault-hook and load rules.
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
- AC25: GIVEN new APC and warehouse SEA/Thrift IDs with the original endpoint and credentials WHEN `Driver.closeConnection` uses an ID after a flag or target change THEN only the original session closes without an APC Thrift handle. Legacy IDs decode, malformed IDs fail without logging identifiers, statement IDs stay raw, and errors match paired routes. Prechange warehouse Thrift IDs may require reopen.
- AC27: GIVEN a live single-chunk APC query with three or more RUNNING polls WHEN transfer
is measured on explicit-override and flag-on routes THEN polls carry no result payload and
the terminal fetch delivers every row once; compare APC Thrift and warehouse SEA traffic.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN flag-on APC staging and paired routes WHEN a complex scalar binding is submitted THEN APC SEA raises its named/numeric driver error before any request, keeps the connection usable, and records its error identity against both baselines. The PR records an owner-verified taxonomy/dashboard update or maintainer handoff.
- AC30: GIVEN flag-on APC staging and paired APC Thrift/warehouse SEA under pool saturation WHEN `HttpConnectionPoolSize`, `HttpMaxConnectionsPerRoute`, and `HttpConnectionRequestTimeout` vary separately for each newly exposed API or linked-download class THEN measured in-flight count, wait, transport error identity, and throughput follow the applicable transport scope; SDK API acquisition ignores the Thrift HTTP wait setting.
- AC31: GIVEN each SELECT mode at its first public exposure WHEN one and 32 paired APC SEA/Thrift callers drain primitive, multi-page, or linked results as applicable THEN rows/errors match, SEA paging ignores `RowsFetchedPerBlock`, and the shared load gate passes. The release gate repeats the combined mode mix.
- AC32: GIVEN flag-on live APC SEA and paired APC Thrift/warehouse SEA legacy, V10 native prepared, and Statement INSERT batches WHEN one and 32 callers use disjoint keys and R6 settings THEN committed rows, counts, errors, and the shared load gate pass for each exposed mode. Record negotiated version; captured V10 responses cannot close native correctness or load gates.
- AC33: GIVEN live APC no-result DDL in either R4 terminal shape WHEN public JDBC executes it THEN `execute` is false, no ResultSet or fetch appears, update count is zero, and `getMoreResults()` makes it `-1`. A zero-column schema remains an error.
- AC34: GIVEN scoped flag-ON staging and default-OFF configuration WHEN connector-service looks up the APC sub-flag for ordinary, partner, and allowlisted JDBC partner User-Agents THEN only the ordinary scoped agent receives true; default lookup and both partner lookups return false. Record flag version and values.
- AC36: GIVEN APC chunks with multiple Arrow batches and inline continuations WHEN JDBC drains them THEN rows appear once and `row_offset` includes all batch rows; a failed page cannot report success. If live staging lacks multiple batches, retain a fixture and leave live proof pending.
- AC39: GIVEN each inline or linked mode at first live exposure WHEN bounded and streaming switches vary for paired callers THEN R4/R17 rows, request patterns, buffer limits, and AC31 load bounds hold; non-selecting switches do not alter rows. Record caller impact and approved exceptions.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF or scoped ON THEN flag-OFF APC stays on Thrift, completed flag-ON calls pass their live gate, and incomplete calls fail before request or false success without fallback. Production assignment waits for T18/T-val.
- AC45: GIVEN linked or inline APC results WHEN `EnableSeaResponseCompression=0` and `1` run under the paired load gate THEN only `0` sends API `Accept-Encoding: identity`; links are unchanged. Rows, API bytes, request counts, throughput, latency, and heap are recorded for both settings and protocol deltas dispositioned.
- AC46: GIVEN paired linked results WHEN `cloudFetchThreadPoolSize`, `LinkPrefetchWindow`, chunk wait, and slow-link settings vary THEN workers/chunks remain within the per-result worker bound, fetched-ahead links within the window, and aggregate use scales with callers. Rows, duration, errors, and AC31 load bounds match baselines; inline pages stay unaffected.
- AC48: GIVEN live APC counted DML after T32 WHEN public update methods execute INSERT/UPDATE/DELETE/MERGE, including zero affected rows, THEN counts match APC Thrift. Missing `num_affected_rows` fails with R4's contract error and leaves the connection usable.
- AC49: GIVEN each exposed live SELECT mode WHEN `setMaxRows` is zero, one, or a page boundary THEN positive limits send SEA `row_limit` and cap rows across continuation while zero remains unlimited; paired rows and request count match R4.
- AC50: GIVEN each typed SHOW handle when introduced on live APC WHEN a delayed operation is submitted with `wait_timeout=0s` THEN the handle returns while RUNNING and another call completes before await. Awaited rows/errors, timeout, cancel, and cleanup match synchronous and paired routes; AC122 separately covers both cross-reference handles. Older/versionless fixtures retain the same contract. Async SHOW omits the sync-only header; procedure SELECTs reject async before request.
- AC51: GIVEN isolated 429/503 and an extra HTTP code, each with and without `Retry-After`, WHEN linked downloads run with those codes absent/present in `ApiRetriableHttpCodes` and short/long `ApiRetryTimeout` THEN configured codes retry without that header and stop at the configured timeout, including listed 429/503 instead of their ordinary budgets. Attempts, caller delay, and exhausted error class/SQLState/vendor code match warehouse SEA. The same settings do not change SDK API retries; API 429/503 retain their own policy. Verify CreateSession exhaustion without write replay. Induced branches follow the shared fault-hook rule; ordinary execution and paired load remain live gates.
- AC52: GIVEN scoped flag-on staging after AC113 WHEN public JDBC completes no-result DDL, transaction commands, and counted DML through direct and polled SEA responses THEN wire captures show R4's no-result or affected-row shape and public counts/rows/errors match paired routes. No no-result command fetches; fixtures alone cannot close either path.
- AC53: GIVEN live prepared scalars or an exposed fallback batch plus older/versionless fixtures WHEN `supportManyParameters` is 0 or 1 THEN R6's native/interpolated request choice has no unresolved markers, `setBytes` follows R6, and rows/counts match paired routes. Record fixture-only branches.
- AC54: GIVEN a referenced table WHEN `getExportedKeys` runs live with `enableMultipleCatalogSupport=1` and `0` THEN all three routes return typed empty and APC SEA sends no cross-reference SHOW. At `0`, APC SEA uses `SELECT CURRENT_CATALOG()` as warehouse SEA does; at `1`, it sends no request. Null table fails before request with warehouse SEA's `SQLException`, SQLState `42000`, and error 1003. Any new Thrift rows reopen parity.
- AC55: GIVEN public APC SEA SQL async SELECT WHEN primitive and linked results complete
THEN `wait_timeout=0s`, targeting, rows/errors, cancel, close, and diagnostics follow R2/R10.
Apply AC31's one/32-caller load bounds to this public path before unguarding it.
- AC60: GIVEN a valid live query within its configured client wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Early timeout is a
correctness failure; only a measured latency difference may receive a documented exception.
- AC61: GIVEN each nonempty metadata method or typed handle at first exposure WHEN paired callers run it THEN rows/errors and the shared load gate pass; null-catalog fan-out makes at most N+2 requests for N eligible catalogs. For equal decoded cells, JDBC CPU per 1,000 cells is at most 110% and peak RSS at most 120% of a minimal Java 11 direct-SEA probe using the same SDK HTTP and Arrow libraries. The final slice repeats the combined mix. The test plan owns probe setup, sampling, and fixture details; typed empties cannot substitute for nonempty samples.
- AC63: GIVEN valid delayed SHOW within its configured metadata wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Early timeout is a
correctness failure; only a measured latency difference may receive a documented exception.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching INSERT runs through public Statement THEN `execute` returns true, its ResultSet contains the affected-row value, and `getUpdateCount` is `-1`.
- AC65: GIVEN each exposed synchronous live APC SHOW after AC115 WHEN `EnableThriftNativeMetadata` is unset/`0`/`1` and `EnableSeaSyncMetadata` is `0`/`1` THEN APC sends the native header for unset/`1`, sends the sync header only at sync `1`, and returns paired JDBC rows. Warehouse SEA with its flag off omits the unset native header even under explicit SEA; a changed server response shape keeps this gate open.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized `getProcedures` and `getProcedureColumns` WHEN each reads pages or links THEN header-free queries yield ordered paired JDBC rows and pass AC61. Query/fetch/decode failure logs and returns the typed empty result; older/versionless interpolation uses fixtures.
- AC82: GIVEN live `getFunctions` with explicit or null catalog WHEN `EnableShowCommandForGetFunctions` is 0 or 1 on paired routes THEN APC SEA uses SHOW at both values and follows warehouse SEA rows/errors; record APC Thrift's native/SHOW and null-catalog differences with caller impact.
- AC83: GIVEN live null-catalog `getSchemas` with `42601` fan-out WHEN one catalog errors, times out, or cancels THEN APC and warehouse SEA warn, omit only that catalog, and keep the connection usable; record lost rows against APC Thrift. Initial object-not-found or empty-pattern yields typed empty; other initial or overall failures propagate.
- AC84: GIVEN each newly exposed live SDK API or linked-download request class WHEN socket, poll, and idle settings vary THEN `socketTimeout` bounds one request, `asyncexecpollinterval` changes later-poll delay, and `IdleHttpConnectionExpiry` evicts idle connections in each applicable pool. In-budget calls complete with paired timing/error identity and usable connections.
- AC85: GIVEN each exposed execution, metadata, download, batch, and failure path WHEN telemetry is enabled by `EnableTelemetry` and the server flag, or by `ForceEnableTelemetry`, THEN R8's redacted event families, operation/format/counts, and error identity match paired routes or have a measured exception. WHEN `TelemetryLogLevel=OFF`, or `EnableTelemetry=0` or the server flag is off without force, THEN no new event exports; local diagnostic logs remain redacted.
- AC86: GIVEN live APC with `EnableSQLValidationForIsValid=1` WHEN
`isValid(timeout)` runs its `SELECT VERSION()` check THEN healthy/failed results, timeout,
and connection usability match APC Thrift and warehouse SEA. AC118 gates its paired
one/32-caller load at first exposure.
- AC87: GIVEN AC123 deployed and single-statement SQL text of 16,777,215/16,777,216/16,777,217 UTF-8 bytes, including multibyte text with fewer UTF-16 code units, WHEN APC SEA executes it THEN the first two sizes pass the server size check and the last propagates the warehouse SEA oversize error without fallback. Record bytes, statement outcome, and APC Thrift difference.
- AC88: GIVEN two accessible catalogs and a metric view WHEN `enableMultipleCatalogSupport` and `EnableMetricViewMetadata` vary on all three live routes THEN catalog access, discovery, table types/rows, and APC session setting match R5; record any server-limited row difference and caller impact.
- AC89: GIVEN live invalid SQL, missing-object, or permission failure WHEN exposed execution methods surface it THEN SQLException class, SQLState, vendor code, and message category match agreeing baselines or warehouse SEA mapping with caller impact; no failed command reports success.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN exposed APC inline or linked results WHEN codec settings request `NONE`, `LZ4_FRAME`, or invalid text THEN decoding follows the manifest codec, including unmarked data and LZ4 despite `NONE`; invalid text retains the warehouse default. Rows and paired load hold; requested/actual codecs are recorded.
- AC102: GIVEN an isolated live write table WHEN transaction and auto-commit settings vary THEN R19's state query, committed/rolled-back rows, counts, and errors match paired routes. Public assignments/commit/rollback have no ResultSet or fetch, count zero then `-1` after `getMoreResults()`; missing state data or failed commands cannot succeed.
- AC105: GIVEN live null-catalog `getTables` receiving `42601` WHEN synchronous APC SEA, APC Thrift, and warehouse SEA calls are compared THEN APC SEA and warehouse SEA return the same typed empty result without catalog fan-out; record omitted APC Thrift rows and caller impact.
- AC106: GIVEN a changed driver-owned APC SEA error WHEN it reaches JDBC THEN a test asserts its `DatabricksDriverErrorCode` name and unique number, and the PR records owner-verified telemetry taxonomy/dashboard updates or maintainer handoff and completes the telemetry-error template. Server errors retain server identity.
- AC107: GIVEN paired one- and 32-caller staging WHEN the first SELECT slice repeatedly opens, queries, and directly closes THEN no live session or statement remains, server counts and client handles/threads return to baseline, retained heap growth is at most 8 MiB, and median SEA/Thrift cycle latency is at most 1.10. Release repeats the assembled mix; exceptions need caller impact and PECO/Perf Eng approval.
- AC108: GIVEN AC123 deployed and live legacy APC INSERT batches with paired routes WHEN `EnableBatchedInserts`, `BatchInsertSize`, `supportManyParameters`, and the 256-parameter limit vary THEN eligible interpolated sets group only up to `BatchInsertSize`, eligible native-scalar sets group only up to the parameter limit, and disabled or ineligible sets execute separately. Record request groups, per-set counts, committed rows, errors, throughput, and the AC32 paired load result for each binding mode/version; AC132 covers oversized chunks.
- AC109: GIVEN concurrent live multi-page inline SELECTs WHEN `ThriftMaxBatchesInMemory` varies around two THEN queued response chunks obey R17 despite multiple batches per chunk, each row arrives once, and retained result memory and AC31 load bounds hold. The test plan owns batch/byte accounting.
- AC111: GIVEN paired inline SELECTs WHEN `EnableInlineStreaming` is 0 or 1 THEN SEA rows/pages/provider and APC queue bound do not change. Record first-row latency, heap, protocol deltas, and caller impact; exceptions require PECO/Perf Eng approval.
- AC112: GIVEN AC119 passed and live multi-page APC SELECT with download disabled at either bounded value WHEN rows are drained THEN all pages remain inline and appear once. Initial size failure prevents ResultSet success; later-page failure propagates during iteration without reporting normal completion.
- AC113: GIVEN T32 deployed on scoped APC staging WHEN direct and polled SEA calls complete no-result DDL, transaction commands, and counted DML including zero THEN captures show R4's no-result shape without a zero-column schema or fetch, and DML's `num_affected_rows` schema/value. Failed commands retain errors. This server-only gate precedes JDBC AC52; fixtures cannot close it.
- AC114: GIVEN a table with imported keys from two parents and a second foreign table WHEN synchronous `getImportedKeys` and `getCrossReference` run on live APC THEN each uses `GET_CROSS_REFERENCE` but filters to its requested table/parent rows with the JDBC column shape. A null foreign table returns typed empty before request; invalid cross-reference table inputs fail before request, while syntax/object-not-found errors return the method's typed empty shape. Compare each method separately with APC Thrift and warehouse SEA.
- AC115: GIVEN T65 deployed on scoped APC staging WHEN direct and polled SEA calls run all seven R5 SHOW operations THEN the APC route accepts `ARROW_STREAM`, dispatches each header to native RPC, and marks direct/polled Arrow IPC and HIVE responses `is_native_metadata_result=true`. Live nonempty samples of both formats and completion paths must decode to native JDBC columns; unavailable samples keep this server gate open. Fixtures cannot close it.
- AC119: GIVEN T66 completed on scoped APC staging WHEN CreateSession sets `can_cloud_download=false` and APC ExecuteStatement still advertises `can_download_result=true` THEN a single-chunk and multi-page query both return inline chunks only. Capture both wire values and the effective result disposition. If links appear or the override cannot be proved, fix the server contract and keep AC8/AC112 open; fixtures cannot close this gate.
- AC116: GIVEN the first JDBC SELECT slice on scoped flag-ON and flag-OFF APC staging WHEN a connection opens, runs primitive SELECT, and closes directly THEN R1 routing and R2 CreateSession/TerminateSession targeting hold, only its session closes, and incomplete statement calls fail before execution. Compare open/close, SELECT errors, and lifecycle with APC Thrift and warehouse SEA; TerminateSession rejection leaves this gate open.
- AC117: GIVEN paired callers WHEN `PreparedStatement.getMetaData()` precedes a bound SELECT THEN metadata and rows match, and request count, median metadata latency, workflow throughput, and peak heap pass the shared load gate. Fixtures cannot close it.
- AC118: GIVEN paired callers with SQL-backed `isValid` enabled WHEN an open connection is repeatedly checked THEN booleans and SELECT counts match, and median call latency, calls per second, and peak heap pass the shared load gate. Fixtures cannot close it.
- AC120: GIVEN new APC and warehouse SEA/Thrift IDs at one and 32 callers WHEN saved-ID close follows a flag/target change with the original endpoint and credentials, including cancellation and statement close, THEN only original sessions/statements close, no live resources remain, and client handles/threads return to baseline. Close latency and errors pass the shared paired gate; AC25 covers legacy limits.
- AC121: GIVEN scoped APC staging after AC115 WHEN synchronous `getColumns` and `getFunctions` read nonempty Arrow IPC and HIVE responses THEN both return R5's JDBC columns, values, nulls, and errors matching paired routes. Unavailable live samples remain open; R13 mask details use fixtures.
- AC122: GIVEN live APC tables with imported keys from two parents and a second foreign table WHEN typed async `getImportedKeys` and `getCrossReference` handles complete THEN each returns its own filtered JDBC rows and typed empties under AC114's input/error rules, independent of the synchronous call. Compare both separately with paired routes and satisfy AC50/61 for each handle.
- AC123: GIVEN T67 deployed on scoped staging WHEN direct APC and warehouse SEA requests send ASCII and multibyte SQL at 16,777,215/16,777,216/16,777,217 UTF-8 bytes THEN the first two pass validation and the last fails with the established oversize error before execution, regardless of UTF-16 length. Fixtures cannot close this server gate.
- AC124: GIVEN each exposed live synchronous APC SHOW WHEN `MetadataOperationTimeout` varies THEN the setting bounds the overall wait and targets cancel; `0` disables it. An in-budget operation completes even if one SEA request timeout passes, and the connection remains usable after timeout.
- AC125: GIVEN a live async SHOW WHEN its request headers are captured THEN it sends R5's metadata-operation and native headers but never the synchronous-only header, regardless of `EnableSeaSyncMetadata`.
- AC126: GIVEN live null-catalog typed async `getTables` receiving `42601` WHEN its handle completes THEN it returns the same typed empty table result as synchronous APC and warehouse SEA without catalog fan-out, with any omitted Thrift rows recorded.
- AC127: GIVEN each exposed live async APC SHOW WHEN `MetadataOperationTimeout` varies THEN await is bounded by the overall wait, cancellation targets the operation, `0` disables the bound, and an in-budget operation completes despite a shorter SEA request timeout.
- AC128: GIVEN linked downloads or recovered warehouse sessions WHEN DEBUG logs and enabled telemetry are captured THEN no credential, presigned URL secret, raw APC target, or compound session/statement ID appears; route and failure identity remain available.

- AC129: GIVEN live null-catalog `getColumns` with `42601` fan-out WHEN one catalog errors, times out, or cancels THEN APC and warehouse SEA omit only that catalog with a warning, preserve partial rows and connection usability, and record the lost APC Thrift rows; initial/overall failures follow R5.
- AC130: GIVEN an exposed APC prepared batch WHEN a complex binding is submitted THEN it raises AC29's named/numeric driver error before any batch request, preserves connection usability, and records the owner-verified taxonomy/dashboard update or maintainer handoff.
- AC131: GIVEN an exposed live APC Statement batch with `NonRowcountQueryPrefixes=INSERT` WHEN a matching command occurs after prior successful commands THEN the batch stops, reports preceding counts, and raises the same batch error identity as APC Thrift and warehouse SEA.
- AC132: GIVEN AC123 deployed and a live legacy INSERT chunk at the UTF-8 SQL-size boundary WHEN an interpolated chunk or one oversized value exceeds 16,777,216 bytes THEN the server raises AC87's error without splitting or fallback; accepted chunks retain committed rows and per-set counts.
- AC133: GIVEN live typed async null-catalog `getSchemas` or `getColumns` with `42601` fan-out WHEN one catalog fails, times out, or cancels THEN the handle warns, omits only that catalog, preserves partial rows and connection usability, and matches synchronous warehouse SEA behavior; overall failures propagate.
- AC134: GIVEN scoped APC staging after AC115 WHEN synchronous `getTables` reads nonempty Arrow IPC and HIVE responses across accessible catalogs and metric views THEN JDBC columns, types, rows, nulls, and errors follow R5 and the paired routes; unavailable live formats remain open.
- AC135: GIVEN scoped APC staging after AC115 WHEN synchronous `getPrimaryKeys`, `getImportedKeys`, and `getCrossReference` read nonempty Arrow IPC and HIVE responses THEN each returns its own R5 JDBC column shape, rows, nulls, and errors against paired routes; unavailable live samples remain open.
- AC136: GIVEN a live flag-on APC SELECT with `EnableHeartbeat=1` WHEN its ResultSet remains open across two configured heartbeat intervals THEN zero APC `/status` requests occur while rows and direct close still succeed.

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

- Server changes beyond T32, T35, T65, T66, and T67; known gaps SC-256188 under SC-256186
  remain external prerequisites for affected dogfood checks. T66 is limited to
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

`plan.json` owns task-to-package mapping and PR ancestry. The first JDBC package
opens, queries, and closes; later packages add completed flag-gated workflows with
their own live acceptance gates. The 14 JDBC packages exceed the 4–7 package target
because the first SELECT, response formats, discovery families, and async handles
need separate reviewable slices under about 1,500 changed lines each. No package is
plumbing-only. Each repository has its own parent chain; independent Universe
result-disposition work branches after command translation.
T17/T35 gate scoped flag-on validation; T32/AC113 gate commands, T65/AC115 gate
SHOW, T66/AC119 gate inline-only results, and T67/AC123 gate the SQL byte boundary.
JDBC uses one ancestry through shared SEA result handling, so assembled validation
runs on all preceding work. Tracked termination fixes gate close proof.
The final JDBC package owns docs/changelog before production assignment.

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

Each task has one observable outcome. `plan.json` names its sole package owner;
R/AC references above supply the shared contract and live gate.

- T1 APC route precedence; T2 APC CreateSession targeting; T3 Original-target TerminateSession; T4 Statement and result targeting; T5 Live-version capability grant; T6 Saved connection close identity.
- T7 Hybrid Arrow-batch SELECT; T8 Single-chunk bare-batch rows; T9 Prepared scalar SELECT; T10 `EnableDirectResults=0` submission and polling; T11 Inline page continuation; T12 Manifest-only first chunk in the first SELECT slice.
- T13 Synchronous SHOW request contract; T14 Arrow IPC metadata rows; T15 Multi-link SELECT continuation; T16 Remaining typed async SHOW handles; T17 Default-off APC client flag; T18 Production assignment gate.
- T19 Independent APC/warehouse routing; T20 Manifest-coded linked data; T21 Pre-execution prepared metadata; T22 No-result DDL outcomes; T23 Session-reachable async metadata API; T24 Async SHOW submit and await.
- T25 Bounded inline chunk queue; T26 Explicit protocol selection; T27 Payload-free RUNNING polls; T28 Counted DML outcomes; T29 APC result-heartbeat suppression; T30 Legacy prepared batches.
- T31 V10 native prepared batches; T32 Reyder terminal command responses; T33 Bounded linked downloads; T34 Query timeout and cancellation; T35 Partner APC-flag exclusion; T36 Inline and linked logical getters.
- T37 HIVE columnar metadata; T38 Public transaction outcomes; T39 Public SQL async lifecycle; T40 RUNNING metadata handle overlap; T41 Async discovery conversion; T42 Synchronous catalog/schema rows.
- T43 Catalog and metric-view settings; T44 Redacted route diagnostics; T45 API response compression; T46 API and link transport scopes; T47 SELECT/fetch failure propagation; T48 Synchronous column metadata.
- T49 Synchronous function metadata; T50 Async null-catalog fan-out; T51 Async column/function/primary-key rows; T52 Async imported/cross-reference rows; T53 Async metadata cancellation/cleanup; T54 Synchronous primary keys.
- T55 Synchronous imported/cross-reference keys; T56 Procedure SELECT rows; T57 Manifest-coded inline data; T58 Inline-only APC session setting; T59 Public row limits; T60 Sequential Statement batches.
- T61 Original-target statement cancel/close; T62 SQL-backed isValid; T63 Inline-streaming switch parity; T64 Post-write batch result failure; T65 Reyder APC SHOW format, native dispatch, native-result flag, and HIVE response contract; T66 Prove or fix CreateSession inline-only override of APC statement download capability; T67 Enforce the SQL-text limit in UTF-8 bytes on the server; T68 Synchronous table discovery rows.
- T-val Assembled live and release evidence.
