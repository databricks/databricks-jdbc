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
- Known server gaps are tracked outside this client plan (SC-256188 under SC-256186 and
  SC-254478). Driver behavior and fake-service tests assume those fixes. Only dogfood validation
  of the affected metadata, inline-only, and session-termination behavior waits for the fixes to
  be deployed.
  The TerminateSession REST binding declares `body: "*"`; the driver tests its DELETE body
  against the assumed server contract with a fake service.
- Reyder's current APC `GetStatementResult` path fetches missing metadata after a successful
  poll, then requires metadata and an Arrow schema to build a manifest. A polled DDL response
  with neither schema nor result data is therefore not yet a proven live server response. Counted
  DML also needs a result schema and affected-rows data: SEA has no separate update-count field,
  and the driver reads the `num_affected_rows` result column. T32 proves both server result
  contracts before their live JDBC gates can pass.
- The earlier always-LZ4 APC assumption is narrower in the current server contract: Reyder
  advertises LZ4 capability to DBR and marks LZ4 only when DBR reports compression. The driver
  must follow the manifest codec, including an uncompressed response (AC45).
- Client-config flags come from connector-service
(`/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`), which returns sub-flags of
`databricks.partnerplatform.clientConfigsFeatureFlags`
(`connector-service/src/featureflags/FeatureFlagsHandler.scala`). It version-maps
`enableSqlExecForJdbc` to `enableSqlExecForJdbc_v3.4.3` for ≥ 3.4.3. For partner User-Agents,
that existing flag may remain true for an allowlisted JDBC partner; other sub-flags currently
pass through unchanged. Q9 requires a separate blanket APC override.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata tabs). R1–R18 state the driver wire contract. The
server also limits SQL text to 16 MB, rejects non-OSS-JDBC clients, and starts rollout opt-in
before defaulting APC to Reyder through the dedicated flag.
- Driver baseline verified against `origin/main` @ `840f3a407` (approved spec #1809;
  `UseBoundedSeaApi` default #1827): APC routing precedes flags and selects Thrift, while
  warehouse routing honors overrides. `DatabricksSdkClient` assumes `Warehouse` for session
  and statement requests, polls `GET /statements/{id}`, and uses `/status` for heartbeats.
  SEA lacks APC `ARROW_BATCH` decoding, async SHOW, and `columnar_data` modeling; the SDK
  transport differs from `DatabricksHttpClient` (R17).
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the 41-file prototype
baseline, not the design; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756)
is unchanged. The prototype enables APC SEA by default and emits `JDBC_APC_ROUTE` markers.
- Parity baseline for review and release ("warehouse" means SQL warehouse using the same driver
  build). A difference is allowed only where the server contract above or existing warehouse
  Thrift-versus-SEA behavior explains it; the listed exception must remain visible to users.

  | Behavior | APC Thrift today | Warehouse SEA today | APC SEA target and disposition |
  | --- | --- | --- | --- |
  | Routing and failure | Default APC route; Thrift OpenSession | Flag or explicit setting; SEA errors propagate | Dedicated flag/override; no SEA-to-Thrift fallback, following warehouse SEA precedent (R1, R7). |
  | Result values and paging | Arrow/CloudFetch or inline; complete JDBC rows | Arrow stream/CloudFetch or bounded inline | Same JDBC values and completeness; inline APC payloads are LZ4 bare `ARROW_BATCH` values, while external links contain complete Arrow IPC streams; both use APC offset paging (R4, R11, R12). |
  | `Statement.setMaxRows` | Sends the statement row limit and stops at the limit | Sends SEA `row_limit` | APC sends `row_limit` and exposes at most that many rows across inline pages or links; zero means unlimited (R4; AC49). |
  | `EnableSQLExecHybridResults=0` with CloudFetch | No SEA disposition | Requests `EXTERNAL_LINKS` rather than hybrid inline/external results | Requests `INLINE_OR_EXTERNAL_LINKS` regardless of the setting: APC contract requires this disposition. External links may still be returned; callers cannot force external-only APC results (R4; AC4). Documented exception. |
  | `EnableQueryResultDownload=0` | Inline Thrift results | Bounded inline Arrow when available; otherwise routing may select Thrift | Remains on SEA, sets `can_cloud_download=false`, and consumes all inline pages. Reyder must honor inline-only delivery despite the hybrid disposition; an inline-size rejection is returned to the caller rather than silently using CloudFetch (R4; AC8). Documented exception. |
  | Metadata and exported keys | Native Thrift RPCs, including potentially nonempty `getExportedKeys` | SEA SHOW queries; current `getExportedKeys` returns empty | Seven SHOW operations match APC Thrift; `getExportedKeys` remains empty under APC SEA, following warehouse SEA until a supported exported-key query exists. This documented exception hides foreign keys referencing the table; verify a populated-key fixture against Thrift and an empty SEA result (R5; AC54). |
  | Scalar parameters and `supportManyParameters` | `1` selects SQL interpolation and permits `setBytes`; `0` uses native bindings where supported and rejects non-null `setBytes` | Same explicit setting and binary behavior | Preserve the setting: `1` interpolates scalar values at every version and permits `setBytes`; `0` uses native scalars at V8+ and safe interpolation below V8, but non-null `setBytes` retains its existing rejection. The V8 gate limits native binding capability, not the user's interpolation choice. Complex values still fail before request (R6; AC6, AC53). |
  | Prepared batches | Native Thrift executor when supported and opted in; otherwise legacy path | Native SEA executor when opted in; otherwise legacy path | Native `parameter_sets` only at V10+ and opt-in; otherwise one request per set. Aggregate-only APC native response reports `SUCCESS_NO_INFO` rather than per-set counts (R6). |
  | Arrow prerequisite | APC Thrift works without Arrow | Explicit `UseThriftClient=0` is considered before the warehouse Arrow check | APC requires Arrow before even an explicit SEA override, because Reyder has no HIVE non-metadata result path; AIX/PowerPC users without Arrow remain on Thrift (R1; AC7). |
  | Result heartbeat | Thrift can check statement status | SEA result heartbeat uses `/status` | APC schedules no result heartbeat because Reyder does not support `GetStatementStatus`; long-lived completed ResultSets have no periodic liveness check (R3; AC24). |
  | `UseBoundedSeaApi=0` | Not used by Thrift | Changes the warehouse SEA inline/provider path | APC still uses `ARROW_BATCH` and `next_chunk_index`/`row_offset` paging because Reyder's wire contract requires it; the setting cannot disable APC paging (R4, R12; AC8). |
  | `EnableBatchedInserts=1` without APC native batching | Eligible sets may be combined into a multi-row INSERT | Legacy SEA may combine sets; native batching is separately opted in | APC executes each set individually (Phase II DD2/DD3 and Q4), preserving counts and stop-on-error but increasing request count and potentially lowering throughput; V10 native mode restores one request for the batch (AC17, AC32). |
  | `BatchInsertSize` | Caps interpolated multi-row INSERT groups | Caps eligible legacy multi-row INSERT groups | Has no effect on APC fallback (one request per set) or native V10 batches; a configured size no longer limits APC batch request count, so the request-count and latency cost is explicit (R6; AC44). |
  | `RowsFetchedPerBlock` | Limits direct-result rows and Thrift fetch request rows | No effect on SEA result page size | Follow warehouse SEA: APC SEA page size comes from Reyder, so changing this property cannot tune its request count, latency, or buffered rows. Compare those effects live against APC Thrift (R17; AC31). |
  | `QueryResultCompressionType` / `EnableQueryResultLZ4Compression` | Requests the configured compression on Thrift | Sends the configured compression on SEA requests | APC advertises LZ4 decode capability to DBR; Reyder marks LZ4 only when DBR reports compressed data. Decode from the response codec, including uncompressed results. The request setting cannot force an APC codec; record byte/CPU impact. An unconditional LZ4 guarantee requires a separate server contract (R4; AC45). Documented server-contract exception. |
  | `EnableStreamingChunkProvider` | Selects the streaming external-link provider when enabled | Also forced by bounded SEA paging | APC external links use a streaming provider regardless of this toggle because totals may be absent; `0` cannot select the legacy provider (R12; AC39). |
  | `EnableInlineStreaming` and `ThriftMaxBatchesInMemory` | Toggle inline streaming and bound its buffered batches | Bounded SEA inline uses a streaming provider and the memory bound | APC inline paging requires streaming even at `EnableInlineStreaming=0`; `ThriftMaxBatchesInMemory` still bounds buffered inline batches (minimum two), while neither property changes external-link concurrency (R12, R17; AC39). |
  | Timeouts, cancel, and errors | Client timeout and Thrift cancel; JDBC errors | `TimeoutHandler`, SEA cancel, SEA error mapping | Follow warehouse SEA timeout, retry, rate-limit, SQLState, and vendor-code behavior; APC target is added to cancel (R14, R17). |
  | Retry tuning (`RateLimitRetry`, `TemporarilyUnavailableRetry`, `TemporarilyUnavailableRetryTimeout`, `RateLimitRetryTimeout`, `ApiRetriableHttpCodes`, `ApiRetryTimeout`) | `DatabricksHttpRetryHandler` applies these to Thrift HTTP requests and their 429/503/custom-code retry budgets | SDK `ApiClient` policy applies to SEA create, execute, result GET, cancel, close, and terminate requests; these JDBC settings do not configure it | Follow the same warehouse SEA policy per request class, including 429, 503, custom codes, timeout and exhaustion mapping. Compare APC Thrift's setting scope, elapsed time, SQLState and vendor code. Do not add retries for non-idempotent calls beyond the SDK policy (R17; AC30, AC51). |
  | `HttpConnectionRequestTimeout` | Bounds a `DatabricksHttpClient` pool wait | Does not configure the SDK pool wait | Follow warehouse SEA: changing it does not change APC SDK pool waits (R17; AC30). |
  | `HttpMaxConnectionsPerRoute` | Limits per-route HTTP connections | Limits the SDK pool's per-route connections | Preserve the SDK pool limit on APC; under saturation, raising it must permit more concurrent SDK requests up to `HttpConnectionPoolSize`, without an APC-only cap (R17; AC30). |
  | `HttpConnectionPoolSize` | Does not size `DatabricksHttpClient`'s fixed-total pool | Sizes the SDK statement/result request pool | Preserve the SDK total pool limit for APC statement/result/link GETs; it does not size the separate presigned-link download pool (R17; AC30). |
  | `ChunkReadyTimeoutSeconds` | Bounds the wait for a downloaded chunk | Bounds waits in SEA inline and external streaming providers | Apply the configured wait to APC inline and external chunks; an exceeded wait reports the existing chunk-ready error, with no incomplete successful ResultSet (R17; AC46). |
  | `CloudFetchSpeedThreshold` | Sets the slow-download warning threshold | Sets the same external-link warning threshold | Preserve warning behavior for APC external links; this setting does not throttle downloads, change rows, or change retry policy (R17; AC46). |
  | `cloudFetchThreadPoolSize` / `LinkPrefetchWindow` | Bound presigned-link download workers and ahead-of-consumption links | Bound the same SEA streaming download path | Preserve both controls for APC links; a larger pool/window may raise concurrency and heap use but must not alter rows. Compare their effect at low/high settings against both baselines (R17; AC46). |
  | `EnableSeaSyncMetadata` | No SEA sync header on Thrift | Controls the sync SHOW header | For APC synchronous SHOW, `1` sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` and `0` omits it; async SHOW always omits it. Compare result parity and response latency at both values (R5; AC43). |
  | CloudFetch throughput and concurrency | Existing download pool, prefetch, and retries | Shared streaming CloudFetch path | Reuse warehouse SEA presigned-link download pool, prefetch, and retry controls; the SDK pool above governs link discovery, not link downloads. No APC-only cap or full-result buffering (R17). |
  | Telemetry and resources | Existing connection/statement telemetry | SEA telemetry and bounded streaming | Retain standard SEA telemetry, add compute/routing outcome, redact APC ids; measure transfer and heap under concurrent fetch (R8, R17). |

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
- R2: Every APC SEA CreateSession, ExecuteStatement, GetStatementResult, GetResultData,
CancelStatement, CloseStatement, and TerminateSession targets
`backend_compute.all_purpose_cluster_compute.cluster_id` and omits `warehouse_id`.
CreateSession, ExecuteStatement, CancelStatement, and TerminateSession carry the target in the
JSON body; TerminateSession uses the standard `/api/2.0/sql/sessions/{session_id}` DELETE binding.
Statement/result GETs and CloseStatement DELETE carry the URL-encoded dotted query field
`backend_compute.all_purpose_cluster_compute.cluster_id` exactly once, alongside `row_offset`
on result-chunk GETs. Polling `GetStatementResult` omits `chunk_index` entirely, including zero;
`GetResultData` instead requires `chunk_index` in its `/result/chunks/{chunk_index}` URL.
Those GETs and CloseStatement DELETE have no request body. Presigned
external-link URLs and headers are unchanged. Warehouse standard and Real-time request
serialization remains unchanged. Dogfood target extraction for TerminateSession depends on the
tracked server binding fix.
- R3: For APC over SEA, execution polling uses `GetStatementResult`. Result heartbeats are
disabled for APC even when `EnableHeartbeat=1`, because `GetStatementStatus` is unsupported and
using full `GetStatementResult` after completion can re-download result payloads. The driver logs
that heartbeat is unsupported and never schedules `checkStatementAlive` or calls `/status`.
- R4: APC non-metadata execution sends `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`,
and a `session_id`, never sends `statements`, and handles bare Arrow batches
in `chunk.arrow_batches` (including multiple batches per chunk). External links are complete
Arrow IPC streams, not bare batches; preserve framing and headers and drain every link.
`EnableQueryResultDownload=0` keeps SEA selected and sets CreateSession
`can_cloud_download=false`; after the tracked server fix, results remain inline despite the
hybrid disposition. An inline-size error propagates without a successful partial ResultSet.
APC compute type, not `isBoundedSeaApiEnabled`, selects this request and session behavior.
Positive `Statement.setMaxRows` sends SEA `row_limit` and caps exposed rows across pages/links;
zero is unlimited, matching APC Thrift.
Successful no-result statements, including zero-column manifests, expose no ResultSet and zero
update count; `getMoreResults()` then changes the count to `-1` (AC33). Sequential Statement
batches record zero for each such command. Counted DML must supply a manifest schema containing
`num_affected_rows` and its row data, including a zero count; the driver must fetch and sum it
as on APC Thrift. An absent or zero-column schema means a no-result statement only for a
successful non-DML SQL command. For `INSERT`, `UPDATE`, `DELETE`, or `MERGE`, a missing
affected-rows column,
row, or count is a contract error; do not infer an expected nonzero value from the response.
Server failures propagate; live polled DDL and DML gates depend on T32. APC advertises LZ4
decompression capability, but decodes inline payloads according to the response manifest's
compression codec; an unmarked response is uncompressed.
- R5: Seven APC metadata SHOW operations (`GET_CATALOGS`, `GET_SCHEMAS`, `GET_TABLES`,
`GET_COLUMNS`, `GET_FUNCTIONS`, `GET_PRIMARY_KEYS`, `GET_CROSS_REFERENCE`) send
`X-Databricks-Metadata-Operation-Type`, request `ARROW_STREAM` with
`INLINE_OR_EXTERNAL_LINKS` regardless of `EnableSQLExecHybridResults`, and retain
`X-Databricks-Require-Thrift-Native-Metadata` where applicable. Synchronous SHOW adds
`X-Databricks-Sea-Can-Run-Fully-Sync: true` only with `EnableSeaSyncMetadata=1`; async SHOW
omits it. Reyder returns native-shaped metadata in Arrow IPC `chunk.attachment` or HIVE
`chunk.columnar_data`/`COLUMN_BASED_SET`; decode per response and retain
`manifest.is_native_metadata_result=true` for JDBC conversion.

`DatabaseMetaData.getExportedKeys` is outside those seven operations. Warehouse SEA returns a
typed empty result because exported keys are not tracked there, while APC Thrift can return
rows via `GetCrossReference`. APC SEA follows warehouse SEA until a supported query exists:
this exception hides referencing foreign keys and needs a populated-key live comparison.

The session-reachable async metadata API takes the seven SHOW operations and JDBC inputs and
returns a typed handle with a JDBC ResultSet. At live CreateSession V9+ (42249), it submits
asynchronously, polls within the metadata timeout, and preserves sync conversion, empty,
failure, and cancellation behavior. The handle retains statement id, operation, JDBC method
(including both cross-reference conversions), and conversion inputs. Below V9 or without a
live version it completes sync SHOW before returning; retrieval does not poll. Serialized ID
versions grant no capability; existing SQL async behavior stays unchanged.

Internal catalog/schema SELECTs and parameterized `getProcedures`/`getProcedureColumns`
SELECTs are ordinary `ARROW_BATCH` queries with hybrid disposition and no metadata headers;
convert their JDBC metadata after ordinary query decoding. Procedure SELECTs use R6's scalar
binding rule; async SHOW rejects them
before request. Warehouse requests stay unchanged.

For V9+ async `GET_SCHEMAS`/`GET_COLUMNS` with null catalog, SQLState `42601` from
`SHOW ... IN ALL CATALOGS` triggers async catalog enumeration and one SHOW per nonempty
catalog. Keep the operation header, omit the sync header, and combine rows using sync
filtering/empty semantics. Warn and skip ordinary per-catalog SQL errors; propagate timeout,
cancellation, other initial errors, and null-SQLState errors.

- R6: Live CreateSession `negotiated_protocol_version` gates native scalar parameters at V8
(42248), async metadata at V9 (42249), and opt-in `EnableNativeBatching=1` INSERT batches
at V10 (42250). Missing or serialized-ID versions grant no capability. Reject complex
bindings (ARRAY/MAP/STRUCT and other complex logical types) before every request, including
fallback batches, with a tested driver-owned error name/code and taxonomy; interpolation's
`toString()` is not lossless SQL. Below V8, scalar prepared executions and fallback batch
sets interpolate safely and omit `parameters`; missing version has a distinct telemetry reason.
Explicit `supportManyParameters=1` selects interpolation for single executions and fallback
sets at every version and permits non-null `setBytes`. With `supportManyParameters=0`,
non-null `setBytes` remains rejected; V8–V9 fallback sets use native scalar `parameters`.
Native V10 uses ordered nested `parameter_sets`, no top-level `parameters`, and
`SUCCESS_NO_INFO` per set on aggregate success; failure claims no successful set. All other
batches use one request per set even with `EnableBatchedInserts=1`, with ordered counts and
stop-on-error. Pre-execution `PreparedStatement.getMetaData()` uses binding-free
`DESCRIBE QUERY` without losing bindings. Warehouse behavior stays unchanged.
- R7: APC uses Thrift only when the pre-connection routing decision selects Thrift (an explicit
`UseThriftClient=1`, `TreatMetadataCatalogNameAsPattern=1` with no explicit `UseThriftClient`, the
APC client-config flag is off or unavailable, or Arrow is unavailable). `UseBoundedSeaApi` does
not control APC routing. Once SEA is selected, server errors—including unsupported
regions—propagate to the client exactly as they do for warehouse SEA; the driver never switches
the connection to Thrift. The warehouse-only Reyden KP001 Thrift→SEA recovery must not change
an APC routing decision either.
- R8: Telemetry and logs identify the compute type (APC vs warehouse) and the chosen protocol for
each connection attempt, including its routing reason and success or failure outcome. Emit a
routing event even when APC SEA CreateSession, APC Thrift OpenSession, warehouse SEA CreateSession,
or warehouse Thrift OpenSession fails. When explicit `UseThriftClient=0` selects SEA alongside
`TreatMetadataCatalogNameAsPattern=1`, the driver logs that the Thrift-only metadata property has no
effect under SEA. APC SEA diagnostics,
including existing execute logs and session stringification, must not print org/cluster ids or
compound session/statement ids; use a redacted compute-type label instead.
- R9: APC SEA session IDs serialize as `s|a|orgId|clusterId|sessionId|protocolVersion-or--`;
internal statement IDs serialize as `s|a|orgId|clusterId|statementId`. The APC prefix selects
only the APC grammar: sessions require exactly six nonempty segments and valid version, statements
exactly five nonempty segments. Malformed IDs fail without exposing input or compute identifiers.
Existing warehouse and raw SEA IDs remain compatible. Public `getConnectionId()` returns the
compound session form for later `Driver.closeConnection`; public statement IDs remain raw server
IDs. Recovered statements use the current connection target; saved-session close uses the saved
protocol and target even after a flag change. Only a live CreateSession version grants capabilities,
not a version embedded in an ID.
- R10: APC ExecuteStatement preserves the existing SEA timeout behavior: `wait_timeout` is unset
for synchronous execution with direct results and `0s` for asynchronous execution or when direct
results are disabled. Reyder accepts `0s` as the SEA duration-string representation of zero; no
APC-specific timeout override is needed.
- R11: For APC inline `ARROW_BATCH` payloads, the driver builds the Arrow schema from
`manifest.schema` (`type_text`/`type_json`, including nested types) and does not require
`manifest.arrow_schema`; a populated `manifest.arrow_schema` or `total_byte_count` does not
change decoding or paging. External links retain their complete Arrow IPC stream and schema.
Until the complete logical-type matrix is supported, any deferred
type fails clearly before a ResultSet can report successful completion; it is never silently
coerced or omitted.
- R12: APC result fetching pages by `next_chunk_index` and `row_offset`, never relies on
`total_chunk_count`, `chunks`, or `total_row_count`, and accepts a `GetStatementResult`
response with or without inline result data. For inline `arrow_batches`, advance `row_offset`
by the sum of batch `row_count` values in the page. For a page with multiple external links,
take both the continuation `next_chunk_index` and the next `row_offset` from the final link in
response order:
`finalLink.row_offset + finalLink.row_count`. A missing final-link `next_chunk_index` ends paging.
- R13: For APC metadata, decode the emitted SEA `columnar_data` physical vectors (boolean,
byte, short, int, long, double, string, binary) using `manifest.schema` and least-significant-bit
first null masks. The server always sets a mask; short masks leave unrepresented rows non-null and
long masks ignore excess bits. The decoded JDBC columns and values match the same metadata
operation over APC Thrift. An unrecognized physical vector fails clearly.
- R14: APC query timeouts use the client-side `TimeoutHandler` + CancelStatement path, as for
warehouses.
- R15: `Statement.addBatch(String)` / `executeBatch` on APC executes SQL commands sequentially,
one ExecuteStatement request per SQL string, preserving the existing stop-on-first-failure and
update-count behavior. It never sends the unsupported `statements` field.
- R16: SEA result formats include `ARROW_BATCH` for APC queries and `COLUMN_BASED_SET`
(wire value 6) for APC metadata while preserving existing warehouse wire values. Unknown future
response formats deserialize safely and fail clearly if decoded; requested `ARROW_STREAM` must
not prevent decoding a `COLUMN_BASED_SET` metadata response.
- R17: APC uses warehouse SEA's SDK retry and pool behavior and its separate streaming
CloudFetch download path for presigned links. Preserve the matrix's tuning-property scopes,
including the SDK total and per-route limits and the non-effect of Thrift retry settings and
`RowsFetchedPerBlock` on SEA fetches. Cover CreateSession, ExecuteStatement, result GET, cancel,
close, and TerminateSession separately; do not add retries to non-idempotent calls beyond SDK
precedent. No APC-only retry policy, concurrency cap, or full-result buffer is allowed. Reyder
advertises LZ4 capability to DBR and marks a response LZ4 only when DBR reports compression;
decode from the response codec. Explain measured APC Thrift differences by a server limit or
warehouse SEA precedent and their client impact (AC30–AC32, AC45–AC46, AC51).
- R18: Every merged JDBC slice exposes its completed public behavior when R1's default-off
APC flag is enabled or `UseThriftClient=0` is explicit. Incomplete public calls fail before network
execution when known, and unsupported response shapes fail before false success; they never
silently switch to Thrift after SEA selection. Keep the server flag OFF without assignment or
ramp until integration gates close. Remove each guard when its slice passes live staging, and
keep ordinary flag-off APC connections on Thrift.

## Clarifications

- Q1: Reuse `UseThriftClient` or add `EnableApcSea`?
A1: Reuse `UseThriftClient` (`0` SEA, `1` Thrift). APC requires Arrow first; then explicit
`UseThriftClient` wins over catalog-pattern preference and the APC flag. Without it, explicit
`TreatMetadataCatalogNameAsPattern=1` forces Thrift. Warehouse precedence stays unchanged.
- Q2: Fall back to Thrift after an APC SEA routing error?
A2: No. Propagate SEA errors as warehouse SEA does.
- Q3: How should Arrow IPC and HIVE `columnar_data` metadata coexist?
A3: Add a reader for Reyder's HIVE `chunk.columnar_data` (metadata DD2/DD4) and choose the
decoder per response, never by requested `ARROW_STREAM` or connection lifetime (R5, R13).
- Q4: Emulate batches below V10 or reject them?
A4: Emulate: no APC `statements` field. V10+ native `parameter_sets` needs
`EnableNativeBatching=1`; otherwise execute each parameter set, even with
`EnableBatchedInserts=1` (Phase II DD2/DD3). Statement SQL batches stay sequential and stop
at the first failure.
- Q5: Add a client denylist for govcloud/non-Reyden regions?
A5: No. Keep the flag off there and propagate any SEA rejection without fallback.
- Q6: Add a special User-Agent or capability header?
A6: No. Preserve the ordinary User-Agent; CreateSession path and `backend_compute` identify APC.
- Q7: Is manifest `type_text`/`type_json` sufficient for all Arrow types?
A7: AC13 must prove lossless reconstruction. If it fails, block release until Reyder supplies
`arrow_schema` and the driver consumes it; never coerce silently or switch to Thrift.
- Q8: Share `enableSqlExecForJdbc` or add a dedicated APC flag?
A8: Add default-off sub-flag
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` for independent
ramp/rollback. Older drivers ignore it; no connector-service version filter is needed.
- Q9: Force the APC flag false for partner User-Agents despite Reyder's OSS prefix allowlist?
A9: Yes, for every partner, including JDBC partners allowlisted for `enableSqlExecForJdbc`.
This separate blanket override (T35) requires service deployment before APC flag assignment;
partner enablement needs its own rollout decision. T17 defines only the flag; the driver consumes false.
- Q10: How is repeated transfer avoided during `GetStatementResult` polling?
A10: At least three RUNNING polls of a large query transfer no result payload; terminal
response and fetch return all rows once (AC27).

## Acceptance Criteria

Each JDBC package proves its completed public call on live staging with the default-off APC
flag enabled or explicit `UseThriftClient=0`, with incomplete calls guarded. T18 repeats the gates
after all packages merge and the flag/service prerequisites deploy. A missing live or
server-dependent check remains pending;
fixtures alone cannot pass it. The development plan owns test procedures and evidence capture.
- AC1: GIVEN an APC path with no explicit `UseThriftClient` or catalog-pattern override WHEN
the APC flag is OFF, absent, lookup-failed, or partner-overridden false THEN public JDBC uses
Thrift regardless of the warehouse flag. GIVEN the same concrete cluster path, Arrow available,
and the APC flag ON WHEN it connects THEN it uses SEA. A legacy `cliservice` path always stays
on Thrift and never sends placeholder `default` ids. AC19 and AC21 cover explicit overrides.
- AC2: GIVEN a cluster HTTP path with APC-SEA enabled (R1) WHEN a connection is opened THEN the
CreateSession request body contains `backend_compute.all_purpose_cluster_compute.cluster_id`
equal to the URL's cluster id and contains no `warehouse_id`.
- AC3: GIVEN live APC SEA execute, poll, fetch, cancel, close, and termination calls WHEN their
requests are observed THEN each carries R2's target in the correct body or query position;
polling omits `chunk_index`, chunk GET includes its URL index, and bodyless calls stay bodyless.
No `/status` call occurs; warehouse requests remain unchanged. Live termination proof waits for
the server binding fix.
- AC4: GIVEN flag-on staging APC inline and linked results WHEN public JDBC drains multi-batch
chunks and large linked files THEN rows, values, metadata, and errors match APC Thrift;
warehouse SEA supplies the request precedent. Sync and async requests retain R4's wire shape
at both `EnableSQLExecHybridResults` settings.
- AC5: GIVEN the seven R5 SHOW operations run synchronously WHEN Arrow IPC and
`columnar_data` responses alternate THEN native-flag conversion, JDBC rows, errors, timeout,
and null-catalog behavior match APC Thrift on flag-on staging. Procedure SELECTs remain ordinary
queries; warehouse metadata requests remain unchanged. AC50 covers the async API.
- AC6: GIVEN `supportManyParameters=0` and a live APC session at V8+ WHEN a scalar prepared
query executes THEN returned values and native `parameters` match APC Thrift and warehouse SEA
on staging. Below V8 or without a live version, safe interpolation preserves values and quoting
and omits `parameters`. GIVEN a prepared statement before execution WHEN `getMetaData()` runs
THEN binding-free `DESCRIBE QUERY` returns JDBC metadata and later execution retains bindings.
AC53 covers the explicit interpolation setting; unavailable live versions remain pending.
- AC7: GIVEN `UseThriftClient=0` and the APC flag ON on AIX/PowerPC with Arrow unavailable WHEN a
connection is opened THEN the Arrow prerequisite takes precedence, the pre-connection routing
decision selects Thrift, and the driver logs the routing reason.
- AC8: GIVEN APC SEA with `EnableQueryResultDownload=0` WHEN a query runs THEN CreateSession sets
`can_cloud_download=false`, all pages stay inline, and an inline-size rejection propagates
without a partial successful ResultSet. `UseBoundedSeaApi=0` or `1` does not change route or
paging. Live inline-only proof awaits the named server fix.
- AC9: GIVEN APC-SEA enabled (R1) AND CreateSession returns an "all-purpose clusters are not
supported" error WHEN a connection is opened THEN the driver propagates a clear connection error,
does not create a Thrift client, and records the failure in telemetry.
GIVEN APC was routed to Thrift and OpenSession returns KP001 THEN the warehouse-only recovery
does not switch it to SEA, regardless of whether the APC flag was off or Arrow was unavailable.
- AC10: GIVEN the first live APC DDL slice and later successful or failed APC/warehouse SEA/Thrift
connections WHEN telemetry and DEBUG logs are captured THEN one route event per attempt identifies
compute, protocol, reason, and outcome. Before that first live slice, synchronous/asynchronous
execution and session diagnostics (including `AllPurposeCluster.toString()` call sites) redact credentials,
org/cluster ids, and compound ids while retaining standard SEA fields.
- AC11: GIVEN standard and Real-time SQL warehouse connections WHEN CreateSession, execute,
poll, fetch, cancel, close, and TerminateSession run THEN serialized request bodies, URLs, and
headers match pre-change fixtures, including warehouse `realtimeSession` behavior.
- AC12: GIVEN a flag-on staging APC-SEA session WHEN a statement is executed with and without direct results
THEN `wait_timeout` is unset and `0s` respectively, Reyder accepts both request shapes, and
`statements` is never sent. GIVEN an asynchronous APC execution with direct results enabled WHEN
the request is captured THEN `wait_timeout` is `0s`, not an omitted or nonzero duration;
compare the public JDBC outcome and wire precedent with APC Thrift and warehouse SEA.
- AC13: GIVEN empty or populated `manifest.arrow_schema` and `total_byte_count` WHEN public
JDBC reads APC `ARROW_BATCH` values and metadata for primitive, decimal, array, map,
struct, interval, geography, geometry, timestamp, timestamp_ntz, and variant columns THEN
they match each live APC Thrift and warehouse SEA staging query; an unavailable type
remains a named pending live gate backed by a contract fixture, never an assumed pass.
- AC14: GIVEN multiple external links with missing totals WHEN public JDBC drains them on live
staging THEN all rows arrive once using the final link's `next_chunk_index` and end offset;
large Arrow IPC streams are complete, and presigned URLs and headers remain unchanged. Compare
APC Thrift rows and warehouse SEA link handling. AC36 covers inline continuation.
- AC15: GIVEN a completed flag-on staging APC query WHEN `GetStatementResult` returns a manifest
without result data, or with inline result data, THEN public JDBC returns the complete ResultSet
with APC Thrift row/value parity; warehouse SEA supplies the request precedent. If staging emits
only one shape, the other remains a named live gate with a contract fixture until observed live.
- AC16: GIVEN an APC native SHOW response with `columnar_data` WHEN its supported boolean, numeric,
string, or binary vectors and least-significant-bit null masks are decoded THEN JDBC values
match APC Thrift; short masks leave later rows non-null and long masks ignore extra bits.
A subsequent Arrow IPC response on the same connection also decodes correctly.
- AC17: GIVEN live V10 and `EnableNativeBatching=1` WHEN a public prepared INSERT batch runs
on staging THEN one ordered `parameter_sets` request, no top-level `parameters`, and
`SUCCESS_NO_INFO` per set on aggregate success match the R6 contract. GIVEN native batching is
unavailable or disabled WHEN the batch runs THEN it sends one request per set, preserves ordered
counts, and stops on failure; V8+ uses native scalar bindings and older sessions interpolate.
Compare public counts, errors, request count, and connection usability with APC Thrift and
warehouse SEA. An unavailable V10 live route remains pending.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. On flag-on staging, compare elapsed time, SQLState/vendor code,
cancel target, and post-timeout connection usability with APC Thrift and warehouse SEA.
- AC19: GIVEN Arrow available and a cluster HTTP path with `UseThriftClient=1` AND the APC flag ON
WHEN a connection is opened THEN the driver uses Thrift; GIVEN Arrow available,
`UseThriftClient=0`, AND the APC flag OFF THEN it uses SEA (client setting takes precedence after
the Arrow prerequisite).
- AC20: GIVEN a warehouse HTTP path WHEN the APC flag is ON or OFF THEN warehouse routing is
unchanged and still follows `enableSqlExecForJdbc`.
- AC21: GIVEN Arrow available and an APC HTTP path with no explicit `UseThriftClient`,
`TreatMetadataCatalogNameAsPattern=1`, and the APC flag ON WHEN a connection is opened THEN the
driver selects Thrift and preserves catalog-pattern metadata semantics; GIVEN the same settings
with explicit `UseThriftClient=0` THEN the explicit override selects SEA and logs that the
Thrift-only metadata parameter has no effect.
- AC22: GIVEN a three-command public `Statement.addBatch(String)` on staging WHEN
`executeBatch` runs THEN it sends sequential ExecuteStatement requests without `statements`
and returns counts in order.
If command two fails, command three is not sent and `BatchUpdateException` contains the
first count. Compare counts, SQLState/vendor code, and request count with APC Thrift;
warehouse SEA supplies the wire precedent.
- AC23: GIVEN CreateSession omits `negotiated_protocol_version` WHEN a prepared query or batch
runs THEN an encoded ID grants no capability: with `supportManyParameters=0`, safe interpolation
works, each fallback set is separate, no `parameters` or `parameter_sets` is sent, and telemetry
records the missing-version downgrade. Compare public values and errors live when available;
AC53 covers `supportManyParameters=1`.
- AC24: GIVEN an APC SEA connection with `EnableHeartbeat=1` WHEN a completed ResultSet
remains open on live staging beyond two heartbeat intervals THEN no heartbeat task, `/status`,
`GetStatementStatus`, or heartbeat result poll occurs; execution polling still works. Compare
request traffic with APC Thrift and warehouse SEA and log the unsupported APC heartbeat.
- AC25: GIVEN APC session and statement IDs WHEN serialized and parsed THEN R9's grammar,
compound public connection ID, raw public statement ID, and warehouse/Thrift compatibility hold.
Malformed APC IDs fail without fallback or identifier disclosure, using
`SESSION_ID_PARSING_EXCEPTION` for sessions; embedded versions grant no capability.
- AC26: GIVEN APC `ARROW_BATCH` and metadata `COLUMN_BASED_SET` plus existing warehouse
`JSON_ARRAY`/`ARROW_STREAM` responses WHEN serialized and decoded THEN wire values and JDBC
results remain correct. Unknown formats deserialize safely and fail clearly on decode; APC native
metadata selects its emitted physical format on live staging.
- AC27: GIVEN a live APC query with at least three RUNNING result polls WHEN response bytes
and rows are observed on explicit-override and flag-on routes THEN RUNNING polls contain no result
payload and terminal/fetch responses deliver all rows once; compare APC Thrift and warehouse SEA.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` and its default is OFF.
Configuration evidence records the exact name, parent batch flag, and default value.
- AC29: GIVEN an APC complex binding at V8+, below V8, or without a live version WHEN a
single or batch execution starts THEN the same driver-owned error name/code appears before
network execution, including after scalar batch sets; `Types.ARRAY` and `Types.STRUCT` are
covered and the connection remains usable. Compare live public JDBC behavior with APC Thrift and
warehouse SEA and retain fixtures for unavailable versions. Scalar values still follow R6's
version gate.
- AC30: GIVEN independent low and high `HttpMaxConnectionsPerRoute` and
`HttpConnectionPoolSize` limits WHEN comparable live APC and warehouse SEA SDK requests saturate
each limit THEN in-flight requests and wait/error outcomes track the configured per-route and
total limits. `HttpConnectionRequestTimeout` retains warehouse SEA scope; presigned links keep
their URL and headers. Compare APC Thrift pool behavior and record client impact. AC51 covers
retry policy.
- AC31: GIVEN matched rows, result size, connection concurrency, and download settings on live
APC SEA, APC Thrift, and warehouse SEA WHEN complete linked and inline results are read THEN
rows match and the comparison records throughput, end-to-end latency, peak heap, transfer bytes,
request count, retries, and errors across at least three repeated runs. `RowsFetchedPerBlock`
changes APC Thrift request size but follows warehouse SEA's no-effect precedent on APC SEA;
record the request-count, latency, and heap impact. A repeatable regression over 10% in median
throughput or latency, or over 20% in peak heap against APC Thrift fails parity unless a matrix
exception names the server/warehouse precedent, reason, client impact, and PECO/Perf Eng approval.
Incomplete rows, an APC-only cap, or heap growing with total result size always block release.
Inline-only proof remains pending until server support is deployed.
- AC32: GIVEN the same prepared INSERT batch on live APC SEA fallback, V10 native mode,
APC Thrift, and warehouse SEA WHEN `executeBatch` completes THEN counts and errors match the R6
contract and at least three runs record request count, sets per second, end-to-end latency, and version.
Fallback must issue one request per set and native V10 one request for the batch. PECO and
Perf Eng must approve a matrix exception with reason and client impact for any repeatable
over-10% sets-per-second loss or latency increase against APC Thrift; without that approval
the release gate fails. The fallback's extra request count is itself a documented exception.
Missing live V10 evidence remains pending.
- AC33: GIVEN successful APC SEA DDL with absent or zero-column manifest and no data WHEN
public JDBC executes it on live staging THEN no ResultSet or fetch occurs, `execute` is false,
update count is zero, and `getMoreResults()` changes the count to `-1`. Failed DDL keeps its
server error; AC48 owns counted DML.
- AC34: GIVEN the deployed default-off APC flag WHEN partner and non-partner User-Agents
request client flags THEN connector-service returns false for every partner, including JDBC
warehouse-allowlisted partners, and retains the ordinary APC value for non-partners. Record the
service version and region before any APC assignment or ramp.
- AC35: GIVEN equivalent live APC SEA, APC Thrift, and warehouse SEA calls WHEN errors and
telemetry are compared THEN APC SEA keeps warehouse SEA SQLState/vendor codes and event fields;
each APC Thrift difference has a matrix precedent or server limit and stated client impact.
- AC36: GIVEN a manifest-only start or multiple inline bare-batch continuations on live staging
WHEN public JDBC drains them THEN rows arrive once using each page's summed `row_count` and
`next_chunk_index`; errors propagate without a partial successful ResultSet. Compare APC Thrift
rows and warehouse SEA paging. AC14 covers linked continuation.
- AC37: GIVEN T32 deployed WHEN a successful APC no-result DDL is closed or reused after polling
on live staging THEN no result fetch or `/status` call occurs, repeated close is safe, and the
connection remains usable; compare APC Thrift and warehouse SEA. Failed DDL retains its error.
- AC38: GIVEN the integrated stack WHEN a public primitive query and retryable fetch
failure run through explicit override and deployed flag on staging THEN rows, order, SQLState,
and vendor code match APC Thrift with warehouse SEA error precedent. Server-dependent cases
remain pending until deployed.
- AC39: GIVEN live APC inline pages and external links WHEN `EnableStreamingChunkProvider`,
`EnableInlineStreaming`, and `ThriftMaxBatchesInMemory` vary THEN rows remain complete and
buffered inline batches follow the matrix's minimum-two bound; compare APC Thrift and warehouse
SEA. An unavailable inline-only server path remains pending.
- AC40: GIVEN any merged JDBC slice with the APC flag OFF and no explicit override WHEN public
JDBC connects THEN APC stays on Thrift. GIVEN `UseThriftClient=0` or an ON flag with Arrow
available WHEN its completed public call runs on live staging THEN that call reaches SEA and
matches the slice's parity gate; an unfinished call or unsupported shape fails before false
success. No flag assignment or ramp occurs before integration.
- AC41: GIVEN a recovered statement or saved APC session after a flag change WHEN fetch,
cancel, close, or termination runs on live staging THEN the current or saved target/protocol
follows R9 and connection usability matches APC Thrift or warehouse SEA. Termination proof waits
for the server binding fix.
- AC42: GIVEN a RUNNING APC statement WHEN public JDBC closes it on live staging THEN one
close request, no `/status` call, and a usable connection match APC Thrift and warehouse SEA.
AC3 covers request targeting.
- AC43: GIVEN live synchronous APC SHOW WHEN `EnableSeaSyncMetadata` changes between 0 and 1
THEN only R5's sync header changes; rows, errors, JDBC values, and latency are compared with
APC Thrift and warehouse SEA. Server-dependent shapes remain pending.
- AC44: GIVEN identical live prepared INSERT batches WHEN `BatchInsertSize` changes between
small and large values THEN APC fallback still sends one request per set and V10 native sends
one per batch; counts and errors remain correct. Record request-count and latency impact against
APC Thrift; unavailable V10 remains pending.
- AC45: GIVEN APC compression settings requesting LZ4, another codec, or none WHEN live APC
SEA queries return LZ4-marked and unmarked responses THEN every value decodes according to
the response manifest, regardless of the request setting; warehouse SEA retains its configured
compression behavior. Compare bytes, decode CPU, and latency with APC Thrift and warehouse SEA;
record the setting's APC scope and client impact. If only one response codec is available live,
the other remains pending with a contract fixture; unconditional LZ4 needs a server prerequisite.
- AC46: GIVEN low and high `cloudFetchThreadPoolSize`, `LinkPrefetchWindow`,
`ChunkReadyTimeoutSeconds`, and `CloudFetchSpeedThreshold` values WHEN live APC linked and inline
results are consumed under contention or slow delivery THEN rows remain complete, link worker
concurrency and lookahead respect the first two controls, chunk waits time out at the configured
limit, and slow-download warnings follow the threshold. Compare warehouse SEA and APC Thrift
concurrency, errors, and connection usability; a no-op or regression over AC31's limits needs
a documented matrix exception with reason and client impact or blocks release.
- AC47: GIVEN a primitive single-chunk APC inline `ARROW_BATCH` result without
`manifest.arrow_schema` WHEN public JDBC consumes it on the guarded staging route THEN
rows, order, JDBC types, and errors match APC Thrift, with warehouse SEA as the request and error
precedent. Continued or linked shapes remain guarded until their slices pass.
- AC48: GIVEN successful APC `INSERT`, `UPDATE`, `DELETE`, or `MERGE` with an affected-rows
schema and data, including zero, WHEN public `executeUpdate` or `executeLargeUpdate` runs on live
staging THEN its count matches APC Thrift. GIVEN any of those DML operations with a missing
`num_affected_rows` column, row, or count WHEN the response is decoded THEN a contract error
occurs rather than successful zero. No-result DDL follows AC33. T32 proves the server prerequisite.
- AC49: GIVEN live APC queries with `setMaxRows(0)`, `1`, and a boundary-crossing limit
WHEN inline or linked results are read THEN positive limits send SEA `row_limit`, zero is
unlimited, and rows never exceed the limit; compare rows and request count with APC Thrift and
warehouse SEA. A boundary unavailable live remains pending with a contract fixture.
- AC50: GIVEN the seven async SHOW operations on live V9+ APC staging WHEN a typed handle
completes THEN rows, errors, null-catalog fallback, timeout, and cancellation match synchronous
APC SHOW and APC Thrift. Below V9 or without live version, the handle completes synchronously
without result polling; procedure SELECTs fail before async submission. Unavailable V9 staging
remains pending.
- AC51: GIVEN comparable live APC and warehouse SEA CreateSession, ExecuteStatement, result GET,
CancelStatement, CloseStatement, and TerminateSession requests WHEN injected 429, 503, custom-code,
or timeout faults succeed or exhaust THEN per-class SDK attempts, elapsed time, SQLState, and
vendor code match warehouse SEA, including its treatment of non-idempotent calls. Vary the six
R17 Thrift retry settings and verify they do not alter SDK policy; compare APC Thrift and record
each setting's differing scope and client impact. Safe fault injection must not duplicate writes.
- AC52: GIVEN Reyder's APC `GetStatementResult` route after RUNNING polls WHEN no-result DDL
or counted DML completes THEN direct server contract evidence returns, respectively, success
with absent or zero-column manifest and no data, or a schema and affected-rows data including
zero. Failed statements retain their errors. AC33, AC37, and AC48 own public JDBC proof.
- AC53: GIVEN `supportManyParameters=0` and `1` on live APC V8+ and older available protocols
WHEN scalar prepared queries, fallback batches, and non-null `setBytes` run through public JDBC
THEN `1` interpolates scalars and permits binary literals while `0` uses native scalar bindings
only at V8+ and rejects non-null `setBytes` as APC Thrift and warehouse SEA do. Below V8, `0`
interpolates scalars but still rejects non-null `setBytes`; unavailable versions stay pending.
- AC54: GIVEN a table with a foreign key referencing it on live APC staging WHEN public
`DatabaseMetaData.getExportedKeys` runs through Thrift and SEA THEN Thrift can return the key
row and APC SEA returns the typed empty result specified by R5, as warehouse SEA does. Record
the missing-key client impact; if no populated foreign-key fixture is available, the live gate
remains pending rather than treating two empty results as proof.
## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). Only the integration JDBC package owns `docs/` and
  `NEXT_CHANGELOG.md`; earlier PRs use `NO_CHANGELOG=true` for the unreleased feature. Every
  completed slice is shippable behind the default-off flag; no partial stack is ramped.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T32 and T35; known gaps SC-256188 under SC-256186 and SC-254478 remain
  external prerequisites for affected dogfood checks.
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

Use ten reviewable JDBC slices (see `plan.json`): no-result DDL, first single-chunk query,
inline continuation, linked/logical results, lifecycle, scalar prepared execution, batches,
synchronous metadata, asynchronous metadata, and integration. Query continuation, lifecycle,
and synchronous metadata branch after the first query; linked results, prepared execution,
and async metadata continue their respective branches. Each slice exposes its completed public
behavior through R1's default-off guarded route and proves it on live staging against APC
Thrift and warehouse SEA. The integration slice joins the branches, owns `docs/` and
`NEXT_CHANGELOG.md`, and repeats the flag-on gates. Keep each PR below roughly 1,500 changed
lines by excluding neighboring behaviors from its scope.

Universe packages own the DDL/DML result contract (T32), default-off flag (T17), and blanket
partner override (T35). T32 gates live DDL/DML; T17 and T35 gate flag-on validation. They are
separate repository PR stacks: every `plan.json` parent is a branch base in the same repo.
The integration package lists the cross-repo deployment gates explicitly rather than treating
them as Git parent edges. T18 blocks release on open live, server, or performance gates.

### Affected Components

- Public JDBC connection, statement, prepared-statement, and metadata APIs.
- SEA request targeting, session and statement identity, result decoding and paging.
- Driver diagnostics and error taxonomy; Reyder and connector-service contracts in Universe.

### Data Model Changes

- APC target and live capability version in session state; internal IDs retain compute identity.
- APC result formats and metadata physical vectors preserve warehouse wire values.

### API Changes

Public JDBC methods remain the same. Add a session-reachable asynchronous metadata API for
the seven SHOW operations. Public connection IDs may encode APC SEA; statement IDs stay raw.

### Risks

- Tracked server fixes gate live metadata, inline-only, and termination checks; contract
  fixtures protect those shapes before deployment.
- AC13/Q7 block release on lost logical types; AC27 blocks repeated poll payload transfer.
- AC11/AC14 protect warehouse request and presigned-link boundaries.

---

## Tasks

Each task appears in one `plan.json` package. Package gates exercise completed behavior through
public JDBC methods using R1's guarded route; T18 repeats the checks after integration.
External gates are pending until proven.

### Phase 1 — Guarded DDL core

- [ ] T1: Add APC compute and SEA result-format support while preserving warehouse wire values (R2, R16).
- [ ] T2: Target APC session, execution, polling GET, cancel, ordinary close, and termination calls without changing warehouse requests (R2).
- [ ] T3: Before the first live APC call, make synchronous/asynchronous execution and session diagnostics identifier-safe (including compute `toString()` logging); then exercise APC SEA DDL through public JDBC and emit redacted route/statement telemetry (R2, R4, R8; AC10).
- [ ] T19: Round-trip APC session and internal statement IDs, retaining raw public statement IDs (R9).
- [ ] T5: Keep live CreateSession capability version separate from serialized ID diagnostics (R6, R9).
- [ ] T28: Return zero count and no ResultSet for successful no-result DDL, including a zero-column manifest; preserve errors (R4).
- [ ] T31: Expose completed public DDL through R1's default-off APC route and guard incomplete calls against false success in every partial slice (R18).

### JDBC slices after the core

- [ ] T7: Suppress APC result heartbeats while retaining execution polling (R3).
- [ ] T8: Complete public primitive single-chunk query values, errors, and cleanup; defer DML counts to T11's next slice (R4, R8, R11).
- [ ] T10: Decode primitive LZ4 inline bare Arrow batches for a single-chunk public query (R4, R11).
- [ ] T11: Fetch every inline continuation, including manifest-only starts, by `next_chunk_index` and `row_offset`; complete counted DML in this slice. T33 covers linked continuation (R4, R12).
- [ ] T25: Target result-chunk GETs once, with null body and unchanged presigned URLs/headers (R2).
- [ ] T26: Prove per-request-class SDK retry scope and both pool limits through live APC and warehouse SEA traffic; keep non-idempotent fault injection safe (R17; AC30, AC51).
- [ ] T33: Stream complete external-link Arrow IPC results, without treating them as bare batches (R4, R11).
- [ ] T34: Preserve `Statement.setMaxRows` across APC SEA inline and linked results (R4).
- [ ] T36: Prove linked-result CloudFetch download, response-codec decoding, streaming controls, chunk waits, and performance parity through public JDBC (R17; AC31, AC39, AC45, AC46).
- [ ] T20: Complete the AC13 logical-type matrix without silent coercion (R11, Q7).
- [ ] T6: Preserve selected APC protocol on connection errors and KP001 Thrift failure (R7).
- [ ] T12: Apply warehouse SEA timeout behavior with targeted APC cancellation (R14).
- [ ] T15: Complete lifecycle route outcomes and asynchronous diagnostic coverage on top of T3's first-slice redaction (R8).
- [ ] T27: Complete timed-out, failed, and repeated-close DDL lifecycle using T2's target (R2, R14).
- [ ] T29: Use the current target for recovered statements and saved protocol/target for saved-session close (R9).
- [ ] T30: Execute Statement SQL batches sequentially with ordered counts and stop-on-error (R15).
- [ ] T9: Gate native scalars at V8, preserve `supportManyParameters` and `setBytes` semantics, and support binding-free pre-execution metadata (R6; AC53).
- [ ] T22: Reject complex bindings at every APC capability level before requests, with a tested error name/code and taxonomy (R6).
- [ ] T21: Support opt-in V10 prepared batches and ordered per-set fallback (R6).
- [ ] T13: Serve synchronous SHOW and procedure SELECT metadata with the R5 request contract and null-catalog fallback.
- [ ] T14: Decode emitted Arrow IPC or columnar native metadata with the manifest native flag and AC16 value/null parity (R5, R13).
- [ ] T37: Preserve warehouse SEA's typed-empty exported-key result on APC SEA and validate the documented APC Thrift difference with a populated-key live fixture (R5; AC54).
- [ ] T23: Expose seven-operation async SHOW with the live V9 gate and completed handles below V9 (R5).
- [ ] T24: Preserve async null-catalog fan-out, errors, timeout, and cancellation (R5).

### Independent Universe prerequisites

- [ ] T17: Define the default-off APC SAFE sub-flag without assigning or ramping it (R1, Q8).
- [ ] T32: Prove APC `GetStatementResult` returns successful no-result DDL without Arrow data and counted DML with `num_affected_rows` schema and data after polling; fix either server path if needed (R4, AC52).
- [ ] T35: In connector-service, override the APC sub-flag to false for every partner User-Agent before flag assignment, without changing the warehouse flag's partner allowlist (Q9, AC34).

### Integration and release

- [ ] T4: Integrate the independently merged JDBC branches and confirm Arrow-first routing, explicit override, catalog-pattern precedence, and the dedicated flag (R1, R7, R18).
- [ ] T16: Document routing, exported-key exception, batch and compression costs, SEA tuning scope, and the user-visible changelog entry; this is the only docs/changelog package.
- [ ] T18: Consolidate local and live evidence, repeat flag-on gates after T17/T35/T32 deployment, and close cross-route checks (AC1–AC54).
- [ ] T-val: Record pass/fail or externally pending evidence for AC1–AC54 and exact commands; do not declare release ready with Q7, Q9, or server/performance gates open.
