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
  SC-254478). Driver behavior and fake-service tests assume those fixes. Affected live staging
  metadata, inline-only, and session-termination gates remain pending until the fixes deploy.
  The TerminateSession REST binding declares `body: "*"`; the driver tests its DELETE body
  against the assumed server contract with a fake service.
- Reyder's current APC `GetStatementResult` path fetches missing metadata after a successful
  poll, then requires metadata and an Arrow schema to build a manifest. A polled DDL response
  with neither schema nor result data is therefore not yet a proven live server response. Counted
  DML also needs a result schema and affected-rows data: SEA has no separate update-count field,
  and the driver reads the `num_affected_rows` result column. T32 proves both server result
  contracts before their live JDBC gates can pass. Successful live result manifests currently
  populate `arrow_schema`; an absent-field decode is a fixture gate for this slice.
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
  `DatabricksConnection.close()` calls `DatabricksSession.close()`, whose SDK delete path casts
  the compute target to `Warehouse`; session close suppresses that failure, so APC cleanup must
  be fixed before the first public slice.
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
  | Result values and paging | Arrow/CloudFetch or inline; complete JDBC rows | Arrow stream/CloudFetch or bounded inline | Same JDBC values and completeness; inline APC payloads are bare `ARROW_BATCH` values decoded by their response codec (LZ4 or uncompressed), while external links contain complete Arrow IPC streams; both use APC offset paging (R4, R11, R12). |
  | `Statement.setMaxRows` | Limits rows | Sends SEA `row_limit` | Match both baselines across pages and links; zero is unlimited (R4; AC49). |
  | Result disposition controls | Thrift inline or CloudFetch | `EnableSQLExecHybridResults=0` requests external-only; disabled download may select Thrift | APC always requests hybrid SEA results; the hybrid toggle cannot force external-only. Disabled download keeps SEA and requests inline-only delivery through `can_cloud_download=false`; inline-size errors propagate. APC contract exceptions (R4; AC4, AC8). |
  | Metadata and exported keys | Native Thrift RPCs; DBR returns empty for parent-only `GetCrossReference` | SEA SHOW queries; current `getExportedKeys` returns empty | Seven SHOW operations match APC Thrift. APC procedure SELECTs omit headers unsupported by Reyder, unlike current warehouse SEA. `getExportedKeys` stays typed empty on all three routes; no populated-key gate (R5; AC54). |
  | Scalar parameters and `supportManyParameters` | `1` interpolates and permits `setBytes`; `0` rejects non-null `setBytes` | Same setting and binary behavior | Preserve the setting; native scalar binding needs V8, and complex values fail before request (R6; AC6, AC53). |
  | Prepared batches | Native Thrift executor when supported and opted in; otherwise legacy path | Native SEA executor when opted in; otherwise legacy path | Native `parameter_sets` only at V10+ and opt-in; otherwise preserve legacy eligible multi-row INSERT optimization and `BatchInsertSize`; ineligible sets execute individually. Aggregate-only APC native response reports `SUCCESS_NO_INFO` rather than per-set counts (R6). |
  | Arrow prerequisite | APC Thrift works without Arrow | Explicit `UseThriftClient=0` is considered before the warehouse Arrow check | APC requires Arrow before even an explicit SEA override, because Reyder has no HIVE non-metadata result path; AIX/PowerPC users without Arrow remain on Thrift (R1; AC7). |
  | Result heartbeat | Thrift can check statement status | SEA result heartbeat uses `/status` | APC schedules no result heartbeat because Reyder does not support `GetStatementStatus`; long-lived completed ResultSets have no periodic liveness check (R3; AC24). |
  | `UseBoundedSeaApi=0` | Not used by Thrift | Changes the warehouse SEA inline/provider path | APC still uses `ARROW_BATCH` and `next_chunk_index`/`row_offset` paging because Reyder's wire contract requires it; the setting cannot disable APC paging (R4, R12; AC8). |
  | `EnableBatchedInserts` / `BatchInsertSize` | Can combine eligible INSERT sets | Legacy SEA may combine sets | Preserve eligible legacy multi-row INSERTs; `BatchInsertSize` caps interpolated chunks, while bound chunks use the existing parameter limit. Native V10 sends one `parameter_sets` request per batch. No APC-specific fallback penalty (R6; AC17, AC32, AC44). |
  | Complex and geospatial result settings | `EnableComplexDatatypeSupport` and `EnableGeoSpatialSupport` control values, metadata, and typed getters | Same controls; inline complex typed getters reject | Match value/metadata conversion for both settings on inline and linked results. Inline `getArray`/`getMap`/`getStruct` retain the existing unsupported error even when complex support is on; linked typed getters work when enabled. This warehouse/Thrift-inline precedent affects callers requiring typed complex values, who must use linked results (R11; AC13). |
  | `RowsFetchedPerBlock` | Tunes Thrift fetch size | No SEA page-size effect | Follow warehouse SEA; cannot tune Reyder page size (R17; AC31). |
  | Compression settings | Request configured Thrift codec | Send configured SEA codec | APC decodes by response codec, including uncompressed batches; request settings cannot force DBR compression. Server-contract exception (R4; AC45). |
  | Streaming toggles and memory bound | `EnableStreamingChunkProvider` / `EnableInlineStreaming` choose providers; `ThriftMaxBatchesInMemory` bounds inline batches | Bounded SEA can force streaming | APC paging always streams; toggles cannot select legacy providers, but inline batches retain the memory bound (minimum two). Server-contract exception (R12, R17; AC39). |
  | Timeouts, cancel, and errors | Client timeout and Thrift cancel; JDBC errors | `TimeoutHandler`, SEA cancel, SEA error mapping | Follow warehouse SEA timeout, retry, rate-limit, SQLState, and vendor-code behavior; APC target is added to cancel (R14, R17). |
  | Thrift retry settings (`RateLimitRetry`, `TemporarilyUnavailableRetry`, `TemporarilyUnavailableRetryTimeout`, `RateLimitRetryTimeout`, `ApiRetriableHttpCodes`, `ApiRetryTimeout`) | Configure `DatabricksHttpRetryHandler` | Do not configure SDK `ApiClient` | Follow warehouse SEA's SDK policy per request class, including non-idempotent calls; record APC Thrift timing and error differences (R17; AC51). |
  | SDK pool settings | `HttpMaxConnectionsPerRoute` limits Thrift routes; `HttpConnectionPoolSize` does not size its fixed-total pool; `HttpConnectionRequestTimeout` bounds its wait | First two limit SDK route/total connections; request timeout does not set SDK wait | Follow warehouse SEA pool scopes; presigned-link downloads use a separate pool (R17; AC30). |
  | CloudFetch controls | Chunk wait, slow-download warning, download workers and prefetch are configurable | Same streaming controls | Preserve `ChunkReadyTimeoutSeconds`, `CloudFetchSpeedThreshold`, `cloudFetchThreadPoolSize`, and `LinkPrefetchWindow` behavior; no APC-only cap or full-result buffering (R17; AC46). |
  | `EnableSeaSyncMetadata` | No SEA sync header on Thrift | Controls the sync SHOW header | For APC synchronous SHOW, `1` sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` and `0` omits it; async SHOW always omits it. Compare result parity and response latency at both values (R5; AC43). |
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
Those GETs and CloseStatement DELETE have no request body. Connection close in the first public
APC SEA slice sends APC-targeted TerminateSession; a server rejection is reported in diagnostics
and is not labeled successful server cleanup. Presigned
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
`X-Databricks-Metadata-Operation-Type` using the existing case-sensitive wire values
`GetCatalogs`, `GetSchemas`, `GetTables`, `GetColumns`, `GetFunctions`, `GetPrimaryKeys`,
and `GetCrossReference`, respectively. They request `ARROW_STREAM` with
`INLINE_OR_EXTERNAL_LINKS` regardless of `EnableSQLExecHybridResults`, and retain
`X-Databricks-Require-Thrift-Native-Metadata` where applicable. Synchronous SHOW adds
`X-Databricks-Sea-Can-Run-Fully-Sync: true` only with `EnableSeaSyncMetadata=1`; async SHOW
omits it. Reyder returns native-shaped metadata in Arrow IPC `chunk.attachment` or HIVE
`chunk.columnar_data`/`COLUMN_BASED_SET`; decode per response and retain
`manifest.is_native_metadata_result=true` for JDBC conversion.

`DatabaseMetaData.getExportedKeys` is outside those seven operations. The design contract's
Metadata Queries tab says DBR returns an empty result for parent-only `GetCrossReference`;
warehouse SEA also returns typed empty. APC SEA returns the same typed empty result without a
server request. Compare all three routes on a table with a referencing foreign key; issuing the
Thrift request alone is not evidence of populated rows.

The session-reachable async metadata API takes the seven SHOW operations and JDBC inputs and
returns a typed handle with a JDBC ResultSet. At live CreateSession V9+ (42249), it submits
asynchronously, polls within the metadata timeout, and preserves sync conversion, empty,
failure, and cancellation behavior. `DatabaseMetaData.getColumns` consumes that handle on V9+
while retaining its synchronous JDBC contract, so async SHOW has a completed public outcome.
The handle retains statement id, operation, JDBC method
(including both cross-reference conversions), and conversion inputs. Below V9 or without a
live version it completes sync SHOW before returning; retrieval does not poll. Serialized ID
versions grant no capability; existing SQL async behavior stays unchanged.

APC internal catalog/schema SELECTs and parameterized `getProcedures`/`getProcedureColumns`
SELECTs are ordinary `ARROW_BATCH` queries with hybrid disposition and no metadata headers;
convert their JDBC metadata after ordinary query decoding. Current warehouse SEA sends the
`GetProcedures`/`GetProcedureColumns` header values, but Reyder does not accept them for APC.
Procedure SELECTs use R6's scalar binding rule; async SHOW rejects them before request.
Warehouse requests stay unchanged.

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
`toString()` is not lossless SQL. Below V8 or without a live version,
`supportManyParameters=0` sends the original SQL without bindings, so unresolved markers fail
as on APC Thrift; the missing-version telemetry reason remains distinct. Explicit
`supportManyParameters=1` selects interpolation for single executions and legacy batch sets at
every version and permits non-null `setBytes`. With `supportManyParameters=0`, non-null
`setBytes` remains rejected; V8–V9 fallback sets use native scalar `parameters`.
Native V10 uses ordered nested `parameter_sets`, no top-level `parameters`, and
`SUCCESS_NO_INFO` per set on aggregate success; failure claims no successful set. All other
batches use the shared legacy executor: eligible INSERT sets combine into multi-row SQL when
`EnableBatchedInserts=1`; `BatchInsertSize` limits interpolated chunks and the existing
parameter limit caps bound chunks;
ineligible sets execute individually. Preserve ordered counts and stop-on-error.
Pre-execution `PreparedStatement.getMetaData()` uses binding-free
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
coerced or omitted. `EnableComplexDatatypeSupport` and `EnableGeoSpatialSupport` retain their
APC Thrift and warehouse SEA effects on JDBC metadata and `getObject` for inline and linked
results. With complex support off, complex values use the established string form and typed
complex getters reject. With it on, linked results expose typed array/map/struct getters;
inline getters retain the existing unsupported error even with the setting on. Geospatial
support off returns string values and metadata, while on returns native geospatial values
where complex support permits them. This inline typed-getter limitation follows both existing
inline paths and affects callers requiring typed values, who must use linked results.
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
silently switch to Thrift after SEA selection. After T17 and T35 deploy, a scoped staging
assignment may enable flag-on validation; production assignment or ramp waits for T18/T-val
to close the integration gates. Remove each guard when its slice passes live staging, and keep
ordinary flag-off APC connections on Thrift.

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
`EnableNativeBatching=1`; otherwise use the existing legacy executor, including eligible
multi-row INSERTs when `EnableBatchedInserts=1`. `BatchInsertSize` caps interpolated chunks;
the existing parameter limit caps bound chunks. The unsupported
`statements` field applies to Statement SQL batches, which stay sequential and stop at the first
failure. This preserves the origin/main optimization; Phase II's per-set fallback describes
native parameter batches, not a prohibition on ordinary multi-row SQL.
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

Each JDBC package proves its completed public call on scoped flag-ON APC staging after T17/T35
deploy, and compares the same operation with APC Thrift and the relevant warehouse SEA behavior.
Explicit `UseThriftClient=0` is an additional override check, not a substitute for flag-ON proof.
Incomplete calls remain guarded. T18 repeats the gates after all packages merge. A missing live or
server-dependent check remains pending;
fixtures alone cannot pass it. The development plan owns test procedures and evidence capture.
- AC1: GIVEN a concrete APC HTTP path and no explicit route override WHEN the APC flag is
OFF, missing, lookup-failed, or partner-forced false THEN public JDBC uses Thrift independently
of the warehouse flag; an ON flag with Arrow available selects SEA. Legacy `cliservice` stays
on Thrift. Verify both routes on staging.
- AC2: GIVEN scoped flag-ON APC staging and a cluster HTTP path WHEN a public connection opens
THEN CreateSession contains `backend_compute.all_purpose_cluster_compute.cluster_id` equal to
the URL's cluster id and no `warehouse_id`; compare the resulting connection with APC Thrift and
the unchanged warehouse SEA CreateSession shape.
- AC3: GIVEN live APC SEA execution, fetch, cancel, close, and termination WHEN requests are
captured THEN each uses R2's body or query target exactly once, with bodyless GET/DELETE calls
and no `/status`; warehouse wire fixtures remain unchanged. Termination waits for its server fix.
- AC4: GIVEN live APC inline and linked results WHEN public JDBC drains multi-batch and
large results THEN primitive rows, metadata, and errors match APC Thrift and warehouse SEA precedent;
R4's request shape holds with either hybrid-results setting.
- AC5: GIVEN live APC synchronous SHOW and procedure metadata WHEN Arrow IPC or
`columnar_data` arrives THEN R5's native conversion, rows, errors, timeout, and null-catalog
behavior match APC Thrift; warehouse requests stay unchanged.
- AC6: GIVEN live APC V8+ and older or missing capability versions WHEN scalar prepared
queries run THEN R6's native/interpolated values and request shape match APC Thrift and
warehouse SEA. Before execution, `getMetaData()` returns JDBC metadata without losing bindings;
unavailable live versions remain pending.
- AC7: GIVEN `UseThriftClient=0` and the APC flag ON on AIX/PowerPC with Arrow unavailable WHEN a
connection is opened THEN the Arrow prerequisite takes precedence, the pre-connection routing
decision selects Thrift, and the driver logs the routing reason.
- AC8: GIVEN live APC SEA with `EnableQueryResultDownload=0` WHEN a query runs THEN
CreateSession disables cloud download, all pages stay inline, and inline-size errors propagate
without partial success. `UseBoundedSeaApi` does not change this; live proof awaits the server fix.
- AC9: GIVEN live APC SEA CreateSession rejection WHEN connecting THEN the error and failed
route event propagate without Thrift fallback. GIVEN APC Thrift OpenSession returns KP001 WHEN
connecting THEN warehouse-only recovery does not select SEA.
- AC10: GIVEN successful or failed APC/warehouse SEA/Thrift connections WHEN telemetry and
DEBUG logs are captured on live staging THEN each attempt identifies compute, protocol, reason,
and outcome without exposing credentials or APC identifiers. The first DDL slice redacts all
reachable sync/async diagnostics; T15 completes route outcomes across failure paths.
- AC11: GIVEN standard and Real-time SQL warehouse connections on live staging WHEN
CreateSession, execute, poll, fetch, cancel, close, and TerminateSession run THEN public JDBC
outcomes and captured request bodies, URLs, and headers match a pre-change `origin/main` run,
including warehouse `realtimeSession` behavior; fixtures protect exact serialization.
- AC12: GIVEN live APC SEA synchronous direct-result, no-direct-result, and asynchronous
executions WHEN requests are captured THEN `wait_timeout` is respectively absent, `0s`, and
`0s`, with no `statements` field; public outcomes match APC Thrift and warehouse SEA precedent.
- AC13: GIVEN available live APC primitive, decimal, array, map, struct, interval,
geography, geometry, timestamp, timestamp_ntz, and variant results WHEN public JDBC reads
inline and linked results with `EnableComplexDatatypeSupport` and `EnableGeoSpatialSupport`
each at 0 and 1 THEN metadata, `getObject`, and typed getter values/errors follow R11 and
match APC Thrift and warehouse SEA for the same disposition and settings. Specifically,
inline `getArray`/`getMap`/`getStruct` reject even when enabled while linked typed getters
work; disabled settings produce the established string fallbacks. Compare optional manifest
fields with fixtures; unavailable live types remain pending. No coercion is silent.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC15: GIVEN live APC query responses with manifest-only or inline-data starts WHEN
public JDBC reads them THEN complete rows and values match APC Thrift, following warehouse SEA
request precedent. A shape unavailable live remains pending with a fixture.
- AC16: GIVEN live flag-ON APC `getCatalogs`, `getSchemas`, `getTables`, and `getColumns`
metadata WHEN Arrow IPC or `columnar_data` physical vectors and least-significant-bit null
masks are decoded THEN JDBC rows, metadata, and errors match APC Thrift and warehouse SEA;
short/long masks retain fixture coverage. A following Arrow IPC response on the same
connection also decodes correctly.
- AC17: GIVEN live APC V10 with native batching enabled WHEN public prepared INSERT batches
succeed or fail THEN one ordered `parameter_sets` request and R6's counts/errors match the
contract. With native mode unavailable or off, eligible legacy INSERT sets combine when
`EnableBatchedInserts=1` and ineligible sets execute individually, with ordered counts and
stop-on-error. Compare APC Thrift and warehouse SEA; unavailable live V10 remains pending.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. On flag-on staging, compare elapsed time, SQLState/vendor code,
cancel target, and post-timeout connection usability with APC Thrift and warehouse SEA.
- AC19: GIVEN Arrow available and a cluster HTTP path on scoped flag-ON APC staging WHEN
`UseThriftClient=1` opens a connection THEN it uses Thrift with APC Thrift behavior; with
`UseThriftClient=0` and the flag OFF it uses SEA and matches the flag-ON APC outcome. The
client setting takes precedence after the Arrow prerequisite.
- AC20: GIVEN a warehouse HTTP path on live staging WHEN the APC flag is ON or OFF THEN
public warehouse JDBC calls and captured routing match the `origin/main` warehouse SEA or
Thrift baseline selected by `enableSqlExecForJdbc`.
- AC21: GIVEN live APC routing with `TreatMetadataCatalogNameAsPattern=1` and the flag ON
WHEN no `UseThriftClient` is explicit THEN Thrift retains catalog-pattern semantics. With
explicit `UseThriftClient=0`, SEA wins and logs that the Thrift-only setting has no effect.
- AC22: GIVEN a live three-command Statement batch WHEN command two succeeds or fails
THEN APC SEA sends sequential requests without `statements`, preserves ordered counts, and
stops before command three on failure; compare APC Thrift counts/errors and warehouse SEA wire shape.
- AC23: GIVEN a live APC CreateSession without a capability version WHEN a public prepared
query or batch runs THEN an encoded ID grants no capability; R6's original-SQL failure for
unresolved markers when `supportManyParameters=0`, interpolation when it is 1, and
missing-version telemetry hold. Compare values and errors with APC Thrift and warehouse SEA.
- AC24: GIVEN a live APC SEA ResultSet with `EnableHeartbeat=1` WHEN it stays open past
two heartbeat intervals THEN no result heartbeat or `/status` request occurs; execution
polling still works. Compare request traffic with APC Thrift and warehouse SEA.
- AC25: GIVEN APC session and statement IDs WHEN serialized, parsed, or malformed THEN R9's
grammar, public IDs, and warehouse/Thrift compatibility hold without identifier disclosure;
an encoded version grants no capability.
- AC26: GIVEN APC query/metadata and warehouse SEA result formats WHEN wire values are
decoded on staging THEN R16's JDBC results and forward-compatible unknown-format error hold;
APC native metadata uses its emitted physical format.
- AC27: GIVEN a live APC query with three or more RUNNING polls WHEN transfer is measured
on explicit-override and flag-on routes THEN polls carry no result payload and final fetch
delivers every row once; compare APC Thrift and warehouse SEA traffic.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` and its default is OFF.
Configuration evidence records the exact name, parent batch flag, and default value.
- AC29: GIVEN live APC complex scalar or batch bindings at available capability versions
WHEN execution starts THEN the same tested driver-owned error name/code occurs before request,
including `Types.ARRAY` and `Types.STRUCT`, while the connection stays usable. Compare
APC Thrift and warehouse SEA; unavailable versions retain fixtures.
- AC30: GIVEN low and high SDK route and total pool limits WHEN comparable live APC and
warehouse SEA requests saturate them THEN concurrency and wait/error outcomes follow R17;
`HttpConnectionRequestTimeout` retains warehouse SEA scope. Record APC Thrift differences.
- AC31: GIVEN the same warm APC cluster, client host, query, row set, result disposition,
concurrency, and applicable tuning settings for live APC SEA and APC Thrift, plus a comparable
warehouse SEA precedent run WHEN complete inline and linked results are read in at least three
paired runs per condition THEN rows match and median throughput/latency, peak heap, transfer
bytes, requests, retries, and errors are recorded. Compare APC SEA's median throughput and
latency and peak heap to APC Thrift on the same APC workload; a repeatable throughput loss or
latency increase over 10%, peak heap increase over 20%, incomplete rows, an APC-only cap, or
heap growth with total result size blocks release. Only a documented server limitation with
the corresponding warehouse SEA precedent, client impact, and PECO/Perf Eng approval may
waive a threshold. `RowsFetchedPerBlock` follows warehouse SEA's no-effect precedent;
inline-only proof awaits server support.
- AC32: GIVEN the same warm APC cluster, client host, INSERT SQL, parameter sets, batch size,
`EnableBatchedInserts` value, and applicable native mode for live APC SEA and APC Thrift,
plus a comparable warehouse SEA precedent run WHEN `executeBatch` completes in at least three
paired runs per condition THEN R6's counts, errors, request count, median sets per second,
median latency, and capability version are recorded. Compare APC SEA to APC Thrift for the
same fallback or native mode; a repeatable over-10% throughput loss or latency increase
blocks release without a documented server limitation, warehouse SEA precedent, client
impact, and PECO/Perf Eng approval. Missing live V10 remains pending.
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
- AC37: GIVEN T32 and the TerminateSession binding fix deployed WHEN a successful APC no-result
DDL is closed or reused after polling on scoped flag-ON staging THEN no result fetch or
`/status` call occurs, repeated statement and connection close are safe, one APC-targeted
TerminateSession succeeds on the server, and an unclosed connection remains usable; compare
APC Thrift and warehouse SEA. Failed DDL retains its error. Without the binding fix, the
first public slice remains gated pending live cleanup proof.
- AC38: GIVEN the integrated stack WHEN a public primitive query and retryable fetch
failure run through explicit override and deployed flag on staging THEN rows, order, SQLState,
and vendor code match APC Thrift with warehouse SEA error precedent. Server-dependent cases
remain pending until deployed.
- AC39: GIVEN live APC inline pages and external links WHEN streaming toggles and
`ThriftMaxBatchesInMemory` vary THEN rows stay complete and R17's inline memory bound holds;
compare APC Thrift and warehouse SEA. Inline-only server proof may remain pending.
- AC40: GIVEN any merged slice WHEN the APC flag is OFF without override THEN public
APC JDBC stays on Thrift. On scoped flag-ON staging, completed public calls pass their live
APC Thrift and warehouse SEA parity gates; explicit SEA override is checked separately.
Incomplete calls fail before false success. Production assignment and ramp wait for T18/T-val.
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
- AC44: GIVEN identical live prepared INSERT batches with `EnableBatchedInserts=1` WHEN
`BatchInsertSize` changes between small and large values THEN eligible interpolated APC legacy
fallback changes multi-row request count as the shared executor does; bound fallback remains
limited by the existing parameter cap, while V10 native sends one `parameter_sets` request per
batch. Counts and errors remain correct. Compare request count,
sets per second, and latency with APC Thrift and warehouse SEA under the same settings;
unavailable V10 remains pending.
- AC45: GIVEN live APC queries under varied compression settings WHEN LZ4-marked and
unmarked responses arrive THEN all values decode by response codec; compare bytes, CPU, and
latency with APC Thrift and warehouse SEA. If one codec is unavailable live, retain its fixture
gate; the first query slice proves both decoders under AC47.
- AC46: GIVEN live APC linked and inline results under low/high CloudFetch controls (R17)
WHEN contention or slow delivery occurs THEN rows remain complete, worker/prefetch limits,
chunk waits, and warnings respond as configured. Compare concurrency, errors, and usability
with APC Thrift and warehouse SEA; unexplained no-op controls or AC31 regressions block release.
- AC47: GIVEN a primitive single-chunk APC inline `ARROW_BATCH` result on scoped flag-ON staging
WHEN public JDBC consumes it THEN rows, order, JDBC types, and errors match APC Thrift, with
warehouse SEA as request and error precedent. A fixture without `manifest.arrow_schema` proves
decoding does not depend on that field; the currently emitted populated live shape is sufficient.
Both LZ4-marked and unmarked inline batches decode, using a fixture for a codec unavailable live.
Continued or linked shapes remain guarded until their slices pass.
- AC48: GIVEN live APC counted `INSERT`, `UPDATE`, `DELETE`, or `MERGE` WHEN public
update methods run THEN counts, including zero, match APC Thrift after T32. A missing
`num_affected_rows` column, row, or value produces a contract error, not successful zero.
- AC49: GIVEN live APC queries with `setMaxRows(0)`, `1`, and a boundary-crossing limit
WHEN inline or linked results are read THEN positive limits send SEA `row_limit`, zero is
unlimited, and rows never exceed the limit; compare rows and request count with APC Thrift and
warehouse SEA. A boundary unavailable live remains pending with a contract fixture.
- AC50: GIVEN live V9+ APC on scoped flag-ON staging WHEN public
`DatabaseMetaData.getColumns` runs THEN it completes through async SHOW and returns rows,
metadata, errors, and timeout/cancel behavior matching synchronous SHOW, APC Thrift, and
warehouse SEA. Typed handles for all seven SHOW operations preserve null-catalog fallback;
below V9 or without a version `getColumns` uses completed synchronous handles, and
procedures reject async submission. Unavailable V9 live remains pending.
- AC51: GIVEN comparable live APC and warehouse SEA request classes under safe retryable
faults WHEN SDK retries succeed or exhaust THEN attempts, elapsed time, SQLState, and vendor
code match warehouse SEA, without replaying writes beyond SDK policy. R17's Thrift retry
settings do not change APC SEA policy; record their APC Thrift scope and client impact.
- AC52: GIVEN Reyder's APC `GetStatementResult` route after RUNNING polls WHEN no-result DDL
or counted DML completes THEN direct server contract evidence returns, respectively, success
with absent or zero-column manifest and no data, or a schema and affected-rows data including
zero. Failed statements retain their errors. AC33, AC37, and AC48 own public JDBC proof.
- AC53: GIVEN live APC prepared scalars and fallback batches across available versions
WHEN `supportManyParameters` is 0 or 1 THEN R6's bindings, original-SQL failure below V8
with setting 0, interpolation with setting 1, and `setBytes` behavior match APC Thrift and
warehouse SEA. Missing live versions remain pending.
- AC54: GIVEN a live APC table with a referencing foreign key WHEN public
`getExportedKeys` runs on scoped flag-ON APC SEA, APC Thrift, and warehouse SEA THEN each
returns a typed empty result, APC SEA issues no exported-key query, and Thrift's parent-only
`GetCrossReference` is observed. If DBR later returns rows on Thrift, record that contract
change and keep APC SEA parity unresolved rather than claiming this gate passed.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The first public DDL JDBC package owns `docs/` and
  `NEXT_CHANGELOG.md` and records the user-visible gated feature there; later packages must
  not claim `NO_CHANGELOG=true` merely because the flag defaults off. Every completed slice
  is shippable behind that flag; no partial stack is ramped.
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

Use reviewable JDBC vertical slices (see `plan.json`): no-result DDL with server session
termination and the user-visible changelog, inline SELECT/continuation and counted DML,
linked results and logical types, result controls, cancel/timeout, saved-session recovery,
scalar prepared execution, batches, two synchronous metadata groups, asynchronous metadata,
and integration. Linked results and lifecycle branch after inline continuation; batches and
metadata branch after scalar execution. Each slice exposes its completed public behavior through
R1's guarded route and proves it on scoped flag-ON staging against APC Thrift and warehouse SEA.
The integration slice joins branches and repeats the gates. Keep each PR below roughly 1,500
changed lines; the number of packages reflects the independent public behavior and review size.

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
  fixtures protect those shapes before deployment. The first public DDL slice cannot pass its
  live cleanup gate until the TerminateSession binding fix is deployed.
- AC13/Q7 block release on lost logical types; AC27 blocks repeated poll payload transfer.
- AC11/AC14 protect warehouse request and presigned-link boundaries.

---

## Tasks

Each task appears in one `plan.json` package. Package gates exercise completed behavior through
public JDBC methods using R1's guarded route; T18 repeats the checks after integration.
External gates are pending until proven.

### Phase 1 — Guarded DDL core

- [ ] T1: Add APC compute and transient session/statement identity sufficient for DDL targeting, plus no-result SEA support, without changing warehouse wire values (R2, R16). T19 completes ID parsing and persistence.
- [ ] T2: Target APC CreateSession, ExecuteStatement, result polling, ordinary statement close, and TerminateSession for the first public DDL connection; later slices add result-chunk GET and cancel targeting (R2).
- [ ] T3: Redact reachable sync/async APC diagnostics before the first public call and emit first-path route/statement telemetry (R8; AC10).
- [ ] T6: Preserve selected APC protocol on connection errors and KP001 Thrift failure (R7).
- [ ] T27: Complete APC-targeted TerminateSession and repeated-close DDL lifecycle in the first public slice; T12 owns cancel and timeout (R2, R14).
- [ ] T28: Return zero count and no ResultSet for successful no-result DDL, including a zero-column manifest; preserve errors (R4).
- [ ] T31: Expose completed public DDL through R1's default-off APC route and guard incomplete calls against false success in every partial slice (R18).
- [ ] T16: Add the user-visible changelog entry in this first public slice and document the gated DDL scope, routing, and known limitations; this is the sole JDBC docs/changelog owner.

### JDBC slices after the core

- [ ] T7: Suppress APC result heartbeats while retaining execution polling (R3).
- [ ] T8: Complete public primitive single-chunk query values, errors, and cleanup; defer DML counts to T11's next slice (R4, R8, R11).
- [ ] T10: Decode primitive bare Arrow batches in both LZ4-marked and unmarked single-chunk inline responses (R4, R11; AC47).
- [ ] T11: Fetch every inline continuation, including manifest-only starts, by `next_chunk_index` and `row_offset`; complete counted DML in this slice. T33 covers linked continuation (R4, R12).
- [ ] T25: Target result-chunk GETs once, with null body and unchanged presigned URLs/headers (R2).
- [ ] T33: Stream complete external-link Arrow IPC results, without treating them as bare batches (R4, R11).
- [ ] T20: Complete the AC13 logical-type matrix without silent coercion (R11, Q7).
- [ ] T34: Preserve `Statement.setMaxRows` across APC SEA inline and linked results (R4).
- [ ] T36: Preserve linked-result CloudFetch controls, chunk waits, and performance parity through public JDBC (R17; AC31, AC39, AC46).
- [ ] T12: Apply warehouse SEA timeout behavior with targeted APC cancellation (R14).
- [ ] T15: Complete lifecycle route outcomes and asynchronous diagnostic coverage on top of T3's first-slice redaction (R8).
- [ ] T19: Round-trip APC session and internal statement IDs, retaining raw public statement IDs (R9).
- [ ] T29: Use the current target for recovered statements and saved protocol/target for saved-session close (R9).
- [ ] T26: Prove per-request-class SDK retry scope and both pool limits through live APC and warehouse SEA traffic in integration; keep non-idempotent fault injection safe (R17; AC30, AC51).
- [ ] T5: Keep live CreateSession capability version separate from serialized ID diagnostics (R6, R9).
- [ ] T9: Gate native scalars at V8, preserve `supportManyParameters` and `setBytes` semantics, and support binding-free pre-execution metadata (R6; AC53).
- [ ] T22: Reject complex bindings at every APC capability level before requests, with a tested error name/code and taxonomy (R6).
- [ ] T21: Support opt-in V10 prepared batches and the shared legacy fallback, including eligible multi-row INSERTs (R6).
- [ ] T30: Execute Statement SQL batches sequentially with ordered counts and stop-on-error (R15).
- [ ] T14: Complete public catalog/schema/table/column metadata with emitted Arrow IPC or columnar native responses, the manifest native flag, null-catalog fallback, and AC16 value/null parity (R5, R13).
- [ ] T13: Complete public function/primary-key/cross-reference SHOW and procedure SELECT metadata with R5's request contract (R5).
- [ ] T37: Preserve typed-empty exported keys on APC SEA and compare public APC Thrift and warehouse SEA empty results on a referencing-key table (R5; AC54).
- [ ] T23: Expose seven-operation async SHOW with the live V9 gate and completed handles below V9; route public `DatabaseMetaData.getColumns` through the handle on V9+ (R5).
- [ ] T24: Preserve async null-catalog fan-out, errors, timeout, and cancellation (R5).

### Independent Universe prerequisites

- [ ] T17: Define the default-off APC SAFE sub-flag without assigning or ramping it (R1, Q8).
- [ ] T32: Prove APC `GetStatementResult` returns successful no-result DDL without Arrow data and counted DML with `num_affected_rows` schema and data after polling; fix either server path if needed (R4, AC52).
- [ ] T35: In connector-service, override the APC sub-flag to false for every partner User-Agent before flag assignment, without changing the warehouse flag's partner allowlist (Q9, AC34).

### Integration and release

- [ ] T4: Integrate the independently merged JDBC branches and confirm Arrow-first routing, explicit override, catalog-pattern precedence, and the dedicated flag (R1, R7, R18).
- [ ] T18: Consolidate local and live evidence, use a scoped staging flag assignment after T17/T35 deploy, repeat flag-on gates after T32 deploy, and close cross-route checks (AC1–AC54).
- [ ] T-val: Record pass/fail or externally pending evidence for AC1–AC54 and exact commands; do not declare release ready with Q7, Q9, or server/performance gates open.
