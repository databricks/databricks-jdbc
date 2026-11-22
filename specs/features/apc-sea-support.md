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
  The TerminateSession REST binding declares `body: "*"` (R2).
- Reyder's current APC `GetStatementResult` path fetches missing metadata after a successful
  poll, then requires an Arrow schema to build a manifest. No-result DDL and counted DML
  therefore need T32's server proof before live JDBC gates pass (R4, AC52). Live result
  manifests currently populate `arrow_schema`; an absent-field decode has a fixture gate.
- The earlier always-LZ4 APC assumption is narrower in the current server contract: Reyder
  marks LZ4 only when DBR reports compression; AC45 covers uncompressed responses.
- Client-config flags come from connector-service
(`/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`), which returns sub-flags of
`databricks.partnerplatform.clientConfigsFeatureFlags`
(`connector-service/src/featureflags/FeatureFlagsHandler.scala`). It version-maps
`enableSqlExecForJdbc` to `enableSqlExecForJdbc_v3.4.3` for ≥ 3.4.3. For partner User-Agents,
that existing flag may remain true for an allowlisted JDBC partner; other sub-flags currently
pass through unchanged. Q9 requires a separate blanket APC override.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata tabs). R1–R18 are the authoritative driver contract;
the matrix and acceptance criteria summarize parity and observable outcomes. The
server also limits SQL text to 16 MB, rejects non-OSS-JDBC clients, and starts rollout opt-in
before defaulting APC to Reyder through the dedicated flag.
- Driver baseline verified against `origin/main` @ `840f3a407` (merged spec #1809;
  `UseBoundedSeaApi` default #1827): APC routing precedes flags and selects Thrift, while
  warehouse routing honors overrides. `DatabricksSdkClient` assumes `Warehouse` for session
  and statement requests, including the separate public SQL `executeAsync` path; polling uses
  `GET /statements/{id}` and heartbeats use `/status`.
  `DatabricksConnection.close()` calls `DatabricksSession.close()`, whose SDK delete path casts
  the compute target to `Warehouse`; session close suppresses that failure, so APC cleanup must
  be fixed before the first public slice. `getConnectionId()` calls `SessionId.create()`, which
  likewise treats every APC session as Thrift and dereferences its absent Thrift handle; the
  first public slice must fix the public ID round trip (R9, AC25).
  SEA lacks APC `ARROW_BATCH` decoding, async SHOW, and `columnar_data` modeling; the SDK
  transport differs from `DatabricksHttpClient` (R17). Public `getExecutionResult` retrieves
  the separate SQL async execution; it is outside the metadata SHOW async path.
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the 41-file prototype
baseline, not the design; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756)
is unchanged. The prototype enables APC SEA by default and emits `JDBC_APC_ROUTE` markers.
- Parity baseline for review and release ("warehouse" means SQL warehouse using the same driver
  build). A difference is allowed only where the server contract above or existing warehouse
  Thrift-versus-SEA behavior explains it; the listed exception must remain visible to users.

  | Behavior | APC Thrift today | Warehouse SEA today | APC SEA target and disposition |
  | --- | --- | --- | --- |
  | Routing and failure | Default Thrift | Flag/override; SEA errors propagate | Dedicated APC flag/override; follow warehouse no-fallback precedent (R1, R7). |
  | Result values and paging | Arrow/CloudFetch or inline; complete JDBC rows | Arrow stream/CloudFetch or bounded inline | Match JDBC values and completeness with APC's batch/link formats and offset paging (R4, R11, R12). |
  | `Statement.setMaxRows` | Limits rows | Sends `row_limit` | Match across pages/links; zero is unlimited (AC49). |
  | Result disposition controls | Thrift inline or CloudFetch | `EnableSQLExecHybridResults=0` requests external-only; disabled download may select Thrift | Reyder's APC `ARROW_BATCH` contract requires `INLINE_OR_EXTERNAL_LINKS`, so the hybrid toggle cannot force external-only: callers may receive inline batches and cannot use that setting to require links. Disabled download keeps SEA and requests inline-only delivery through CreateSession `session_confs["can_cloud_download"]="false"`; oversized inline results fail instead of moving to Thrift. Bound delivery and resource use at both settings (R4; AC4, AC8, AC56). |
  | Metadata and exported keys | Native Thrift RPCs; parent-only `GetCrossReference` is empty | SEA SHOW; `getExportedKeys` is empty | Match seven SHOW values; omit unsupported APC procedure headers. Exported keys stay typed empty on all routes, so callers cannot inspect referenced keys through this method (R5; AC54). |
  | Scalar parameters and `supportManyParameters` | Interpolation and `setBytes` depend on setting | Same | Match setting; native binding needs V8 (R6; AC6). |
  | Complex parameter bindings | `setObject` accepts ARRAY/STRUCT and serializes values | Accepts and serializes the same types | Reyder's APC parameter contract rejects complex logical types; APC SEA rejects before request rather than misquoting `toString()`. Callers must use scalar bindings or APC Thrift; this is a documented parity exception (R6; AC29). |
  | Prepared batches | Native when opted in; otherwise legacy executor | Same | Preserve legacy multi-row INSERT optimization; V10 native aggregate response yields `SUCCESS_NO_INFO`, so callers lose per-set counts (R6). |
  | Arrow prerequisite | Works without Arrow | Explicit SEA override precedes Arrow check | APC checks Arrow first: Reyder has no HIVE query result path, so AIX/PowerPC without Arrow stays on Thrift (R1; AC7). |
  | Result heartbeat | Checks status | Uses `/status` | Unsupported by Reyder; long-lived APC ResultSets lose periodic liveness checks (R3; AC24). |
  | `UseBoundedSeaApi=0` | No effect | Changes warehouse inline provider | Reyder's APC paging still applies; setting cannot disable it (R4, R12; AC8). |
  | Complex and geospatial result settings | Settings control values, metadata, and getters | Same; inline typed complex getters reject | Match values and metadata; inline typed complex getters retain the warehouse/Thrift-inline rejection, so callers needing them use linked results (R11; AC13). |
  | `RowsFetchedPerBlock` and compression | Thrift fetch size and codec controls | No SEA page-size effect; SEA codec request | Follow warehouse SEA paging; decode by response codec, which the setting cannot force on DBR (R4, R17; AC31, AC45). |
  | Streaming toggles and memory bound | `EnableStreamingChunkProvider` / `EnableInlineStreaming` choose providers; `ThriftMaxBatchesInMemory` bounds inline batches | Bounded SEA can force streaming | Reyder's `next_chunk_index`/`row_offset` APC paging requires a streaming provider, so the toggles cannot select legacy providers; callers lose provider choice but retain bounded inline memory (minimum two batches). Verify equal rows and bounded heap at both toggle values (R12, R17; AC39, AC56). |
  | Timeouts, cancel, and errors | Thrift path | SEA timeout/cancel/error mapping | Follow warehouse SEA with APC cancel target. A Reyder request timeout or extra `GetStatementResult` latency may end a valid long-running query or metadata call earlier than APC Thrift; measure completion, elapsed time, SQLState/vendor code, and client impact, then block or document an approved exception (R10, R14; AC60, AC63). |
  | HTTP 429/503 and `Retry-After` | `DatabricksHttpRetryHandler` honors `Retry-After`, `RateLimitRetry`, `RateLimitRetryTimeout`, and temporary-unavailable retry settings for eligible Thrift HTTP methods | SDK `ApiClient` uses its own retry policy; exhausted CreateSession 429 becomes `DatabricksRateLimitException` | Follow warehouse SEA's SDK policy and exception mapping for APC CreateSession, ExecuteStatement, result GET, cancel, close, and TerminateSession. APC Thrift retry properties do not tune SDK attempts; stopped-cluster 503 and exhausted 429 may change wait time and final exception/SQLState/vendor code. Capture the exact difference and client impact (R17; AC51, AC62). |
  | SDK pool settings | Route cap and wait timeout apply; pool-size setting does not size fixed total | SDK route and total caps apply; wait setting does not | Follow warehouse SEA scopes; links use a separate pool (R17; AC30). |
  | CloudFetch controls | Download workers, prefetch, chunk wait, and speed warning | Same scopes; chunk wait also applies inline | Preserve warehouse SEA scopes and bounds for APC; link-only controls do not affect inline pages (R17; AC46). |
  | `EnableSeaResponseCompression` | No SEA request | Controls SDK HTTP response compression | Same for APC SEA API responses; it does not select DBR result LZ4 or compress presigned-link downloads (R17; AC45). |
  | `EnableSeaSyncMetadata` | No SEA header | Controls sync SHOW header | Same on APC sync SHOW; async omits it (R5; AC43). |
  | Concurrent metadata and null-catalog fan-out | Thrift metadata RPCs | SEA SHOW and catalog enumeration | Bound request amplification, median and tail latency, Reyder CPU and memory at expected load; a documented exception needs client impact and approval (R5, R17; AC61). |
  | Telemetry and resources | Thrift telemetry | SEA telemetry/streaming | Add compute/routing outcome, redact APC IDs, measure transfer/heap (R8, R17). |

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

- R3: For APC over SEA, execution polling uses `GetStatementResult`. Result heartbeats are
disabled for APC even when `EnableHeartbeat=1`, because `GetStatementStatus` is unsupported and
using full `GetStatementResult` after completion can re-download result payloads. The driver logs
that heartbeat is unsupported and never schedules `checkStatementAlive` or calls `/status`.
- R4: APC non-metadata execution sends `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`,
and a `session_id`, never sends `statements`, and handles bare Arrow batches
in `chunk.arrow_batches` (including multiple batches per chunk). External links are complete
Arrow IPC streams, not bare batches; preserve framing and headers and drain every link.
`EnableQueryResultDownload=0` keeps SEA selected and sets CreateSession
`session_confs["can_cloud_download"]="false"` (a string-valued map entry, not a top-level
field); after the tracked server fix, results remain inline despite the
hybrid disposition. An inline-size error propagates without a successful partial ResultSet.
`EnableSQLExecHybridResults=0` cannot request external-only APC results because this APC
format has only hybrid disposition; callers may observe inline data instead of links. Result
completeness, request count, and heap at either value are bounded by AC56.
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
filtering/empty semantics. Match the current synchronous fan-out: warn and omit a catalog
on *any* per-catalog `SQLException`, including a timeout, cancellation, or null SQLState;
propagate catalog-enumeration and initial SHOW errors. A timeout or cancellation of the
overall metadata operation still fails the call. This partial-row behavior is observable
on both routes; record skipped catalogs and preserve a usable connection (AC57, AC58).

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
- R7: R1 selects the protocol before connecting; `UseBoundedSeaApi` does not affect that
decision. After SEA is selected, server errors, including unsupported regions, propagate
without Thrift fallback. Warehouse-only KP001 Thrift-to-SEA recovery does not change APC routing.
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
APC-specific timeout override is needed. Public SQL `executeAsync` and `getExecutionResult`
must use the APC request, target, polling, and decoding path, separately from async metadata.
The server request timeout for long-running APC execution and metadata must be measured
separately from JDBC query and metadata wait timeouts, including the added
`GetStatementResult` round trip (AC60, AC63).
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
- R14: APC query timeouts use the warehouse `TimeoutHandler` and targeted CancelStatement path.
A valid query or metadata call within its configured client wait must not fail early from the
Reyder/SEA request timeout (AC60, AC63).
- R15: `Statement.addBatch(String)` / `executeBatch` on APC executes SQL commands sequentially,
one ExecuteStatement request per SQL string, preserving the existing stop-on-first-failure and
update-count behavior. It never sends the unsupported `statements` field.
- R16: SEA result formats include `ARROW_BATCH` for APC queries and `COLUMN_BASED_SET`
(wire value 6) for APC metadata while preserving existing warehouse wire values. Unknown future
response formats deserialize safely and fail clearly if decoded; requested `ARROW_STREAM` must
not prevent decoding a `COLUMN_BASED_SET` metadata response.
- R17: APC SEA follows warehouse SEA's SDK retry, pool, and separate streaming CloudFetch
download behavior. Preserve the matrix's property scopes; Thrift retry settings and
`RowsFetchedPerBlock` do not tune SEA. Match SDK policy and warehouse SEA exception mapping
for 429/503 and `Retry-After` by request class without adding write retries. No APC-only
concurrency cap or full-result buffer is allowed. Record APC Thrift differences and caller
impact under AC30–AC32, AC45–AC46, AC51, and AC61–AC62.
- R18: Every merged JDBC slice exposes its completed public behavior when R1's default-off
APC flag is enabled or `UseThriftClient=0` is explicit. Incomplete public calls, including
`executeAsync`/`getExecutionResult` before T39, fail before network execution when known;
unsupported response shapes fail before false success. They never silently switch to Thrift
after SEA selection. After T17 and T35 deploy, a scoped staging
assignment may enable flag-on validation; production assignment or ramp waits for T18/T-val
to close the integration gates. Remove each guard when its slice passes live staging, and keep
ordinary flag-off APC connections on Thrift.

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
`BatchInsertSize` and the parameter limit retain their scopes. Only opt-in V10 uses native
`parameter_sets`. Statement SQL batches stay sequential without `statements` (R6, R15).
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

Each JDBC package proves its completed public call on scoped flag-ON APC staging after T17/T35
deploy, against APC Thrift and relevant warehouse SEA behavior. Explicit `UseThriftClient=0`
is checked separately. An unavailable live or server-dependent shape remains pending with
fixture coverage; T18 repeats the gates after integration.
- AC1: GIVEN a concrete APC HTTP path and no explicit route override WHEN the APC flag is
OFF, missing, lookup-failed, or partner-forced false THEN public JDBC uses Thrift independently
of the warehouse flag; an ON flag with Arrow available selects SEA. Legacy `cliservice` stays
on Thrift. Verify both routes on staging.
- AC2: GIVEN scoped flag-ON APC staging and a cluster HTTP path WHEN a public connection opens
THEN CreateSession contains `backend_compute.all_purpose_cluster_compute.cluster_id` equal to
the URL's cluster id and no `warehouse_id`; compare the resulting connection with APC Thrift and
the unchanged warehouse SEA CreateSession shape.
- AC3: GIVEN live APC SEA execution, fetch, cancel, close, and termination WHEN requests are
captured THEN each uses R2's body or query target exactly once. Statement/result GET and
CloseStatement DELETE have no body; TerminateSession DELETE carries its APC target in a JSON
body. No APC `/status` call occurs; warehouse wire fixtures remain unchanged. Termination waits
for its server fix. Each slice checks the request classes it first exposes.
- AC4: GIVEN live APC linked results under either hybrid-results setting WHEN public JDBC
drains them THEN primitive rows, metadata, errors, and R4's request shape match the baselines.
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
CreateSession sends `session_confs["can_cloud_download"]="false"`, all pages stay inline, and
inline-size errors propagate without partial success. `UseBoundedSeaApi` does not change this;
live proof awaits the server fix.
- AC9: GIVEN live APC SEA CreateSession rejection WHEN connecting THEN the error and failed
route event propagate without Thrift fallback. GIVEN APC Thrift OpenSession returns KP001 WHEN
connecting THEN warehouse-only recovery does not select SEA.
- AC10: GIVEN successful or failed APC/warehouse SEA/Thrift connections WHEN telemetry and
DEBUG logs are captured on live staging THEN each attempt identifies compute, protocol, reason,
and outcome without exposing credentials or APC identifiers. T3 covers the first DDL path;
T15 covers later lifecycle paths.
- AC11: GIVEN standard and Real-time SQL warehouse connections on live staging WHEN
CreateSession, execute, poll, fetch, cancel, close, and TerminateSession run THEN public JDBC
outcomes and captured request bodies, URLs, and headers match a pre-change `origin/main` run,
including warehouse `realtimeSession` behavior; fixtures protect exact serialization.
- AC12: GIVEN live APC SEA synchronous direct-result and no-direct-result executions WHEN
requests are captured THEN `wait_timeout` is respectively absent and `0s`, with no
`statements` field; public outcomes match APC Thrift and warehouse SEA precedent. AC55 owns
public SQL asynchronous execution and result retrieval.
- AC13: GIVEN available live primitive, decimal, array, map, struct, interval, geography,
geometry, timestamp, timestamp_ntz, and variant results WHEN public APC JDBC reads inline
and linked results with both complex and geospatial settings at 0 and 1 THEN metadata,
`getObject`, and typed getters match R11 and the route baselines. Inline typed complex getters
reject; linked getters work when enabled, and disabled settings retain string fallbacks.
Unavailable live types remain pending with fixtures; no coercion is silent.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC15: GIVEN live APC manifest-only and inline-data starts WHEN public JDBC reads them
THEN complete rows match APC Thrift; an unavailable shape remains pending with a fixture.
- AC16: GIVEN live flag-ON APC `getCatalogs`, `getSchemas`, `getTables`, and `getColumns`
metadata WHEN Arrow IPC or `columnar_data` physical vectors and least-significant-bit null
masks are decoded THEN JDBC rows, metadata, and errors match APC Thrift and warehouse SEA;
short/long masks retain fixtures, and a following Arrow IPC response decodes correctly.
- AC17: GIVEN live APC V10 with native batching enabled WHEN public prepared INSERT batches
succeed or fail THEN one ordered `parameter_sets` request has R6's counts/errors. With native
mode off or unavailable, eligible legacy INSERTs combine and ineligible sets execute singly;
counts and stop-on-error match APC Thrift. Unavailable live V10 remains pending.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. Compare elapsed time, SQLState/vendor code, target, and
post-timeout connection usability on flag-ON staging.
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
- AC22: GIVEN a live Statement INSERT batch with a successful and a failing middle command
WHEN `executeBatch` runs THEN APC SEA sends sequential requests without `statements`, returns
ordered counts, stops on failure, and matches APC Thrift errors. AC32 gates its throughput.
- AC23: GIVEN a live APC CreateSession without a capability version WHEN a public prepared
query or batch runs THEN an encoded ID grants no capability; R6's original-SQL failure for
unresolved markers when `supportManyParameters=0`, interpolation when it is 1, and
missing-version telemetry hold against both route baselines.
- AC24: GIVEN a live APC SEA ResultSet with `EnableHeartbeat=1` WHEN it stays open past
two heartbeat intervals THEN no result heartbeat or `/status` request occurs; execution
polling still works.
- AC25: GIVEN the first public APC SEA connection WHEN `getConnectionId()` is called and its
value is passed to `Driver.closeConnection` THEN the R9 compound session ID round-trips and
closes the correct APC session even after a flag change, without a Thrift-handle dereference.
APC internal statement IDs, malformed-ID rejection, warehouse/Thrift compatibility, and no
capability from encoded versions also hold before the first slice ships.
- AC26: GIVEN live APC query/metadata and warehouse SEA responses WHEN their emitted wire
formats are decoded on staging THEN JDBC results and physical formats follow R16. A separate
fixture with an unknown future format must deserialize safely and fail clearly on decode; it
does not require an unavailable live server response.
- AC27: GIVEN a live APC query with three or more RUNNING polls WHEN transfer is measured
on explicit-override and flag-on routes THEN polls carry no result payload and final fetch
delivers every row once; compare APC Thrift and warehouse SEA traffic.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN live APC complex scalar or batch bindings at available capability versions
WHEN execution starts THEN the same tested driver-owned error name/code occurs before request,
including `Types.ARRAY` and `Types.STRUCT`, while the connection stays usable. Compare
APC Thrift and warehouse SEA; unavailable versions retain fixtures.
- AC30: GIVEN comparable live APC and warehouse SEA SDK pool saturation WHEN route/total limits and wait
settings vary THEN concurrency and wait/error outcomes follow warehouse SEA, while APC links drain through
the separate download pool. Record APC Thrift differences and linked throughput.
- AC31: GIVEN paired warm APC SEA/Thrift inline and linked result runs at single and concurrent
load, with a separate warehouse Thrift/SEA precedent WHEN rows, latency, throughput, peak heap,
bytes, requests, retries, and errors are compared THEN incomplete rows, an APC-only cap, heap
growth with result size, repeatable throughput loss or latency increase over 10%, or peak heap
increase over 20% blocks release. Any exception needs a server limit, warehouse precedent,
caller impact, and PECO/Perf Eng approval; unavailable inline-only proof remains pending.
- AC32: GIVEN paired warm APC SEA/Thrift 1,000-row prepared and Statement INSERT batches at
single and concurrent load, with separate warehouse Thrift/SEA precedent WHEN counts, errors,
requests, throughput, latency, and capability version are compared THEN repeatable throughput
loss or latency increase over 10% in either batch API blocks release without a documented
server limit, warehouse precedent, caller impact, and PECO/Perf Eng approval. Missing live V10
remains pending only for its native prepared mode.
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
TerminateSession succeeds on the server, and an unclosed connection remains usable. Failed
DDL retains its error; without the binding fix, the first public slice stays gated.
- AC38: GIVEN the integrated stack WHEN a public primitive query and retryable fetch
failure run through explicit override and deployed flag on staging THEN rows, order, SQLState,
and vendor code match APC Thrift with warehouse SEA error precedent. Server-dependent cases
remain pending until deployed.
- AC39: GIVEN live inline pages and external links WHEN streaming toggles and `ThriftMaxBatchesInMemory`
vary THEN rows/errors remain equal, APC uses no legacy provider, and buffered inline batches obey the
configured bound (minimum two) and AC31's heap gate.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF THEN APC uses Thrift; on scoped flag-ON staging
completed calls pass their live parity gate. Known incomplete calls fail before SEA requests without Thrift
fallback; public SQL async stays guarded until T39 and production assignment waits for T18/T-val.
- AC41: GIVEN a recovered statement or saved APC session after a flag change WHEN fetch,
cancel, close, or termination runs on live staging THEN the current or saved target/protocol
follows R9 and connection usability matches APC Thrift or warehouse SEA. Termination proof waits
for the server binding fix.
- AC42: GIVEN a RUNNING APC statement WHEN public JDBC closes it on live staging THEN one
close request, no `/status` call, and a usable connection match APC Thrift and warehouse SEA.
AC3 covers request targeting.
- AC43: GIVEN live synchronous APC SHOW WHEN `EnableSeaSyncMetadata` changes between 0 and 1
THEN only R5's sync header changes; rows, errors, and JDBC values match APC Thrift and
warehouse SEA. AC61 owns the common metadata load and latency gate.
- AC44: GIVEN live legacy and native prepared INSERT batches WHEN `EnableBatchedInserts` and
`BatchInsertSize` vary THEN eligible legacy requests honor multi-row and chunk limits, native V10 uses
ordered `parameter_sets`, and counts/errors match APC Thrift. AC32 gates throughput.
- AC45: GIVEN live LZ4-marked and unmarked APC results WHEN consumed under compression settings THEN values
decode by response codec and bytes/CPU/latency are compared with APC Thrift and warehouse SEA; an
unavailable codec keeps a fixture gate.
- AC46: GIVEN live linked results WHEN CloudFetch worker, prefetch, slow-link, and chunk-wait controls vary
THEN rows remain complete and each control retains the warehouse SEA scope in the matrix. Chunk wait also
applies inline; AC31 gates throughput and heap.
- AC47: GIVEN a live primitive single-chunk APC inline result WHEN public JDBC consumes it THEN rows, order,
types, and errors match APC Thrift, while both hybrid settings retain the APC request. LZ4/unmarked and
absent-`arrow_schema` fixtures pass; continuation and links stay guarded until their slices pass.
- AC48: GIVEN live APC counted `INSERT`, `UPDATE`, `DELETE`, or `MERGE` WHEN public
update methods run THEN counts, including zero, match APC Thrift after T32. A missing
`num_affected_rows` column, row, or value produces a contract error, not successful zero.
- AC49: GIVEN live APC queries with `setMaxRows(0)`, `1`, and a boundary-crossing limit
WHEN inline or linked results are read THEN positive limits send SEA `row_limit`, zero is
unlimited, and rows never exceed the limit; compare rows and request count with APC Thrift and
warehouse SEA. A boundary unavailable live remains pending with a contract fixture.
- AC50: GIVEN live V9+ APC WHEN all seven typed SHOW handles are consumed THEN values, errors,
timeout/cancel, cleanup, and public conversions match sync SHOW and route baselines, including distinct
cross-reference conversions. Below V9 or without live version they complete synchronously; procedure SELECTs
reject async submission.
- AC51: GIVEN safe retryable faults for each exposed SEA request class, including 429/503 with and without
`Retry-After`, WHEN retries succeed or exhaust THEN APC SEA attempts, timing, exception class, SQLState, and
vendor code match warehouse SEA without extra write replay. Record APC Thrift property scope and caller
impact.
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
- AC55: GIVEN public APC SEA `executeAsync` WHEN `getExecutionResult` polls and consumes a primitive result
THEN `wait_timeout=0s`, rows/errors, targeting, cancellation, close, and diagnostics follow R2/R10 and route
baselines. Both public methods stay guarded until this passes live staging.
- AC56: GIVEN live inline and linked results WHEN hybrid and streaming toggles vary THEN rows/errors and
AC31's heap bound hold, with request disposition and bytes recorded against APC Thrift and warehouse SEA.
- AC57: GIVEN live V9+ and older APC sessions WHEN public `getColumns` consumes its typed handle THEN async
polling or sync fallback, headers, rows, errors, and timeout/cancel follow R5 and route baselines.
Null-catalog recovery keeps R5's partial-row rule and AC61's request bound; overall timeout fails.
- AC58: GIVEN live V9+ and older APC sessions WHEN catalog, schema, and table typed handles are consumed
THEN public results, error/cleanup behavior, null-catalog schema recovery, and sync fallback follow R5 and
route baselines, including AC61's request bound.
- AC59: GIVEN live V9+ and older APC sessions WHEN function, primary-key, and cross-reference typed handles
are consumed THEN public values, distinct imported-key/cross-reference conversions, error/cleanup behavior,
and sync fallback match R5 and route baselines.
- AC60: GIVEN a valid long-running APC query within its configured client wait WHEN it crosses the measured
single-request timeout THEN APC SEA completes, including result polling, without premature server timeout.
Record duration, retry/final error identity, and connection usability against APC Thrift and warehouse SEA;
a server limit remains pending without cause, caller impact, and PECO approval.
- AC61: GIVEN paired warm APC SEA/Thrift and separate warehouse Thrift/SEA metadata loads,
including 32 concurrent callers and null-catalog fan-out WHEN synchronous and async SHOW run
THEN a `42601` recovery sends at most N+2 requests for N nonempty catalogs. Record median/p95
latency, requests, Reyder CPU/peak memory, errors, and timeouts. Repeatable latency regression
over 10% or Reyder CPU/peak-memory increase over 20% at matched load blocks release without a
documented server limit, caller impact, and PECO/Perf Eng approval.
- AC62: GIVEN stopped-cluster 503 with `Retry-After` and exhausted CreateSession 429 WHEN APC SEA, warehouse
SEA, and APC Thrift are compared under Thrift retry-setting changes THEN APC SEA follows warehouse SDK
delay/exception behavior, including `DatabricksRateLimitException`; Thrift settings affect only APC Thrift.
Record final SQLState/vendor code and caller impact.
- AC63: GIVEN valid delayed APC SHOW within its configured metadata wait WHEN it crosses the measured
single-request timeout THEN APC SEA completes, including polling, without premature server timeout. Record
duration, retry/final error identity, and connection usability against APC Thrift and warehouse SEA; a
server limit remains pending without cause, caller impact, and PECO approval.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The first JDBC package alone owns `docs/` and
  `NEXT_CHANGELOG.md` and records the complete gated feature. Later stacked PRs use
  `NO_CHANGELOG=true` only as internal implementation slices of that same unreleased feature:
  no driver release or production APC assignment may occur between slices. This basis must be
  stated in each PR; if a release is cut early, its visible slice needs a changelog update
  before release. Completed slices remain shippable behind the flag, with no partial ramp.
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

Use a linear JDBC stack of reviewable public outcomes (see `plan.json`), with Universe
prerequisites proceeding in parallel. Each child inherits all earlier JDBC slices, so the
last release package can run integrated gates without merging sibling feature diffs. The
first slice includes connection-ID round trips as well as public no-result DDL. Result,
batch, lifecycle, and metadata outcomes then ship one at a time behind the default-off flag.
Split a package further if its review diff approaches 1,500 lines. The number of required
outcomes and the integrated release gate require more than seven JDBC packages.

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

### API Changes

Public JDBC methods remain the same. Add a session-reachable asynchronous metadata API for
the seven SHOW operations. Public connection IDs may encode APC SEA; statement IDs stay raw.

### Risks

- Tracked server fixes gate live metadata, inline-only, and termination checks; fixtures
  protect those shapes before deployment. DDL cleanup requires the TerminateSession binding fix.
- AC13/Q7 block release on lost logical types; AC27 blocks repeated poll payload transfer.
- AC11/AC14 protect warehouse request and presigned-link boundaries.

---

## Tasks

Each task appears in one `plan.json` package. The package that first exposes a public call
also owns its live gate; T18 repeats open checks on the complete linear stack.

### Phase 1 — Guarded DDL core

- [ ] T1: Add APC compute/session identity and no-result DDL support without changing warehouse wire values (R2, R16).
- [ ] T2: Target the first public DDL CreateSession, ExecuteStatement, poll, statement close, and TerminateSession; preserve the request contract for later slices (R2).
- [ ] T3: Redact first-path diagnostics and emit route/statement telemetry (R8; AC10).
- [ ] T4: Route the first public APC SEA call with Arrow-first, explicit override, catalog-pattern precedence, and the dedicated flag (R1, R7, R18).
- [ ] T6: Preserve selected APC protocol on connection errors and KP001 Thrift failure (R7).
- [ ] T27: Complete targeted TerminateSession and repeated-close DDL lifecycle (R2).
- [ ] T28: Expose no ResultSet and zero count for successful no-result DDL (R4).
- [ ] T31: Expose guarded public DDL through the default-off APC route (R18).
- [ ] T19: Round-trip APC session and internal statement IDs before any public APC SEA call,
  retaining raw public statement IDs (R9; AC25).
- [ ] T29: Use the saved protocol and target for public saved-session close, even after a flag
  change (R9; AC25).
- [ ] T16: Document the gated feature and add the sole JDBC changelog entry.

### JDBC slices after the core

- [ ] T7: Suppress APC result heartbeats while retaining execution polling (R3).
- [ ] T8: Complete public primitive single-chunk SELECT, including values, errors, and cleanup (R4, R11).
- [ ] T10: Decode LZ4-marked and unmarked bare inline batches (R4, R11; AC47).
- [ ] T11: Fetch inline continuation, including manifest-only starts, by `next_chunk_index` and `row_offset` (R4, R12).
- [ ] T25: Target bodyless result-chunk GETs without altering presigned links (R2).
- [ ] T38: Complete public counted DML, including a zero count and contract errors for missing counts (R4).
- [ ] T33: Stream complete external-link Arrow IPC results, without treating them as bare batches (R4, R11).
- [ ] T20: Complete the AC13 logical-type matrix without silent coercion (R11, Q7).
- [ ] T34: Preserve `Statement.setMaxRows` across APC SEA inline and linked results (R4).
- [ ] T36: Preserve CloudFetch controls, streaming memory, and performance bounds (R17; AC31, AC39, AC46, AC56).
- [ ] T12: Apply warehouse SEA timeout behavior with targeted APC cancellation and long-running server-timeout evidence (R10, R14; AC60).
- [ ] T15: Complete lifecycle route outcomes and async diagnostic coverage (R8).
- [ ] T44: Use the current connection target for recovered statements (R9; AC41).
- [ ] T39: Complete public SQL `executeAsync` and `getExecutionResult` with APC targeting, result retrieval, errors, cleanup, and diagnostics (R2, R10; AC55).
- [ ] T26: Prove SDK retry, rate-limit, and pool behavior for exposed request classes (R17; AC30, AC51, AC62).
- [ ] T5: Keep live CreateSession capability version separate from serialized ID diagnostics (R6, R9).
- [ ] T9: Gate native scalars at V8, preserve `supportManyParameters` and `setBytes` semantics, and support binding-free pre-execution metadata (R6; AC53).
- [ ] T22: Reject complex bindings before requests with tested error identity and taxonomy (R6).
- [ ] T21: Support opt-in V10 prepared batches and the shared legacy fallback, including eligible multi-row INSERTs (R6).
- [ ] T30: Execute Statement SQL batches sequentially with ordered counts and stop-on-error (R15).
- [ ] T14: Complete public `getCatalogs` across emitted formats and synchronous SHOW controls (R5, R13; AC16, AC43).
- [ ] T43: Complete public `getSchemas`, `getTables`, and `getColumns`, including null-catalog behavior, metadata load, and slow-call timeout (R5, R13; AC16, AC61, AC63).
- [ ] T13: Complete public function SHOW metadata with R5's request contract (R5).
- [ ] T37: Complete public primary-key/imported-key/cross-reference metadata and typed-empty exported keys (R5; AC54).
- [ ] T42: Complete public procedure and procedure-column SELECT metadata without unsupported APC headers (R5, R6).
- [ ] T23: Complete V9-gated typed async `getColumns`, including null-catalog fan-out and below-V9 sync fallback (R5; AC57).
- [ ] T40: Complete typed async `GET_CATALOGS`, `GET_SCHEMAS`, and `GET_TABLES` handles and their public conversions, including null-catalog schemas (R5; AC58).
- [ ] T41: Complete typed async `GET_FUNCTIONS`, `GET_PRIMARY_KEYS`, and `GET_CROSS_REFERENCE` handles, including both public imported-key and cross-reference conversions (R5; AC59).
- [ ] T24: Complete shared async error/timeout/cancel coordination before `getColumns` exposure; later async slices validate it for each added operation (R5; AC50, AC61).

### Independent Universe prerequisites

- [ ] T17: Define the default-off APC SAFE sub-flag without assigning or ramping it (R1, Q8).
- [ ] T32: Prove and, if needed, fix Reyder's no-result DDL and counted-DML result contracts (R4; AC52).
- [ ] T35: In connector-service, override the APC sub-flag to false for every partner User-Agent before flag assignment, without changing the warehouse flag's partner allowlist (Q9, AC34).

### Integration and release

- [ ] T18: Run integrated local and live evidence on the full linear JDBC stack after T17/T35 and T32 deploy; close cross-route checks (AC1–AC63).
- [ ] T-val: Record pass/fail or externally pending evidence for AC1–AC63 and exact commands; keep release blocked on open server, type, partner, or performance gates.
