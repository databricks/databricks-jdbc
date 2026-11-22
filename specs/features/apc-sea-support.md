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
- Client-config flags come from connector-service
(`/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`), which returns every sub-flag of
the batch flag `databricks.partnerplatform.clientConfigsFeatureFlags`
(`connector-service/src/featureflags/FeatureFlagsHandler.scala`). It special-cases
`enableSqlExecForJdbc` (version-mapped to `enableSqlExecForJdbc_v3.4.3` for ≥ 3.4.3, forced
`false` for partner User-Agents); other sub-flags pass through unchanged.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata design tabs). Driver-relevant points:
  - Requests: `warehouse_id` null; `backend_compute.all_purpose_cluster_compute.cluster_id` on
  CreateSession(V2), TerminateSession, ExecuteStatement, GetStatementResult, GetResultData,
  CancelStatement, CloseStatement; `session_id` required on ExecuteStatement;
  `disposition=INLINE_OR_EXTERNAL_LINKS`; `wait_timeout` unset (direct results) or the SEA duration
  string `0s` (parsed by the service as zero for async execution), any nonzero value rejected;
  `on_wait_timeout` ignored; `statements` (batch commands) unsupported; SQL text ≤ 16 MB.
  Non-OSS-JDBC clients are rejected.
  - Non-metadata results: `format=ARROW_BATCH` (bare Arrow record batches, no schema header).
  Schema is reconstructed from `manifest.schema` (`ColumnInfo` with `type_text`/`type_json`);
  `manifest.arrow_schema` and `total_byte_count` may be populated by current Reyder responses,
  but the driver must also accept them empty. `total_chunk_count`, `chunks`, and
  `total_row_count` may be empty. Client fetches until `next_chunk_index` is empty;
  `GetResultData.chunk_index` is cosmetic and `row_offset` is the real offset.
  `GetStatementResult` may or may not include result data alongside the manifest.
  `GetStatementStatus` is unsupported. Arrow-disabled customers stay on Thrift (no HIVE for
  non-metadata queries).
  - Metadata contract: supported SHOW commands via ExecuteStatement
  with `X-Databricks-Metadata-Operation-Type`;
  synchronous calls send `X-Databricks-Sea-Can-Run-Fully-Sync: true` when
  `EnableSeaSyncMetadata=1` (the default), while disabled or asynchronous calls omit that header;
  `format=ARROW_STREAM`. The server runs native Thrift
  metadata RPCs, sets
  `is_native_metadata_result=true`, ignores `X-Databricks-Require-Thrift-Native-Metadata`, and
  returns metadata inline in one of two response representations: Arrow IPC in
  `chunk.attachment` (the primary/majority case), or HIVE-shaped `chunk.columnar_data`
  (`ColumnarData`/`ColumnVector` with little-endian null masks). The client must multiplex
  between these representations for each response, including a `COLUMN_BASED_SET` response
  format for older runtimes (SEA `Format` wire value 6); it must not bind a connection to one
  decoder based on the requested `ARROW_STREAM` format or an assumed DBR version.
  - Capability: CreateSessionResponse `negotiated_protocol_version` (APC only): parameters at
  ≥ 42248 (V8), asynchronous metadata operations at ≥ 42249 (V9), and `parameter_sets` batch
  inserts at ≥ 42250 (V10). An absent version grants none of these capabilities.
  - Query timeout: follow the warehouse SEA path (client-side `TimeoutHandler`).
  - Govcloud / regions without Reyder: server errors propagate to the client; the driver does not
  fall back to Thrift.
  - Rollout (M15): opt-in first, then default APC to Reyder. This spec implements that with a
  dedicated APC client-config flag and a client override (R1, Q8).
- Driver baseline verified against `origin/main` @ `840f3a407` (including approved spec #1809
  and the `UseBoundedSeaApi` default change #1827): APC currently routes to Thrift before
  `UseThriftClient` or flags; warehouse routing honors those controls and AIX/PowerPC disables
  Arrow by default. SEA session and statement requests assume warehouse targets. Statement
  polling uses `GET /statements/{id}`; result heartbeat uses unsupported APC `/status`.
  The result set creates no decoder without result data, and SEA decoding lacks `ARROW_BATCH`.
  Metadata SHOW is synchronous, with no `columnar_data` model; native SEA prepared batching
  reports warehouse support only. Existing SEA CloudFetch and bounded inline paths stream
  results, while SDK retries and pooling differ from the separate `DatabricksHttpClient`
  property scope described in R17. These are starting conditions, not prescribed edits.
- Draft implementation PR [#1771](https://github.com/databricks/databricks-jdbc/pull/1771)
ports the 41-file APC-over-SEA prototype onto current `main`; frozen benchmarking PR
[#1756](https://github.com/databricks/databricks-jdbc/pull/1756) remains unchanged. The draft
adds APC request targeting and Arrow-batch result handling, enables the prototype by default,
uses `UseThriftClient=0/1` to select SEA/Thrift, and emits `JDBC_APC_ROUTE` markers. It is an
iteration baseline, not the production implementation described by the requirements below.
- Parity baseline for review and release ("warehouse" means SQL warehouse using the same driver
  build). A difference is allowed only where the server contract above or existing warehouse
  Thrift-versus-SEA behavior explains it; the listed exception must remain visible to users.

  | Behavior | APC Thrift today | Warehouse SEA today | APC SEA target and disposition |
  | --- | --- | --- | --- |
  | Routing and failure | Default APC route; Thrift OpenSession | Flag or explicit setting; SEA errors propagate | Dedicated flag/override; no SEA-to-Thrift fallback, following warehouse SEA precedent (R1, R7). |
  | Result values and paging | Arrow/CloudFetch or inline; complete JDBC rows | Arrow stream/CloudFetch or bounded inline | Same JDBC values and completeness; APC wire uses LZ4 `ARROW_BATCH` and offset paging because Reyder emits bare batches (R4, R11, R12). |
  | `EnableSQLExecHybridResults=0` with CloudFetch | No SEA disposition | Requests `EXTERNAL_LINKS` rather than hybrid inline/external results | Requests `INLINE_OR_EXTERNAL_LINKS` regardless of the setting: APC contract requires this disposition. External links may still be returned; callers cannot force external-only APC results (R4; AC4). Documented exception. |
  | `EnableQueryResultDownload=0` | Inline Thrift results | Bounded inline Arrow when available; otherwise routing may select Thrift | Remains on SEA, sets `can_cloud_download=false`, and consumes all inline pages. Reyder must honor inline-only delivery despite the hybrid disposition; an inline-size rejection is returned to the caller rather than silently using CloudFetch (R4; AC8). Documented exception. |
  | Metadata | Native Thrift RPCs | SEA SHOW queries and native-shaped responses | Same JDBC columns and rows; SHOW uses Arrow IPC or `columnar_data` because Reyder chooses either representation (R5, R13). |
  | Scalar parameters | Native where protocol permits; otherwise driver behavior | SEA native bindings or existing interpolation | Native at V8+, interpolation below V8; APC protocol gate is a server capability limit (R6). |
  | Prepared batches | Native Thrift executor when supported and opted in; otherwise legacy path | Native SEA executor when opted in; otherwise legacy path | Native `parameter_sets` only at V10+ and opt-in; otherwise one request per set. Aggregate-only APC native response reports `SUCCESS_NO_INFO` rather than per-set counts (R6). |
  | Arrow prerequisite | APC Thrift works without Arrow | Explicit `UseThriftClient=0` is considered before the warehouse Arrow check | APC requires Arrow before even an explicit SEA override, because Reyder has no HIVE non-metadata result path; AIX/PowerPC users without Arrow remain on Thrift (R1; AC7). |
  | Result heartbeat | Thrift can check statement status | SEA result heartbeat uses `/status` | APC schedules no result heartbeat because Reyder does not support `GetStatementStatus`; long-lived completed ResultSets have no periodic liveness check (R3; AC24). |
  | `UseBoundedSeaApi=0` | Not used by Thrift | Changes the warehouse SEA inline/provider path | APC still uses `ARROW_BATCH` and `next_chunk_index`/`row_offset` paging because Reyder's wire contract requires it; the setting cannot disable APC paging (R4, R12; AC8). |
  | `EnableBatchedInserts=1` without APC native batching | Eligible sets may be combined into a multi-row INSERT | Legacy SEA may combine sets; native batching is separately opted in | APC executes each set individually (Phase II DD2/DD3 and Q4), preserving counts and stop-on-error but increasing request count and potentially lowering throughput; V10 native mode restores one request for the batch (AC17, AC32). |
  | Timeouts, cancel, and errors | Client timeout and Thrift cancel; JDBC errors | `TimeoutHandler`, SEA cancel, SEA error mapping | Follow warehouse SEA timeout, retry, rate-limit, SQLState, and vendor-code behavior; APC target is added to cancel (R14, R17). |
  | Statement GET retry settings | `DatabricksHttpClient` uses `DatabricksHttpRetryHandler` and `RateLimitRetry` | SDK `ApiClient` uses its own retry policy; `RateLimitRetry` and `HttpConnectionRequestTimeout` are not wired to that SDK transport today | Use the same SDK policy as warehouse SEA for APC statement/result GETs; `RateLimitRetry=0` does not disable either SEA route's SDK retries, and `HttpConnectionRequestTimeout` does not govern either SEA route's SDK pool wait. Document this warehouse precedent and test identical APC/warehouse behavior (R17; AC30). |
  | CloudFetch throughput and concurrency | Existing download pool, prefetch, and retries | Shared HTTP and streaming CloudFetch path | Reuse warehouse SEA download pool, prefetch, retry, and connection-limit controls; no APC-only cap or full-result buffering (R17). |
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
on result-chunk GETs. Those GETs and CloseStatement DELETE have no request body. Presigned
external-link URLs and headers are unchanged. Warehouse standard and Real-time request
serialization remains unchanged. Dogfood target extraction for TerminateSession depends on the
tracked server binding fix.
- R3: For APC over SEA, execution polling uses `GetStatementResult`. Result heartbeats are
disabled for APC even when `EnableHeartbeat=1`, because `GetStatementStatus` is unsupported and
using full `GetStatementResult` after completion can re-download result payloads. The driver logs
that heartbeat is unsupported and never schedules `checkStatementAlive` or calls `/status`.
- R4: APC non-metadata execution sends `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`,
and a `session_id`, never sends `statements`, and handles LZ4-compressed Arrow batches for both
inline `chunk.arrow_batches` (including multiple batches per chunk) and external links.
`EnableQueryResultDownload=0` keeps the connection on SEA but
sets the CreateSession `can_cloud_download=false` session config. With the assumed server fix,
results must remain inline even though
the request disposition is `INLINE_OR_EXTERNAL_LINKS`. The driver follows every inline
page until the server indicates completion. If the server cannot serve the complete result inline,
the driver propagates the server error and must not expose a successfully completed, truncated
ResultSet. APC compute type—not
`isBoundedSeaApiEnabled`—selects this format/disposition and enables the session-config behavior.
For a successful statement with no result schema and no result data (including DDL), the driver
reports no ResultSet: `Statement.execute` returns `false`, `getResultSet()` returns `null`, and
`executeUpdate`/`executeLargeUpdate` and the current update count return `0`. `getMoreResults()`
then returns `false` and changes the update count to `-1`. Sequential `Statement` batches record
`0` for each such command. A successful DML response with an affected-row count continues to
report that count; a missing schema must never be treated as a decoding error or cause a result
fetch. This rule applies to successful responses only; server failures still propagate.
- R5: The seven APC metadata SHOW operations (`GET_CATALOGS`, `GET_SCHEMAS`,
`GET_TABLES`, `GET_COLUMNS`, `GET_FUNCTIONS`, `GET_PRIMARY_KEYS`, and
`GET_CROSS_REFERENCE`) send `X-Databricks-Metadata-Operation-Type`, request
`ARROW_STREAM` with `INLINE_OR_EXTERNAL_LINKS` even when
`EnableSQLExecHybridResults=0`, and preserve the existing
`X-Databricks-Require-Thrift-Native-Metadata` header when applicable. Synchronous
SHOW sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only with
`EnableSeaSyncMetadata=1`; disabled sync and all async SHOW omit it. Reyder ignores
the Thrift-native header and returns native-shaped metadata. Each response is decoded
from Arrow IPC `chunk.attachment` or HIVE-shaped `chunk.columnar_data`, including
`COLUMN_BASED_SET` manifests, according to that response rather than the requested
format or a connection-level choice.

A session-reachable async metadata API accepts the seven SHOW operations and their JDBC
inputs, returning a typed handle and a completed JDBC metadata ResultSet. At live
CreateSession protocol version V9+ (42249), it submits asynchronously with the operation
header, polls under the existing metadata timeout, and applies the same conversion,
empty-result, failure, and cancellation behavior as sync metadata. The handle retains the
typed statement id, operation, JDBC method (including the two
`GET_CROSS_REFERENCE` conversions), and conversion inputs. Below V9 or with no live
version, it completes the synchronous SHOW path before returning an already-completed
handle; result retrieval performs no poll. A reconstructed session-id version cannot
grant async capability. Existing SQL async submission and completion remain unchanged.

SHOW must be distinguished from SELECT, not inferred merely from
`StatementType.METADATA` or a non-null operation type. Internal catalog/schema SELECTs
and the parameterized `getProcedures`/`getProcedureColumns` SELECTs use
`ARROW_BATCH`, `INLINE_OR_EXTERNAL_LINKS`, no metadata headers, and ordinary query
decoding before JDBC metadata conversion. Procedure SELECTs retain native scalar
parameters at V8+; below V8 or with no live version, interpolate safely and omit the
`parameters` property. The async SHOW API rejects procedure SELECT operations before a
request. Warehouse request behavior is unchanged.

For V9+ async `GET_SCHEMAS` and `GET_COLUMNS` with a null catalog, SQL state
`42601` from `SHOW ... IN ALL CATALOGS` triggers async catalog enumeration and a SHOW
per nonempty catalog. Each request retains its operation header and omits the sync
header; the handle produces one ResultSet with the synchronous fallback's filtering and
empty-result behavior. Ordinary per-catalog SQL errors warn and skip that catalog;
cancellation and metadata timeout propagate. Other initial errors, a null SQL state,
or a non-null catalog do not trigger fan-out.

- R6: Use the live CreateSession `negotiated_protocol_version` to gate native scalar parameters
at V8 (42248), asynchronous metadata at V9 (42249), and native parameterized INSERT batches at
V10 (42250) with `EnableNativeBatching=1`; an absent version grants none. A serialized diagnostic
version cannot grant capabilities. Native complex bindings (ARRAY, MAP, STRUCT, and other complex
logical types) fail before any request with a tested driver-owned error name/code and taxonomy
classification. Below V8, each prepared execution, including each fallback batch set, uses the
existing safe SQL interpolation and omits the `parameters` property; a missing version has a
distinct telemetry downgrade reason. At V8–V9, fallback batches use native scalar `parameters`
one set at a time. Native V10 sends ordered nested `parameter_sets` and no top-level `parameters`;
on aggregate success, return `SUCCESS_NO_INFO` per set, and on failure claim no set succeeded.
All other APC prepared batches execute one request per set even with `EnableBatchedInserts=1`,
preserve ordered counts, and stop on failure. A pre-execution `PreparedStatement.getMetaData()`
uses a binding-free `DESCRIBE QUERY`, preserves the original bindings, and works at every version.
Warehouse behavior is unchanged.
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
- R11: For APC `ARROW_BATCH` results, the driver builds the Arrow schema from
`manifest.schema` (`type_text`/`type_json`, including nested types) and does not require
`manifest.arrow_schema`; a populated `manifest.arrow_schema` or `total_byte_count` does not
change decoding or paging. Until the complete logical-type matrix is supported, any deferred
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
- R17: APC SEA uses the same SDK `ApiClient` retry policy and SDK connection manager as warehouse
SEA for statement/result GETs. On current `origin/main`, `RateLimitRetry` is consumed by
`DatabricksHttpRetryHandler` for the separate `DatabricksHttpClient` path, and
`HttpConnectionRequestTimeout` is also wired only there; neither property controls SDK statement
GET retries or SDK pool wait on either SEA route. `HttpMaxConnectionsPerRoute` does configure the
SDK pool through `ClientConfigurator`. APC SEA keeps warehouse SEA CloudFetch download retries,
`cloudFetchThreadPoolSize`, link prefetch, streaming, and connection pool controls. Do not add an
APC-only retry policy, lower concurrency cap, or full-result heap buffer. Document the existing
SEA setting scope in `docs/` and compare APC and warehouse SEA under identical SDK settings.
Compare APC SEA against APC Thrift and warehouse SEA/Thrift under the same workload and settings;
record throughput, latency, peak heap, retries, batch INSERT throughput/request count, and error
SQLState/vendor code. Any measured
client-visible difference without a server-contract limit or warehouse SEA precedent is a release
blocker until corrected or explicitly documented as an exception with its client impact.

## Clarifications

- Q1: What is the client setting — reuse `UseThriftClient` on cluster URLs (prototype
behavior; `0` → SEA, `1` → Thrift), or a new dedicated property (e.g. `EnableApcSea`)?
A1: Reuse `UseThriftClient`: with Arrow available, `0` forces SEA and `1` forces Thrift. Arrow
unavailability is a hard prerequisite and selects Thrift even when `UseThriftClient=0`; otherwise
an explicit value takes precedence over `TreatMetadataCatalogNameAsPattern=1` and the APC
client-config flag. When `UseThriftClient` is unset, `TreatMetadataCatalogNameAsPattern=1` forces
Thrift before consulting the APC flag, matching warehouse behavior. The Arrow prerequisite remains
APC-only; warehouse routing is not reordered.
- Q2: Should an explicit client setting (`UseThriftClient=0`) still auto-fall back to Thrift on
unsupported-routing errors, or
fail loudly so users notice (prototype uses `no_fallback=true`)?
A2: Do not fall back. Match warehouse SEA: once SEA is selected, connection errors propagate to
the caller and the driver never switches the connection to Thrift.
- Q3: Because each metadata response can contain either Arrow IPC or `columnar_data`, can the
existing SEA result pipeline multiplex between them or does it need a new converter?
A3: Needs a new reader. Reyder returns HIVE metadata as the new SEA `chunk.columnar_data`
shape (design doc, metadata DD2 option 3 / DD4 option 2); the driver must multiplex per response
between it and the primary Arrow IPC representation. It must not infer the representation from
the requested `ARROW_STREAM` format or select one decoder for the lifetime of the connection
(R5, R13).
- Q4: Should batch commands (`executeBatch`) be emulated client-side when
`negotiated_protocol_version` < V10, or rejected with a clear error?
A4: Emulate. `statements` is unsupported for APC; batch parameterized inserts use
`parameter_sets` only at ≥ 42250 (V10) with `EnableNativeBatching=1` and a supported native
executor, matching warehouse opt-in behavior. Otherwise use one execution per parameter set,
even if `EnableBatchedInserts=1` (design doc, Phase II DD2/DD3). Non-parameterized `Statement`
batches always use the driver's existing
sequential executor: one ExecuteStatement per SQL command, stopping on the first failure.
- Q5: Do govcloud / non-Reyden regions need a client-side denylist, or is keeping the APC flag
off and propagating a server rejection sufficient?
A5: No client-side denylist. Keep the APC client-config flag off in unsupported regions. If SEA is
explicitly selected or mis-targeted there, propagate the server error without falling back.
- Q6: Is there a server-side requirement on the `User-Agent` or a client-capability header to
identify APC-SEA traffic for rollout dashboards?
A6: No special `User-Agent` or client-capability header is needed. The CreateSession path and
`backend_compute` targeting distinguish all-purpose-cluster traffic from warehouse traffic;
preserve the driver's ordinary User-Agent unchanged.
- Q7: For `ARROW_BATCH`, can `type_text`/`type_json` in `manifest.schema` reconstruct every Arrow
type the driver needs (nested, interval, geospatial, decimal), or does the driver need Reyder
to also populate `arrow_schema`?
A7: The driver implementation must prove lossless reconstruction through AC13 for the complete
type matrix. If any listed type cannot be reconstructed from `type_text`/`type_json`, APC SEA
release is blocked until Reyder populates `arrow_schema` (and the driver consumes it); the driver
must not silently coerce the type or switch an established connection to Thrift.
- Q8: Should APC share `enableSqlExecForJdbc` (already 100% ON) or get its own client-config
flag?
A8: Its own flag, so APC ramps from 0% independently and can be rolled back without touching
warehouses. Proposed name (not yet created):
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc`, a new sub-flag
of the existing batch flag, default off. Older drivers ignore the unknown name, so no
connector-service version filtering is needed.
- Q9: Should connector-service force the APC flag to `false` for partner User-Agents, as it does
for `enableSqlExecForJdbc`? Reyder's driver-prefix allowlist only checks the
`DatabricksJDBCDriverOSS` prefix, so partner tools on the OSS driver would pass it.
A9: Yes. Apply the same partner User-Agent override as `enableSqlExecForJdbc`: connector-service
returns the APC flag as `false` for partner User-Agents. Partner enablement requires a separate
explicit rollout decision rather than inheriting the OSS driver ramp. This approved service policy
is tracked outside this client-only plan; T17 creates only the SAFE sub-flag. The driver treats
the returned false value as it treats any false flag.
- Q10: How is repeated result transfer avoided while APC polls `GetStatementResult`?
A10: A RUNNING response must be status-only for result payload purposes. Keep a bounded-polling
integration gate: at least three RUNNING polls of a large-result query transfer no result chunks
or attachment bytes, then the completed response and subsequent fetch yield every row once.

## Acceptance Criteria

Each client-visible criterion is also subject to the applicable flag-on live staging comparison
in the package gate table under Plan. A package records the result for its exposed behavior
before the child package starts. A server-fix-dependent check is recorded as externally pending,
not passed, until the fix is deployed; its local contract check still runs.

- AC1: GIVEN a cluster HTTP path with no client setting AND the APC flag OFF or absent (or the
flag lookup fails), even with `enableSqlExecForJdbc` ON, WHEN a connection is opened THEN the
driver uses `DatabricksThriftServiceClient` and no SEA request is sent; GIVEN the APC flag ON
AND Arrow available THEN it uses SEA. GIVEN a partner User-Agent for which connector-service
returns the APC sub-flag as `false` THEN the driver uses Thrift. GIVEN the legacy `cliservice`
HTTP path THEN the driver continues to use Thrift and never sends placeholder `default` ids to SEA.
- AC2: GIVEN a cluster HTTP path with APC-SEA enabled (R1) WHEN a connection is opened THEN the
CreateSession request body contains `backend_compute.all_purpose_cluster_compute.cluster_id`
equal to the URL's cluster id and contains no `warehouse_id`.
- AC3: GIVEN an APC SEA session WHEN a statement is executed, polled, fetched, cancelled,
closed, or its session terminated THEN every SEA request uses the R2 APC target and no `/status`
call occurs. Captured statement/result GETs and CloseStatement DELETE have null bodies and the
cluster query field once; TerminateSession DELETE has the target body. Standard and Real-time
warehouse request fixtures remain unchanged. GIVEN a saved APC SEA or Thrift connection ID and
a flag change WHEN `Driver.closeConnection` runs THEN it uses the saved protocol and target.
Live TerminateSession target extraction remains pending the tracked server binding fix.
- AC4: GIVEN an APC query returning inline LZ4 bare Arrow batches, multiple batches in one
chunk, or external Arrow IPC links WHEN the full ResultSet is read THEN row order, values,
logical types, and metadata match APC Thrift. Inline batches use the reconstructed schema;
external streams remain intact. GIVEN sync or async execution with
`EnableSQLExecHybridResults=0` WHEN the request is captured THEN it still sends
`ARROW_BATCH`, `INLINE_OR_EXTERNAL_LINKS`, the current session id, and no `statements` field.
- AC5: GIVEN APC SEA synchronous or live-V9+ asynchronous SHOW metadata for all seven
operations in R5 WHEN each public operation completes with Arrow IPC or `columnar_data` THEN
request headers, format, disposition, JDBC columns, values, and errors match R5 and the same
APC Thrift operation. The decoder follows each response representation, including
`COLUMN_BASED_SET`, even when responses alternate on one connection. Async handles preserve
metadata type and terminal, cancel, and timeout behavior; below V9 or with no live version,
they complete through the synchronous path. Null-catalog schemas/columns preserve the `42601`
fan-out and filtering in both modes. Internal catalog/schema and procedure SELECTs remain
ordinary `ARROW_BATCH` queries without metadata headers; procedure bindings follow the V8 gate.
An async procedure request fails before network I/O. Warehouse metadata requests remain unchanged.
Live metadata checks depend on the tracked server fix; fake-service contract checks run first.
- AC6: GIVEN an APC-SEA session with `negotiated_protocol_version` below V8 WHEN a
parameterized PreparedStatement is executed THEN the driver uses the non-native
(client-interpolated) path, omits the `parameters` property rather than sending `[]`, and emits a
parameter-downgrade log and telemetry signal; escaping and injection tests with quotes, comments,
delimiters, and backslashes produce the same values as the existing client-interpolated path
without executing injected SQL. The same below-V8 and missing-version behavior applies to both
parameterized procedure metadata SELECTs, which call `IDatabricksClient.executeStatement`
directly.
GIVEN an APC prepared SELECT with `setObject(...)` bindings WHEN `getMetaData()` is called before
execution at below-V8, missing-version, or V8+ capability THEN the driver sends one
`DESCRIBE QUERY` with only real placeholders quoted as `'?'` and no `parameters` property,
returns its JDBC `ResultSetMetaData`, and leaves the original prepared SQL unexecuted and its
bindings available for later execution. Quoted `?` in the original SQL remains unchanged; no
parameter-count exception occurs.
- AC7: GIVEN `UseThriftClient=0` and the APC flag ON on AIX/PowerPC with Arrow unavailable WHEN a
connection is opened THEN the Arrow prerequisite takes precedence, the pre-connection routing
decision selects Thrift, and the driver logs the routing reason.
- AC8: GIVEN APC-SEA enabled (R1) AND `EnableQueryResultDownload=0` WHEN a connection is opened
THEN the driver remains on SEA, sets CreateSession session config `can_cloud_download=false`, and
reads inline Arrow without following external links; setting `UseBoundedSeaApi` to either `0` or
`1` does not change APC protocol selection or its `next_chunk_index`/`row_offset` paging behavior.
GIVEN an inline result spans multiple pages THEN the complete result is returned; GIVEN the server
cannot serve the complete result inline THEN its error is propagated and no partial ResultSet is
reported as successful.
Unit and fake-service checks run before dogfood; dogfood needs the tracked inline-only server fix
deployed in the test region.
- AC9: GIVEN APC-SEA enabled (R1) AND CreateSession returns an "all-purpose clusters are not
supported" error WHEN a connection is opened THEN the driver propagates a clear connection error,
does not create a Thrift client, and records the failure in telemetry.
GIVEN APC was routed to Thrift and OpenSession returns KP001 THEN the warehouse-only recovery
does not switch it to SEA, regardless of whether the APC flag was off or Arrow was unavailable.
- AC10: GIVEN telemetry enabled and APC or warehouse connections selected by each supported
routing reason WHEN each connection attempt succeeds or fails THEN exactly one routing event is
emitted for that attempt with compute type, selected protocol, routing reason, and success or
failure outcome. Captured failure events cover APC SEA CreateSession, APC Thrift OpenSession,
warehouse SEA CreateSession, and warehouse Thrift OpenSession; none contains credentials or a
compound statement id. Captured APC SEA logs and telemetry from synchronous and
asynchronous execute, session stringification, and connection failure contain no raw org id,
cluster id, or compound session/statement id while retaining compute type and routing reason.
At each package gate, compare standard execution, result, metadata, batch, cancellation, and
failure telemetry fields with warehouse SEA for operations that package exposes; record the
corresponding APC Thrift event and any documented SEA precedent. No operation silently drops
its standard event.
- AC11: GIVEN standard and Real-time SQL warehouse connections WHEN CreateSession, execute,
poll, fetch, cancel, close, and TerminateSession run THEN serialized request bodies, URLs, and
headers match pre-change fixtures, including warehouse `realtimeSession` behavior.
- AC12: GIVEN an APC-SEA session WHEN a statement is executed with and without direct results
THEN `wait_timeout` is unset and `0s` respectively, Reyder accepts both request shapes, and
`statements` is never sent. GIVEN an asynchronous APC execution with direct results enabled WHEN
the request is captured THEN `wait_timeout` is `0s`, not an omitted or nonzero duration.
- AC13: GIVEN an APC `ARROW_BATCH` response with empty `arrow_schema` and a `manifest.schema`
containing primitive, decimal, array, map, struct, interval, geography, geometry, timestamp,
timestamp_ntz, and variant columns WHEN the ResultSet is read THEN values and
`ResultSetMetaData` match the same query over Thrift. The same rows and metadata result when
`arrow_schema` and `total_byte_count` are populated.
- AC14: GIVEN an APC result with empty `total_chunk_count`/`total_row_count` spread across
several chunks WHEN the ResultSet is iterated THEN the driver fetches with `row_offset` starting
at zero and advancing by the sum of inline `arrow_batches.row_count` values on each page until
`next_chunk_index` is empty, returning every row exactly once. Each fetch URL for the SEA
GetResultData endpoint contains both
`row_offset` and the dotted APC compute query
field exactly once, while each presigned external-link URL and its headers are used unchanged.
GIVEN multiple external links in one page with no page-level `row_count` THEN the next
SEA fetch uses the final link's `next_chunk_index` and
`row_offset = finalLink.row_offset + finalLink.row_count`, returning every row once.
- AC15: GIVEN a completed APC statement WHEN `GetStatementResult` returns a manifest without
result data, and separately WHEN it returns a manifest with result data THEN both produce the
full correct ResultSet.
- AC16: GIVEN a current APC metadata SHOW response with `columnar_data` WHEN any boolean,
byte, short, int, long, double, string, or binary vector emitted by those operations is decoded
THEN JDBC metadata columns and values match the same APC Thrift operation. Least-significant-bit-first null masks
preserve null positions across byte boundaries; a short mask leaves later rows non-null and a
long mask ignores excess bits. GIVEN the next response on the same connection uses Arrow IPC
`attachment` THEN it decodes correctly through that response's representation. This criterion
covers metadata columns the seven native operations can emit; it does not require synthetic
complex-valued metadata columns or an absent mask.
- AC17: GIVEN a live V10 APC session with `EnableNativeBatching=1` WHEN a parameterized
INSERT batch succeeds THEN one request sends ordered nested `parameter_sets`, no top-level
`parameters`, and both JDBC batch APIs return `SUCCESS_NO_INFO` per set rather than fabricated
counts. A failed native request marks all sets `EXECUTE_FAILED` without claiming partial success.
GIVEN native batching is disabled or unavailable WHEN the batch runs THEN one execution per set
uses native scalar parameters at V8+ or safe interpolation below V8; missing versions act as
below V8. No empty `parameters` field or combined multi-row INSERT is sent. Ordered counts
and stop-on-first-failure behavior match APC Thrift where the wire permits.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException.
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
- AC22: GIVEN an APC-SEA `Statement` with three SQL strings added through `addBatch(String)` WHEN
`executeBatch` runs THEN the driver sends three sequential ExecuteStatement requests without a
`statements` field and returns update counts in input order; GIVEN the second command fails THEN it
does not execute the third and throws `BatchUpdateException` with the first command's count.
- AC23: GIVEN an APC CreateSession response with no `negotiated_protocol_version` WHEN a
parameterized query or prepared batch is executed THEN the driver treats the capability as below
V8, uses the tested client-interpolation path even with `supportManyParameters=0`, never sends
`parameters` or `parameter_sets` properties, including empty arrays, and emits telemetry
identifying the protocol version as missing.
Each fallback batch set sends escaped SQL separately without native bindings.
Pre-execution `getMetaData()` with existing bindings also succeeds through the binding-free
`DESCRIBE QUERY` path in AC6.
- AC24: GIVEN an APC-SEA connection with `EnableHeartbeat=1` WHEN a completed ResultSet remains
open beyond multiple heartbeat intervals THEN no heartbeat task is scheduled, no
`GetStatementStatus`, `/status`, or heartbeat `GetStatementResult` request is sent, and the driver
logs that heartbeat is unsupported for APC.
- AC25: GIVEN APC session and statement IDs with present or absent negotiated version
WHEN serialized and parsed THEN they follow the exact R9 grammar; warehouse and Thrift IDs
remain compatible, public connection ID is compound, and public statement ID is raw. A
serialized version cannot grant capabilities without a live CreateSession. GIVEN malformed
APC-prefixed session or statement IDs with missing, extra, empty, or invalid segments WHEN
parsed THEN they fail without warehouse fallthrough or disclosure of input/compute identifiers;
malformed sessions use `SESSION_ID_PARSING_EXCEPTION`. GIVEN a recovered statement or saved
connection after flag change WHEN fetch, cancel, close, or termination runs THEN the current
statement target or saved session target and protocol are used, respectively.
- AC26: GIVEN APC `ARROW_BATCH`, metadata `COLUMN_BASED_SET`, and existing warehouse
`JSON_ARRAY`/`ARROW_STREAM` wire responses WHEN their formats are serialized and decoded THEN
exact wire names and JDBC rows/metadata are preserved; `COLUMN_BASED_SET` selects columnar
metadata even after an `ARROW_STREAM` request. An unknown future format deserializes safely
and fails clearly if result decoding is attempted.
- AC27: GIVEN an APC query with a large result and at least three RUNNING
`GetStatementResult` polls WHEN request and response payloads are captured in an integration run
THEN those RUNNING responses contain no attachment, Arrow batches, columnar data, or external
links, and transfer zero result-payload bytes; the terminal response plus result fetch returns
all rows once. Record the poll count and payload-byte measurements as a release gate.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` and its default is OFF.
Configuration evidence records the exact name, parent batch flag, and default value.
- AC29: GIVEN an APC session at V8 or newer and a prepared single execution or native
`parameter_sets` batch containing an ARRAY, MAP, STRUCT, or other complex logical binding WHEN
execution is attempted THEN a clear unsupported-parameter SQLException with its driver-owned
error name and numeric code is raised before any ExecuteStatement request. A captured fake-service
request log stays empty, including when a complex value appears after scalar sets in a batch;
scalar bindings still use native parameters under the R6 capability gates. Cover at least
`setObject(index, value, Types.ARRAY)` and `Types.STRUCT`.
- AC30: GIVEN APC SEA and warehouse SEA with the same SDK retry and pool settings WHEN
statement/result GETs receive retryable 429/503 responses or exhaust retries THEN attempt count,
retry timing, HTTP status, SQLState, and vendor code match. `RateLimitRetry=0` and
`HttpConnectionRequestTimeout` retain their current warehouse SEA scope; `HttpMaxConnectionsPerRoute`
sets the same SDK pool limit for both. Presigned links retain their original URL/headers and
existing CloudFetch retry path. Record paired transport evidence and any exception.
- AC31: GIVEN equivalent fixed-schema rows through APC Thrift, warehouse Thrift, warehouse
SEA, and APC SEA under identical client tuning WHEN single and eight concurrent connections
fetch CloudFetch and inline-only results THEN all rows match and the release record reports
median throughput, adjusted latency, peak heap, transfer bytes, and retry/error outcomes.
Against APC Thrift, APC SEA reaches at least 85% throughput, at most 115% adjusted latency,
and at most 125% peak heap or 32 MiB extra, whichever permits more. If warehouse SEA versus
warehouse Thrift breaches a bound, APC may match that measured ratio within 15 percentage
points for throughput/latency and 25 for heap, with the precedent and client impact recorded.
For paired APC SEA and warehouse SEA transport replays, compare equivalent rows while allowing
each route's required wire format. Each route makes no duplicate or unnecessary result download;
bytes transferred beyond one required fetch of each route's page or link are at most 5%
of that route's required result payload bytes. The configured CloudFetch download pool and prefetch window bound concurrency and buffered chunks
independently of total link count; no APC-only cap or full-result buffering appears. An
unexplained breach blocks release. Inline-only dogfood remains pending until its server fix.
- AC32: GIVEN equal 100-set INSERT workloads with a controlled service delay and
`EnableBatchedInserts=1` WHEN APC Thrift legacy, APC SEA fallback, and APC SEA V10 native opt-in
run THEN the release record reports median throughput, adjusted latency, and request counts.
APC fallback sends 100 requests, native V10 sends one, and native throughput is at least 85%
of APC fallback. Record APC fallback's request-count cost against Thrift; an adjusted latency
gap above 15% after accounting for those extra requests requires a documented server or
warehouse precedent and client impact, otherwise release is blocked.
- AC33: GIVEN a successful APC SEA DDL response with neither result schema nor result data WHEN
`Statement.execute`, `executeUpdate`, and `executeLargeUpdate` are called through public JDBC
THEN `execute` returns `false`, `getResultSet()` is `null`, both update methods and the current
update count return `0`, and `getMoreResults()` returns `false` before the update count becomes
`-1`. A `Statement` batch containing such a command records `0` in its input position. The
fake service observes no result fetch. A successful DML response with a reported affected-row
count still returns that count, while a failed statement raises its server error; neither case
is misclassified as a schema-less DDL success (R4, R15).

- AC34: GIVEN the default-off APC SAFE sub-flag exists WHEN APC flag assignment or ramp is
considered THEN deployment evidence shows connector-service returns that sub-flag as `false`
for a partner User-Agent and retains its ordinary value for a non-partner OSS JDBC User-Agent.
The evidence identifies the deployed service version and test region. Without it, assignment
and ramp remain blocked even if AC28 passes (Q9).

- AC35: GIVEN equivalent execution, result fetch, metadata, prepared batch, cancellation,
and error scenarios on APC SEA, APC Thrift, and warehouse SEA WHEN the public JDBC calls and
standard telemetry events are compared at each package gate THEN APC SEA preserves the
warehouse SEA SQLState/vendor-code mapping and event fields for its completed operations;
where APC Thrift differs, the parity matrix identifies the warehouse SEA precedent or server
limit, reason, and client impact. An unexplained missing event, changed error, or unredacted
identifier fails the package gate.

- AC36: GIVEN a public APC primitive query with a manifest-only first result, multiple inline
pages or external links, and no total-count fields WHEN its ResultSet is consumed THEN every
row is returned exactly once and fetch errors propagate before successful completion. GIVEN a
logical type deferred to T20 WHEN the same path is used THEN it fails with a clear unsupported
result type error instead of returning incomplete rows or an incorrectly converted value.
The flag-on live comparison with APC Thrift covers row completeness; warehouse SEA establishes
the fetch and error precedent.

- AC37: GIVEN an APC SEA flag-on connection WHEN public JDBC executes schema-less DDL and
ordinarily closes the statement and connection THEN `execute` reports no ResultSet, the update
count is zero, and close completes without unsupported status requests. Live flag-on staging
compares the DDL outcome and route with APC Thrift and warehouse SEA; the local request capture
verifies target shape and one redacted APC routing/statement event while server TerminateSession
extraction remains pending its binding fix.

## Constraints

- APC default protocol is decided by the dedicated APC flag (R1, default off); after the Arrow
availability prerequisite, an explicit client setting always wins; when it is unset,
`TreatMetadataCatalogNameAsPattern=1` forces Thrift before the flag is consulted. No change to
warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The first public APC execution package owns `docs/`
  and `NEXT_CHANGELOG.md` and carries the user-visible changelog entry. Later stacked PRs
  complete that unreleased feature behind the default-off flag and use `NO_CHANGELOG=true`.
  Do not release a partial stack.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server/Reyder/SEA implementation and universe server packages. Known gaps SC-256188 under
  SC-256186 and SC-254478 are external prerequisites for affected dogfood checks.
- Connector-service partner User-Agent override implementation (Q9); the driver consumes the
  flag value returned by the service.
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

Build seven JDBC PRs in a linear stack and one independent Universe SAFE-definition PR.
Each JDBC package exposes an end-to-end public behavior behind the default-off flag. The
first opens, executes, and ordinarily closes schema-less DDL; the second completes cancellation,
timeout, saved-session close, heartbeat, and routing telemetry. The third exposes a **complete**
primitive query: manifest-only starts, inline and external continuations, and all pages are
handled before public `executeQuery` is enabled. Deferred logical types fail clearly rather
than returning a partial or coerced ResultSet. Later packages complete logical types and
transport parity, prepared execution and batches, synchronous metadata, and asynchronous
metadata plus final release evidence. Each PR targets roughly 1,500 changed lines or less.
The first JDBC package alone owns `docs/` and `NEXT_CHANGELOG.md`; later stacked PRs use
`NO_CHANGELOG=true` for the unreleased feature. No package assigns or ramps the flag.

Live package gates use a flag-on APC staging account and an equivalent APC Thrift run; a
warehouse SEA run establishes the precedent for SEA-only behavior. Record observed rows,
counts, SQLState/vendor code, telemetry, and request/transfer evidence as applicable, plus
build/unit/fake-service results. The listed server dependencies remain externally pending
until the fix is deployed in the test region; they do not delay independent local checks.

| JDBC package | Public live staging gate and comparison | External dependency |
| --- | --- | --- |
| APC DDL open | Connect with flag on/off and explicit override; execute/close schema-less DDL, compare count and routing with APC Thrift and warehouse SEA. | TerminateSession target extraction pending server binding fix. |
| APC lifecycle | Cancel and time out running DDL; close saved session after flag change; inspect heartbeat absence, error and routing telemetry against APC Thrift and warehouse SEA. | Saved-session termination target extraction pending binding fix. |
| APC complete primitive query | Read a multi-page primitive result, including manifest-only first result and inline/external delivery; compare all rows, SQLState, and fetch telemetry with APC Thrift and warehouse SEA. | Inline-only dogfood pending server fix. |
| APC logical types and transport | Compare each AC13 type, retry and rate-limit errors, concurrent CloudFetch/inline resource and transfer gates with APC Thrift and warehouse SEA. | Inline-only dogfood pending server fix. |
| APC prepared execution and batches | Exercise live V8/V10 available paths, DML counts and 100-set INSERT; compare values, requests, errors, and telemetry with APC Thrift and warehouse SEA. | Record unavailable protocol versions as externally pending. |
| APC synchronous metadata | Run seven synchronous SHOW operations and procedure metadata against APC Thrift, checking emitted representation and errors; warehouse SEA supplies request precedent. | Metadata dogfood pending server fix. |
| APC asynchronous metadata and release | Run V9 async SHOW, null-catalog, timeout/cancel, and final cross-route parity; check all remaining AC evidence and partner override. | Metadata and Q9 service evidence pending deployment. |

### Affected Components

- Public JDBC connection, statement, prepared-statement, and metadata APIs.
- SEA request targeting, session and statement identity, result decoding and paging.
- Driver telemetry and error taxonomy; SAFE flag definition in Universe.

### Data Model Changes

- APC compute target and negotiated capability version in session state; APC session and
  internal statement identifiers retain compute identity without changing public raw
  statement IDs.
- SEA result format, Arrow batch and schema fields, and metadata columnar data are represented
  without changing warehouse wire values or accepting unknown result formats as decodable.

### API Changes

The public JDBC methods remain the same. A session-reachable asynchronous metadata API is
added for the seven SHOW operations; its completed handle also works below V9 through the
synchronous path. The public connection ID can encode APC SEA, while the public statement ID
continues to be the raw server ID.

### Risks

- Reyder metadata, inline-only delivery, and TerminateSession targeting require the tracked
  server fixes for dogfood validation. Fake-service contract tests run before those deployments.
- The Q9 partner override is absent from today's connector-service handling for new sub-flags.
  No APC flag assignment or ramp is release-ready until deployment evidence shows partner
  User-Agents receive `false`; T18 records that external gate.
- Bare Arrow batches can lose logical types if reconstructed schema is incomplete. AC13 and Q7
  block release on a mismatch.
- Polling must not repeatedly transfer result payload. AC27 measures this before release.
- APC targeting must never alter presigned CloudFetch URLs or warehouse and Real-time request
  bodies. AC11 and AC14 guard those boundaries.

---

## Tasks

Each task belongs to exactly one package in `plan.json`. A package passes its public live
staging gate (or records the named external dependency) and local contract checks before its
child starts. T18 consolidates evidence; it does not postpone earlier package validation.

### Phase 1 — APC DDL open

- [ ] T1: Add APC compute and SEA result-format support while preserving warehouse wire values (R2, R16).
- [ ] T2: Carry APC targets on session, execution, cancel, and termination calls without changing warehouse requests (R2).
- [ ] T3: Open public APC SEA, execute schema-less DDL, and ordinarily close; target each request and emit redacted APC routing/statement telemetry (R1, R2, R4, R8).
- [ ] T4: Apply Arrow-first APC routing, explicit override, catalog-pattern precedence, and the default-off dedicated flag without changing warehouse routing (R1, R7).
- [ ] T19: Round-trip APC session and internal statement IDs, retain raw public statement IDs, and safely reject malformed forms (R9).
- [ ] T28: Return a zero-count no-ResultSet outcome for successful schema-less DDL without unnecessary fetch; preserve DML counts and errors (R4).
- [ ] T16: Document routing, batch cost, SEA tuning scope, and the user-visible changelog entry; this is the only docs/changelog package.

### Phase 2 — APC lifecycle

- [ ] T6: Preserve selected protocol on connection errors and APC KP001 Thrift failure (R7).
- [ ] T7: Suppress APC result heartbeats and unsupported status calls while retaining execution polling (R3).
- [ ] T12: Apply warehouse SEA timeout behavior with targeted APC cancellation (R14).
- [ ] T15: Complete routing outcomes across APC and warehouse success/failure and redact APC IDs in synchronous/asynchronous diagnostics (R8).
- [ ] T27: Target APC statement cancel and close, including timeout cancellation (R2, R14).
- [ ] T29: Use the current target for recovered statement operations and the saved protocol/target for public saved-session close (R9).

### Phase 3 — Complete primitive query

- [ ] T8: Expose primitive public APC queries with the R4/R10 request shape only after complete fetching and clear rejection of deferred logical types (R4, R8, R11).
- [ ] T10: Decode primitive LZ4 bare Arrow batches, including multiple batches per chunk and external-link streams (R4, R11).
- [ ] T11: Fetch every inline or external continuation, including manifest-only first results, by final `next_chunk_index` and `row_offset` (R12).
- [ ] T25: Target result/chunk GETs once, with null body and unchanged presigned URLs/headers (R2).

### Phase 4 — Logical types and transport

- [ ] T20: Complete the AC13 logical-type matrix from `type_text`/`type_json` without silent coercion (R11, Q7).
- [ ] T26: Establish paired APC/warehouse SDK retry, pool, CloudFetch, and presigned-link parity without APC-only transport controls (R17).

### Phase 5 — Prepared execution and batches

- [ ] T5: Keep the live CreateSession capability version separate from serialized ID diagnostics (R6, R9).
- [ ] T9: Gate native scalar preparation at V8, interpolate below it, and support binding-free pre-execution metadata (R6).
- [ ] T21: Support opt-in V10 native batches, per-set fallback, sequential Statement batches, and correct counts (R6, R15).
- [ ] T22: Reject native complex bindings before requests with tested error name/code and telemetry taxonomy (R6).

### Phase 6 — Synchronous metadata

- [ ] T13: Complete synchronous SHOW and procedure SELECT metadata, with correct request shape and null-catalog fallback (R5).
- [ ] T14: Decode each emitted Arrow IPC or columnar metadata representation with AC16 value/null parity (R5, R13).

### Independent Universe flag package

- [ ] T17: Define the default-off APC SAFE sub-flag under the existing client-config batch flag without assigning or ramping it (R1, Q8).

### Phase 7 — Asynchronous metadata and release

- [ ] T23: Expose seven-operation asynchronous SHOW metadata with the live V9 gate and completed synchronous handles below V9 (R5).
- [ ] T24: Preserve async null-catalog fan-out, errors, timeout, and cancellation (R5).
- [ ] T18: Consolidate per-package live and local evidence and final AC27/AC30–AC32 cross-route gates; keep server/Q9 checks pending until demonstrated (AC1–AC37).
- [ ] T-val: Record pass/fail or externally pending evidence for AC1–AC37, exact commands and release measurements; do not declare release readiness with Q7, Q9, or server-gated checks open.
