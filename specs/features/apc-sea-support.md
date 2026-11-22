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
(`/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`), which returns sub-flags of
`databricks.partnerplatform.clientConfigsFeatureFlags`
(`connector-service/src/featureflags/FeatureFlagsHandler.scala`). It version-maps
`enableSqlExecForJdbc` to `enableSqlExecForJdbc_v3.4.3` for ≥ 3.4.3. For partner User-Agents,
that existing flag may remain true for an allowlisted JDBC partner; other sub-flags currently
pass through unchanged. Q9 requires a separate blanket APC override.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata design tabs). Driver-relevant points:
  - APC requests target `backend_compute.all_purpose_cluster_compute.cluster_id` instead of
  `warehouse_id` (R2). ExecuteStatement requires `session_id`, hybrid disposition, and unset or
  `0s` wait timeout; nonzero wait timeouts and `statements` batches are unsupported. SQL text is
  limited to 16 MB; non-OSS-JDBC clients are rejected (R4, R10, R15).
  - Non-metadata results are LZ4 `ARROW_BATCH` without a schema header. Manifest schema is
  authoritative; optional Arrow schema/count fields can be absent. Fetching follows
  `next_chunk_index` and `row_offset`; a statement result may omit initial result data. There
  is no APC status RPC or non-metadata HIVE result (R3, R4, R11, R12).
  - SHOW metadata requests use `ARROW_STREAM` and operation/sync headers. Reyder returns native
  metadata as Arrow IPC `attachment` or HIVE `columnar_data`, sometimes with
  `COLUMN_BASED_SET` format (wire value 6); decoding follows each response (R5, R13, R16).
  - The live negotiated version gates scalar parameters at V8 (42248), async metadata at V9
  (42249), and native prepared batches at V10 (42250); absent version grants none (R6).
  Timeout follows warehouse SEA; unsupported-region errors propagate without fallback. Rollout
  M15 starts opt-in, then defaults APC to Reyder through the dedicated flag/override (R1, Q8).
- Driver baseline verified against `origin/main` @ `840f3a407` (including approved spec #1809
  and `UseBoundedSeaApi` default change #1827): `DatabricksConnectionContext` routes APC to
  Thrift before the explicit setting or flags; warehouse routing honors them and checks
  AIX/PowerPC Arrow availability after an explicit SEA override. `DatabricksSdkClient` casts
  CreateSession compute to `Warehouse` and builds warehouse-oriented statement requests;
  `getStatementResult` polls `GET /statements/{id}`, ordinary `closeStatement` sends DELETE,
  and `checkStatementAlive` calls `/status`. `DatabricksResultSet` has no decoder without
  result data; `ExecutionResultFactory` lacks SEA `ARROW_BATCH`. Metadata SHOW is synchronous
  and `ResultData` has no `columnar_data` model. SEA CloudFetch and bounded inline paths
  stream results; SDK transport settings differ from `DatabricksHttpClient` (R17).
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the 41-file prototype
baseline, not the design; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756)
is unchanged. The prototype enables APC SEA by default and emits `JDBC_APC_ROUTE` markers.
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
  | `BatchInsertSize` | Caps interpolated multi-row INSERT groups | Caps eligible legacy multi-row INSERT groups | Has no effect on APC fallback (one request per set) or native V10 batches; a configured size no longer limits APC batch request count, so the request-count and latency cost is explicit (R6; AC44). |
  | `EnableStreamingChunkProvider` | Selects the streaming external-link provider when enabled | Also forced by bounded SEA paging | APC external links use a streaming provider regardless of this toggle because totals may be absent; `0` cannot select the legacy provider (R12; AC39). |
  | `EnableInlineStreaming` and `ThriftMaxBatchesInMemory` | Toggle inline streaming and bound its buffered batches | Bounded SEA inline uses a streaming provider and the memory bound | APC inline paging requires streaming even at `EnableInlineStreaming=0`; `ThriftMaxBatchesInMemory` still bounds buffered inline batches (minimum two), while neither property changes external-link concurrency (R12, R17; AC39). |
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
- R18: A partial PR stack must fail closed for APC SEA calls whose result path is not yet
implemented: public query, prepared execution/batches, and metadata calls raise a clear
unsupported-operation SQLException before network execution until their owning slice provides
complete values, errors, and cleanup. A later slice removes each guard only with its own live
functional and non-functional gate. This is a development-stack contract; no partial stack is
released or assigned the APC flag.

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
This separate blanket override requires service deployment before APC flag assignment; partner
enablement needs its own rollout decision. T17 defines only the flag; the driver consumes false.
- Q10: How is repeated transfer avoided during `GetStatementResult` polling?
A10: At least three RUNNING polls of a large query transfer no result payload; terminal
response and fetch return all rows once (AC27).

## Acceptance Criteria

Each client-visible criterion has a package gate under Plan. Until the T17 flag and Q9 service
override are deployed in staging, live APC checks use explicit `UseThriftClient=0`; flag-on checks
remain pending and cannot be marked passed by an override. Server-fix-dependent live checks also
remain pending until deployed; local contract checks still run. No partial stack is released.

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
R5 request shape, JDBC columns, values, and errors match APC Thrift, including alternating
Arrow IPC/`COLUMN_BASED_SET` responses. Async handles preserve terminal, cancel, timeout,
and null-catalog fan-out behavior; below V9 they complete synchronously. Procedure SELECTs
remain ordinary queries and an async procedure request fails before network I/O. Warehouse
metadata requests remain unchanged. Server-fix-dependent live checks remain pending.
- AC6: GIVEN an APC-SEA session with `negotiated_protocol_version` below V8 WHEN a
parameterized PreparedStatement or procedure metadata SELECT is executed THEN safe interpolation
preserves quoted values and delimiters without executing injected SQL, omits `parameters`
entirely, and emits the parameter-downgrade signal. GIVEN a prepared SELECT with bindings WHEN
`getMetaData()` runs before execution at any capability version THEN a binding-free `DESCRIBE
QUERY` quotes only real placeholders, returns JDBC metadata, and leaves SQL and bindings
available for later execution.
- AC7: GIVEN `UseThriftClient=0` and the APC flag ON on AIX/PowerPC with Arrow unavailable WHEN a
connection is opened THEN the Arrow prerequisite takes precedence, the pre-connection routing
decision selects Thrift, and the driver logs the routing reason.
- AC8: GIVEN APC-SEA enabled (R1) AND `EnableQueryResultDownload=0` WHEN a connection is opened
THEN the driver remains on SEA, sets CreateSession session config `can_cloud_download=false`, and
reads inline Arrow without following external links; setting `UseBoundedSeaApi` to either `0` or
`1` does not change APC protocol selection or its `next_chunk_index`/`row_offset` paging behavior.
GIVEN multiple inline pages or a server inline-size rejection THEN all rows arrive or the error
propagates without a successful partial ResultSet. Live inline-only evidence needs the tracked
server fix; local contract checks run first.
- AC9: GIVEN APC-SEA enabled (R1) AND CreateSession returns an "all-purpose clusters are not
supported" error WHEN a connection is opened THEN the driver propagates a clear connection error,
does not create a Thrift client, and records the failure in telemetry.
GIVEN APC was routed to Thrift and OpenSession returns KP001 THEN the warehouse-only recovery
does not switch it to SEA, regardless of whether the APC flag was off or Arrow was unavailable.
- AC10: GIVEN telemetry enabled and APC or warehouse connections selected by each supported
routing reason WHEN each connection attempt succeeds or fails THEN exactly one routing event is
emitted for that attempt with compute type, selected protocol, routing reason, and success or
failure outcome for APC SEA/Thrift and warehouse SEA/Thrift opens. APC logs and telemetry from
sync/async execution, session display, and failures contain no credentials, raw org/cluster id,
or compound IDs. At each package gate, exposed operations retain standard warehouse SEA event
fields; the record identifies any APC Thrift difference and its precedent.
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
THEN JDBC columns/values match APC Thrift. Least-significant-bit-first null masks preserve
positions across byte boundaries; short masks leave later rows non-null and long masks ignore
excess bits. A later Arrow IPC `attachment` on that connection also decodes correctly. This
covers vectors native metadata can emit, without synthetic complex columns or absent masks.
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
APC-prefixed IDs with missing, extra, empty, or invalid segments WHEN parsed THEN they fail
without warehouse fallthrough or disclosure of input/compute identifiers; malformed sessions
use `SESSION_ID_PARSING_EXCEPTION`.
- AC26: GIVEN APC `ARROW_BATCH`, metadata `COLUMN_BASED_SET`, and existing warehouse
`JSON_ARRAY`/`ARROW_STREAM` wire responses WHEN their formats are serialized and decoded THEN
exact wire names and JDBC rows/metadata are preserved; `COLUMN_BASED_SET` selects columnar
metadata even after an `ARROW_STREAM` request. An unknown future format deserializes safely
and fails clearly if result decoding is attempted.
- AC27: GIVEN an APC query on flag-on staging that produces at least three RUNNING
`GetStatementResult` polls WHEN request and response payloads are measured THEN those RUNNING
responses contain no attachment, Arrow batches, columnar data, or external links, and transfer
zero result-payload bytes; the terminal response plus result fetch returns all rows once. Record
poll count, result-payload bytes on RUNNING responses, terminal/fetch bytes, and the same
workload's APC Thrift or warehouse SEA transfer for the primitive-query gate. A local capture
also verifies the exact RUNNING response shape. Explicit-override evidence is provisional until
the flag-on check runs.
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
- AC31: GIVEN the same fixed-schema row count, result size, fetch size, and client settings on
warmed APC Thrift, warehouse Thrift, warehouse SEA, and APC SEA connections WHEN five measured
runs of one and eight concurrent complete-result reads use CloudFetch and inline-only delivery
THEN every row matches and the record reports medians of rows/second and raw wall-clock latency
from `executeQuery` entry through final `ResultSet.next()`; no server-time subtraction or other
latency adjustment is applied. Record peak live heap above the pre-run baseline, transferred
result bytes, retry/error outcomes, and per-route request counts. APC SEA achieves at least 85%
of APC Thrift throughput, at most 115% latency, and at most 125% peak heap or 32 MiB more,
whichever permits more. If warehouse SEA versus warehouse Thrift breaches a bound, compare route
ratios: APC SEA/Thrift throughput must be at least warehouse SEA/Thrift throughput minus 0.15;
APC latency and heap ratios may be at most the corresponding warehouse ratios plus 0.15 and
0.25, respectively, with the precedent and client impact recorded. Count compressed result
payload bytes, excluding HTTP headers: APC and warehouse SEA each transfer at most 5% more
than one required fetch of each page/link's payload; no duplicate download, APC-only concurrency
cap, or full-result heap buffer is allowed. An unexplained breach blocks release. Inline-only
dogfood remains pending until its server fix.
- AC32: GIVEN identical 100-set INSERT values/schema, warmed connections,
`EnableBatchedInserts=1`, and a fake service adding exactly 50 ms before each ExecuteStatement
response WHEN five measured APC Thrift legacy, APC SEA fallback, and APC SEA V10 native opt-in
batch runs complete THEN report median sets/second, raw wall-clock latency from `executeBatch`
entry through returned counts, and request count. For each run, adjusted latency is raw latency
minus `50 ms × ExecuteStatement request count` (floor at zero); take the median only after this
per-run adjustment. APC fallback sends 100 requests, native V10 sends one, native throughput is
at least 85% of fallback, and
`max(0, fallback adjusted - Thrift adjusted) / max(Thrift adjusted, 1 ms) > 0.15`
needs a documented server or warehouse precedent and client impact. Also report unadjusted live staging
latency, throughput, and request counts for the same workload; do not apply the fake-service
adjustment to live data. An unexplained breach blocks release.
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

- AC37: GIVEN an APC SEA connection selected by explicit override or the deployed APC flag
WHEN public JDBC executes schema-less DDL that remains RUNNING for at least one poll and
ordinarily closes the statement and connection THEN `execute` reports no ResultSet, the update
count is zero, polling GET and CloseStatement use the APC target, and close sends no `/status`
request. Compare the live DDL outcome with APC Thrift and warehouse SEA; capture the GET and
DELETE target/body shape and redacted routing/statement event locally. Flag-on evidence remains
pending until T17 and the Q9 override are deployed; TerminateSession extraction remains pending
its tracked binding fix.

- AC38: GIVEN a primitive APC query on staging selected by explicit override or the deployed
flag WHEN single and eight concurrent complete-result reads run on warmed connections THEN
rows, ordering, SQLState/vendor codes, a retryable fetch failure, throughput, raw latency, and
peak heap are compared with APC Thrift and warehouse SEA. Use AC31's five-run measurement window,
workload, and 85%/115%/125%-or-32-MiB bounds for CloudFetch primitive rows. A failure or missing
live measurement keeps the primitive-query gate provisional; inline-only and flag-on checks
remain pending until their named server and rollout prerequisites are deployed.
- AC39: GIVEN APC inline pages and external links WHEN `EnableStreamingChunkProvider` and
`EnableInlineStreaming` are each set to `0` and `1`, and `ThriftMaxBatchesInMemory` is set to
`1` and `3`, THEN row completeness and peak buffered inline batches follow the parity-matrix
dispositions; disabling either streaming toggle cannot truncate a result, and the effective
minimum two-batch buffer is recorded.
- AC40: GIVEN a partial stack checkpoint and an APC SEA connection selected explicitly WHEN
public query, prepared, batch, or metadata APIs not yet owned by that checkpoint are called
THEN each fails with a clear unsupported-operation SQLException before ExecuteStatement or
result fetch; already completed operations retain their results, errors, and close behavior.
Each owning package removes only its corresponding guard and repeats this check for the others.
- AC41: GIVEN a recovered statement or saved APC connection after a flag change WHEN fetch,
cancel, close, or termination runs THEN recovered statements use the current connection's APC
target and saved-session close uses the saved protocol and target.
- AC42: GIVEN RUNNING APC DDL and ordinary statement close WHEN requests are captured THEN the
statement GET and CloseStatement DELETE carry the encoded APC query target exactly once and no
request body, and no unsupported `/status` call occurs. The public DDL call and close complete
on staging with explicit SEA override; flag-on repetition follows AC37's prerequisite.
- AC43: GIVEN the seven synchronous APC SHOW operations and procedure metadata SELECTs WHEN
called through public JDBC on staging THEN the SHOW results, emitted Arrow IPC or columnar
representation, null-catalog fallback, procedure bindings, errors, and JDBC columns/values
match APC Thrift; warehouse SEA establishes the request precedent. Local captures verify R5
headers and per-response decoding. Server-fix-dependent live cases remain pending until deployed.
- AC44: GIVEN identical prepared INSERT batches with `BatchInsertSize=1` and `1000` WHEN APC
fallback and native V10 paths run THEN fallback sends one request per set and native sends one
request for the batch at both settings; counts and errors are unchanged. The release record
reports the lost multi-row sizing effect and its latency/request-count impact against APC Thrift.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). Only the first JDBC package owns `docs/` and
  `NEXT_CHANGELOG.md`; later PRs use `NO_CHANGELOG=true` for the unreleased feature. Do not
  release a partial stack.
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

Build eight reviewable JDBC PRs in a linear stack and one independent Universe flag PR. Eight
keeps single prepared execution, prepared batching, Statement batching/lifecycle, and each
metadata mode independently reviewable near the 1,500-line target. The first slice owns public
schema-less DDL, including RUNNING polling GET, ordinary CloseStatement, and unsupported-call
guards (R18). Later slices remove only the guards for behavior they complete. A checkpoint may
advance provisionally after local contract and explicit-override live checks, but it is not a
passed, shippable gate while flag-on or server-dependent evidence is pending. No partial stack
is released, assigned the flag, or represented as APC parity.

T17 defines a default-off flag independently. Flag-on staging gates require T17 deployed, a
staging-only flag assignment, and deployment evidence for Q9's blanket connector-service
partner override (AC34). Until then, use `UseThriftClient=0` for live APC SEA versus APC Thrift
and warehouse SEA comparisons, record flag-on as pending, and continue independent work.
After those prerequisites, repeat each flag-on gate before release; flag-off checks remain
mandatory. Named Reyder fixes likewise leave affected live checks pending until deployed.
Each package records rows/counts, SQLState/vendor code, standard telemetry, request/transfer
shape, and non-functional data relevant to its exposed behavior; T18 consolidates the record.

| JDBC slice | Live gate against APC Thrift and warehouse SEA | Pending prerequisite |
| --- | --- | --- |
| DDL open | Poll a RUNNING schema-less DDL, report zero count, ordinarily close, and verify route/error/target; unsupported public calls fail closed (AC33, AC37, AC40, AC42). | Flag-on needs T17/Q9; live TerminateSession target needs binding fix. |
| DDL lifecycle and Statement batch | Cancel/time out DDL, close saved session after flag change, run ordered DDL batch, and compare errors/telemetry (AC18, AC22, AC41). | Flag-on needs T17/Q9; saved-session target needs binding fix. |
| Primitive query | Read every inline/external page and manifest-only start; compare rows, retries, transfer, five-run CloudFetch throughput/latency/heap, and ≥3 RUNNING polls (AC27, AC36, AC38). | Flag-on needs T17/Q9; inline-only dogfood needs server fix. |
| Logical types and transport | Compare AC13 types, SDK retry/pool effects, tuning settings, and full concurrent CloudFetch/inline parity (AC30, AC31, AC39). | Flag-on needs T17/Q9; inline-only dogfood needs server fix. |
| Single prepared execution | Compare V8 scalar, below-V8 interpolation, pre-execution metadata, and complex-binding rejection (AC6, AC23, AC29). | Flag-on needs T17/Q9; unavailable live V8 remains pending. |
| Prepared batch | Compare ordered fallback/native V10 counts and 100-set cost (AC17, AC32, AC44). | Flag-on needs T17/Q9; unavailable live V10 remains pending. |
| Synchronous metadata | Compare seven SHOW and procedure SELECT values/errors, Arrow IPC/columnar representations, and null-catalog fallback (AC16, AC43). | Flag-on needs T17/Q9; metadata dogfood needs server fix. |
| Asynchronous metadata/release | Compare V9 SHOW, null-catalog fan-out, timeout/cancel, and rerun every previously pending flag-on gate (AC5, AC34, AC35). | Metadata and partner-service deployment evidence must close before release. |

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
- The Q9 blanket partner override is absent from today's connector-service handling for new
  sub-flags. No APC flag assignment, including staging flag-on gates, precedes its deployment;
  T18 records the external evidence.
- Bare Arrow batches can lose logical types if reconstructed schema is incomplete. AC13 and Q7
  block release on a mismatch.
- Polling must not repeatedly transfer result payload. AC27 measures this before release.
- APC targeting must never alter presigned CloudFetch URLs or warehouse and Real-time request
  bodies. AC11 and AC14 guard those boundaries.

---

## Tasks

Each task belongs to exactly one package in `plan.json`. An exposed behavior gets local and
explicit-override live evidence before its child starts. A missing external prerequisite is
recorded as pending, never passed; T18 closes those gates before release.

### Phase 1 — APC DDL open

- [ ] T1: Add APC compute and SEA result-format support while preserving warehouse wire values (R2, R16).
- [ ] T2: Target APC session, execution, polling GET, cancel, ordinary close, and termination calls without changing warehouse requests (R2).
- [ ] T3: Open public APC SEA, poll/execute schema-less DDL, ordinarily close, and emit redacted APC routing/statement telemetry (R1, R2, R4, R8).
- [ ] T4: Apply Arrow-first APC routing, explicit override, catalog-pattern precedence, and the default-off dedicated flag without changing warehouse routing (R1, R7).
- [ ] T19: Round-trip APC session and internal statement IDs, retain raw public statement IDs, and safely reject malformed forms (R9).
- [ ] T28: Return a zero-count no-ResultSet outcome for successful schema-less DDL without unnecessary fetch; preserve DML counts and errors (R4).
- [ ] T6: Preserve selected protocol on connection errors and APC KP001 Thrift failure (R7).
- [ ] T31: Guard public APC SEA calls not yet supported by the partial stack before network execution (R18).
- [ ] T16: Document routing, batch cost, SEA tuning scope, and the user-visible changelog entry; this is the only docs/changelog package.

### Phase 2 — DDL lifecycle and Statement batch

- [ ] T12: Apply warehouse SEA timeout behavior with targeted APC cancellation (R14).
- [ ] T15: Complete routing outcomes across APC and warehouse success/failure and redact APC IDs in synchronous/asynchronous diagnostics (R8).
- [ ] T27: Verify targeted cancel/close on timed-out, failed, and repeated-close DDL using T2's target shape; do not duplicate ordinary-close targeting (R2, R14).
- [ ] T29: Use the current target for recovered statement operations and the saved protocol/target for public saved-session close (R9).
- [ ] T30: Execute Statement SQL batches sequentially, preserving order, counts, and stop-on-error without `statements` (R15).

### Phase 3 — Complete primitive query

- [ ] T7: Suppress APC result heartbeats and unsupported status calls while retaining execution polling (R3).
- [ ] T8: Remove the primitive-query guard only with complete R4/R10 fetching and clear rejection of deferred logical types (R4, R8, R11, R18).
- [ ] T10: Decode primitive LZ4 bare Arrow batches, including multiple batches per chunk and external-link streams (R4, R11).
- [ ] T11: Fetch every inline or external continuation, including manifest-only first results, by final `next_chunk_index` and `row_offset` (R12).
- [ ] T25: Target result-chunk GETs once, with null body and unchanged presigned URLs/headers; polling GET was completed in T2 (R2).

### Phase 4 — Logical types and transport

- [ ] T20: Complete the AC13 logical-type matrix from `type_text`/`type_json` without silent coercion (R11, Q7).
- [ ] T26: Establish paired APC/warehouse SDK retry, pool, CloudFetch, and presigned-link parity without APC-only transport controls (R17).

### Phase 5 — Single prepared execution

- [ ] T5: Keep the live CreateSession capability version separate from serialized ID diagnostics (R6, R9).
- [ ] T9: Gate native scalar preparation at V8, interpolate below it, and support binding-free pre-execution metadata (R6).
- [ ] T22: Reject native complex bindings before requests with tested error name/code and telemetry taxonomy (R6).

### Phase 6 — Prepared batches

- [ ] T21: Support opt-in V10 native batches and ordered per-set fallback with correct counts; Statement batches are owned by T30 (R6).

### Phase 7 — Synchronous metadata

- [ ] T13: Complete synchronous SHOW and procedure SELECT metadata, with correct request shape and null-catalog fallback (R5).
- [ ] T14: Decode each emitted Arrow IPC or columnar metadata representation with AC16 value/null parity (R5, R13).

### Independent Universe flag package

- [ ] T17: Define the default-off APC SAFE sub-flag under the existing client-config batch flag without assigning or ramping it (R1, Q8).

### Phase 8 — Asynchronous metadata and release

- [ ] T23: Expose seven-operation asynchronous SHOW metadata with the live V9 gate and completed synchronous handles below V9 (R5).
- [ ] T24: Preserve async null-catalog fan-out, errors, timeout, and cancellation (R5).
- [ ] T18: Consolidate per-package live and local evidence, repeat flag-on gates after T17/Q9 deployment, and close final cross-route checks (AC1–AC44).
- [ ] T-val: Record pass/fail or externally pending evidence for AC1–AC44, exact commands and release measurements; do not declare release readiness with Q7, Q9, or server-gated checks open.
