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
  with neither schema nor result data is therefore not yet a proven live server response. T32
  supplies a successful no-result contract (including a zero-column manifest) before the DDL
  live gate can pass.
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
  | Result values and paging | Arrow/CloudFetch or inline; complete JDBC rows | Arrow stream/CloudFetch or bounded inline | Same JDBC values and completeness; inline APC payloads are LZ4 bare `ARROW_BATCH` values, while external links contain complete Arrow IPC streams; both use APC offset paging (R4, R11, R12). |
  | `Statement.setMaxRows` | Sends the statement row limit and stops at the limit | Sends SEA `row_limit` | APC sends `row_limit` and exposes at most that many rows across inline pages or links; zero means unlimited (R4; AC49). |
  | `EnableSQLExecHybridResults=0` with CloudFetch | No SEA disposition | Requests `EXTERNAL_LINKS` rather than hybrid inline/external results | Requests `INLINE_OR_EXTERNAL_LINKS` regardless of the setting: APC contract requires this disposition. External links may still be returned; callers cannot force external-only APC results (R4; AC4). Documented exception. |
  | `EnableQueryResultDownload=0` | Inline Thrift results | Bounded inline Arrow when available; otherwise routing may select Thrift | Remains on SEA, sets `can_cloud_download=false`, and consumes all inline pages. Reyder must honor inline-only delivery despite the hybrid disposition; an inline-size rejection is returned to the caller rather than silently using CloudFetch (R4; AC8). Documented exception. |
  | Metadata | Native Thrift RPCs | SEA SHOW queries and native-shaped responses | Same JDBC columns and rows; SHOW uses Arrow IPC or `columnar_data` because Reyder chooses either representation (R5, R13). |
  | Scalar parameters | Native where protocol permits; otherwise driver behavior | SEA native bindings or existing interpolation | Native at V8+, interpolation below V8; APC protocol gate is a server capability limit. Complex values fail before an APC request at every version because neither native APC binding nor existing interpolation defines lossless complex literals (R6). |
  | Prepared batches | Native Thrift executor when supported and opted in; otherwise legacy path | Native SEA executor when opted in; otherwise legacy path | Native `parameter_sets` only at V10+ and opt-in; otherwise one request per set. Aggregate-only APC native response reports `SUCCESS_NO_INFO` rather than per-set counts (R6). |
  | Arrow prerequisite | APC Thrift works without Arrow | Explicit `UseThriftClient=0` is considered before the warehouse Arrow check | APC requires Arrow before even an explicit SEA override, because Reyder has no HIVE non-metadata result path; AIX/PowerPC users without Arrow remain on Thrift (R1; AC7). |
  | Result heartbeat | Thrift can check statement status | SEA result heartbeat uses `/status` | APC schedules no result heartbeat because Reyder does not support `GetStatementStatus`; long-lived completed ResultSets have no periodic liveness check (R3; AC24). |
  | `UseBoundedSeaApi=0` | Not used by Thrift | Changes the warehouse SEA inline/provider path | APC still uses `ARROW_BATCH` and `next_chunk_index`/`row_offset` paging because Reyder's wire contract requires it; the setting cannot disable APC paging (R4, R12; AC8). |
  | `EnableBatchedInserts=1` without APC native batching | Eligible sets may be combined into a multi-row INSERT | Legacy SEA may combine sets; native batching is separately opted in | APC executes each set individually (Phase II DD2/DD3 and Q4), preserving counts and stop-on-error but increasing request count and potentially lowering throughput; V10 native mode restores one request for the batch (AC17, AC32). |
  | `BatchInsertSize` | Caps interpolated multi-row INSERT groups | Caps eligible legacy multi-row INSERT groups | Has no effect on APC fallback (one request per set) or native V10 batches; a configured size no longer limits APC batch request count, so the request-count and latency cost is explicit (R6; AC44). |
  | `QueryResultCompressionType` / `EnableQueryResultLZ4Compression` | Requests the configured compression on Thrift | Sends the configured compression on SEA requests | Reyder always returns LZ4 APC results. APC requests use LZ4 and the decoder follows the response; values selecting another codec cannot change APC wire compression. This can raise client decompression CPU but reduce transferred bytes versus an uncompressed choice; measure both (R4; AC45). Documented server-contract exception. |
  | `EnableStreamingChunkProvider` | Selects the streaming external-link provider when enabled | Also forced by bounded SEA paging | APC external links use a streaming provider regardless of this toggle because totals may be absent; `0` cannot select the legacy provider (R12; AC39). |
  | `EnableInlineStreaming` and `ThriftMaxBatchesInMemory` | Toggle inline streaming and bound its buffered batches | Bounded SEA inline uses a streaming provider and the memory bound | APC inline paging requires streaming even at `EnableInlineStreaming=0`; `ThriftMaxBatchesInMemory` still bounds buffered inline batches (minimum two), while neither property changes external-link concurrency (R12, R17; AC39). |
  | Timeouts, cancel, and errors | Client timeout and Thrift cancel; JDBC errors | `TimeoutHandler`, SEA cancel, SEA error mapping | Follow warehouse SEA timeout, retry, rate-limit, SQLState, and vendor-code behavior; APC target is added to cancel (R14, R17). |
  | Statement GET retry settings | `DatabricksHttpClient` uses `DatabricksHttpRetryHandler` and `RateLimitRetry` | SDK `ApiClient` uses its own retry policy; `RateLimitRetry` and `HttpConnectionRequestTimeout` are not wired to that SDK transport today | Use the same SDK policy as warehouse SEA for APC statement/result GETs; `RateLimitRetry=0` does not disable either SEA route's SDK retries, and `HttpConnectionRequestTimeout` does not govern either SEA route's SDK pool wait. Document this warehouse precedent and test identical APC/warehouse behavior (R17; AC30). |
  | `HttpMaxConnectionsPerRoute` | Limits per-route HTTP connections | Limits the SDK pool's per-route connections | Preserve the SDK pool limit on APC; under saturation, raising it must permit more concurrent SDK requests up to `HttpConnectionPoolSize`, without an APC-only cap (R17; AC30, AC46). |
  | `HttpConnectionPoolSize` | Does not size `DatabricksHttpClient`'s fixed-total pool | Sizes the SDK statement/result request pool | Preserve the SDK total pool limit for APC statement/result/link GETs; it does not size the separate presigned-link download pool (R17; AC46). |
  | `ChunkReadyTimeoutSeconds` | Bounds the wait for a downloaded chunk | Bounds waits in SEA inline and external streaming providers | Apply the configured wait to APC inline and external chunks; an exceeded wait reports the existing chunk-ready error, with no incomplete successful ResultSet (R17; AC46). |
  | `CloudFetchSpeedThreshold` | Sets the slow-download warning threshold | Sets the same external-link warning threshold | Preserve warning behavior for APC external links; this setting does not throttle downloads, change rows, or change retry policy (R17; AC46). |
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
on result-chunk GETs. Those GETs and CloseStatement DELETE have no request body. Presigned
external-link URLs and headers are unchanged. Warehouse standard and Real-time request
serialization remains unchanged. Dogfood target extraction for TerminateSession depends on the
tracked server binding fix.
- R3: For APC over SEA, execution polling uses `GetStatementResult`. Result heartbeats are
disabled for APC even when `EnableHeartbeat=1`, because `GetStatementStatus` is unsupported and
using full `GetStatementResult` after completion can re-download result payloads. The driver logs
that heartbeat is unsupported and never schedules `checkStatementAlive` or calls `/status`.
- R4: APC non-metadata execution sends `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`,
and a `session_id`, never sends `statements`, and handles LZ4-compressed inline bare Arrow batches
in `chunk.arrow_batches` (including multiple batches per chunk). CloudFetch external links contain
complete Arrow IPC streams; decode them through the streaming external-link path, preserving their
schema/framing and link headers, rather than as bare batches. A linked result must drain completely.
`EnableQueryResultDownload=0` keeps the connection on SEA but
sets the CreateSession `can_cloud_download=false` session config. With the assumed server fix,
results must remain inline even though
the request disposition is `INLINE_OR_EXTERNAL_LINKS`. The driver follows every inline
page until the server indicates completion. If the server cannot serve the complete result inline,
the driver propagates the server error and must not expose a successfully completed, truncated
ResultSet. APC compute type—not
`isBoundedSeaApiEnabled`—selects this format/disposition and enables the session-config behavior.
For positive `Statement.setMaxRows`, send SEA `row_limit` and expose no more than that many rows
across inline pages or external links; zero remains unlimited. Match APC Thrift and warehouse SEA.
For a successful statement with no result columns or data (including a zero-column manifest), the driver
reports no ResultSet: `Statement.execute` returns `false`, `getResultSet()` returns `null`, and
`executeUpdate`/`executeLargeUpdate` and the current update count return `0`. `getMoreResults()`
then returns `false` and changes the update count to `-1`. Sequential `Statement` batches record
`0` for each such command. Once the APC result reader is available, a successful DML response
with an affected-row count reports that count; absent or zero-column schema must not cause a
result fetch or decoding error.
Server failures still propagate. Live polled DDL depends on T32's server contract.
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
format or a connection-level choice. Every native SHOW response sets
`manifest.is_native_metadata_result=true` in either physical format; the driver retains this
field so the existing native metadata column conversion runs.

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
version cannot grant capabilities. Complex bindings (ARRAY, MAP, STRUCT, and other complex
logical types) fail before any request at every negotiated version, including below-V8 and
missing-version sessions and fallback batches, with a tested driver-owned error name/code and
taxonomy classification. Do not pass complex values through the existing interpolator: its
generic `toString()` rendering does not define valid, lossless SQL literals. Below V8, each
scalar-only prepared execution, including each fallback batch set, uses the existing safe SQL
interpolation and omits the `parameters` property; a missing version has a
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
- R17: APC uses warehouse SEA's SDK retry policy and pool for statement/result/link
GETs and its separate streaming CloudFetch download path for presigned links.
`HttpMaxConnectionsPerRoute` and `HttpConnectionPoolSize` set SDK per-route and total
limits through `ClientConfigurator`; neither sizes the presigned-link pool.
`RateLimitRetry` and `HttpConnectionRequestTimeout` apply to
`DatabricksHttpClient`, not SDK GET retry or pool wait on either SEA route.
Preserve the matrix's remaining setting scopes, including chunk-ready wait,
slow-download warning, link prefetch, streaming, and synchronous metadata headers.
No APC-only retry policy, concurrency cap, or full-result buffer is allowed.
Reyder always returns LZ4 for APC; request LZ4 and decode the returned codec
regardless of compression settings, while warehouse SEA remains configurable.
A measured APC Thrift difference without a server limit or warehouse SEA precedent
must be corrected or documented with client impact before release (AC30–AC32, AC45–AC46).
- R18: Until the integration package activates R1, production APC routing remains on Thrift,
including explicit `UseThriftClient=0`. A package-local test route exercises completed slices
through public JDBC APIs end to end; known incomplete prepared, batch, and metadata calls on
that test route fail before network execution. An unsupported query result shape cannot be known
until the response arrives and must fail clearly before reporting successful completion or
silently omitting rows. Each slice removes only its own test-route guard
after its functional and non-functional gate. No partial package exposes this route through a
public connection setting or receives an APC flag assignment.

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

Before activation, package gates use public JDBC calls through a package-local SEA test route;
production APC routing stays on Thrift. After the stack integrates, staging checks use explicit
`UseThriftClient=0` and the deployed flag. Flag-on checks remain pending until T17 and Q9 deploy;
server-dependent checks remain pending until their fixes deploy. Local evidence does not pass a
missing live gate. A package's `plan.json` acceptance list identifies the contract it implements;
its completion gate is the package-local live staging comparison stated in that package's notes.
Flag-on clauses are separate T18 release gates, not evidence that a pre-activation slice passed.

- AC1: On flag-off, absent-flag, lookup-failure, partner-overridden-false, or legacy
`cliservice` APC staging connections, public JDBC uses Thrift and sends no SEA request even
if the warehouse flag is ON. With the APC flag ON and Arrow available, it uses SEA. The
legacy path never sends placeholder `default` ids.
- AC2: GIVEN a cluster HTTP path with APC-SEA enabled (R1) WHEN a connection is opened THEN the
CreateSession request body contains `backend_compute.all_purpose_cluster_compute.cluster_id`
equal to the URL's cluster id and contains no `warehouse_id`.
- AC3: Capture APC SEA execute, poll, fetch, cancel, ordinary close, and termination
requests on staging: each has R2's target; GETs and CloseStatement DELETE have no body
and one encoded cluster query field; TerminateSession DELETE carries the target body.
No `/status` call occurs, and warehouse fixtures remain unchanged. Live termination
extraction waits for its server binding fix.
- AC4: Public JDBC drains inline LZ4 bare batches and complete linked Arrow IPC
streams, including multi-batch chunks and large files, with APC Thrift row/value/metadata
parity on staging. Sync and async requests use R4's format, disposition, session id,
and no `statements` field even at `EnableSQLExecHybridResults=0`.
- AC5: GIVEN the seven R5 SHOW operations run synchronously WHEN Arrow IPC and
`columnar_data` responses alternate THEN native-flag conversion, JDBC rows, errors, timeout,
and null-catalog behavior match APC Thrift on flag-on staging. Procedure SELECTs remain ordinary
queries; warehouse metadata requests remain unchanged. AC50 covers the async API.
- AC6: GIVEN an APC session below V8 WHEN prepared SQL or a procedure metadata SELECT binds values
THEN safe interpolation preserves quoting and omits `parameters`. WHEN `getMetaData()` runs
before prepared execution at any version THEN binding-free `DESCRIBE QUERY` returns JDBC
metadata without changing the SQL or bindings. On flag-on staging, compare pre-execution column
names, JDBC types, and unchanged later execution bindings with APC Thrift and warehouse SEA; test
below-V8 or missing-version interpolation through the package-local route if staging is V8+.
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
- AC10: GIVEN each APC/warehouse SEA/Thrift route succeeds or fails WHEN telemetry is captured THEN
one route event per attempt identifies compute, protocol, reason, and outcome. Execution and
session diagnostics retain standard SEA fields and redact credentials, org/cluster ids, and
compound ids.
- AC11: GIVEN standard and Real-time SQL warehouse connections WHEN CreateSession, execute,
poll, fetch, cancel, close, and TerminateSession run THEN serialized request bodies, URLs, and
headers match pre-change fixtures, including warehouse `realtimeSession` behavior.
- AC12: GIVEN an APC-SEA session WHEN a statement is executed with and without direct results
THEN `wait_timeout` is unset and `0s` respectively, Reyder accepts both request shapes, and
`statements` is never sent. GIVEN an asynchronous APC execution with direct results enabled WHEN
the request is captured THEN `wait_timeout` is `0s`, not an omitted or nonzero duration.
- AC13: With empty or populated `manifest.arrow_schema` and `total_byte_count`,
read APC `ARROW_BATCH` values and JDBC metadata for primitive, decimal, array, map,
struct, interval, geography, geometry, timestamp, timestamp_ntz, and variant columns.
Compare each live staging query with APC Thrift and warehouse SEA; an unavailable type
remains a named pending live gate backed by a contract fixture, never an assumed pass.
- AC14: GIVEN inline batches or multiple external links with missing totals WHEN APC pages are
read THEN all rows arrive exactly once using `next_chunk_index` and `row_offset` (the sum of
inline batch counts or final link offset plus count). A large linked Arrow IPC stream is read
completely through the external-link provider, independently of inline bare-batch decoding. Each
result GET has one encoded APC target and `row_offset`; presigned URLs and headers remain unchanged.
- AC15: GIVEN a completed APC statement WHEN `GetStatementResult` returns a manifest without
result data, and separately WHEN it returns a manifest with result data THEN both produce the
full correct ResultSet.
- AC16: GIVEN an APC native SHOW response with `columnar_data` WHEN its supported boolean, numeric,
string, or binary vectors and least-significant-bit null masks are decoded THEN JDBC values
match APC Thrift; short masks leave later rows non-null and long masks ignore extra bits.
A subsequent Arrow IPC response on the same connection also decodes correctly.
- AC17: At live V10 with `EnableNativeBatching=1`, an INSERT batch sends one ordered
`parameter_sets` request, no top-level `parameters`, and returns `SUCCESS_NO_INFO` per
set on aggregate success without claiming partial success on failure. Otherwise it sends
one request per set, with V8+ scalar bindings or below-V8 interpolation, ordered counts,
stop-on-first-failure, and no multi-row rewrite. Compare with APC Thrift on staging.
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
- AC22: A three-command public `Statement.addBatch(String)` on staging sends
sequential ExecuteStatement requests without `statements` and returns counts in order.
If command two fails, command three is not sent and `BatchUpdateException` contains the
first count. Compare counts, SQLState/vendor code, and request count with APC Thrift;
warehouse SEA supplies the wire precedent.
- AC23: GIVEN CreateSession omits `negotiated_protocol_version` WHEN a prepared query or batch
runs THEN capability is below V8 even if an ID encodes a version: safe interpolation works with
`supportManyParameters=0`, each batch set is separate, no `parameters` or `parameter_sets`
property is sent, and telemetry records the missing-version downgrade. AC6 covers metadata.
- AC24: GIVEN an APC-SEA connection with `EnableHeartbeat=1` WHEN a completed ResultSet remains
open beyond multiple heartbeat intervals THEN no heartbeat task is scheduled, no
`GetStatementStatus`, `/status`, or heartbeat `GetStatementResult` request is sent, and the driver
logs that heartbeat is unsupported for APC. Keep a completed ResultSet open on the package-local
staging route for at least two configured intervals, capture requests, and compare the lack of
APC status traffic with APC Thrift and warehouse SEA.
- AC25: GIVEN APC session/statement IDs WHEN serialized and parsed THEN R9's grammar, public raw
statement ID, compound connection ID, and warehouse/Thrift compatibility hold; an embedded
version grants no capability. Malformed APC-prefixed IDs fail without warehouse fallthrough or
identifier disclosure, using `SESSION_ID_PARSING_EXCEPTION` for sessions.
- AC26: GIVEN APC `ARROW_BATCH`, metadata `COLUMN_BASED_SET`, and existing warehouse
`JSON_ARRAY`/`ARROW_STREAM` wire responses WHEN their formats are serialized and decoded THEN
exact wire names and JDBC rows/metadata are preserved; `COLUMN_BASED_SET` selects columnar
metadata even after an `ARROW_STREAM` request. An unknown future format deserializes safely
and fails clearly if result decoding is attempted. The core verifies wire values; synchronous
metadata closes the full decoding criterion with flag-on staging and APC Thrift comparison.
- AC27: GIVEN a package-local and later flag-on staging APC query with at least three RUNNING result polls WHEN payload
bytes are measured THEN RUNNING responses transfer zero result bytes and terminal/fetch calls
return all rows once. Record poll count, bytes, and APC Thrift/warehouse SEA comparison;
package-local proof closes the slice and flag-on proof remains a T18 release gate.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` and its default is OFF.
Configuration evidence records the exact name, parent batch flag, and default value.
- AC29: GIVEN APC bindings include ARRAY, MAP, STRUCT, or another complex value at V8+, below V8,
or with no live version WHEN single or batch execution starts THEN the same tested driver-owned
error name/code appears before any request, including a complex value after scalar batch sets.
Cover `Types.ARRAY` and `Types.STRUCT`; no complex value reaches native binding or interpolation.
Scalar values still bind natively at V8+ and interpolate below V8.
- AC30: GIVEN paired APC and warehouse SEA SDK GETs receive retryable 429/503 responses WHEN
retries succeed or exhaust THEN attempts, timing, status, SQLState, and vendor code match.
`RateLimitRetry=0` and `HttpConnectionRequestTimeout` retain warehouse SEA scope;
`HttpMaxConnectionsPerRoute` sets the SDK pool's per-route limit while
`HttpConnectionPoolSize` sets its total limit. Under a saturated paired SDK workload, changing
the per-route limit changes concurrent in-flight requests up to the total limit on APC and
warehouse SEA alike. Presigned links keep URL/headers. Induce 429 and 503 on result GETs through
a controlled staging fault injector on the package-local APC route and warehouse SEA; compare
with APC Thrift's retry/error outcome. If staging cannot induce either status, keep that live
case pending and retain local transport fixtures.
- AC31: GIVEN fixed schema, row count, result size, fetch size, and identical settings on warmed APC Thrift, warehouse
Thrift, warehouse SEA, and APC SEA WHEN five complete-result reads run at one and eight
connections for linked and inline-only results THEN rows match. Report median rows/second,
unadjusted `executeQuery`-to-final-`next()` latency, peak live heap above baseline, compressed
payload bytes, retries/errors, and request counts. Between two five-run windows, each route's
median must vary by at most 5%. APC SEA needs ≥85% of APC Thrift throughput, ≤115% latency, and
≤125% heap or 32 MiB extra, whichever permits more; a warehouse SEA/Thrift breach permits an
APC ratio within 5% relative of that warehouse ratio. APC and warehouse SEA transfer ≤5% more
payload bytes than one required fetch per page/link. Unexplained excess, an APC-only concurrency
cap, or full-result buffering blocks release; a server exception must state client impact.
Inline-only live proof waits for its server fix.
- AC32: GIVEN the same 100-set INSERT, warmed connections, and `EnableBatchedInserts=1` WHEN
five APC Thrift legacy, APC SEA fallback, and V10 native opt-in runs execute THEN report median
sets/second, raw `executeBatch` latency, and request count. A fake service adds 50 ms per
ExecuteStatement; subtract `50 ms × request count` per run before taking adjusted medians.
Fallback sends 100 requests, native V10 sends one and reaches ≥85% of fallback throughput. An
adjusted fallback latency >115% of Thrift needs a documented precedent or server limit and client
impact. On live staging, report unadjusted values versus APC Thrift and warehouse SEA: native
V10 needs ≥85% of Thrift throughput and ≤115% latency, with the AC31 warehouse-ratio exception.
An unexplained breach blocks release; unavailable live V10 evidence stays pending.
- AC33: GIVEN a successful APC SEA DDL response with neither result schema nor result data WHEN
`Statement.execute`, `executeUpdate`, and `executeLargeUpdate` are called through public JDBC
THEN `execute` returns `false`, `getResultSet()` is `null`, both update methods and the current
update count return `0`, and `getMoreResults()` returns `false` before the update count becomes
`-1`. The same result follows a zero-column manifest; no result fetch is sent. A failed statement
raises its server error. DML counts wait for the query reader (AC48); Statement batch counts are AC22.

- AC34: GIVEN the default-off APC SAFE sub-flag exists WHEN APC flag assignment or ramp is
considered THEN deployment evidence shows connector-service returns that sub-flag as `false`
for a partner User-Agent and retains its ordinary value for a non-partner OSS JDBC User-Agent.
The evidence identifies the deployed service version and test region. Without it, assignment
and ramp remain blocked even if AC28 passes (Q9).

- AC35: GIVEN equivalent completed calls across APC SEA, APC Thrift, and warehouse SEA WHEN live
errors and telemetry are compared at each release gate THEN APC SEA retains warehouse SEA
SQLState/vendor codes and event fields. Every APC Thrift difference has a matrix precedent or
server limit, reason, and client impact; missing events or identifiers fail the gate.
- AC36: A staging public query with manifest-only start and inline or linked
continuations returns all rows once; fetch failures propagate and unsupported logical
types fail before false success. Compare with APC Thrift and warehouse SEA.
- AC37: After T32 deploys, public JDBC DDL with a RUNNING poll on the package-local
route matches AC33 and APC Thrift; repeat through explicit override and the deployed
flag after integration. Warehouse SEA supplies the request precedent. The core live gate
and later flag-on gate remain pending until their respective prerequisites deploy.
- AC38: After integration, run a primitive public query and retryable fetch
failure through explicit override and then the deployed flag on staging. Rows, order,
SQLState, and vendor code match APC Thrift with warehouse SEA as error precedent; AC31
owns performance. Server-dependent inline checks remain pending until deployed.
- AC39: GIVEN APC inline pages and external links WHEN `EnableStreamingChunkProvider` and
`EnableInlineStreaming` are each set to `0` and `1`, and `ThriftMaxBatchesInMemory` is set to
`1` and `3`, THEN row completeness and peak buffered inline batches follow the parity-matrix
dispositions; disabling either streaming toggle cannot truncate a result, and the effective
minimum two-batch buffer is recorded. Compare flag-on staging with APC Thrift and warehouse
SEA; where inline-only server support is missing, leave that check pending.
- AC40: GIVEN a partial stack checkpoint WHEN a production APC connection sets
`UseThriftClient=0` or the APC flag is ON THEN it still uses Thrift. WHEN the package-local
test route calls a known unfinished prepared, batch, or metadata API THEN it fails before
ExecuteStatement; an unsupported query response fails before false successful completion.
Each package validates its completed public JDBC call on
that test route and checks that the production route is still closed.
- AC41: GIVEN a recovered statement or saved APC connection after a flag change WHEN fetch,
cancel, close, or termination runs on flag-on staging THEN recovered statements use the current
connection's APC target and saved-session close uses the saved protocol and target, matching
APC Thrift or warehouse SEA where applicable. Binding-fix-dependent termination stays pending.
- AC42: GIVEN a RUNNING APC statement and ordinary close WHEN requests are captured THEN the
statement GET and CloseStatement DELETE carry the encoded APC query target exactly once, have
no body, and make no `/status` call. Compare the flag-on staging close outcome with APC Thrift
and warehouse SEA; AC3 covers the remaining request targets.
- AC43: GIVEN the seven synchronous APC SHOW operations and procedure metadata SELECTs WHEN
called through public JDBC on flag-on staging THEN Arrow IPC and columnar SHOW responses,
null-catalog fallback, procedure bindings, errors, and JDBC values match APC Thrift;
warehouse SEA establishes the request precedent. At `EnableSeaSyncMetadata=1` and `0`, capture
the R5 header change and compare rows, errors, and response latency; the setting cannot change
JDBC values. Both SHOW formats retain the native metadata flag. Server-dependent live cases
remain pending.
- AC44: GIVEN identical prepared INSERT batches with `BatchInsertSize=1` and `1000` WHEN APC
fallback and native V10 paths run on flag-on staging THEN fallback sends one request per set and
native sends one request for the batch at both settings; counts and errors are unchanged versus
APC Thrift where the wire permits. Record the lost sizing effect and latency/request-count
impact; unavailable V10 stays pending.
- AC45: GIVEN `QueryResultCompressionType` or `EnableQueryResultLZ4Compression` values selecting
LZ4, another codec, or no compression WHEN an APC SEA query executes on flag-on staging THEN
the APC request and response use LZ4, values decode correctly, and warehouse SEA still sends
its configured value. Compare compressed bytes, decode CPU, and end-to-end latency with APC
Thrift and warehouse SEA; record the client impact of the fixed APC codec.
- AC46: GIVEN low and high `cloudFetchThreadPoolSize` and `LinkPrefetchWindow` values WHEN
presigned-link downloads saturate the CloudFetch download pool on flag-on staging THEN download
concurrency and prefetch follow those controls. Separately saturate APC and warehouse SEA SDK
statement/result/link GETs while varying `HttpConnectionPoolSize` and
`HttpMaxConnectionsPerRoute`; the total and per-route SDK limits respectively change in-flight
GETs and pool waits, without claiming an effect on presigned-link downloads. Report median
throughput, raw latency, and peak heap for one and eight connections against APC Thrift and
warehouse SEA under identical values. With low and high `ChunkReadyTimeoutSeconds`, make a chunk exceed the lower wait and
complete within the higher one; compare timeout error name/code and connection usability with
APC Thrift and warehouse SEA on both inline and external paths. With low and high
`CloudFetchSpeedThreshold`, use the same slow external download and compare warning emission,
rows, bytes, retries, and latency; the threshold must not throttle or alter results. Use a
controlled slow-link staging probe to induce the conditions; local fixtures alone do not close
the live gate. A no-op APC control or unexplained extra
resource cost blocks release.

- AC47: GIVEN a primitive single-chunk APC query with inline LZ4 `ARROW_BATCH` and no
`manifest.arrow_schema` WHEN public JDBC consumes it on the package-local staging route THEN
rows, order, JDBC types, and fetch errors match APC Thrift; warehouse SEA supplies the request
and error precedent. Production APC remains on Thrift, and linked or continuation results remain
guarded until AC4/AC14 pass. Record a one-connection throughput/heap baseline for this slice.
- AC48: GIVEN a successful APC DML response with an affected-row count WHEN public
`executeUpdate` or `executeLargeUpdate` runs on the package-local staging route THEN the reported
count matches APC Thrift, and warehouse SEA establishes the result-format precedent; missing
or zero-column schema causes no fetch or decode error. A server failure retains its error.
- AC49: GIVEN identical queries with `Statement.setMaxRows(0)`, `1`, and a limit crossing an
inline page or external-link boundary WHEN consumed on package-local and flag-on staging routes
THEN positive limits are sent as SEA `row_limit`, zero is unlimited, and returned rows never exceed
the limit. Compare row count, order, and JDBC values with APC Thrift and warehouse SEA; the
driver stops continuation GETs after reaching the limit. A server contract fixture covers a
boundary unavailable live.
- AC50: GIVEN the seven R5 SHOW operations through the session-reachable async metadata API
WHEN live CreateSession is V9+ THEN the typed handle polls to the same JDBC rows, errors,
null-catalog fallback, timeout, and cancellation outcome as synchronous APC SHOW and APC Thrift
on package-local and flag-on staging routes. Below V9 or with no live version, the returned
handle is already complete and result retrieval sends no poll. Async procedure SELECTs fail
before network I/O. If V9+ staging is unavailable, the live gate remains pending.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). Only the integration JDBC package owns `docs/` and
  `NEXT_CHANGELOG.md`; earlier PRs use `NO_CHANGELOG=true` for the unreleased feature. Do not
  release a partial stack.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T32; known gaps SC-256188 under SC-256186 and SC-254478 remain
  external prerequisites for affected dogfood checks.
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

Use nine reviewable JDBC slices in one parent chain and two independent Universe
prerequisites (see `plan.json`). The first JDBC slice proves no-result DDL through public
JDBC on a package-local staging route; production APC remains on Thrift, including an
explicit SEA override. Primitive inline query, linked/paged query, logical types and
transport parity, lifecycle, prepared execution, synchronous metadata, and async metadata
then each close an end-to-end gate. The final slice alone activates production APC routing,
owns `docs/` and `NEXT_CHANGELOG.md`, and repeats integrated flag-on checks. A partial
stack is never released. The single JDBC chain also puts shared execution, error, and
diagnostic edits in one branch ancestry; packages target fewer than 1,500 changed lines.

Git parents apply within one repository. The external deployment gates are explicit:
`apc-no-result-ddl-contract` (T32) precedes the core live DDL gate;
`apc-safe-flag-definition` (T17) plus Q9's connector-service override precede
flag-on validation. These Universe packages cannot be Git parents of JDBC PRs. Other
named server fixes keep only their affected live checks pending. Every slice compares
its completed public JDBC behavior on the package-local staging route with APC Thrift
and warehouse SEA, records throughput/latency/resource and error evidence appropriate
to that behavior, and verifies production routing remains closed. T18 reruns those
comparisons after activation and blocks release on any open gate.

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

Each task appears in one `plan.json` package. Package gates exercise completed behavior through
public JDBC methods with the package-local route selector while production APC routing stays
on Thrift; T18 repeats the live checks after activation. External gates are pending until proven.

### Phase 1 — Closed-route DDL core

- [ ] T1: Add APC compute and SEA result-format support while preserving warehouse wire values (R2, R16).
- [ ] T2: Target APC session, execution, polling GET, cancel, ordinary close, and termination calls without changing warehouse requests (R2).
- [ ] T3: Exercise APC SEA DDL through public JDBC under the package-local route selector and emit redacted route and statement telemetry (R2, R4, R8).
- [ ] T19: Round-trip APC session and internal statement IDs, retaining raw public statement IDs (R9).
- [ ] T5: Keep live CreateSession capability version separate from serialized ID diagnostics (R6, R9).
- [ ] T28: Return zero count and no ResultSet for successful no-result DDL, including a zero-column manifest; preserve errors (R4).
- [ ] T31: Keep production APC routing on Thrift and guard incomplete calls only in the package-local SEA test route (R18).

### JDBC slices after the core

- [ ] T7: Suppress APC result heartbeats while retaining execution polling (R3).
- [ ] T8: Complete public primitive-query values, DML counts, errors, and cleanup on the test route (R4, R8, R11).
- [ ] T10: Decode primitive LZ4 inline bare Arrow batches for a single-chunk public query (R4, R11).
- [ ] T33: Stream complete external-link Arrow IPC results, without treating them as bare batches (R4, R11).
- [ ] T34: Preserve `Statement.setMaxRows` across APC SEA inline and linked results (R4).
- [ ] T11: Fetch every inline or external continuation, including manifest-only starts, by `next_chunk_index` and `row_offset` (R12).
- [ ] T25: Target result-chunk GETs once, with null body and unchanged presigned URLs/headers (R2).
- [ ] T20: Complete the AC13 logical-type matrix without silent coercion (R11, Q7).
- [ ] T26: Prove query SDK retry/pool, CloudFetch download, compression, and presigned-link parity with paired routes (R17).
- [ ] T6: Preserve selected APC protocol on connection errors and KP001 Thrift failure (R7).
- [ ] T12: Apply warehouse SEA timeout behavior with targeted APC cancellation (R14).
- [ ] T15: Complete route outcomes and redact identifiers in synchronous/asynchronous diagnostics (R8).
- [ ] T27: Complete timed-out, failed, and repeated-close DDL lifecycle using T2's target (R2, R14).
- [ ] T29: Use the current target for recovered statements and saved protocol/target for saved-session close (R9).
- [ ] T30: Execute Statement SQL batches sequentially with ordered counts and stop-on-error (R15).
- [ ] T9: Gate scalar bindings at V8, interpolate below it, and support binding-free pre-execution metadata (R6).
- [ ] T22: Reject complex bindings at every APC capability level before requests, with a tested error name/code and taxonomy (R6).
- [ ] T21: Support opt-in V10 prepared batches and ordered per-set fallback (R6).
- [ ] T13: Serve synchronous SHOW and procedure SELECT metadata with the R5 request contract and null-catalog fallback.
- [ ] T14: Decode emitted Arrow IPC or columnar native metadata with the manifest native flag and AC16 value/null parity (R5, R13).
- [ ] T23: Expose seven-operation async SHOW with the live V9 gate and completed handles below V9 (R5).
- [ ] T24: Preserve async null-catalog fan-out, errors, timeout, and cancellation (R5).

### Independent Universe prerequisites

- [ ] T17: Define the default-off APC SAFE sub-flag without assigning or ramping it (R1, Q8).
- [ ] T32: Make APC `GetStatementResult` return a successful no-result DDL outcome after polling, including an absent or zero-column manifest, without requiring Arrow result data (R4, AC33, AC37).

### Integration and release

- [ ] T4: Activate Arrow-first APC routing, explicit override, catalog-pattern precedence, and the dedicated flag only after all JDBC slices merge (R1, R7, R18).
- [ ] T16: Document routing, batch and compression costs, SEA tuning scope, and the user-visible changelog entry; this is the only docs/changelog package.
- [ ] T18: Consolidate local and live evidence, repeat flag-on gates after T17/Q9/T32 deployment, and close cross-route checks (AC1–AC50).
- [ ] T-val: Record pass/fail or externally pending evidence for AC1–AC50 and exact commands; do not declare release ready with Q7, Q9, or server-gated checks open.
