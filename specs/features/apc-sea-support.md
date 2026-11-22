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
- Driver baseline verified against `origin/main` @ `840f3a407` (which includes the
  approved spec from #1809 and the `UseBoundedSeaApi` default change in #1827):
  - `AllPurposeCluster` parses concrete `/o/<org>/<cluster>` paths, while legacy
    `cliservice` uses placeholder ids. `DatabricksConnectionContext.getClientTypeFromContext`
    currently routes every APC to Thrift before consulting `UseThriftClient` or flags.
    Warehouse routing honors explicit `UseThriftClient`, then catalog-pattern, CloudFetch,
    Arrow, and `enableSqlExecForJdbc` gates. `UseBoundedSeaApi` defaults to `1` independently
    of the server flag; AIX/PowerPC disables Arrow by default.
  - `DatabricksSession.open` chooses `DatabricksThriftServiceClient` or
    `DatabricksSdkClient`. Its KP001 Thrift-to-SEA recovery checks the default Thrift path
    without restricting compute type. The old HTTP 307/429 SEA-to-Thrift fallbacks are gone.
  - `DatabricksSdkClient` assumes warehouse targets in session, execution, and statement
    calls. It uses warehouse-scoped CreateSession and an `x-databricks-rt-session` response
    header for warehouse Real-time follow-up URLs. Statement polling uses
    `GET /statements/{id}`, while result heartbeats call unsupported
    `GET /statements/{id}/status`. Standard statement GET/DELETE requests currently serialize
    SDK request bodies.
  - Warehouse SEA with CloudFetch and `EnableSQLExecHybridResults=0` requests
    `EXTERNAL_LINKS`; with CloudFetch off, bounded inline Arrow is available. The existing
    bounded provider and CloudFetch reader supply streaming, LZ4, and external-link handling.
    SEA result decoding currently accepts `ARROW_STREAM` and `JSON_ARRAY` only.
  - `DatabricksResultSet` constructs schema and a decoder only when result data is present.
    Its update-count path iterates result rows; a successful APC DDL response without schema
    or result data therefore needs an explicit no-result path (R4, AC33).
  - `DatabricksMetadataQueryClient` has synchronous SHOW metadata only. The seven SHOW
    operations are distinct from internal catalog/schema SELECTs and the two parameterized
    procedure SELECTs. Sync null-catalog schemas/columns already fan out after SQL state
    `42601`. `ResultData` has no `columnar_data`; `ColumnInfo` has no `type_json`.
  - Native prepared batching exists for warehouse SEA and eligible APC Thrift, but SEA
    reports support only for warehouses. The legacy batch path can combine rows with
    `EnableBatchedInserts=1`, and its individual path does not interpolate each set.
    `PreparedStatement.getMetaData()` uses a quoted-placeholder `DESCRIBE QUERY` path.
  - `DatabricksSdkClient` execute logs and `DatabricksSession.toString()` can expose APC
    identifiers if APC is routed through SEA. `RateLimitRetry` and
    `HttpConnectionRequestTimeout` affect `DatabricksHttpClient`, while SDK statement
    requests use the SDK retry and pool policy; `HttpMaxConnectionsPerRoute` reaches the
    SDK pool.
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
- R2: For APC over SEA, every SEA call (CreateSession, ExecuteStatement, GetStatementResult,
GetResultData, CancelStatement, CloseStatement, and TerminateSession) carries
`backend_compute.all_purpose_cluster_compute.cluster_id` and omits `warehouse_id`. The driver's
`deleteSession` method and existing `DeleteSessionRequest` model implement the TerminateSession server RPC.
CreateSession, ExecuteStatement, and CancelStatement encode `backend_compute` in the JSON body.
GetStatementResult, GetResultData, and CloseStatement encode the cluster id as the
URL-encoded dotted query field
`backend_compute.all_purpose_cluster_compute.cluster_id`. Query fields are merged with existing
parameters such as `row_offset` through a query builder; they are not appended as a second `?`
fragment. The `row_offset` field is added only to the SEA
`GET /statements/{id}/result/chunks/{n}` GetResultData request. Presigned external-link URLs and
their required headers are used byte-for-byte as
returned, without adding the compute field or otherwise modifying them. The driver-owned body
models are `CreateSessionRequest`, `ExecuteStatementRequest`, `CancelStatementRequest`, and
`DeleteSessionRequest`. `GetStatementRequest` and `GetStatementResultChunkNRequest` come from
the SDK, so APC GET targeting is built on the request URL rather than by changing the SDK classes.
CloseStatement DELETE is also targeted in the URL. For APC, GetStatementResult GET (both execution
polling and explicit result fetch), GetResultData GET (both chunk-fetch paths), and CloseStatement
DELETE have a null request body: do not serialize the SDK request object or send `{}` or an empty
JSON body. This overrides the current `DatabricksSdkClient.serializeStatementRequest` behavior on
standard statement URLs only for these APC bindings. Preserve warehouse request bodies, including
the standard and Real-time URL variants.
TerminateSession uses the standard `/api/2.0/sql/sessions/{session_id}` DELETE binding, which
declares `body: "*"`: send `backend_compute` in the `DeleteSessionRequest` JSON body. Preserve
the existing warehouse DELETE serialization and verify the outgoing APC DELETE body with a
fake-service request test; dogfood verifies server target extraction after the external fix.
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

- R6: The driver records `negotiated_protocol_version` from CreateSession and gates native
parameters at ≥ 42248 (V8). For APC sessions at V8 or newer, native `parameters` and
`parameter_sets` accept only scalar bindings: reject ARRAY, MAP, STRUCT, and other complex logical
parameter types with a clear unsupported-parameter SQLException before sending any
ExecuteStatement request, regardless of whether the value came from `setObject` or a prepared
batch. A batch with any such binding is rejected before its first set is sent; below V8, the
existing interpolation path applies. The error uses a driver-owned error code and its emitted
name/numeric code are tested and classified in the telemetry taxonomy per repository rules.
It sends `parameter_sets` only for a parameterized INSERT at
≥ 42250 (V10) when `EnableNativeBatching=1` and an APC-capable `NativeBatchExecutor` is selected;
the version alone never enables native batching. The native ExecuteStatement JSON contains
`"parameter_sets":[{"parameters":[...]}]`, one nested `parameters` array per ordered input set,
and omits the mutually exclusive top-level `parameters` field entirely. For APC below V10, with
native batching off, or without that executor, execute each parameter set separately rather than
combining sets through `EnableBatchedInserts`. V8–V9 use native `parameters` per execution; below V8 uses the existing
interpolation and escaping path even when `supportManyParameters` is unset. For prepared-batch
fallback below V8, interpolate each set's SQL separately and send an empty parameter map to
`executeInternal`. For APC, the SEA request builder omits the `parameters` property entirely
whenever the resulting map is empty, including interpolated single executions and procedure
SELECTs; `"parameters":[]` does not satisfy this rule. Preserve warehouse serialization for an
empty map. Setting the interpolation flag alone does not affect
`LegacyPreparedStatementBatchExecutor.executeIndividualStatements`. When an APC session
below V8 downgrades to client-side parameter interpolation, the driver records that downgrade
in logs and telemetry. A missing protocol version is treated conservatively as below V8:
native parameters and `parameter_sets` are disabled, and telemetry distinguishes a missing-version downgrade from a
present but old version. Only the live CreateSession response on the current connection grants
capabilities. Keep its nullable negotiated version in a session field distinct from the
diagnostic version parsed from `SessionId`; recovery without CreateSession leaves the live field
absent. A reconstructed version must never enable native `parameters`, `parameter_sets`, or
asynchronous metadata by itself.
For APC `PreparedStatement.getMetaData()` before execution, build `DESCRIBE QUERY` from the
original SQL with real placeholders quoted as literal `'?'`, then execute that metadata query
with no original parameter bindings at any protocol version. Do not run ordinary interpolation
over the quoted DESCRIBE SQL: its literals are deliberately excluded by `SQLInterpolator`, so
passing the original bindings would cause a parameter-count error below V8 or with a missing
version. This metadata lookup does not execute the original prepared SQL or change its bindings.
One native `parameter_sets` execution has one aggregate response, without affected-row counts
for individual input sets. On success, return a JDBC count array with one
`Statement.SUCCESS_NO_INFO` entry per input set in order; do not divide or copy the aggregate
count into fabricated per-set counts. If native execution fails, report batch failure without
claiming any individual set succeeded; mark each set `Statement.EXECUTE_FAILED` in the
`BatchUpdateException` counts.
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
- R9: `IDatabricksComputeResource` handling in `DatabricksSdkClient`, `SessionId`, and
`StatementId` is type-safe — no unchecked cast to `Warehouse`. `ImmutableSessionInfo` records the
client type, and `SessionId.create` dispatches on it rather than compute-resource class. New APC
SEA identifiers serialize a compute discriminator, org id, cluster id, and raw session/statement
id: sessions use `s|a|orgId|clusterId|sessionId|protocolVersion-or--`, and statements use
`s|a|orgId|clusterId|statementId`. The `-` marker preserves an absent protocol version. Dispatch
is by prefix: any identifier whose first two segments are exactly `s|a` uses the APC grammar only,
never the warehouse grammar. Session ids must have exactly six segments and statement ids exactly
five; any other count, or an empty segment, raises the malformed-id error (`SESSION_ID_PARSING_EXCEPTION`
for sessions). Parsing of all other identifiers remains backward-compatible with existing warehouse
`s|warehouseId|sessionId[|r]` and raw SEA statement ids. The compound session form is also returned by the public
`IDatabricksConnection.getConnectionId()` API so callers can later close that session; the
compound statement form is internal routing serialization only: use a separate
internal statement-id serializer. `StatementId.toString()`, `toSQLExecStatementId()`, and public
`IDatabricksResultSet.getStatementId()` continue to expose only the raw server statement id;
statement-id-bearing logs and telemetry use that raw id, not the compound form.
`DatabricksConnection.getStatement(rawId)` attaches the current connection's
APC SEA session and compute target to its reconstructed internal `StatementId`; fetch, cancel,
and close use that target. It does not change the public raw id or infer a target from id text.
`Driver.closeConnection(url, info, connectionId)` deserializes the public connection id before
client construction and selects SEA or Thrift from that saved session type, not a fresh APC flag
decision. It terminates the saved APC target even if the flag changed after the session opened.
- R10: APC ExecuteStatement preserves the existing SEA timeout behavior: `wait_timeout` is unset
for synchronous execution with direct results and `0s` for asynchronous execution or when direct
results are disabled. Reyder accepts `0s` as the SEA duration-string representation of zero; no
APC-specific timeout override is needed.
- R11: For APC `ARROW_BATCH` results, the driver builds the Arrow schema from
`manifest.schema` (`type_text`/`type_json`, including nested types) and does not require
`manifest.arrow_schema`; a populated `manifest.arrow_schema` or `total_byte_count` does not
change decoding or paging.
- R12: APC result fetching pages by `next_chunk_index` and `row_offset`, never relies on
`total_chunk_count`, `chunks`, or `total_row_count`, and accepts a `GetStatementResult`
response with or without inline result data. For inline `arrow_batches`, advance `row_offset`
by the sum of batch `row_count` values in the page. For a page with multiple external links,
take both the continuation `next_chunk_index` and the next `row_offset` from the final link in
response order:
`finalLink.row_offset + finalLink.row_count`. A missing final-link `next_chunk_index` ends paging.
- R13: The driver can read the SEA `columnar_data` (HIVE) result shape — the current
`sql_exec_api_service.proto` `ColumnVector` oneof arms `bool_values`, `byte_values`,
`i16_values`, `i32_values`, `i64_values`, `double_values`, `string_values`, and
`binary_values` — into a JDBC ResultSet. Decode each physical vector using the
`manifest.schema` logical type and apply least-significant-bit-first null masks, including
placeholder values at null positions. A missing or short mask treats all rows beyond its last
bit as non-null; a mask longer than `ceil(row_count/8)` ignores every excess bit and byte.
Boolean, byte, short, int, long, double, string, and binary values must retain their JDBC values;
string vectors also cover logical decimal,
array, map, struct, union, and null types named by the schema. New oneof arms require an
explicit model/reader update.
- R14: APC query timeouts use the client-side `TimeoutHandler` + CancelStatement path, as for
warehouses.
- R15: `Statement.addBatch(String)` / `executeBatch` on APC executes SQL commands sequentially,
one ExecuteStatement request per SQL string, preserving the existing stop-on-first-failure and
update-count behavior. It never sends the unsupported `statements` field.
- R16: SEA request and response models use a driver-owned `SqlExecResultFormat` enum rather than
the SDK-owned `com.databricks.sdk.service.sql.Format`. It preserves existing wire values, adds
`ARROW_BATCH` and `COLUMN_BASED_SET` (wire value 6), serializes exact API names, tolerantly maps
unknown response values to `UNKNOWN`, and produces a clear unsupported-format error if an unknown
value reaches result decoding. `COLUMN_BASED_SET` selects the `columnar_data` reader for metadata
even when the request asked for `ARROW_STREAM`.
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

- AC1: GIVEN a cluster HTTP path with no client setting AND the APC flag OFF or absent (or the
flag lookup fails), even with `enableSqlExecForJdbc` ON, WHEN a connection is opened THEN the
driver uses `DatabricksThriftServiceClient` and no SEA request is sent; GIVEN the APC flag ON
AND Arrow available THEN it uses SEA. GIVEN a partner User-Agent for which connector-service
returns the APC sub-flag as `false` THEN the driver uses Thrift. GIVEN the legacy `cliservice`
HTTP path THEN the driver continues to use Thrift and never sends placeholder `default` ids to SEA.
- AC2: GIVEN a cluster HTTP path with APC-SEA enabled (R1) WHEN a connection is opened THEN the
CreateSession request body contains `backend_compute.all_purpose_cluster_compute.cluster_id`
equal to the URL's cluster id and contains no `warehouse_id`.
- AC3: GIVEN an open APC-SEA session WHEN a statement is executed, polled, fetched, cancelled, and
closed, and separately when the session is terminated, THEN every corresponding SEA request
includes the APC
`backend_compute` using the body/query encoding in R2 and the driver issues no
`GetStatementStatus` or `/status` call. Captured fake-service requests assert a null body for
every APC GetStatementResult GET (execution poll and explicit result fetch), GetResultData GET
(external-link and inline chunk fetch), and CloseStatement DELETE, while the cluster query field
is present exactly once. Warehouse fixtures retain their existing bodies for the same bindings.
A fake-service fixture asserts the TerminateSession
DELETE body contains `backend_compute`; dogfood verifies server target extraction after the
external binding fix is deployed. GIVEN `getConnectionId()` from that open session and an APC
flag change from ON to OFF, WHEN public `Driver.closeConnection(url, info, connectionId)` is
called THEN it selects SEA from the serialized session type and sends the targeted
TerminateSession request. A saved APC Thrift id still selects Thrift if the flag later turns ON.
- AC4: GIVEN an APC-SEA query whose LZ4-compressed Arrow result is returned inline or through
multi-chunk external links WHEN the ResultSet is iterated THEN all rows and types match the same
query executed over Thrift, with each payload decompressed before the generated Arrow schema
message is prepended to inline bare record batches. External-link payloads remain complete Arrow
IPC streams and are passed through the existing CloudFetch stream reader without a new schema.
An inline `chunk.arrow_batches` containing two or
more batches yields every row in batch order, with the sum of batch `row_count` values equal to
the number of rows read. GIVEN ordinary APC synchronous and asynchronous executions with
`EnableSQLExecHybridResults=0` WHEN a fake service captures each ExecuteStatement request THEN
both contain `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`, and the current
`session_id`, and neither contains `statements`; the hybrid-results setting does not change those
fields.
- AC5: GIVEN an APC SEA connection WHEN each public synchronous metadata method backed by
`GET_CATALOGS`, `GET_SCHEMAS`, `GET_TABLES`, `GET_COLUMNS`, `GET_FUNCTIONS`,
`GET_PRIMARY_KEYS`, or `GET_CROSS_REFERENCE` (both imported keys and cross references) reaches
the server THEN its SHOW request uses `ARROW_STREAM`,
`INLINE_OR_EXTERNAL_LINKS`, and the matching metadata-operation header. It sends the
sync header only with `EnableSeaSyncMetadata=1`; `EnableSQLExecHybridResults=0` does not change
the disposition. Where supported, the existing Thrift-native metadata header remains present.
Each operation returns the same JDBC columns and rows as Thrift for both Arrow IPC
`chunk.attachment` and HIVE-shaped `chunk.columnar_data`, including a
`COLUMN_BASED_SET` manifest. Alternate the two payloads on one connection to prove
response-level decoder selection; preserve the synchronous null-catalog schemas/columns
`42601` fallback and existing empty-result behavior.

GIVEN internal catalog/schema SELECTs or either parameterized procedure SELECT on that APC
connection THEN the requests use `ARROW_BATCH`, omit both metadata headers, and pass ordinary
query results through their existing JDBC metadata converters. The procedure SELECTs retain
native scalar bindings at V8+; below V8 or with a missing live version, captured requests
contain escaped/interpolated SQL and no `parameters` property. Quoted, backslash, and
SQL-delimiter patterns retain their values. An async request for either procedure operation
fails before a SEA request. Warehouse metadata request fixtures remain unchanged.

GIVEN a session-reachable async request for each of the seven SHOW operations at live V9+
THEN submission omits the sync header but retains the operation header, `ARROW_STREAM`, and
`INLINE_OR_EXTERNAL_LINKS`; a typed handle completes through PENDING/RUNNING to terminal
state. Completion retains `StatementType.METADATA` and produces the same JDBC columns and
rows as its synchronous counterpart for both legal payloads, including both
`GET_CROSS_REFERENCE` conversions. Failure, cancellation, and metadata timeout propagate.
At V8, below V8, or with no live version, the API instead returns a completed synchronous
handle without async submit or result polling; an altered serialized version cannot change
that gate.

GIVEN V9+ async `GET_SCHEMAS` or `GET_COLUMNS` with a null catalog WHEN
`SHOW ... IN ALL CATALOGS` fails with SQL state `42601` THEN completion asynchronously
enumerates catalogs and queries each nonempty catalog, preserving the operation header and
omitting the sync header. Rows and pattern filtering match the synchronous fallback, including
an empty catalog list. A non-syntax error, null SQL state, or non-null catalog does not fan out;
ordinary per-catalog SQL errors are warned and skipped, while cancellation and timeout
propagate. Unit and fake-service evidence precedes dogfood; dogfood waits for the tracked
metadata server fix.

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
GIVEN a response page with multiple external links and no page-level `row_count` THEN the next
SEA fetch uses the final link's `next_chunk_index` and
`row_offset = finalLink.row_offset + finalLink.row_count`; a fixture with at least two links on
that page and a following page returns every row once.
- AC15: GIVEN a completed APC statement WHEN `GetStatementResult` returns a manifest without
result data, and separately WHEN it returns a manifest with result data THEN both produce the
full correct ResultSet.
- AC16: GIVEN `columnar_data` chunks covering `bool_values`, `byte_values`, `i16_values`,
`i32_values`, `i64_values`, `double_values`, `string_values`, and `binary_values`, plus empty
vectors and least-significant-bit-first null masks crossing a
byte boundary WHEN they are read THEN nulls and values land at the right row positions. Fixtures
with an absent mask, a mask shorter than `ceil(row_count/8)`, and a longer mask prove that
unrepresented rows are non-null and excess bits/bytes do not affect any row or cause an index
failure;
GIVEN string vectors with logical decimal, array, map, struct, union, and null schema types THEN
the JDBC values and `ResultSetMetaData` match pinned Thrift fixtures for every type: for example,
`DECIMAL(5,2)` text `123.45` yields a `BigDecimal` with scale 2 and JDBC `Types.DECIMAL` with
precision 5/scale 2; `ARRAY<INT>` with `[1,null]` yields the same array elements and JDBC type as
Thrift; `MAP<STRING,INT>` with `{"k":1}` and `STRUCT<x:INT>` with `{"x":1}` yield the same
logical objects and nested type metadata as Thrift when complex-type support is enabled. A union
fixture pins its Thrift JDBC value and type code, and a null fixture yields SQL `NULL` with
`wasNull()=true`; a reader that returns the raw wire text for a type whose Thrift value is a
different object fails the test;
GIVEN the next metadata operation on the same connection returns Arrow IPC in `attachment` THEN
it is decoded through the Arrow path,
not the previously used `columnar_data` path.
- AC17: GIVEN an APC-SEA session with `negotiated_protocol_version` ≥ 42250 (V10),
`EnableNativeBatching=1`, and a supported native executor WHEN `executeBatch` runs a
parameterized INSERT THEN one request uses `parameter_sets`, and successful `executeBatch` and
`executeLargeBatch` return one `Statement.SUCCESS_NO_INFO` count per input set in order; the
serialized request has one `{"parameters":[...]}` object per input set in `parameter_sets` and
has no top-level `parameters` property; the aggregate affected-row count is never presented as an individual set count. GIVEN that native
request fails THEN `BatchUpdateException` reports `Statement.EXECUTE_FAILED` for every set and
does not claim a partial success. GIVEN V10 with `EnableNativeBatching=0` or no supported
executor, or version 42248–42249 (V8–V9), THEN it sends one execution per set with native
`parameters` and no `parameter_sets`. GIVEN a version below 42248 (below V8) or a missing
version THEN it sends one execution per set with client-side interpolation and neither
`parameters` nor `parameter_sets` property, including no empty arrays; a captured batch request
verifies that each set's bound values
were escaped into its own SQL text and no native bindings were sent.
For every non-native case, `EnableBatchedInserts=1` does not combine sets into a multi-row INSERT;
update counts preserve input order and a failure stops subsequent sets.
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
For a prepared batch, each set takes the interpolation path before
`LegacyPreparedStatementBatchExecutor.executeIndividualStatements` calls `executeInternal`;
captured requests show escaped values in separate SQL statements with no native bindings.
Calling `getMetaData()` before execution with bound `setObject(...)` values also succeeds on this
missing-version session through the binding-free `DESCRIBE QUERY` path in AC6.
- AC24: GIVEN an APC-SEA connection with `EnableHeartbeat=1` WHEN a completed ResultSet remains
open beyond multiple heartbeat intervals THEN no heartbeat task is scheduled, no
`GetStatementStatus`, `/status`, or heartbeat `GetStatementResult` request is sent, and the driver
logs that heartbeat is unsupported for APC.
- AC25: GIVEN an APC-SEA session with org id, cluster id, session id, and a present or absent
negotiated protocol version WHEN its `SessionId` and a derived `StatementId` are round-tripped
through their internal serializers THEN client type, APC compute target, raw ids, and serialized protocol-version field
round-trip unchanged; existing warehouse and Thrift serialized identifiers still parse identically. Creating
an APC SEA `SessionId` never dereferences a Thrift session handle. The serialized APC forms match
the R9 formats exactly, including `-` for a missing protocol version. Public
`IDatabricksConnection.getConnectionId()` returns the compound APC session form so a caller can
pass it to `Driver.closeConnection`; it is not written to logs or telemetry. Public
`IDatabricksResultSet.getStatementId()` and `StatementId.toString()` return the raw server
statement id. Statement-id-bearing logs and telemetry use the raw id and never expose the
internal compound form, org id, or cluster id.
GIVEN a serialized APC `SessionId` with missing, extra, empty, or invalid segments or a malformed
version THEN parsing raises `SESSION_ID_PARSING_EXCEPTION`; the exception, logs, and telemetry
describe only the structural defect and never include the serialized input, org id, or cluster
id. GIVEN a serialized version altered from the current connection's live CreateSession version
THEN reconstruction may display the altered value for diagnostics but cannot enable native
`parameters` or `parameter_sets`.
GIVEN `connection.getStatement(resultSet.getStatementId())` on that APC SEA connection, WHEN the
recovered statement fetches, cancels, and closes THEN all three SEA requests carry the current
connection's `backend_compute`, while `getStatementId()` remains the raw server id.
- AC26: GIVEN an ExecuteStatement request with `SqlExecResultFormat.ARROW_BATCH` WHEN serialized
THEN its JSON contains `"format":"ARROW_BATCH"`; GIVEN a manifest with `ARROW_BATCH` WHEN
deserialized THEN APC decoding is selected; GIVEN an unknown future format THEN deserialization
succeeds as `UNKNOWN` and result decoding raises the standard unsupported-format error. GIVEN an
`ARROW_STREAM` metadata request followed by a `COLUMN_BASED_SET` response THEN the enum retains
that wire value and metadata decodes from `columnar_data`. GIVEN pre-change warehouse fixtures
with `JSON_ARRAY` and `ARROW_STREAM` manifests WHEN each is deserialized and passed through
`ExecutionResultFactory` and `DatabricksResultSet` THEN the enum round-trips the exact original
wire name, `JSON_ARRAY` selects `InlineJsonResult`, `ARROW_STREAM` selects `ArrowStreamResult`,
and rows and metadata match the pre-change fixtures.
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
- AC30: GIVEN APC SEA and warehouse SEA sessions with identical SDK retry configuration and
connection-pool settings WHEN a fake service returns 429/503 with a fixed `Retry-After` on a
statement result GET or SEA chunk GET and then succeeds THEN both routes make the same number
of SDK attempts and honor the same retry schedule within 250 ms per interval. When SDK retries
are exhausted, APC retains the warehouse SEA error's HTTP
status, SQLState, and vendor-code mapping. Repeat with `RateLimitRetry=0`: both SEA routes still
follow the SDK policy, and the request count remains equal; this property only controls the
separate `DatabricksHttpClient` retry handler on `origin/main`. With
`HttpConnectionRequestTimeout` set, assert the same SDK pool-wait behavior for both routes and
record that the property is not wired into `ClientConfigurator`; with
`HttpMaxConnectionsPerRoute=1`, verify both routes share the configured SDK pool limit. An APC
presigned CloudFetch link uses the existing download retry path and original URL/headers; no
APC-specific retry or error mapping is introduced (R17).
- AC31: GIVEN the same 100,000-row, fixed-schema result fetched over APC Thrift, warehouse
Thrift, warehouse SEA, and APC SEA with the same `cloudFetchThreadPoolSize`,
`HttpMaxConnectionsPerRoute`, and link-prefetch settings (and a recorded
`HttpConnectionRequestTimeout`, whose SDK scope is stated in AC30) WHEN
single-connection and eight-concurrent-connection runs are repeated three times each THEN the
release record contains median rows/second, end-to-end latency, peak heap, transfer bytes, retry
count, and SQLState/vendor code for injected timeout and rate-limit errors for all four routes.
Run each mode in alternating order on the same host/network and fixed driver build, after one
warm-up; retain raw runs and use the median of three measured runs. A paired fake-service replay
serves the same row payload, link count, response delays, and errors to APC SEA and warehouse
SEA; subtract the injected server delay from end-to-end latency before comparison. APC SEA
passes this driver-cost gate when its median rows/second is at least 85% of warehouse SEA, its
median adjusted latency is at most 115%, peak heap is at most 125% or 32 MiB higher (whichever
allows more), and client-added transfer bytes are at most 5% of each route's captured server
response bytes. All rows must match, and at eight connections there must be no APC-only queue, thread,
or connection cap. The bounded-provider fixture with 2 versus 20 equal-sized links records peak
simultaneous downloads at or below `cloudFetchThreadPoolSize` while eventually consuming all 20
links. Peak complete chunks buffered before row consumption must be bounded by the configured
prefetch window plus active downloads, independent of total link count. Record link scheduling
at two `linkPrefetchWindow` settings. Measure client-added result transfer as the positive
difference between APC SEA and paired warehouse SEA response-body bytes (SEA and presigned-link
payloads, excluding headers and TLS framing), divided by the warehouse SEA bytes; the replay
must serve byte-identical result payloads.
Dogfood four-route measurements are reported separately with server execution and network
timings, for both CloudFetch and `EnableQueryResultDownload=0` inline-only results. In each mode,
APC SEA passes against APC Thrift when its median rows/second is at least 85%, adjusted median
latency at most 115%, and peak heap at most 125% or 32 MiB higher, whichever allows more. If the
same-mode warehouse SEA/Thrift pair breaches a bound, APC SEA may instead match that measured
ratio within 15 percentage points for throughput or latency and 25 percentage points for heap;
record the warehouse precedent or server-contract limit and client impact in the parity table.
For inline-only results, record the bounded provider's prefetch and memory settings separately:
CloudFetch thread-pool settings do not govern inline buffering. Any unexplained threshold
failure blocks release (R17).
- AC32: GIVEN a 100-set parameterized INSERT workload with a fixed per-request service delay,
matched row contents, and `EnableBatchedInserts=1` WHEN APC Thrift legacy, APC SEA V8–V9
fallback, APC SEA V10 fallback with native opt-out, and APC SEA V10 native opt-in each run three
times after warm-up THEN record median rows/second, latency, and ExecuteStatement request count
for each mode. APC SEA fallback makes exactly 100 requests, while V10 native makes exactly one;
the APC Thrift legacy multi-row request count is recorded as the Q4 performance exception.
Native APC SEA throughput must be no lower than 85% of APC SEA fallback under the controlled
service delay. Report the fallback-to-Thrift throughput ratio and explain the request-count
cost in release notes; any adjusted-latency gap above 15% after subtracting
`(APC fallback requests - APC Thrift legacy requests) × fixed per-request delay` needs a
server-contract or warehouse precedent recorded in the parity table or
blocks release (R6, R17).

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

Build seven JDBC PRs and one independent Universe SAFE-definition PR. The first JDBC slice
executes schema-less DDL through public JDBC, with targeted polling, cancel and close, timeout,
no unsupported heartbeat, and redacted diagnostics. The second adds a primitive single-chunk
query using that safe lifecycle. Later slices complete paging and types, parameters and batches,
synchronous metadata, asynchronous metadata, and release evidence. Each PR is behind the
default-off APC flag; explicit
`UseThriftClient=0` remains an opt-in. Every exposed operation accepts both legal server
response shapes applicable to it. A package's acceptance list in `plan.json` names only full
criteria completed by that package; its notes state the narrower public JDBC gate.

JDBC packages form a linear stack because they edit common SDK, result, and session lines.
The Universe SAFE-definition PR is an independent root. Keep each implementation PR below
about 1,500 changed lines by limiting its exposed behavior to the slice named here. The
first JDBC PR is the sole owner of `docs/` and `NEXT_CHANGELOG.md`, because it first makes
APC statement behavior publicly usable. Later PRs use `NO_CHANGELOG=true` as changes to the
unreleased stack. No package performs flag assignment or ramp.

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

Each task belongs to exactly one package in `plan.json`. Implementation owners choose internal
class and helper boundaries. Public JDBC and fake-service tests demonstrate each slice; T18 and
T-val record final release evidence.

### Phase 1 — Wire models and compute targeting

- [ ] T1: Add APC compute and SEA result-format models, preserving warehouse wire values and
  unsupported-format behavior (R2, R16).
- [ ] T2: Represent APC targets on session, execution, cancel, and termination request bodies,
  preserving warehouse serialization (R2).
- [ ] T3: Open a public APC SEA connection through `Driver`, execute a successful schema-less
  DDL command, and close the statement and connection. Target CreateSession, execution,
  polling, and TerminateSession on standard URLs; never send placeholder `cliservice` ids or
  log raw org/cluster/compound ids (R1, R2, R4, R8).
- [ ] T19: Round-trip APC session and internal statement identifiers, retain raw public
  statement IDs, and reject malformed identifiers without exposing them in diagnostics (R9).

### Phase 2 — Routing and session lifecycle

- [ ] T4: Implement Arrow-first APC routing, explicit override, catalog-pattern precedence,
  default-off APC flag, and unchanged warehouse routing (R1, R7).
- [ ] T5: Record client type and the live CreateSession protocol version separately from the
  version serialized in a session identifier; only the live value grants capabilities (R6, R9).
- [ ] T6: Preserve the selected protocol on APC connection errors and KP001 Thrift failure;
  leave warehouse recovery unchanged (R7).
- [ ] T7: Disable APC result heartbeats and unsupported status RPCs while retaining ordinary
  execution polling (R3).
- [ ] T27: Target ordinary APC statement cancel and close, including timeout cancellation,
  without emitting raw APC identifiers. Recovered-statement targeting is T29 (R2, R8, R14).
- [ ] T29: Use the current connection target for recovered statement fetch/cancel/close, and
  the serialized session type and target for public `Driver.closeConnection` after a flag
  change (R9).

### Phase 3 — Execution and results

- [ ] T8: Execute a public single-chunk primitive APC query with the R4/R10 request shape,
  including inline-only session configuration and safe request/log redaction. Capture sync and
  async request shapes with hybrid results disabled (R4, R8, R10).
- [ ] T10: Decode LZ4 bare Arrow batches from a single result chunk using
  `manifest.schema`, preserving the external-link stream path and warehouse decoding (R4, R11).
- [ ] T28: Treat successful schema-less, data-less DDL as a zero-count, no-ResultSet result
  without an unnecessary result fetch; preserve DML affected counts and errors (R4, R15).
- [ ] T25: Target APC result and chunk GETs with one compute query field, null body, correct
  `row_offset`, and untouched presigned URLs and headers (R2).
- [ ] T11: Complete inline and external multi-page reading using the final continuation and
  row offset, including a manifest-only first result (R12).
- [ ] T20: Complete the AC13 Arrow logical-type matrix from
  `type_text`/`type_json` without silent coercion (R11, Q7).
- [ ] T9: Gate single prepared execution on the live V8 capability, interpolate below V8,
  and support binding-free pre-execution `DESCRIBE QUERY` metadata (R6).
- [ ] T21: Support V10 opt-in native APC parameter batches, one-per-set fallback, sequential
  Statement batches, and correct success/failure update counts (R6, R15).
- [ ] T22: Reject native complex bindings before sending any request, with a tested
  driver-owned error name/code and telemetry taxonomy disposition (R6).
- [ ] T12: Use warehouse SEA timeout semantics and targeted APC cancellation (R14).

### Phase 4 — Metadata, telemetry, and release

- [ ] T15: Emit one routing event per connection attempt, including failures; redact APC
  org/cluster and compound IDs in synchronous and asynchronous diagnostics (R8).
- [ ] T16: Document the public APC routing override, batch fallback cost, and SEA tuning
  exceptions; add the user-visible changelog entry in the first JDBC PR.
- [ ] T13: Complete synchronous SHOW and procedure SELECT metadata through public JDBC,
  with operation-specific request shape, native conversion, and existing null-catalog fallback
  (R5).
- [ ] T14: Decode either Arrow IPC or `columnar_data` per metadata response, including the
  physical vector and logical-value parity of AC16 (R5, R13).
- [ ] T23: Add the seven-operation, session-reachable async SHOW API and live V9 gate.
  Complete both legal payloads and below-V9 synchronous handles before exposure (R5).
- [ ] T24: Preserve the null-catalog schemas/columns fan-out and error, timeout, and
  cancellation behavior in async metadata (R5).
- [ ] T17: (Universe) Define the default-off APC sub-flag under the existing client-config
  batch flag; do not assign or ramp it (R1, Q8).
- [ ] T26: Verify paired APC and warehouse SDK retry, pool, and presigned-link behavior
  without introducing APC-only transport controls (R17).

### Validation

- [ ] T18: Run build, unit, fake-service, and dogfood checks across the complete JDBC stack.
  Record AC27 polling and AC30–AC32 parity measurements. Dogfood metadata, inline-only, and
  TerminateSession checks wait for the tracked server fixes; local contract checks run first.
  Verify the Universe flag exists and the Q9 connector-service partner override is deployed
  before any APC flag assignment or ramp. Record externally gated checks as pending, never
  passed, until evidence exists (R1–R17; AC1–AC34).
- [ ] T-val: Record a pass/fail or externally pending evidence link for every AC1–AC34,
  exact commands and failures, the AC13 type decision, and release-gate measurements. Do not
  declare release readiness while Q7, Q9, or server-gated checks remain open.
