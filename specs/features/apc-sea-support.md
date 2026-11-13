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

- Server side is shipped: `SqlExecBackendRouter` (universe `cmdexec/sql-exec-api`) routes a
request to Reyden when it carries `backend_compute.all_purpose_cluster_compute.cluster_id`; this
route is guarded by `databricks.sqlexecapi.enable_apc_reyder_routing`. APC does not support
`GetStatementStatus` — clients must poll with `GetStatementResult`. In this document, Reyder is
the SEA service and Reyden is the runtime backend.
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
  Schema comes only in `manifest.schema` (`ColumnInfo` with `type_text`/`type_json`);
  `manifest.arrow_schema`, `total_chunk_count`, `chunks`, `total_row_count`, and
  `total_byte_count` are empty. Client fetches until `next_chunk_index` is empty;
  `GetResultData.chunk_index` is cosmetic and `row_offset` is the real offset.
  `GetStatementResult` may or may not include result data alongside the manifest.
  `GetStatementStatus` is unsupported. Arrow-disabled customers stay on Thrift (no HIVE for
  non-metadata queries).
  - Metadata: SHOW commands via ExecuteStatement with `X-Databricks-Metadata-Operation-Type`;
  synchronous calls send `X-Databricks-Sea-Can-Run-Fully-Sync: true` when
  `EnableSeaSyncMetadata=1` (the default), while disabled or asynchronous calls omit that header;
  `format=ARROW_STREAM` (server rejects `COLUMN_BASED_SET`). Reyder always runs native Thrift
  metadata RPCs, sets
  `is_native_metadata_result=true`, ignores `X-Databricks-Require-Thrift-Native-Metadata`, and
  returns metadata inline in one of two response representations: Arrow IPC in
  `chunk.attachment` (the primary/majority case), or HIVE-shaped `chunk.columnar_data`
  (`ColumnarData`/`ColumnVector` with little-endian null masks). The client must multiplex
  between these representations for each response; it must not bind a connection to one decoder
  based on the requested `ARROW_STREAM` format or an assumed DBR version.
  - Capability: CreateSessionResponse `negotiated_protocol_version` (APC only): parameters at
  ≥ 42248 (V8), `parameter_sets` batch inserts at ≥ 42250 (V10).
  - Query timeout: follow the warehouse SEA path (client-side `TimeoutHandler`).
  - Govcloud / regions without Reyder: server errors propagate to the client; the driver does not
  fall back to Thrift.
  - Rollout (M15): opt-in first, then default APC to Reyder. This spec implements that with a
  dedicated APC client-config flag and a client override (R1, Q8).
- Driver today (as of `origin/main` @ `0cb8c241`, 2026-11-12):
  - `AllPurposeCluster` (`common/AllPurposeCluster.java`) holds `orgId` + `clusterId`, parsed
  from `/o/<org>/<cluster>` HTTP paths (including `sql/protocolv1/o/<org>/<cluster>`);
  the legacy `cliservice` path maps to placeholder `default` ids.
  - `DatabricksConnectionContext#getClientTypeFromContext` returns `THRIFT` for any
  `AllPurposeCluster` before any other check, so `UseThriftClient=0` is ignored on cluster URLs.
  For warehouses: `UseThriftClient=1/0` is honored; `TreatMetadataCatalogNameAsPattern=1` forces
  Thrift when `UseThriftClient` is unset; absent either explicit setting, Thrift is also forced
  when CloudFetch is explicitly off (`EnableQueryResultDownload=0`, default `1`) AND bounded SEA
  inline Arrow is unavailable; AIX/PowerPC without Arrow uses Thrift. With defaults, the deciding
  gate is the server flag
  `databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc`, which this spec
  assumes is 100% ON — so default warehouse connections use SEA with bounded inline Arrow.
  - Arrow defaults off on AIX/PowerPC unless `EnableArrow=1` is explicit; it is always on
  elsewhere, where `EnableArrow=0` is deprecated and ignored.
  - `isBoundedSeaApiEnabled` (`UseBoundedSeaApi`) uses `resolveFeatureFlag`, which returns `false`
  for any non-warehouse compute unless the user sets the param explicitly.
  - `DatabricksSession#open` builds `DatabricksThriftServiceClient` or `DatabricksSdkClient`.
  The HTTP 307 fallback was removed in #1749; the 429 fallback and `SeaCircuitBreakerManager`
  were removed in #1744. There is no remaining SEA→Thrift fallback. `UseQueryForMetadata` was
  removed in #1747, and SEA always uses `DatabricksMetadataQueryClient`.
  - `DatabricksSdkClient#createSession` POSTs to `/api/2.0/sql/warehouses/{id}/sessions`
  (`WAREHOUSE_SESSION_PATH`, #1746); the `x-databricks-rt-session` response header selects
  warehouse-scoped follow-up URLs (`realtimeSession`) vs standard `/sessions` and
  `/statements` URLs.
  - Warehouse is assumed throughout: `(Warehouse)` casts in `DatabricksSdkClient` (createSession,
  session/statement paths, deleteSession, execute, error messages), `SessionId`, and
  `StatementId`. APC cannot be served without removing these.
  - Execution polling uses `GET /statements/{id}`, while `DatabricksSdkClient#checkStatementAlive`
  builds `GET /statements/{id}/status` inline for result heartbeats. The constant
  `STATEMENT_STATUS_PATH_WITH_ID` is unused, but the unsupported status RPC is still called when
  heartbeats are enabled.
  - Bounded SEA inline Arrow (#1734, `SeaInlineArrowChunkProvider`, LZ4 in
  `DecompressionUtil`) and CloudFetch external links are the existing SEA result paths.
  - `DatabricksSdkClient` already sends `X-Databricks-Metadata-Operation-Type` for SEA metadata
  via `DatabricksMetadataQueryClient`. `ResultManifest` already has
  `is_native_metadata_result`, and `DatabricksResultSet` already uses it to select native-shaped
  metadata; `ResultData` does not yet have `columnar_data`, and `ColumnInfo` has `type_text` but
  not `type_json`.
  - `PreparedStatementBatchExecutor` requires `EnableNativeBatching=1` (default `0`), a parsed
  parameterized INSERT, and a supported `NativeBatchExecutor` for native batching.
  `DatabricksPreparedStatement` currently supplies no native executor, so it uses the legacy
  path. With `EnableBatchedInserts=1`, that path may combine parameter sets into a multi-row
  INSERT. `supportManyParameters=1` currently selects interpolation at statement construction.
  - `DatabricksSession#open` has warehouse Thrift→SEA recovery for Reyden KP001 errors; APC
  routing must remain fixed after the initial choice, including on that error path.
- Draft implementation PR [#1771](https://github.com/databricks/databricks-jdbc/pull/1771)
ports the 41-file APC-over-SEA prototype onto current `main`; frozen benchmarking PR
[#1756](https://github.com/databricks/databricks-jdbc/pull/1756) remains unchanged. The draft
adds APC request targeting and Arrow-batch result handling, enables the prototype by default,
uses `UseThriftClient=0/1` to select SEA/Thrift, and emits `JDBC_APC_ROUTE` markers. It is an
iteration baseline, not the production implementation described by the requirements below.

## Requirements

- R1: APC routing to SEA is controlled by a dedicated APC client-config flag (Q8), separate from
the warehouse flag `enableSqlExecForJdbc`, with this precedence: Arrow unavailable → Thrift,
including when `UseThriftClient=0`; otherwise an explicit `UseThriftClient` value wins (`0` → SEA,
`1` → Thrift); when `UseThriftClient` is unset, explicit
`TreatMetadataCatalogNameAsPattern=1` → Thrift; otherwise the APC flag decides (ON → SEA, OFF,
absent, or lookup failure → Thrift). The explicit setting's precedence over the metadata parameter
matches warehouse behavior, but Arrow-first is APC-specific: warehouse routing remains unchanged
and still lets explicit `UseThriftClient=0` select SEA before its Arrow check.
`enableSqlExecForJdbc` has no effect on APC routing. This applies to HTTP paths with a concrete
`/o/<org>/<cluster>` target; the legacy `cliservice` path has only placeholder `default` ids and
continues on Thrift rather than sending an invalid APC SEA target.
- R2: For APC over SEA, every SEA call (CreateSession, ExecuteStatement, GetStatementResult,
GetResultData, CancelStatement, CloseStatement, and TerminateSession) carries
`backend_compute.all_purpose_cluster_compute.cluster_id` and omits `warehouse_id`. The driver's
`deleteSession` method and `DeleteSessionRequest` model implement the TerminateSession server RPC.
CreateSession, ExecuteStatement, and CancelStatement encode `backend_compute` in the JSON body.
GetStatementResult, GetResultData, CloseStatement, and the driver's bodiless TerminateSession
DELETE encode the cluster id as the URL-encoded dotted query field
`backend_compute.all_purpose_cluster_compute.cluster_id`. Query fields are merged with existing
parameters such as `row_offset` through a query builder; they are not appended as a second `?`
fragment. Only the SEA `GET /statements/{id}/result/chunks/{n}` GetResultData request gets these
query fields. Presigned external-link URLs and their required headers are used byte-for-byte as
returned, without adding the compute field or otherwise modifying them. The driver-owned body
models are `CreateSessionRequest`, `ExecuteStatementRequest`, and `CancelStatementRequest`;
`GetStatementRequest` and `GetStatementResultChunkNRequest` come from the SDK, so APC GET/DELETE
targeting is built on the request URL rather than by changing those SDK classes or relying on
the existing `DeleteSessionRequest` warehouse query serialization.
- R3: For APC over SEA, execution polling uses `GetStatementResult`. Result heartbeats are
disabled for APC even when `EnableHeartbeat=1`, because `GetStatementStatus` is unsupported and
using full `GetStatementResult` after completion can re-download result payloads. The driver logs
that heartbeat is unsupported and never schedules `checkStatementAlive` or calls `/status`.
- R4: APC non-metadata execution sends `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`,
and a `session_id`, never sends `statements`, and handles LZ4-compressed Arrow batches for both
inline results and external links. `EnableQueryResultDownload=0` keeps the connection on SEA but
sets the CreateSession `can_cloud_download=false` session config, requiring inline results even
though the request disposition remains `INLINE_OR_EXTERNAL_LINKS`. The driver follows every inline
page until the server indicates completion. If the server cannot serve the complete result inline,
the driver propagates the server error and must not expose a successfully completed, truncated
ResultSet. APC compute type—not
`isBoundedSeaApiEnabled`—selects this format/disposition and enables the session-config behavior.
- R5: APC metadata operations go through `DatabricksMetadataQueryClient` as SHOW commands with
`X-Databricks-Metadata-Operation-Type`; synchronous calls send
`X-Databricks-Sea-Can-Run-Fully-Sync: true` only when `EnableSeaSyncMetadata=1`; synchronous calls
with that setting disabled and all asynchronous calls omit the sync header. APC preserves the
existing `X-Databricks-Require-Thrift-Native-Metadata: true` behavior when
`isThriftNativeMetadataEnabled()` is true and the operation supports it, although Reyder ignores
that header because APC metadata is always native. Requests use `format=ARROW_STREAM` and
multiplex per response between Arrow IPC in `chunk.attachment` (the primary path) and HIVE-shaped
`chunk.columnar_data` (the alternate path). Decoder selection uses the returned response
representation, not the requested format or a cached connection-level choice. Both
representations use native-Thrift-shaped metadata decoding when
`is_native_metadata_result=true`.
- R6: The driver records `negotiated_protocol_version` from CreateSession and gates native
parameters at ≥ 42248 (V8). It sends `parameter_sets` only for a parameterized INSERT at
≥ 42250 (V10) when `EnableNativeBatching=1` and an APC-capable `NativeBatchExecutor` is selected;
the version alone never enables native batching. For APC below V10, with native batching off, or
without that executor, execute each parameter set separately rather than combining sets through
`EnableBatchedInserts`. V8–V9 use native `parameters` per execution; below V8 uses the existing
interpolation and escaping path even when `supportManyParameters` is unset. When an APC session
below V8 downgrades to client-side parameter interpolation, the driver records that downgrade in logs and
telemetry. A missing protocol version is treated conservatively as below V8: native parameters
and `parameter_sets` are disabled, and telemetry distinguishes a missing-version downgrade from a
present but old version.
- R7: APC uses Thrift only when the pre-connection routing decision selects Thrift (an explicit
`UseThriftClient=1`, `TreatMetadataCatalogNameAsPattern=1` with no explicit `UseThriftClient`, the
APC client-config flag is off or unavailable, or Arrow is unavailable). `UseBoundedSeaApi` does
not control APC routing. Once SEA is selected, server errors—including unsupported
regions—propagate to the client exactly as they do for warehouse SEA; the driver never switches
the connection to Thrift. The warehouse-only Reyden KP001 Thrift→SEA recovery must not change
an APC routing decision either.
- R8: Telemetry and logs identify the compute type (APC vs warehouse) and the chosen protocol for
each connection, including the routing reason and any connection failure.
- R9: `IDatabricksComputeResource` handling in `DatabricksSdkClient`, `SessionId`, and
`StatementId` is type-safe — no unchecked cast to `Warehouse`. `ImmutableSessionInfo` records the
client type, and `SessionId.create` dispatches on it rather than compute-resource class. New APC
SEA identifiers serialize a compute discriminator, org id, cluster id, and raw session/statement
id: sessions use `s|a|orgId|clusterId|sessionId|protocolVersion-or--`, and statements use
`s|a|orgId|clusterId|statementId`. The `-` marker preserves an absent protocol version. Parsing
remains backward-compatible with existing warehouse `s|warehouseId|sessionId[|r]` and raw SEA
statement ids. These compound forms are internal routing serialization only: use a separate
internal statement-id serializer. `StatementId.toString()`, `toSQLExecStatementId()`, public
`IDatabricksResultSet.getStatementId()`, logs, and telemetry continue to expose only the raw
server statement id.
- R10: APC ExecuteStatement preserves the existing SEA timeout behavior: `wait_timeout` is unset
for synchronous execution with direct results and `0s` for asynchronous execution or when direct
results are disabled. Reyder accepts `0s` as the SEA duration-string representation of zero; no
APC-specific timeout override is needed.
- R11: For APC `ARROW_BATCH` results, the driver builds the Arrow schema from
`manifest.schema` (`type_text`/`type_json`, including nested types) and does not require
`manifest.arrow_schema`.
- R12: APC result fetching pages by `next_chunk_index` and `row_offset`, never relies on
`total_chunk_count`, `chunks`, or `total_row_count`, and accepts a `GetStatementResult`
response with or without inline result data.
- R13: The driver can read the SEA `columnar_data` (HIVE) result shape — every `ColumnVector`
value type used by metadata results, with null masks — into a JDBC ResultSet.
- R14: APC query timeouts use the client-side `TimeoutHandler` + CancelStatement path, as for
warehouses.
- R15: `Statement.addBatch(String)` / `executeBatch` on APC executes SQL commands sequentially,
one ExecuteStatement request per SQL string, preserving the existing stop-on-first-failure and
update-count behavior. It never sends the unsupported `statements` field.
- R16: SEA request and response models use a driver-owned `SqlExecResultFormat` enum rather than
the SDK-owned `com.databricks.sdk.service.sql.Format`. It preserves existing wire values, adds
`ARROW_BATCH`, serializes exact API names, tolerantly maps unknown response values to `UNKNOWN`,
and produces a clear unsupported-format error if an unknown value reaches result decoding.

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
A6: No special `User-Agent` or client-capability header is needed. The request path distinguishes
warehouse and all-purpose-cluster traffic; preserve the driver's ordinary User-Agent unchanged.
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
explicit rollout decision rather than inheriting the OSS driver ramp.

## Acceptance Criteria

- AC1: GIVEN a cluster HTTP path with no client setting AND the APC flag OFF or absent (or the
flag lookup fails), even with `enableSqlExecForJdbc` ON, WHEN a connection is opened THEN the
driver uses `DatabricksThriftServiceClient` and no SEA request is sent; GIVEN the APC flag ON
AND Arrow available THEN it uses SEA. GIVEN a partner User-Agent WHEN connector-service returns
the APC sub-flag THEN its value is `false` regardless of the underlying flag assignment; an OSS
User-Agent receives the assigned value. GIVEN the legacy `cliservice` HTTP path THEN the driver
continues to use Thrift and never sends placeholder `default` ids to SEA.
- AC2: GIVEN a cluster HTTP path with APC-SEA enabled (R1) WHEN a connection is opened THEN the
CreateSession request body contains `backend_compute.all_purpose_cluster_compute.cluster_id`
equal to the URL's cluster id and contains no `warehouse_id`.
- AC3: GIVEN an open APC-SEA session WHEN a statement is executed, polled, fetched, cancelled, and
closed, and separately when the session is terminated, THEN every corresponding SEA request
includes the APC
`backend_compute` using the body/query encoding in R2 and the driver issues no
`GetStatementStatus` or `/status` call.
- AC4: GIVEN an APC-SEA query whose LZ4-compressed Arrow result is returned inline or through
multi-chunk external links WHEN the ResultSet is iterated THEN all rows and types match the same
query executed over Thrift, with each payload decompressed before the generated Arrow schema
message is prepended to the record batches.
- AC5: GIVEN an APC-SEA session WHEN `DatabaseMetaData.getTables/getColumns/getSchemas/getCatalogs`
is called synchronously with `EnableSeaSyncMetadata=1` THEN the request is a SHOW command with
`format=ARROW_STREAM`, `X-Databricks-Metadata-Operation-Type` set, and
`X-Databricks-Sea-Can-Run-Fully-Sync: true`; GIVEN `EnableSeaSyncMetadata=0` or an asynchronous
metadata call THEN the sync header is absent. GIVEN `isThriftNativeMetadataEnabled()` and a
supported operation THEN `X-Databricks-Require-Thrift-Native-Metadata: true` remains present.
WHEN any response contains Arrow IPC in
`chunk.attachment` or HIVE-shaped data in `chunk.columnar_data` with
`is_native_metadata_result=true` THEN the driver selects the matching decoder for that response
and the ResultSet matches the Thrift metadata result. Both representations must work on the same
connection without reconnecting or changing protocol.
- AC6: GIVEN an APC-SEA session with `negotiated_protocol_version` below V8 WHEN a
parameterized PreparedStatement is executed THEN the driver uses the non-native
(client-interpolated) path instead of sending SEA parameters and emits a parameter-downgrade log
and telemetry signal; escaping and injection tests with quotes, comments, delimiters, and
backslashes produce the same values as the existing client-interpolated path without executing
injected SQL.
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
- AC9: GIVEN APC-SEA enabled (R1) AND CreateSession returns an "all-purpose clusters are not
supported" error WHEN a connection is opened THEN the driver propagates a clear connection error,
does not create a Thrift client, and records the failure in telemetry.
GIVEN APC was routed to Thrift and OpenSession returns KP001 THEN the warehouse-only recovery
does not switch it to SEA, regardless of whether the APC flag was off or Arrow was unavailable.
- AC10: GIVEN APC or warehouse connections selected by each supported routing reason WHEN
connection telemetry is emitted THEN its event records compute type, selected protocol, and
routing reason; GIVEN an APC SEA CreateSession failure THEN it records failure without emitting
credentials or a compound statement id.
- AC11: GIVEN standard and Real-time SQL warehouse connections WHEN CreateSession, execute,
poll, fetch, cancel, close, and TerminateSession run THEN serialized request bodies, URLs, and
headers match pre-change fixtures, including warehouse `realtimeSession` behavior.
- AC12: GIVEN an APC-SEA session WHEN a statement is executed with and without direct results
THEN `wait_timeout` is unset and `0s` respectively, Reyder accepts both request shapes, and
`statements` is never sent.
- AC13: GIVEN an APC `ARROW_BATCH` response with empty `arrow_schema` and a `manifest.schema`
containing primitive, decimal, array, map, struct, interval, geography, geometry, timestamp,
timestamp_ntz, and variant columns WHEN the ResultSet is read THEN values and
`ResultSetMetaData` match the same query over Thrift.
- AC14: GIVEN an APC result with empty `total_chunk_count`/`total_row_count` spread across
several chunks WHEN the ResultSet is iterated THEN the driver fetches with `row_offset` starting
at zero and advancing by the previous page's row count until `next_chunk_index` is empty, and
returns every row exactly once; each fetch URL for the SEA GetResultData endpoint contains both
`row_offset` and the dotted APC compute query
field exactly once, while each presigned external-link URL and its headers are used unchanged.
- AC15: GIVEN a completed APC statement WHEN `GetStatementResult` returns a manifest without
result data, and separately WHEN it returns a manifest with result data THEN both produce the
full correct ResultSet.
- AC16: GIVEN `columnar_data` chunks covering every metadata `ColumnVector` value variant,
including string and integer vectors, empty vectors, and little-endian null masks crossing a
byte boundary WHEN they are read THEN nulls and values land at the right row positions;
GIVEN the next metadata operation on the same connection returns Arrow IPC in `attachment` THEN
it is decoded through the Arrow path,
not the previously used `columnar_data` path.
- AC17: GIVEN an APC-SEA session with `negotiated_protocol_version` ≥ 42250 (V10),
`EnableNativeBatching=1`, and a supported native executor WHEN `executeBatch` runs a
parameterized INSERT THEN the request uses `parameter_sets` and returns one update count per
input set. GIVEN V10 with `EnableNativeBatching=0` or no supported executor, or version
42248–42249 (V8–V9), THEN it sends one execution per set with native `parameters` and no
`parameter_sets`. GIVEN a version below 42248 (below V8) or a missing version THEN it sends one
execution per set with client-side interpolation and neither `parameters` nor `parameter_sets`.
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
`parameters` or `parameter_sets`, and emits telemetry identifying the protocol version as missing.
- AC24: GIVEN an APC-SEA connection with `EnableHeartbeat=1` WHEN a completed ResultSet remains
open beyond multiple heartbeat intervals THEN no heartbeat task is scheduled, no
`GetStatementStatus`, `/status`, or heartbeat `GetStatementResult` request is sent, and the driver
logs that heartbeat is unsupported for APC.
- AC25: GIVEN an APC-SEA session with org id, cluster id, session id, and a present or absent
negotiated protocol version WHEN its `SessionId` and a derived `StatementId` are serialized and
deserialized THEN client type, APC compute target, raw ids, and protocol capability round-trip
unchanged; existing warehouse and Thrift serialized identifiers still parse identically. Creating
an APC SEA `SessionId` never dereferences a Thrift session handle. The serialized APC forms match
the R9 formats exactly, including `-` for a missing protocol version. Public
`IDatabricksResultSet.getStatementId()`, `StatementId.toString()`, logs, and telemetry return the
raw server statement id and never expose the internal compound form, org id, or cluster id.
- AC26: GIVEN an ExecuteStatement request with `SqlExecResultFormat.ARROW_BATCH` WHEN serialized
THEN its JSON contains `"format":"ARROW_BATCH"`; GIVEN a manifest with `ARROW_BATCH` WHEN
deserialized THEN APC decoding is selected; GIVEN an unknown future format THEN deserialization
succeeds as `UNKNOWN` and result decoding raises the standard unsupported-format error.

## Constraints

- APC default protocol is decided by the dedicated APC flag (R1, default off); after the Arrow
availability prerequisite, an explicit client setting always wins; when it is unset,
`TreatMetadataCatalogNameAsPattern=1` forces Thrift before the flag is consulted. No change to
warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`) and `NEXT_CHANGELOG.md` updated.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server-side SEA/Reyden APC routing (already shipped).
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

1. Remove the unconditional `AllPurposeCluster → THRIFT` early return in
   `getClientTypeFromContext` and route APC with this precedence:
   1. Arrow unavailable → Thrift, regardless of client setting or APC flag.
   2. Explicit `UseThriftClient`: `1` → Thrift, `0` → SEA.
   3. Explicit `TreatMetadataCatalogNameAsPattern=1` → Thrift.
   4. Otherwise the dedicated APC flag (`APC_SQL_EXEC_FLAG_NAME`, Q8): ON → SEA; OFF, absent, or
      lookup failure → Thrift. `SQL_EXEC_FLAG_NAME` and `UseBoundedSeaApi` do not decide APC
      protocol selection.

   Once selected, SEA errors propagate without switching to Thrift. Server side (universe,
   separate PR): create the APC sub-flag under
   `databricks.partnerplatform.clientConfigsFeatureFlags`, default off, and target it only to
   Reyder-supported regions. No connector-service version filtering is needed because older
   drivers ignore unknown sub-flags.
   In `connector-service/src/featureflags/FeatureFlagsHandler.scala`, unconditionally apply the
   same partner User-Agent override as the warehouse flag so
   `enableSqlExecForJdbcApc` is returned as `false` for partners.
2. Add wire models `BackendCompute { AllPurposeClusterCompute all_purpose_cluster_compute }` and
   `AllPurposeClusterCompute { cluster_id }`. Add optional `backend_compute` to the driver-owned
   JSON body models; encode APC targeting for SDK-owned GET models and bodiless DELETEs in the URL.
3. Add a small `SeaComputeTarget` helper in `dbclient/impl/sqlexec` that, given an
   `IDatabricksComputeResource`, produces both the request target (`warehouse_id` or
   `backend_compute`) and the URL family. Warehouses keep today's behavior
   (`WAREHOUSE_SESSION_PATH` for CreateSession, `realtimeSession` header for follow-ups). APC uses
   the standard `/api/2.0/sql/sessions/` and `/statements/` paths with `backend_compute` in the
   JSON body for CreateSession, ExecuteStatement, and CancelStatement. For GetStatementResult,
   GetResultData, CloseStatement, and the bodiless TerminateSession DELETE, append the URL-encoded
   dotted query field `backend_compute.all_purpose_cluster_compute.cluster_id`; do not rely on
   JSON bodies for GET/DELETE bindings that declare no body. Use a query builder to merge the
   compute field with `row_offset` and any other existing parameters rather than concatenating
   query strings. APC never sets `realtimeSession`. Route every `(Warehouse)` cast in
   `DatabricksSdkClient`, `SessionId`, and `StatementId` through the helper. Do not pass
   `SeaComputeTarget` through to CloudFetch: use presigned external-link URLs and headers exactly
   as returned. Add `DatabricksClientType` to `ImmutableSessionInfo`; make `SessionId.create`
   dispatch on it. Preserve legacy warehouse identifier parsing, and add APC SEA session and
   statement forms carrying the compute discriminator, org/cluster ids, raw id, and (for sessions)
   nullable negotiated protocol version so reconstruction cannot turn APC into warehouse compute
   or lose capabilities. Keep compound serialization internal; public statement-id APIs,
   `StatementId.toString()`, logs, and telemetry continue returning the raw server id.
4. APC execute request: `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`, `session_id`,
   no `statements`; preserve existing timeout behavior (`wait_timeout` unset for synchronous
   direct results and `0s` for async or direct results disabled). Execution polling uses
   `GET /statements/{id}` with `GetStatementResult`. Disable result heartbeat registration for
   APC instead of routing `checkStatementAlive` through a full-result endpoint; APC never calls
   `/status`. Accept polling responses with or without inline result data. Preserve
   `EnableQueryResultDownload=0` by setting CreateSession session config
   `can_cloud_download=false`. Select the APC format/disposition and set this session config via
   an explicit APC compute-type check, not `isBoundedSeaApiEnabled`.
5. APC result decoding: add an `ArrowBatchSchemaBuilder` that converts `manifest.schema`
   (`type_text`/`type_json`) into an Arrow `Schema`. Decompress each LZ4 payload first, then prepend
   the generated Arrow schema message to the bare record batches so the existing Arrow reader
   (`SeaInlineArrowChunkProvider` for inline, CloudFetch for external links) can consume them. For
   APC, enter the streaming provider through an explicit compute-type check, never through
   `isBoundedSeaApiEnabled`; page by `next_chunk_index`/`row_offset` and never read
   `total_chunk_count`/`total_row_count`.
   Replace the SDK-owned `Format` in driver SEA models with a driver-owned
   `SqlExecResultFormat` that includes `ARROW_BATCH` and `UNKNOWN`; update request serialization,
   manifest deserialization, result-factory/result-set switches, and telemetry mappings.
6. APC metadata: send `format=ARROW_STREAM`; synchronous metadata sends
   `X-Databricks-Sea-Can-Run-Fully-Sync: true` only when `EnableSeaSyncMetadata=1`, while disabled
   and asynchronous calls omit it. Preserve the existing
   `X-Databricks-Require-Thrift-Native-Metadata: true` condition even though Reyder ignores it.
   Add a `ColumnarDataResult` reader for `chunk.columnar_data` (null masks,
   string/integer/etc. vectors). For every metadata response, multiplex between
   `chunk.attachment` (Arrow IPC, primary path) and
   `chunk.columnar_data` (alternate path) using the returned payload representation; do not cache
   a decoder choice at connection scope or infer it solely from the requested format or DBR
   version. When `is_native_metadata_result=true`, use the Thrift-native metadata column decoders
   instead of the SHOW-command decoders for either representation.
7. Query timeout for APC reuses `TimeoutHandler` → CancelStatement with `backend_compute`.
8. Persist `negotiated_protocol_version` in `ImmutableSessionInfo`; expose capability checks:
   native parameters at ≥ 42248, and `parameter_sets` for parameterized INSERTs only at ≥ 42250
   with `EnableNativeBatching=1` and an APC-capable `NativeBatchExecutor`. Wire that executor into
   `DatabricksPreparedStatement`; the current constructor uses an unsupported executor. Otherwise
   execute each set separately, bypassing the legacy multi-row `EnableBatchedInserts` optimization
   for APC. Force the tested interpolation path below V8 even when `supportManyParameters=0`;
   treat a missing version as below V8 and record a distinct missing-version downgrade signal.
   Keep non-parameterized `Statement` batches on `DatabricksBatchExecutor`, which sends one
   ExecuteStatement per SQL and never populates `statements`.
9. Preserve warehouse SEA error behavior in `DatabricksSession#open`: APC CreateSession failures
   propagate without constructing a Thrift client or changing the selected protocol. Scope the
   existing warehouse KP001 Thrift→SEA recovery to warehouses so an APC choice of Thrift stays
   on Thrift.
10. Emit compute type, protocol, routing reason, and connection failure in telemetry and debug
    logs. Keep the ordinary JDBC User-Agent unchanged; request paths distinguish APC from
    warehouse traffic.

### Affected Components

- `src/main/java/com/databricks/jdbc/api/impl/DatabricksConnectionContext.java` — APC routing
  (Arrow availability > explicit `UseThriftClient` >
  `TreatMetadataCatalogNameAsPattern=1` > APC flag), `APC_SQL_EXEC_FLAG_NAME`, client type.
- universe `feature-flag/configs/` (new APC sub-flag) and
  `connector-service/src/featureflags/FeatureFlagsHandler.scala` (unconditional partner
  User-Agent override).
- `src/main/java/com/databricks/jdbc/api/impl/DatabricksSession.java` — verify APC SEA errors propagate without
fallback; keep warehouse KP001 Thrift→SEA recovery off APC.
- `src/main/java/com/databricks/jdbc/api/impl/SessionInfo.java` — client type and
  `negotiatedProtocolVersion`.
- `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClient.java` — compute targeting, APC URL
family, disposition/format selection.
- `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/PathConstants.java` — reuse standard session/statement
paths for APC (no new constants expected).
- `src/main/java/com/databricks/jdbc/dbclient/impl/common/SessionId.java` and
  `src/main/java/com/databricks/jdbc/dbclient/impl/common/StatementId.java` — dispatch by client
  type; add backward-compatible APC compute/capability serialization and remove warehouse-only
  assumptions.
- `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/` — new `SeaComputeTarget` helper.
- `src/main/java/com/databricks/jdbc/model/client/sqlexec/` — `BackendCompute`,
  `AllPurposeClusterCompute`; `backend_compute` on the driver-owned
  CreateSession/ExecuteStatement/Cancel request bodies. `DeleteSessionRequest` implements the
  TerminateSession RPC for warehouses; APC uses URL targeting. SDK-owned GetStatement and
  GetStatementResultChunkN requests use URL targeting; CloseStatement uses URL targeting because
  the DELETE binding has no body.
- `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksMetadataQueryClient.java` — APC metadata
format/header, native-result decoder selection.
- `src/main/java/com/databricks/jdbc/api/impl/arrow/` — `ArrowBatchSchemaBuilder`; schema prepend for bare
`ARROW_BATCH` record batches; `row_offset` paging for APC.
- `src/main/java/com/databricks/jdbc/api/impl/` — new `ColumnarDataResult` reader for
  `chunk.columnar_data`.
- `src/main/java/com/databricks/jdbc/model/core/` — driver-owned `SqlExecResultFormat`;
  `ColumnarData`/`ColumnVector` models, `columnar_data` on `ResultData`, and `type_json` on
  `ColumnInfo`. The manifest already
  has `is_native_metadata_result`; add `negotiated_protocol_version` to CreateSessionResponse in
  `model/client/sqlexec/`.
- `src/main/java/com/databricks/jdbc/telemetry/` — compute type, protocol, routing-reason,
  parameter-downgrade, and failure fields.
- `src/test/java/com/databricks/jdbc/` — unit tests per AC; fake-service / integration tests.
- `NEXT_CHANGELOG.md`, `docs/` — document APC routing and the `UseThriftClient` override.

### Data Model Changes

- New SEA wire models `BackendCompute` and `AllPurposeClusterCompute` (JSON:
`{"backend_compute": {"all_purpose_cluster_compute": {"cluster_id": "..."}}}`).
- `ImmutableSessionInfo` gains `clientType` and nullable `negotiatedProtocolVersion`.
- Response models gain `chunk.columnar_data` (`ColumnarData { repeated ColumnVector columns }`,
`ColumnVector { bytes null_mask; oneof values }`) and
`CreateSessionResponse.negotiated_protocol_version`; `ResultManifest.is_native_metadata_result`
already exists. `ColumnInfo` gains `type_json` to support the nested Arrow type matrix.
- New driver-owned `SqlExecResultFormat` replaces the SDK `Format` in SEA request/response models,
preserves existing JSON values, adds `ARROW_BATCH`, and maps unknown response values to `UNKNOWN`.

### API Changes

- `UseThriftClient` becomes meaningful on cluster URLs; previously it was ignored there.
Public statement ids remain the raw server ids; compound APC routing serialization is internal.
No public Java API changes.

### Risks

- Two kill switches with different behavior: turning off the APC client flag moves new
connections back to Thrift cleanly, while turning off `enable_apc_reyder_routing` makes the
server reject new APC SEA calls without fallback—and does nothing for sessions already open.
Prefer the client flag for rollback; the driver caches client-config flags and refreshes them on
the server-provided TTL (default 15 minutes,
`DatabricksDriverFeatureFlagsContext`), so rollback is not instant.
- Building the Arrow schema from `manifest.schema` may lose fidelity for nested, interval,
geospatial, or decimal types (Q7) — mitigated by AC13 type-matrix tests against Thrift and, if
needed, asking Reyder to populate `arrow_schema`.
- The `columnar_data` reader is a new, alternate metadata path while Arrow IPC remains the
  primary/majority path. Keep it isolated and metadata-only, but select it per response rather
  than assuming all responses from a connection or DBR version use the same representation.
- Native metadata results have different columns than SHOW-command results; choosing the wrong
decoder silently corrupts `DatabaseMetaData` — gate strictly on `is_native_metadata_result`.
- APC sessions below protocol V8 use client-side parameter interpolation. Reuse the existing
  escaping implementation, cover SQL-injection edge cases in AC6, and emit the downgrade signal
  required by R6 so operators can identify affected sessions. Treat a missing version
  conservatively as below V8 and distinguish it in telemetry so rollout or deserialization gaps
  are visible. The current prepared-statement interpolation choice is made before session
  capability is known, so override it for APC below V8 without changing warehouse behavior.
- The existing `EnableBatchedInserts` legacy path can combine several prepared parameter sets;
  bypass it for APC whenever `parameter_sets` is unavailable so the one-execution-per-set contract
  in AC17 remains true. The native executor is currently unwired and must be added explicitly.
- Missing `backend_compute` on any one request type causes server-side 400s mid-statement —
mitigated by centralizing in `SeaComputeTarget`, explicit per-binding body/query encoding in R2,
and AC3 serialized-request coverage of every call.
- Applying APC query parameters to a presigned external-link URL breaks its signature and can leak
  the cluster id to storage. Keep compute targeting confined to SEA requests and assert unchanged
  CloudFetch URLs and headers in AC14.
- Accidentally changing warehouse request bodies — mitigated by AC11 regression tests with
serialized-body assertions.
- Server-side routing disablement causes visible connection failures until the cached client flag
refreshes; mitigate with flag sequencing and R8 telemetry.
- Execution polling with `GET /statements/{id}` may fetch first-chunk data repeatedly; confirm
server returns status-only payloads while running. Do not use this endpoint for post-completion
heartbeats; APC heartbeat is disabled by R3.
- The SEA routing code has changed substantially through #1744, #1746, #1747, and #1749; rebase
often and do not restore the removed circuit-breaker, rate-limit, or HTTP 307 fallback paths.
- APC on the standard `/sessions` URL must not be mistaken for a Real-time session; the
`x-databricks-rt-session` header handling must stay warehouse-only.

---

## Tasks

### Phase 1 — Wire models and compute targeting

- [ ] T1: Add `BackendCompute`, `AllPurposeClusterCompute`, and driver-owned
  `SqlExecResultFormat` models with JSON serialization/deserialization tests, including
  `ARROW_BATCH` and unknown values (R16; AC26).
- [ ] T2: Add optional `backend_compute` to driver-owned CreateSession, ExecuteStatement, and
  CancelStatement JSON body models; verify that the driver's DeleteSession request covers the
  server's TerminateSession RPC. T3 owns APC query targeting. Do not edit SDK-owned
  GetStatement/GetStatementResultChunkN classes or add a body to GET/DELETE bindings (R2).
- [ ] T3: Add `SeaComputeTarget` (request target + URL family) and route every `(Warehouse)`
  cast in `DatabricksSdkClient`, `SessionId`, and `StatementId` through it; keep warehouse and
  Real-time routing byte-identical. Add serialized-request tests for the JSON-body and dotted-query
  encodings in R2, including TerminateSession and merging the compute field with `row_offset` (R2,
  R9; AC2, AC3, AC11, AC14). Assert that external-link URLs and headers remain unchanged. Add
  backward-compatible APC SEA `SessionId`/`StatementId` serialization and round-trip tests,
  dispatching `SessionId.create` by client type while keeping public statement ids raw (R9; AC25).

### Phase 2 — Routing and session lifecycle

- [ ] T4: Replace the `AllPurposeCluster → THRIFT` early return in `getClientTypeFromContext`
  with Arrow prerequisite > explicit `UseThriftClient` >
  `TreatMetadataCatalogNameAsPattern=1` > APC flag routing. Keep `UseBoundedSeaApi` out of APC
  protocol selection; preserve Thrift for the legacy `cliservice` placeholder path (R1, R7;
  AC1, AC7, AC8, AC19, AC20, AC21).
- [ ] T5: Persist client type and `negotiated_protocol_version` from CreateSession into
  `SessionInfo`, including through APC `SessionId` reconstruction (R6, R9; AC23, AC25).
- [ ] T6: Add tests that APC CreateSession failures propagate and never construct or switch to a
  Thrift client; scope the existing warehouse KP001 Thrift→SEA recovery so an APC Thrift choice
  does not switch protocol on KP001 (R7; AC9).

### Phase 3 — Execution and results

- [ ] T7: Disable result heartbeat registration for APC, with an unsupported-heartbeat log, and
  add tests that execution polling, fetch, cancel, and close use standard `/statements` URLs with
  `backend_compute` and never hit `/status` (R2, R3; AC3, AC24).
- [ ] T8: APC execute request shape: `ARROW_BATCH`, `INLINE_OR_EXTERNAL_LINKS`, `session_id`,
  no `statements`, `wait_timeout` unset/`0s`, and CreateSession `can_cloud_download=false` when
  result download is disabled. Use an APC compute-type check for format/disposition and session
  config; do not gate them on `isBoundedSeaApiEnabled` (R4, R10; AC4, AC8, AC12).
- [ ] T9: Gate APC native parameters at ≥ 42248 and `parameter_sets` for parameterized INSERTs
  at ≥ 42250 with `EnableNativeBatching=1` and a supported `NativeBatchExecutor`; wire that
  executor into `DatabricksPreparedStatement`. Below V10, when opt-in is off, or without an
  executor, send one execution per set and bypass `EnableBatchedInserts` multi-row combining.
  Below V8, force existing interpolation regardless of `supportManyParameters`; add escaping,
  injection, update-count, and parameter-downgrade logging/telemetry tests, including a missing
  version. Keep non-parameterized batches sequential with one ExecuteStatement per SQL (R6,
  R15; AC6, AC17, AC22, AC23).
- [ ] T10: Add `ArrowBatchSchemaBuilder` and prepend the built schema to bare record batches for
  inline and external-link results after LZ4 decompression; cover primitive, decimal, nested,
  interval, geospatial, timestamp/timestamp_ntz, and variant types. Add `type_json` to
  `ColumnInfo` and use the T1 `SqlExecResultFormat` in result-factory/result-set switches and
  telemetry (R4, R11, R16; AC4, AC13, AC26, Q7).
- [ ] T11: Route APC through its dedicated `next_chunk_index`/`row_offset` paging independent of
  `UseBoundedSeaApi`; handle `GetStatementResult` with and without inline data and never read
  chunk/row totals (R12; AC14, AC15).
- [ ] T12: Wire APC query timeout through `TimeoutHandler` → CancelStatement with
  `backend_compute` (R14; AC18).

### Phase 4 — Metadata and telemetry

- [ ] T13: APC metadata through `DatabricksMetadataQueryClient` and the SDK execute/result path:
  `format=ARROW_STREAM`, sync header, multiplex per response between Arrow IPC `attachment` and
  `columnar_data`, and pick native
  decoders when `is_native_metadata_result=true`; test sync-header enabled, disabled, and async
  cases plus the preserved require-native header condition (R5; AC5).
- [ ] T14: Add `ColumnarData`/`ColumnVector` models and the `ColumnarDataResult` reader with
  null-mask handling (R13; AC16).
- [ ] T15: Add compute type, protocol, routing reason, and connection failure to telemetry and
  debug logs (R8; AC10).
- [ ] T16: Document APC routing (APC flag, `UseThriftClient` override) in `docs/` and add a
  `NEXT_CHANGELOG.md` entry.
- [ ] T17: (universe) Create the APC sub-flag under
  `databricks.partnerplatform.clientConfigsFeatureFlags` in SAFE, default off, and add the
  connector-service partner User-Agent override that forces it false (R1; AC1, Q9).

### Validation

- [ ] T18: Run `mvn test -pl jdbc-core -am` and an end-to-end run against a dogfood APC with the
  APC flag on and off and with `UseThriftClient` set to 0 and 1; record the cluster runtime,
  region, protocol observed, and result for each configuration without storing credentials.
- [ ] T-val: Record a pass/fail evidence link for each AC1–AC26 (unit, fake-service, or dogfood
  run as appropriate), plus the exact build/test commands and failures. Verify warehouse request
  serialization and the AC13 type matrix before calling the JDBC stack ready; keep the release
  blocked under Q7 if any required Arrow type is lossy.
