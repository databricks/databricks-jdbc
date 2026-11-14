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
  ≥ 42248 (V8), `parameter_sets` batch inserts at ≥ 42250 (V10).
  - Query timeout: follow the warehouse SEA path (client-side `TimeoutHandler`).
  - Govcloud / regions without Reyder: server errors propagate to the client; the driver does not
  fall back to Thrift.
  - Rollout (M15): opt-in first, then default APC to Reyder. This spec implements that with a
  dedicated APC client-config flag and a client override (R1, Q8).
- Driver today (as of `origin/main` @ `1e6fe871`):
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
  - `DatabricksSdkClient.serializeStatementRequest` serializes SDK request objects for standard
  statement URLs, including polling and explicit result fetches, chunk fetches, and
  CloseStatement. APC uses those standard URLs, but the SEA REST bindings for
  GetStatementResult, GetResultData, and CloseStatement declare no request body.
  - Bounded SEA inline Arrow (#1734, `SeaInlineArrowChunkProvider`, LZ4 in
  `DecompressionUtil`) and CloudFetch external links are the existing SEA result paths.
  - `DatabricksSdkClient` already sends `X-Databricks-Metadata-Operation-Type` for SEA metadata
  via `DatabricksMetadataQueryClient`. `ResultManifest` already has
  `is_native_metadata_result`, and `DatabricksResultSet` already uses it to select native-shaped
  metadata; `ResultData` does not yet have `columnar_data`, and `ColumnInfo` has `type_text` but
  not `type_json`.
  `DatabricksMetadataQueryClient` currently calls only synchronous `executeStatement`;
  sessions expose it through `IDatabricksMetadataClient`, which has no async method.
  `IDatabricksClient.executeStatementAsync` has no `MetadataOperationType` argument, and
  `DatabricksSdkClient.executeStatementAsync` fixes the statement type to `SQL`.
  `DatabricksSdkClient.getStatementResult` also builds the completed ResultSet with
  `StatementType.SQL`, so async metadata needs a typed completion call as well as a typed submit.
  `ExecutionResultFactory` and `DatabricksResultSet` currently select only `ARROW_STREAM` and
  `JSON_ARRAY` SEA decoders.
  - `PreparedStatementBatchExecutor` requires `EnableNativeBatching=1` (default `0`), a parsed
  parameterized INSERT, and a supported `NativeBatchExecutor` for native batching.
  `DatabricksPreparedStatement` currently supplies no native executor, so it uses the legacy
  path. With `EnableBatchedInserts=1`, that path may combine parameter sets into a multi-row
  INSERT. `supportManyParameters=1` currently selects interpolation at statement construction.
  `LegacyPreparedStatementBatchExecutor.executeIndividualStatements` passes raw SQL and bindings
  directly to its statement executor; only its multi-row branch interpolates today.
  `DatabricksPreparedStatement.getMetaData()` before execution instead runs `DESCRIBE QUERY`:
  `getMetaDataFromDescribeQuery` quotes each real `?` with
  `SQLInterpolator.surroundPlaceholdersWithQuotes` but passes the original bindings to the new
  statement. `SQLInterpolator.interpolateSQL` ignores those quoted literals and rejects the
  nonempty binding map, so APC capability-based interpolation needs a separate metadata path.
  - `DatabricksSession#open` has Reyden KP001 Thrift→SEA recovery intended for warehouses, but
  its failure handler checks only the default Thrift path, not the compute type; an APC Thrift
  choice can therefore switch to SEA. Scope this recovery to warehouses so APC routing remains
  fixed after the initial choice.
  - `DatabricksConnection.getConnectionId()` publicly returns `SessionId.toString()`.
  `com.databricks.client.jdbc.Driver.closeConnection(url, info, connectionId)` currently chooses
  a client from a fresh URL/flag decision before deserializing that id. It must use the saved
  session type when a flag changes between open and close.
  - `DatabricksSession.getCurrentCatalog()` and `getCurrentCatalogAndSchema()` execute SELECTs
  with `StatementType.METADATA` but no `MetadataOperationType`. Seven
  `DatabricksMetadataQueryClient` operations build SHOW commands; `listProcedures` and
  `listProcedureColumns` instead build parameterized `information_schema` SELECTs through
  `CommandConstants` while passing `GET_PROCEDURES` and `GET_PROCEDURE_COLUMNS`. Both enum values
  have `isThriftNativeSupported=false`. `DatabricksSdkClient.getRequest` currently receives only
  the statement type, so APC metadata format selection must distinguish SHOW from these SELECTs,
  not merely test whether the operation type is non-null. Both procedure methods pass their
  parameter maps directly to `IDatabricksClient.executeStatement`, bypassing prepared-statement
  interpolation. `DatabricksSdkClient.getRequest` sets `parameters` even for an empty map, so an
  empty list is serialized rather than the field being absent.
  With a null catalog, synchronous `listSchemas` and `listColumns` catch the older-runtime
  parse-syntax SQL state from `SHOW ... IN ALL CATALOGS`, list catalogs, and issue per-catalog
  SHOW requests before combining JDBC metadata rows. An async completion path must preserve
  that fallback rather than treating the initial statement failure as final.
  - `DatabricksPreparedStatement.setObject(index, value, Types.ARRAY/Types.STRUCT)` accepts
  those JDBC types even though `setArray` is unsupported; the current SEA parameter mapper
  serializes a bound value's type name and string value without a complex-type check.
  - `DatabricksSdkClient` logs `computeResource.toString()` during execution, and
  `AllPurposeCluster.toString()` and `DatabricksSession.toString()` include org and cluster ids.
  APC SEA diagnostics must avoid those raw identifiers.
  - `ArrowStreamResult.convertToChunkLinkFetchResult` already derives external-link
  continuation from the final link; APC paging must preserve that behavior while adding its
  compute query field and testing a multi-link page followed by another page.
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
- R5: The seven APC metadata operations built as SHOW commands by
`DatabricksMetadataQueryClient` (`GET_CATALOGS`, `GET_SCHEMAS`, `GET_TABLES`, `GET_COLUMNS`,
`GET_FUNCTIONS`, `GET_PRIMARY_KEYS`, and `GET_CROSS_REFERENCE`) send
`X-Databricks-Metadata-Operation-Type`; synchronous SHOW calls send
`X-Databricks-Sea-Can-Run-Fully-Sync: true` only when `EnableSeaSyncMetadata=1`; synchronous calls
with that setting disabled and all asynchronous calls omit the sync header. Add a callable
asynchronous metadata path through `IDatabricksSession.getDatabricksMetadataClient()` and
`IDatabricksMetadataClient`: `executeMetadataAsync(MetadataAsyncRequest, IDatabricksSession)`
returns a `MetadataAsyncHandle`, and `getMetadataExecutionResult(MetadataAsyncHandle,
IDatabricksSession)` returns the completed JDBC metadata ResultSet. The request carries the
supported SHOW operation and existing JDBC arguments, from which the query client builds the SHOW
SQL and conversion inputs. The handle retains the typed `StatementId`, metadata operation, JDBC
metadata method (needed because `GET_CROSS_REFERENCE` serves imported keys and cross references),
and those inputs per operation. Completion polls `IDatabricksClient.getStatementResult` to a terminal state
under the existing metadata timeout, preserves each synchronous operation's empty-result and
syntax-error fallback handling, propagates other failures and cancellation, and applies the
same `MetadataResultSetBuilder` conversion as the synchronous operation. Add an
`IDatabricksClient.executeStatementAsync` overload that carries `MetadataOperationType` and
`StatementType.METADATA` through the SDK request and header builders, plus a typed
`getStatementResult` overload so the completed ResultSet remains `StatementType.METADATA`.
Keep the existing SQL async submit and completion calls unchanged. APC SHOW requests keep
`disposition=INLINE_OR_EXTERNAL_LINKS` even when `EnableSQLExecHybridResults=0`. APC preserves the
existing `X-Databricks-Require-Thrift-Native-Metadata: true` behavior when
`isThriftNativeMetadataEnabled()` is true and the operation supports it, although Reyder ignores
that header because APC metadata is always native. Requests use `format=ARROW_STREAM` and
multiplex per response between Arrow IPC in `chunk.attachment` (the primary path) and HIVE-shaped
`chunk.columnar_data` (the alternate path, including responses with
`manifest.format=COLUMN_BASED_SET`). Decoder selection uses the returned response
representation, not the requested format or a cached connection-level choice. Both
representations use native-Thrift-shaped metadata decoding when
`is_native_metadata_result=true`.
`StatementType.METADATA` and a non-null `MetadataOperationType` do not alone identify a SHOW
operation: internal catalog/schema SELECTs have no operation type, while `listProcedures` and
`listProcedureColumns` pass `GET_PROCEDURES` and `GET_PROCEDURE_COLUMNS` for parameterized SELECTs.
For APC, all four SELECT paths use `ARROW_BATCH` and `INLINE_OR_EXTERNAL_LINKS`, omit both
metadata headers (`X-Databricks-Metadata-Operation-Type` and
`X-Databricks-Sea-Can-Run-Fully-Sync`), and decode as ordinary query results before their existing
JDBC metadata conversion. Only the seven explicit SHOW operations use `ARROW_STREAM`, the SHOW
headers, and native-metadata decoding. The async metadata API supports those seven SHOW
operations; it rejects either procedure SELECT operation with a clear unsupported-operation
error. Below V8, or when the negotiated version is missing, interpolate bound values in both
procedure SELECTs with `SQLInterpolator` before calling `executeStatement` and omit native
parameters. At V8 or newer, preserve their native scalar parameter path. Pass an explicit
SHOW/SELECT signal to request-format and header selection; do not change warehouse request
behavior.
For async `GET_SCHEMAS` and `GET_COLUMNS` with a null catalog and multiple-catalog support,
if the initial `SHOW ... IN ALL CATALOGS` completes with the synchronous path's parse-syntax
SQL state `42601`, enumerate catalogs and issue a separate async SHOW for each nonempty catalog.
The handle must retain the original JDBC patterns plus the typed ids of fallback requests until
completion; each request keeps its operation header and omits the sync header. Combine the
completed per-catalog rows through the same schema or column metadata builder and filtering as
the synchronous fallback. A non-syntax error, a null SQL state, or a non-null catalog does not
trigger this fan-out; apply the corresponding synchronous empty-result or propagated-error rule.
Preserve the synchronous fallback's per-catalog error handling and propagate cancellation and
metadata timeout. This may require multiple SEA requests for one async handle;
the caller still receives one completed JDBC ResultSet.
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
capabilities. A version reconstructed from serialized `SessionId` is diagnostic state and must
never enable native `parameters` or `parameter_sets` by itself.
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
or warehouse Thrift OpenSession fails. APC SEA diagnostics,
including existing execute logs and session stringification, must not print org/cluster ids or
compound session/statement ids; use a redacted compute-type label instead.
- R9: `IDatabricksComputeResource` handling in `DatabricksSdkClient`, `SessionId`, and
`StatementId` is type-safe — no unchecked cast to `Warehouse`. `ImmutableSessionInfo` records the
client type, and `SessionId.create` dispatches on it rather than compute-resource class. New APC
SEA identifiers serialize a compute discriminator, org id, cluster id, and raw session/statement
id: sessions use `s|a|orgId|clusterId|sessionId|protocolVersion-or--`, and statements use
`s|a|orgId|clusterId|statementId`. The `-` marker preserves an absent protocol version. Parsing
remains backward-compatible with existing warehouse `s|warehouseId|sessionId[|r]` and raw SEA
statement ids. The compound session form is also returned by the public
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
- AC5: GIVEN an APC-SEA session WHEN each of `getCatalogs`, `getSchemas`, `getTables`,
`getColumns`, `getFunctions`, `getPrimaryKeys`, and both imported-key and cross-reference calls
backed by `GET_CROSS_REFERENCE` is called synchronously with inputs that reach the server, THEN
a captured request for every operation uses its SHOW SQL, `format=ARROW_STREAM`,
`disposition=INLINE_OR_EXTERNAL_LINKS`, the matching
`X-Databricks-Metadata-Operation-Type`, and, with `EnableSeaSyncMetadata=1`,
`X-Databricks-Sea-Can-Run-Fully-Sync: true`. Repeat every synchronous request with
`EnableSeaSyncMetadata=0` and assert the sync header is absent while its SHOW SQL, operation
header, format, and disposition remain the same. GIVEN an asynchronous
metadata call through `session.getDatabricksMetadataClient()` THEN the sync header is absent while
the async request retains the operation-type header, `ARROW_STREAM` format, and
`INLINE_OR_EXTERNAL_LINKS` disposition. A fake-service assertion covers both sync and async
metadata with `EnableSQLExecHybridResults=0`: call
`session.getDatabricksMetadataClient().executeMetadataAsync(MetadataAsyncRequest.forCatalogs(),
session)`, receive its pending handle, and call `getMetadataExecutionResult(handle, session)`
through that interface after PENDING/RUNNING and terminal `GetStatementResult` responses.
Assert the returned JDBC metadata columns and rows match the synchronous operation for both Arrow
IPC and `columnar_data` responses. The
completed ResultSet must retain `StatementType.METADATA`; a test verifies metadata update-count
semantics rather than the SQL statement path. GIVEN `isThriftNativeMetadataEnabled()` and a
supported operation THEN `X-Databricks-Require-Thrift-Native-Metadata: true` remains present.
WHEN any response contains Arrow IPC in
`chunk.attachment` or HIVE-shaped data in `chunk.columnar_data` with
`is_native_metadata_result=true` THEN the driver selects the matching decoder for that response
and the ResultSet matches the Thrift metadata result. Both representations must work on the same
connection without reconnecting or changing protocol. A response may report
`manifest.format=COLUMN_BASED_SET` after the `ARROW_STREAM` request; that response uses the
columnar reader. Unit and fake-service checks run before dogfood; dogfood needs the tracked
metadata server fix deployed in the test region. GIVEN internal catalog/schema SELECTs on that
APC connection with `StatementType.METADATA` and no `MetadataOperationType` THEN their requests
use `ARROW_BATCH`, omit both metadata operation and sync metadata headers, and decode through the
non-metadata result path. GIVEN `getProcedures` and `getProcedureColumns` on APC WHEN the fake
service captures their requests THEN both execute the parameterized `information_schema` SELECT
from `CommandConstants`, use `ARROW_BATCH` and `INLINE_OR_EXTERNAL_LINKS`, omit both metadata
headers despite their non-null `MetadataOperationType`, and pass the ordinary query result to
their existing JDBC metadata converters rather than the SHOW/native metadata decoder. At V8 or
newer, captured requests retain native scalar `parameters`; below V8 and with a missing version,
each request has safely interpolated SQL and no `parameters` property. Verify quoted and
backslash-containing patterns and a pattern containing SQL delimiters against the existing
interpolator's escaping behavior. An async metadata request for either procedure SELECT operation
fails with an unsupported-operation error
before a SEA request is sent. Warehouse metadata request fixtures remain unchanged.
GIVEN each supported SHOW operation (`GET_CATALOGS`, `GET_SCHEMAS`, `GET_TABLES`, `GET_COLUMNS`,
`GET_FUNCTIONS`, `GET_PRIMARY_KEYS`, `GET_CROSS_REFERENCE`) with its operation-specific JDBC
arguments WHEN submitted through the session-reachable async metadata API THEN a captured
request uses that operation's SHOW SQL, operation header, `ARROW_STREAM`, and
`INLINE_OR_EXTERNAL_LINKS` without the sync header; its handle retains the typed statement id,
operation, and converter inputs. After PENDING/RUNNING and terminal completion, typed result
fetch retains `StatementType.METADATA` and the converted columns and rows match the synchronous
operation for the same arguments. Exercise Arrow IPC and `columnar_data` across this matrix,
including alternating representations on one connection and the native column layout for every
operation; verify failure, cancellation, and metadata timeout propagation.

For the async part of AC5, use nonempty inputs that reach the server and assert the following
conversion matrix against each synchronous method's columns and rows:

| Operation | Async request inputs | JDBC metadata conversion |
| --- | --- | --- |
| `GET_CATALOGS` | none, with multiple-catalog support enabled | `listCatalogs` |
| `GET_SCHEMAS` | catalog, schema pattern | `listSchemas` |
| `GET_TABLES` | catalog, schema/table patterns, nonempty table types | `listTables` |
| `GET_COLUMNS` | catalog, schema/table/column patterns | `listColumns` |
| `GET_FUNCTIONS` | catalog, schema/function patterns | `listFunctions` |
| `GET_PRIMARY_KEYS` | catalog, schema, table | `listPrimaryKeys` |
| `GET_CROSS_REFERENCE` | catalog/schema/table for imported keys; parent and foreign catalog/schema/table for cross references | `listImportedKeys` and `listCrossReferences`, preserving the distinct conversion inputs |

GIVEN multiple-catalog support and a null catalog for async `GET_SCHEMAS` or `GET_COLUMNS` on an
older runtime WHEN the initial `SHOW ... IN ALL CATALOGS` finishes with parse-syntax SQL state
`42601` THEN completion issues async `SHOW CATALOGS` and a separate async SHOW for each of at
least two returned nonempty catalogs. Each fallback request retains its operation
header, `ARROW_STREAM`, and `INLINE_OR_EXTERNAL_LINKS`, and omits the sync header. The one
completed ResultSet has the same JDBC columns and row multiset as the corresponding synchronous
fallback, including schema/table/column pattern filtering and catalog values. Test both
operations with rows from both catalogs and assert the number and SQL of requests; an empty
catalog list returns the same empty JDBC result as sync. A non-syntax error, null SQL state, or
non-null catalog does not fan out and follows the synchronous empty-result or propagated-error
rule; cancellation or metadata timeout during a fallback request propagates to the caller.

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
Configuration evidence records the exact name, parent batch flag, and default value; no flag
assignment or connector-service change is required for this criterion.
- AC29: GIVEN an APC session at V8 or newer and a prepared single execution or native
`parameter_sets` batch containing an ARRAY, MAP, STRUCT, or other complex logical binding WHEN
execution is attempted THEN a clear unsupported-parameter SQLException with its driver-owned
error name and numeric code is raised before any ExecuteStatement request. A captured fake-service
request log stays empty, including when a complex value appears after scalar sets in a batch;
scalar bindings still use native parameters under the R6 capability gates. Cover at least
`setObject(index, value, Types.ARRAY)` and `Types.STRUCT`.

## Constraints

- APC default protocol is decided by the dedicated APC flag (R1, default off); after the Arrow
availability prerequisite, an explicit client setting always wins; when it is unset,
`TreatMetadataCatalogNameAsPattern=1` forces Thrift before the flag is consulted. No change to
warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The final APC-enabling JDBC package owns `docs/` and
  `NEXT_CHANGELOG.md`; earlier packages preserve current public behavior while APC still routes
  to Thrift and use `NO_CHANGELOG=true` in their PR bodies. The enabling PR adds the user-visible
  changelog entry.
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

1. Remove the unconditional `AllPurposeCluster → THRIFT` early return in
   `getClientTypeFromContext` and route APC with this precedence:
   1. Arrow unavailable → Thrift, regardless of client setting or APC flag.
   2. Explicit `UseThriftClient`: `1` → Thrift, `0` → SEA.
   3. Explicit `TreatMetadataCatalogNameAsPattern=1` → Thrift.
   4. Otherwise the dedicated APC flag (`APC_SQL_EXEC_FLAG_NAME`, Q8): ON → SEA; OFF, absent, or
      lookup failure → Thrift. `SQL_EXEC_FLAG_NAME` and `UseBoundedSeaApi` do not decide APC
      protocol selection.

   Once selected, SEA errors propagate without switching to Thrift. T17 creates the APC SAFE
   sub-flag under `databricks.partnerplatform.clientConfigsFeatureFlags`, default off. Assignment,
   ramp, and the Q9 connector-service partner override are external to this plan.
2. Add wire models `BackendCompute { AllPurposeClusterCompute all_purpose_cluster_compute }` and
   `AllPurposeClusterCompute { cluster_id }`. Add optional `backend_compute` to the driver-owned
   JSON body models, including `DeleteSessionRequest`; encode APC targeting for SDK-owned GET
   models and CloseStatement DELETE in the URL.
3. Add a small `SeaComputeTarget` helper in `dbclient/impl/sqlexec` that, given an
   `IDatabricksComputeResource`, produces both the request target (`warehouse_id` or
   `backend_compute`) and the URL family. Warehouses keep today's behavior
   (`WAREHOUSE_SESSION_PATH` for CreateSession, `realtimeSession` header for follow-ups). APC uses
   the standard `/api/2.0/sql/sessions/` and `/statements/` paths with `backend_compute` in the
   JSON body for CreateSession, ExecuteStatement, and CancelStatement. For GetStatementResult,
   GetResultData and CloseStatement, append the URL-encoded dotted query field
   `backend_compute.all_purpose_cluster_compute.cluster_id` and pass a null body at every APC GET
   and CloseStatement call site, bypassing standard-URL request serialization for those bindings;
   use a JSON body for the
   TerminateSession DELETE binding's declared `body: "*"`. Assert the outgoing DELETE body in a
   fake-service request test; verify server target extraction in dogfood
   after the external binding fix is deployed. Use a query builder to merge the
   compute field with `row_offset` and any other existing parameters rather than concatenating
   query strings. APC never sets `realtimeSession`. Route every `(Warehouse)` cast in
   `DatabricksSdkClient`, `SessionId`, and `StatementId` through the helper. Do not pass
   `SeaComputeTarget` through to CloudFetch: use presigned external-link URLs and headers exactly
   as returned. Add `DatabricksClientType` to `ImmutableSessionInfo`; make `SessionId.create`
   dispatch on it. Preserve legacy warehouse identifier parsing, and add APC SEA session and
   statement forms carrying the compute discriminator, org/cluster ids, raw id, and (for sessions)
   nullable negotiated protocol version so reconstruction cannot turn APC into warehouse compute.
   The reconstructed version is diagnostic only; only the live CreateSession response on this
   connection enables native parameter capabilities. Reject malformed APC segments with
   `SESSION_ID_PARSING_EXCEPTION` and structural-only errors, logs, and telemetry. The compound
   APC session form is public through `DatabricksConnection.getConnectionId()`; the compound
   statement form stays internal. Public statement-id APIs and `StatementId.toString()` return
   the raw server statement id, while logs and telemetry never expose either compound form. On
   `DatabricksConnection.getStatement(rawId)`, attach the current APC SEA connection's session
   and compute target to the reconstructed internal id before any fetch, cancel, or close.
   In `Driver.closeConnection`, deserialize `connectionId` first and choose the client using the
   saved session type; use that session's APC compute target even if fresh flag lookup selects
   Thrift. Test both flag-change directions and preserve warehouse close behavior.
4. APC execute request: `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`, `session_id`,
   no `statements`; preserve existing timeout behavior (`wait_timeout` unset for synchronous
   direct results and `0s` for async or direct results disabled). Execution polling uses
   `GET /statements/{id}` with `GetStatementResult`. Disable result heartbeat registration for
   APC instead of routing `checkStatementAlive` through a full-result endpoint; APC never calls
   `/status`. Accept polling responses with or without inline result data. Preserve
   `EnableQueryResultDownload=0` by setting CreateSession session config
   `can_cloud_download=false`. Select the APC format/disposition and set this session config via
   an explicit APC compute-type check, not `isBoundedSeaApiEnabled`. Gate the inline-only
   dogfood inline-only assertion on deployment of the external SC fix.
5. APC result decoding: add `ArrowBatch { batch, row_count }` and repeated
   `ResultData.arrow_batches` for inline chunks, plus an `ArrowBatchSchemaBuilder` that converts
   `manifest.schema` (`type_text`/`type_json`) into an Arrow `Schema`. Decompress each LZ4 batch
   payload first, then prepend the generated Arrow schema message to inline bare record batches
   in their original order for `SeaInlineArrowChunkProvider`. CloudFetch external links already
   contain complete Arrow IPC streams; pass them unchanged to the existing stream reader. For
   APC, enter the streaming provider through an explicit compute-type check, never through
   `isBoundedSeaApiEnabled`; page by `next_chunk_index`/`row_offset` and never read
   `total_chunk_count`/`total_row_count`. For inline pages, advance by the sum of batch row
   counts. For multiple external links, continue with the final link in response order using its
   `next_chunk_index` and `row_offset + row_count`.
   Replace the SDK-owned `Format` in driver SEA models with a driver-owned
   `SqlExecResultFormat` that includes `ARROW_BATCH`, `COLUMN_BASED_SET`, and `UNKNOWN`; update
   request serialization, manifest deserialization, result-factory/result-set switches, and telemetry mappings. T1
   changes both model types and their existing caller/telemetry conversions in one compilable PR;
   T10 adds the APC decoder branch.
6. Send `format=ARROW_STREAM` only for the seven APC SHOW operations enumerated in R5; pass an
   explicit SHOW/SELECT signal into `DatabricksSdkClient.getRequest` and header selection.
   `GET_PROCEDURES` and `GET_PROCEDURE_COLUMNS` carry operation types but execute parameterized
   `information_schema` SELECTs through `CommandConstants`: use `ARROW_BATCH`,
   `INLINE_OR_EXTERNAL_LINKS`, no metadata headers, and ordinary result decoding before their
   existing metadata conversion. Gate their parameter maps through the live APC protocol version:
   V8+ uses native scalar bindings; below V8 and missing versions use `SQLInterpolator` and omit
   the `parameters` property. Internal catalog/schema SELECTs follow the same APC request and
   decoder path. Add an `IDatabricksClient.executeStatementAsync` overload in both implementations
   with the operation/type signal, preserving the existing SQL async call. Add a callable async
   metadata path on `IDatabricksMetadataClient`, implemented by
   `DatabricksMetadataQueryClient` and reached from the session. Use
   `executeMetadataAsync(MetadataAsyncRequest, IDatabricksSession)` and
   `getMetadataExecutionResult(MetadataAsyncHandle, IDatabricksSession)`; the request carries the
   metadata operation and existing JDBC arguments. Return a per-operation handle with the typed
   `StatementId`, operation type, and conversion inputs; poll to terminal under the metadata
   timeout, then complete it through a typed
   `IDatabricksClient.getStatementResult` call and the same `MetadataResultSetBuilder` mapping
   used by the synchronous operation for all seven SHOW operations. Reject the two procedure
   SELECT operations from this async API before request submission. The async request preserves
   the operation header without the sync header; the completed result retains
   `StatementType.METADATA`.
   For null-catalog async schemas and columns, handle an older-runtime parse-syntax failure of
   `SHOW ... IN ALL CATALOGS` by asynchronously listing catalogs and submitting one SHOW per
   catalog. Retain typed fallback statement ids and the original patterns in the handle, then
   combine rows with the same JDBC metadata conversion as the synchronous fallback. Keep the
   sync header absent on every fallback request and test results from two catalogs.
   Keep `disposition=INLINE_OR_EXTERNAL_LINKS` for APC SHOW even with
   `EnableSQLExecHybridResults=0`. Synchronous
   SHOW metadata sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only when `EnableSeaSyncMetadata=1`, while disabled
   and asynchronous calls omit it. Preserve the existing
   `X-Databricks-Require-Thrift-Native-Metadata: true` condition even though Reyder ignores it.
   Add a `ColumnarDataResult` reader for `chunk.columnar_data` (null masks,
   string/integer/etc. vectors); missing and short masks imply non-null trailing rows, and
   long masks have ignored excess bits. For every metadata response, multiplex between
   `chunk.attachment` (Arrow IPC, primary path) and
   `chunk.columnar_data` (alternate path, also valid with response format `COLUMN_BASED_SET`)
   using the returned payload representation; do not cache
   a decoder choice at connection scope or infer it solely from the requested format or DBR
   version. When `is_native_metadata_result=true`, use the Thrift-native metadata column decoders
   instead of the SHOW-command decoders for either representation.
7. Query timeout for APC reuses `TimeoutHandler` → CancelStatement with `backend_compute`.
8. Persist `negotiated_protocol_version` in `ImmutableSessionInfo`; expose capability checks:
   native parameters at ≥ 42248, and `parameter_sets` for parameterized INSERTs only at ≥ 42250
   with `EnableNativeBatching=1` and an APC-capable `NativeBatchExecutor`. Wire that executor into
   `DatabricksPreparedStatement` through an `IDatabricksClient` method that accepts ordered
   `List<BatchParameterSet>` with the current session/statement context and returns one `long`
   update count per set. Call it through `session.getDatabricksClient()` so the
   `DatabricksSession` proxy exposes it; implement its SEA behavior and a clear unsupported
   Thrift behavior. Native request serialization puts each set in a nested
   `parameter_sets[].parameters` array and omits top-level `parameters`. For one successful native
   request, fill the returned array with `Statement.SUCCESS_NO_INFO` per input set because the response has only an aggregate count;
   on failure mark every set `Statement.EXECUTE_FAILED` in the batch exception.
   The current constructor uses an unsupported executor. Otherwise
   execute each set separately, bypassing the legacy multi-row `EnableBatchedInserts` optimization
   for APC. Force the tested interpolation path below V8 even when `supportManyParameters=0`;
   in `LegacyPreparedStatementBatchExecutor.executeIndividualStatements`, interpolate each
   fallback set before execution and pass an empty parameter map to `executeInternal`. Treat a
   missing version as below V8 and record a distinct missing-version downgrade signal. In the
   APC SEA request builder, leave `parameters` unset whenever the map is empty, including after
   interpolation; preserve warehouse serialization and assert property absence on the wire.
   Keep non-parameterized `Statement` batches on `DatabricksBatchExecutor`, which sends one
   ExecuteStatement per SQL and never populates `statements`.
   For pre-execution APC `PreparedStatement.getMetaData()`, execute its quoted-placeholder
   `DESCRIBE QUERY` with an empty binding map and without interpolating the transformed SQL.
   Retain the original bindings for subsequent execution; test below-V8, missing-version, and
   V8+ sessions.
9. Preserve warehouse SEA error behavior in `DatabricksSession#open`: APC CreateSession failures
   propagate without constructing a Thrift client or changing the selected protocol. Scope the
   existing warehouse KP001 Thrift→SEA recovery to warehouses so an APC choice of Thrift stays
   on Thrift.
10. Emit compute type, protocol, routing reason, and connection failure in telemetry and debug
    logs. Audit existing APC SEA execute logs and `DatabricksSession.toString()` for raw
    org/cluster ids and compound identifiers; replace those diagnostics with a redacted compute
    label and assert the captured output contains neither id. Keep the ordinary JDBC User-Agent
    unchanged; request paths distinguish APC from warehouse traffic.

### Affected Components

- `src/main/java/com/databricks/jdbc/api/impl/DatabricksConnectionContext.java` — APC routing
  (Arrow availability > explicit `UseThriftClient` >
  `TreatMetadataCatalogNameAsPattern=1` > APC flag), `APC_SQL_EXEC_FLAG_NAME`, client type.
- SAFE APC sub-flag creation (T17); no connector-service files in this plan.
- `src/main/java/com/databricks/jdbc/api/impl/DatabricksSession.java` — verify APC SEA errors propagate without
fallback; keep warehouse KP001 Thrift→SEA recovery off APC.
- `src/main/java/com/databricks/jdbc/api/impl/SessionInfo.java` — client type and
  `negotiatedProtocolVersion`.
- `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClient.java` — compute targeting, APC URL
family, disposition/format selection.
- `src/main/java/com/databricks/jdbc/dbclient/IDatabricksClient.java` and
  `dbclient/impl/thrift/DatabricksThriftServiceClient.java` — callable native-batch interface
  through the session proxy and explicit Thrift unsupported behavior; async metadata
  operation/type signal and typed metadata completion through both client implementations.
- `src/main/java/com/databricks/jdbc/dbclient/IDatabricksMetadataClient.java` and
  `dbclient/impl/sqlexec/DatabricksEmptyMetadataClient.java` — session-reachable async
  metadata submit/completion contract and an explicit unsupported empty-client behavior.
- `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/PathConstants.java` — reuse standard session/statement
paths for APC (no new constants expected).
- `src/main/java/com/databricks/jdbc/dbclient/impl/common/SessionId.java` and
  `src/main/java/com/databricks/jdbc/dbclient/impl/common/StatementId.java` — dispatch by client
  type; add backward-compatible APC compute/capability serialization and remove warehouse-only
  assumptions.
- `src/main/java/com/databricks/jdbc/api/impl/DatabricksConnection.java` — attach current APC
  SEA target when reconstructing a statement from a public raw id; expose the APC connection id.
- `src/main/java/com/databricks/client/jdbc/Driver.java` — choose the close client from the
  deserialized public connection id, including after a flag change.
- `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/` — new `SeaComputeTarget` helper.
- `src/main/java/com/databricks/jdbc/model/client/sqlexec/` — `BackendCompute`,
  `AllPurposeClusterCompute`; `backend_compute` on the driver-owned
  CreateSession/ExecuteStatement/Cancel/TerminateSession request bodies. `DeleteSessionRequest`
  implements TerminateSession for both compute types; preserve warehouse query serialization
  and add an APC JSON body. SDK-owned GetStatement and
  GetStatementResultChunkN requests use URL targeting; CloseStatement uses URL targeting because
  the DELETE binding has no body.
- `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksMetadataQueryClient.java` — APC metadata
format/header, callable async metadata path, native-result decoder selection.
- `src/main/java/com/databricks/jdbc/api/impl/arrow/` — `ArrowBatchSchemaBuilder`; schema prepend for bare
`ARROW_BATCH` record batches; `row_offset` paging for APC.
- `src/main/java/com/databricks/jdbc/api/impl/` — new `ColumnarDataResult` reader for
  `chunk.columnar_data`.
- `src/main/java/com/databricks/jdbc/model/core/` — driver-owned `SqlExecResultFormat`;
  `ArrowBatch`, `ColumnarData`/`ColumnVector` models, `arrow_batches` and `columnar_data` on
  `ResultData`, and `type_json` on `ColumnInfo`. The manifest already has
  `is_native_metadata_result`; add `negotiated_protocol_version` to CreateSessionResponse in
  `model/client/sqlexec/`.
- `src/main/java/com/databricks/jdbc/telemetry/` — compute type, protocol, routing-reason,
  parameter-downgrade, and failure fields.
- `src/test/java/com/databricks/jdbc/` — unit tests per AC; fake-service / integration tests.
- `NEXT_CHANGELOG.md`, `docs/` — document APC routing and the `UseThriftClient` override.

### Data Model Changes

- New SEA wire models `BackendCompute` and `AllPurposeClusterCompute` (JSON:
`{"backend_compute": {"all_purpose_cluster_compute": {"cluster_id": "..."}}}`).
- `ImmutableSessionInfo` gains `clientType` and nullable `negotiatedProtocolVersion`.
- Response models gain repeated `chunk.arrow_batches` (`ArrowBatch { bytes batch;
int64 row_count }`) and `chunk.columnar_data` (`ColumnarData { repeated ColumnVector columns }`,
`ColumnVector { bytes null_mask; oneof values }`), plus
`CreateSessionResponse.negotiated_protocol_version`; `ResultManifest.is_native_metadata_result`
already exists. `ColumnInfo` gains `type_json` to support the nested Arrow type matrix.
- ExecuteStatement gains optional `parameter_sets`, an ordered array of objects each holding
  a `parameters` array. Native batch requests omit the mutually exclusive top-level `parameters`.
- New driver-owned `SqlExecResultFormat` replaces the SDK `Format` in SEA request/response models,
preserves existing JSON values, adds `ARROW_BATCH` and `COLUMN_BASED_SET`, and maps unknown
response values to `UNKNOWN`.

### API Changes

- `UseThriftClient` becomes meaningful on cluster URLs; previously it was ignored there.
Public statement ids remain raw server ids. The existing public `getConnectionId()` returns the
compound APC session id needed by the existing public `Driver.closeConnection` API; this changes
the returned identifier format for APC SEA without adding a Java method. Compound statement
serialization remains internal. The internal `IDatabricksClient` interface gains the
native-batch method.

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
- Pre-execution `PreparedStatement.getMetaData()` builds a DESCRIBE query with quoted placeholder
  literals but retains the original bindings. Ordinary interpolation then sees zero replaceable
  placeholders and rejects the map. Give APC DESCRIBE its own empty binding map and assert that
  the original prepared bindings still work on subsequent execution.
- Async null-catalog schemas and columns need more than one statement on older runtimes. Keep
  fallback request ids and patterns in the metadata handle, use async requests throughout, and
  compare combined JDBC rows with the synchronous fallback rather than treating the expected
  parse-syntax error as final failure.
- The existing `EnableBatchedInserts` legacy path can combine several prepared parameter sets;
  bypass it for APC whenever `parameter_sets` is unavailable so the one-execution-per-set contract
  in AC17 remains true. The native executor is currently unwired and must be added explicitly.
  Native `parameter_sets` yields only one aggregate affected-row count, so report
  `SUCCESS_NO_INFO` per set instead of fabricating individual counts.
   For V8+ APC sessions, reject any ARRAY, MAP, STRUCT, or other complex logical binding before
   native serialization of a single execution or any batch set; validate the whole batch before
   its first request.
   Reuse a matching driver-owned error code or add a unique one, test its emitted name and number,
   and record its driver/server/user classification in the telemetry dashboard and automation
   taxonomy with a PR link or an explicit maintainer follow-up.
- Closing an APC SEA connection after the flag turns off can select Thrift from a fresh URL and
  fail to terminate the SEA session; AC3 covers saved-session client selection through the public
  close API.
- Missing `backend_compute` on any one request type causes server-side 400s mid-statement —
mitigated by centralizing in `SeaComputeTarget`, explicit per-binding body/query encoding in R2,
and AC3 serialized-request coverage of every call.
- Standard statement URLs currently trigger request-body serialization for GET and
  CloseStatement, despite their bodiless REST bindings. APC must pass a null body at every such
  call site; AC3 asserts the captured body and AC11 guards warehouse serialization.
- Applying APC query parameters to a presigned external-link URL breaks its signature and can leak
  the cluster id to storage. Keep compute targeting confined to SEA requests and assert unchanged
  CloudFetch URLs and headers in AC14.
- Accidentally changing warehouse request bodies — mitigated by AC11 regression tests with
serialized-body assertions.
- Existing APC compute stringification exposes org and cluster ids in execute and session logs;
  AC10 requires captured-log redaction assertions across those paths.
- Server-side routing disablement causes visible connection failures until the cached client flag
refreshes; mitigate with flag sequencing and R8 telemetry.
- Execution polling with `GET /statements/{id}` may fetch first-chunk data repeatedly; AC27
requires measured status-only RUNNING responses before release. Do not use this endpoint for
post-completion heartbeats; APC heartbeat is disabled by R3.
- The SEA routing code has changed substantially through #1744, #1746, #1747, and #1749; rebase
often and do not restore the removed circuit-breaker, rate-limit, or HTTP 307 fallback paths.
- APC on the standard `/sessions` URL must not be mistaken for a Real-time session; the
`x-databricks-rt-session` header handling must stay warehouse-only.

---

## Tasks

The implementation stack lands T4, T6, T7, T12, T15, and T16 in its final APC-enabling PR,
after the wire, targeting, execution, batching, and metadata packages. Until that PR, the
unchanged APC routing gate keeps the new SEA code unreachable through public APC connections.
Those earlier PRs use `NO_CHANGELOG=true`; the enabling PR is the sole owner of
`NEXT_CHANGELOG.md` and `docs/`. T18 and T-val form a separate child validation package after
implementation review; its completion includes external dogfood gates.

### Phase 1 — Wire models and compute targeting

- [ ] T1: Add `BackendCompute`, `AllPurposeClusterCompute`, and driver-owned
  `SqlExecResultFormat` models with JSON serialization/deserialization tests, including
  `ARROW_BATCH`, `COLUMN_BASED_SET`, and unknown values. Change `ExecuteStatementRequest.setFormat` and
  `ResultManifest.getFormat` with their existing callers in the same compilable package:
  convert the SDK `Format` in `DatabricksSdkClient.getRequest`, and adapt
  `ExecutionResultFactory` → `TelemetryHelper` → `TelemetryCollector` format telemetry for the
  new enum. Migrate existing test callers of `ResultManifest.setFormat` and
  `ExecuteStatementRequest.setFormat` in `DatabricksSdkClientTest`,
  `DatabricksResultSetArrowConversionTest`, `DatabricksResultSetTest`,
  `ExecutionResultFactoryTest`, `InlineJsonResultTest`, and
  `DatabricksResultSetMetaDataTest` in this first independently compilable PR. Preserve warehouse
  format values. In this PR, round-trip `JSON_ARRAY` and `ARROW_STREAM` manifest fixtures and
  assert their pre-change `InlineJsonResult` and `ArrowStreamResult` decoders and rows still work;
  T10 adds the APC result decoder (R16; AC26).
- [ ] T2: Add optional `backend_compute` to driver-owned CreateSession, ExecuteStatement, and
  CancelStatement JSON body models and `DeleteSessionRequest` for TerminateSession's declared
  DELETE body. Preserve warehouse serialization. T3 owns APC query targeting. Do not edit
  SDK-owned GetStatement/GetStatementResultChunkN classes or add bodies to APC GET and
  CloseStatement DELETE bindings (R2).
- [ ] T3: Add `SeaComputeTarget` (request target + URL family) and route every `(Warehouse)`
  cast in `DatabricksSdkClient`, `SessionId`, and `StatementId` through it; keep warehouse and
  Real-time routing byte-identical. Add serialized-request tests for the JSON-body and dotted-query
  encodings in R2, including TerminateSession's JSON body and merging the compute field with
  `row_offset` (R2, R9; AC2, AC3, AC11, AC14). Pass a null body for APC execution polls, explicit
  result GETs, both GetResultData chunk-fetch paths, and CloseStatement DELETE even though the
  current serializer supplies bodies on standard URLs; assert null captured bodies and unchanged
  warehouse bodies for those call sites. Assert that external-link URLs and headers remain
  unchanged. Add
  backward-compatible APC SEA `SessionId`/`StatementId` serialization and round-trip tests,
  including malformed APC session segments and redacted structural-only diagnostics,
  dispatching `SessionId.create` by client type while keeping public statement ids raw. Make
  `DatabricksConnection.getStatement(rawId)` attach the current APC SEA target and test fetch,
  cancel, and close on the recovered statement. Verify public `getConnectionId()` returns the
  compound APC session id. Change public `Driver.closeConnection` to deserialize that id before
  choosing the SEA/Thrift client; test targeted APC SEA termination after the flag turns off,
  APC Thrift termination after it turns on, and unchanged warehouse close behavior (R9; AC3,
  AC25).

### Phase 2 — Routing and session lifecycle

- [ ] T4: Replace the `AllPurposeCluster → THRIFT` early return in `getClientTypeFromContext`
  with Arrow prerequisite > explicit `UseThriftClient` >
  `TreatMetadataCatalogNameAsPattern=1` > APC flag routing. Keep `UseBoundedSeaApi` out of APC
  protocol selection; preserve Thrift for the legacy `cliservice` placeholder path (R1, R7;
  AC1, AC7, AC8, AC19, AC20, AC21).
- [ ] T5: Persist client type and `negotiated_protocol_version` from CreateSession into
  `SessionInfo`, including through APC `SessionId` reconstruction. Keep the reconstructed
  version diagnostic only; use the live CreateSession response for native capability decisions and
  test an altered serialized version cannot enable either native feature. Update both
  `DatabricksSdkClient.createSession` and `DatabricksThriftServiceClient.createSession` producers;
  Thrift sessions, including warehouse Thrift, must retain `THRIFT` (R6, R9; AC23, AC25).
- [ ] T6: Add tests that APC CreateSession failures propagate and never construct or switch to a
  Thrift client; scope the existing warehouse KP001 Thrift→SEA recovery so an APC Thrift choice
  does not switch protocol on KP001 (R7; AC9).

### Phase 3 — Execution and results

- [ ] T7: Disable result heartbeat registration for APC, with an unsupported-heartbeat log, and
  add tests that execution polling, fetch, cancel, and close use standard `/statements` URLs with
  `backend_compute` and never hit `/status`. Add a fake-service status-only RUNNING-poll fixture;
  T18 owns the measured AC27 dogfood release gate (R2, R3; AC3, AC24, AC27).
- [ ] T8: APC execute request shape: `ARROW_BATCH`, `INLINE_OR_EXTERNAL_LINKS`, `session_id`,
  no `statements`, `wait_timeout` unset/`0s`, and CreateSession `can_cloud_download=false` when
  result download is disabled. Use an APC compute-type check for format/disposition and session
  config; do not gate them on `isBoundedSeaApiEnabled`. Capture ordinary sync and async requests
  with `EnableSQLExecHybridResults=0`; assert all three fields and the async `0s` timeout
  (R4, R10; AC4, AC8, AC12).
- [ ] T9: Gate APC native parameters at ≥ 42248 and `parameter_sets` for parameterized INSERTs
  at ≥ 42250 with `EnableNativeBatching=1` and a supported `NativeBatchExecutor`; wire that
  executor into `DatabricksPreparedStatement` through a callable `IDatabricksClient` method
  accepting ordered `List<BatchParameterSet>` and returning per-set counts, available on the
  `DatabricksSession` proxy; test the proxy call, implement SEA `parameter_sets` behavior and an
  explicit unsupported Thrift implementation. Model and serialize `parameter_sets` as an ordered
  array of `{"parameters":[...]}` objects, and ensure the native request builder does not
  populate top-level `parameters`; assert mutual exclusivity on the captured wire request.
  Below V10, when opt-in is off, or without an
  executor, send one execution per set and bypass `EnableBatchedInserts` multi-row combining.
  For a successful native request return `Statement.SUCCESS_NO_INFO` for every input set in both
  batch APIs; on native failure return `EXECUTE_FAILED` for every set in the batch exception.
  Assert a single aggregate server count is never reported as a per-set count.
  Below V8, force existing interpolation regardless of `supportManyParameters`, including in
  `LegacyPreparedStatementBatchExecutor.executeIndividualStatements`: interpolate each fallback
  set with `SQLInterpolator` before `executeInternal` and pass an empty parameter map. Change the
  APC branch of `DatabricksSdkClient.getRequest` to leave `parameters` unset for an empty map;
  assert the serialized property is absent for single and fallback-batch interpolation, while
  warehouse empty-map fixtures remain unchanged. A flag change alone leaves that individual path
  sending raw bindings. Add captured-request escaping,
  injection, update-count, and parameter-downgrade logging/telemetry tests, including a missing
  version. For APC `PreparedStatement.getMetaData()` before execution, send the
  quoted-placeholder `DESCRIBE QUERY` with no bindings and bypass interpolation of that
  transformed SQL; test `setObject(...)` followed by `getMetaData()` below V8, with a missing
  version, and at V8+, then execute the original statement with its bindings intact. Keep
  non-parameterized batches sequential with one ExecuteStatement per SQL (R6,
  R15; AC6, AC17, AC22, AC23, AC29). On V8+ APC sessions, reject complex logical bindings before
  native single-query serialization or any batch request, including ARRAY/STRUCT accepted via `setObject`; captured
  fake-service tests assert no request and the emitted unsupported-parameter error name/code.
  Record the error classification in the telemetry dashboard and automation taxonomy, or link an
  explicit maintainer request in the PR, and complete the telemetry-error PR section.
- [ ] T10: Add `ArrowBatchSchemaBuilder` and prepend the built schema to inline bare record
  batches after LZ4 decompression. Keep external-link payloads as complete Arrow IPC streams and
  use the existing CloudFetch reader without schema prepending. Add `ArrowBatch` and repeated
  `ResultData.arrow_batches` models, decoding all batches in each inline chunk in order using
  their `row_count`; do not expect APC inline data in `attachment`. Cover primitive, decimal, nested,
  interval, geospatial, timestamp/timestamp_ntz, and variant types. Add `type_json` to
  `ColumnInfo` and use the T1 `SqlExecResultFormat` in result-factory/result-set switches
  (R4, R11, R16; AC4, AC13, AC26, Q7).
- [ ] T11: Route APC through its dedicated `next_chunk_index`/`row_offset` paging independent of
  `UseBoundedSeaApi`; handle `GetStatementResult` with and without inline data and never read
  chunk/row totals. For inline pages, use the sum of batch row counts; for a page with multiple
  external links, use the final link's `next_chunk_index` and `row_offset + row_count`. Test two
  links followed by another page, with every row returned once (R12; AC14, AC15).
- [ ] T12: Wire APC query timeout through `TimeoutHandler` → CancelStatement with
  `backend_compute` (R14; AC18).

### Phase 4 — Metadata and telemetry

- [ ] T13: APC metadata through `DatabricksMetadataQueryClient` and the SDK execute/result path:
  pass an explicit SHOW/SELECT signal into `DatabricksSdkClient.getRequest` and select
  `format=ARROW_STREAM`, `disposition=INLINE_OR_EXTERNAL_LINKS`, and metadata headers only for
  the seven R5 APC SHOW operations, even with `EnableSQLExecHybridResults=0`. The two
  procedure operations carry non-null operation types but run parameterized SELECTs; use
  `ARROW_BATCH`, `INLINE_OR_EXTERNAL_LINKS`, no metadata headers, and ordinary result decoding
  before their existing JDBC metadata conversion. For `listProcedures` and
  `listProcedureColumns`, use the live APC version to retain scalar native parameters at V8+ or
  interpolate their parameter maps with `SQLInterpolator` below V8 and when the version is
  missing; call `executeStatement` with an empty map in the latter cases so T9's APC request
  builder omits `parameters`. Capture both SELECT requests and assert escaped SQL and field
  absence for old and missing versions. Add async submit and completion
  methods `executeMetadataAsync(MetadataAsyncRequest, IDatabricksSession)` and
  `getMetadataExecutionResult(MetadataAsyncHandle, IDatabricksSession)` on
  `IDatabricksMetadataClient`, so a caller obtains the metadata client from
  `IDatabricksSession.getDatabricksMetadataClient()`, submits a SHOW operation, and receives an
  internal per-operation handle retaining its typed `StatementId`, `MetadataOperationType`, and
  conversion inputs. The request carries the metadata operation and existing JDBC arguments;
  the query client builds the SHOW SQL and converter from those inputs for every supported SHOW
  metadata operation. Reject procedure SELECT operations from the async API before sending a
  request. Implement both methods in `DatabricksMetadataQueryClient`; completion
  polls PENDING/RUNNING to terminal under the existing metadata timeout, propagates failure or
  cancellation, calls a typed `IDatabricksClient.getStatementResult` overload, and applies the same
  `MetadataResultSetBuilder` operation-specific conversion as synchronous metadata. Add explicit
  unsupported behavior in `DatabricksEmptyMetadataClient` and the Thrift metadata client.
  Extend `IDatabricksClient.executeStatementAsync` with an overload in both client implementations to
  carry `MetadataOperationType` and `StatementType.METADATA` into the request and header builders;
  add a typed result-fetch overload in both implementations so the SDK does not construct the
  completed metadata ResultSet with its current hard-coded `StatementType.SQL`. Preserve existing
  SQL async calls and completion. Test sync and async fake-service requests: the async path retains
  the operation header and APC format/disposition but omits the sync header. Internal
  `getCurrentCatalog`/`getCurrentCatalogAndSchema` SELECTs keep `ARROW_BATCH` with no metadata
  headers despite `StatementType.METADATA`; assert both procedure SELECT operations do the same
  despite their non-null operation types, and warehouse requests stay unchanged. For SHOW
  responses, multiplex per response between Arrow IPC `attachment` and
  `columnar_data` (including response `format=COLUMN_BASED_SET`), and pick native
  decoders when `is_native_metadata_result=true`; capture synchronous requests for all seven SHOW
  operations with the sync setting both enabled and disabled, plus async requests and the
  preserved require-native header condition. Assert a session-reachable async
  submit returns a pending handle and typed completion returns converted metadata with correct
  metadata update-count semantics for Arrow IPC and `columnar_data`. Cover all seven SHOW
  operations with operation-specific arguments, headers, typed completion, conversion, failure,
  cancellation, and timeout fixtures. For null-catalog async `GET_SCHEMAS` and `GET_COLUMNS`,
  add parse-syntax fallback fixtures that list at least two catalogs, issue async per-catalog SHOW
  requests with no sync header, and combine rows to match the synchronous fallback. Test empty
  catalogs, non-syntax/null-state errors, and fallback cancellation/timeout (R5; AC5).
- [ ] T14: Add `ColumnarData`/`ColumnVector` models and the `ColumnarDataResult` reader with
  all eight R13 oneof variants and null-mask handling; test physical-to-logical conversion,
  empty vectors, placeholder values, and mask bits across a byte boundary. Test an absent mask,
  a short mask with non-null trailing rows, and a long mask with ignored excess bits/bytes;
  neither valid encoding may fail on a mask index. Assert logical
  decimal, array, map, struct, union, and null JDBC values and metadata against pinned Thrift
  fixtures; add any missing union type-name mapping needed to preserve that parity (R13; AC16).
- [ ] T15: Add compute type, protocol, routing reason, and connection failure to telemetry and
  debug logs. Capture one routing event for each successful APC/warehouse route and each failed
  APC SEA, APC Thrift, warehouse SEA, and warehouse Thrift open attempt. Audit existing
  synchronous/asynchronous `DatabricksSdkClient` execute logs,
  `DatabricksSession.toString()`, and failure diagnostics for raw APC org/cluster ids and compound
  identifiers; replace them with redacted compute labels and add captured-log and telemetry
  assertions (R8; AC10, AC25).
- [ ] T16: In the final APC-enabling JDBC PR, document APC routing (APC flag,
  `UseThriftClient` override) in `docs/` and add the user-visible `NEXT_CHANGELOG.md` entry.
- [ ] T17: (universe, SAFE flag creation only) Create the APC sub-flag under
  `databricks.partnerplatform.clientConfigsFeatureFlags`, default off; verify its name, parent
  batch flag, and default. Do not change connector-service or assign/ramp the flag (R1; AC28, Q8).

### Validation

- [ ] T18: Run `mvn test -pl jdbc-core -am` and local fake-service integration checks on a
  branch containing T1–T16, including T9. After package review, run all dogfood checks that
  do not depend on the external SC fixes against an APC with the APC flag on and off
  and `UseThriftClient` set to 0 and 1. The flag-on dogfood case requires T17's flag creation
  and an externally prepared test-region assignment; verify that T17's Universe package is
  approved before starting the flag-on dogfood run. Assignment and ramp are outside this plan.
  The JDBC and Universe PR stacks have separate roots because a stacked PR can only branch from
  a parent in the same repository; this cross-repository validation gate is checked at T18.
  Record cluster runtime, region, protocol observed, and
  result for each configuration without credentials. Run dogfood AC5 after its metadata fix is
  deployed, dogfood AC8 after its inline-only fix is deployed, and the TerminateSession portion
  of dogfood AC3 after its binding fix is deployed. Run unaffected portions of AC3 and AC8 with
  the other dogfood checks. Unit and fake-service checks for these ACs run before deployment.
  Include the AC27 bounded
  polling run in the pre-release dogfood gate. Implementation package approval does not wait for
  deployment; validation package completion and release readiness do (R1–R16; AC1–AC29).
- [ ] T-val: Record a pass/fail evidence link for each AC1–AC29 (unit, fake-service, dogfood,
  or SAFE configuration as appropriate), plus exact build/test commands and failures. Mark
  externally gated dogfood checks pending until the corresponding SC fix is deployed; retain their local test evidence.
  Verify warehouse request serialization, AC13 type matrix, and AC27 polling before calling the
  JDBC stack release-ready; keep release blocked under Q7 if any required Arrow type is lossy.
