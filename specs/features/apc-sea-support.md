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
- SC-256188/SC-256186 gate affected live metadata and session termination until fixes deploy.
  SC-254478 covers per-statement `session_confs`, not the CreateSession download
  override (T8/AC119); TerminateSession declares `body: "*"` (R2).
- Reyder's APC terminal translation currently requires an Arrow schema when metadata is
  present. T2 proves direct and polled command shapes; absent `arrow_schema`
  needs a fixture because live manifests populate it.
- Reyder currently discards APC `result_compression`, advertises LZ4 decompression capability
  to DBR, and reports the actual manifest codec. The JDBC setting cannot select
  APC's effective codec (R4/AC101).
- Reyder accepts APC `ARROW_BATCH` but currently rejects the contract's `ARROW_STREAM`
  metadata request, native SHOW dispatch, and HIVE results. SEA's
  `ReyderClient.ALLOWLISTED_HEADERS` omits the three R5 headers. T3/T7 close
  catalog Arrow/HIVE gaps; T4–T6 extend the other operations.
- Connector-service returns `databricks.partnerplatform.clientConfigsFeatureFlags`
  sub-flags at `/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`.
  `FeatureFlagsHandler.scala` remaps `enableSqlExecForJdbc` to
  `enableSqlExecForJdbc_v3.4.3` at ≥3.4.3 and may allowlist a JDBC partner;
  Q9 requires a blanket APC override.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata tabs). The intended SQL-text limit is 16 MiB
  (16,777,216 UTF-8 bytes). Warehouse `ExecuteStatementOperation` compares
  Scala string `.length`; AC87 measures the deployed APC byte boundary without
  changing warehouse validation. The server rejects non-OSS-JDBC clients and
  starts rollout opt-in through the dedicated flag.
- Driver baseline: `origin/main` @ `840f3a407` (spec #1809; bounded SEA default
  #1827). APC routes to Thrift; SEA assumes warehouse targeting and a `/status`
  heartbeat. Saved IDs can misroute close after flag or target changes. APC Arrow
  batches, async SHOW, and columnar metadata need driver support. Warehouse SEA
  sends `FAST` and echoes session versions; Reyder's APC response has none (R6).
  `DatabricksStatement.shouldReturnResultSet` classifies every `SET` as producing
  a ResultSet, creating R4's assignment exception.
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the prototype
baseline, not the design; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756)
is unchanged.
- Parity baseline for review and release (same driver build on all routes). `Warehouse
  precedent` means APC SEA follows warehouse SEA where it differs from APC Thrift;
  an `exception` requires the cited limit, measured caller impact, and approval.

  | Behavior | APC Thrift | Warehouse SEA precedent | APC SEA disposition and caller impact |
  | --- | --- | --- | --- |
  | Routing, close, paging | Direct Thrift; live close; Arrow inline/links | Explicit protocol, original-target close, SEA continuation | Warehouse precedent for protocol and paging; live-close and JDBC-row parity. Saved IDs close sessions only (R1/R9/R12). |
  | Codec and download-off | Requested codec; inline when download is off | Actual manifest codec; session requests inline | Codec exception: Reyder ignores the requested codec, so `NONE` cannot remove APC decompression. T8 must prove link-free inline results or expose size/page failures (R4/AC101/112/119). |
  | Hybrid-results disabled | Thrift result provider selection follows its own download settings | With `EnableSQLExecHybridResults=0`, warehouse SEA requests `EXTERNAL_LINKS` when cloud download is enabled | APC SEA still requests `INLINE_OR_EXTERNAL_LINKS`: Reyder's APC contract does not offer external-only selection. This can add inline bytes, first-row delay, and retained heap relative to warehouse SEA and APC Thrift; AC145 measures and dispositions the difference before exposure. |
  | Idle cursor | Heartbeat preserves it | SEA status heartbeat | Exception: APC status RPC is unsupported. AC136 measures the survival window; shorter unapproved lifetime blocks release (R3). |
  | SHOW and async metadata | Native RPC; synchronous JDBC | Native-shaped SHOW; synchronous JDBC | Warehouse precedent for Arrow/HIVE conversion and headers; optional typed async handles are additive. Null-catalog tables return typed empty on `42601`, possibly omitting Thrift rows; exported keys return typed empty without a request on the documented service. Procedure SELECTs omit rejected APC headers (R5/AC54/75/105). |
  | Bindings and batches | Complex values; ordered per-set counts; `EnableBatchedInserts` | Scalar SEA binding; native/fallback batches | Unsupported complex binding follows warehouse precedent where shared, otherwise APC V8 exception. Legacy chunking and counts retain parity. APC V10 aggregate count loses per-set counts and reports `SUCCESS_NO_INFO`; AC29/32/108 measure the caller and speed deltas (R6). |
  | Single prepared writes and CALL | Prepared DML counts and IN-only CALL result/error behavior | Prepared DML and CALL share SEA prepared execution | APC SEA exposes single prepared INSERT/UPDATE/DELETE/MERGE after AC146 and IN-only CALL after AC147. A CALL with no result returns `execute=false` from its actual no-result shape; callers branching on the old CALL classifier can observe this difference (R18). |
  | Commands and transactions | `SET` classified as ResultSet; side effects | Same `SET` classifier; wrapped errors | Exception: no-result APC assignment makes direct `Statement.execute("SET AUTOCOMMIT = ...")` return false; connection methods preserve side effects and error identity (AC52/102). |
  | Streaming controls | Batch queue and selectable inline/link providers | Bounded SEA queue and link behavior | `ThriftMaxBatchesInMemory` counts APC response chunks, which may contain multiple batches. APC ignores `EnableInlineStreaming` and always uses bounded linked streaming because totals are unreliable; AC39/109 measure first-row latency, heap, and link expiry (R17). |
  | Session mode | Thrift OpenSession; no SEA execution mode | CreateSession `FAST` and observed session-version echo | Exception required by Reyder's absent APC session version: CreateSession `DEFAULT`, later requests omit `session_version`; capability comes only from negotiated protocol (R6/AC23/143). |
  | Unset native-metadata setting | Native Thrift RPC | Header depends on the warehouse flag when unset | Exception: APC sends require-native on supported SHOW because its property default is `1`; explicit `0` omits it. Rows must still match (R5/AC65). |
  | Retry and write outcome | Thrift HTTP 429/503 controls govern its requests | SDK API retries ignore those controls; linked downloads use them | Warehouse precedent: APC SDK calls, including non-idempotent statement POSTs, inherit SDK retries. A retried write can have an uncertain commit; release notes disclose this caller risk, and linked downloads retain HTTP-handler controls. AC51 records attempts and error identity. |
  | SQL size | Deployed boundary pending AC87 | Character-count validator in checked-out warehouse code; deployed boundary pending AC87 | APC contract is 16,777,216 UTF-8 bytes. AC87 proves deployed accept/reject points and records caller impact of any route difference; a mismatch keeps release open. |
  | Timeouts and cancellation | Deployed latency/error baseline pending AC18/60/63 | SEA request and client wait behavior, targeted cancel | Warehouse precedent for request/client-wait separation; AC18/60/63 measure elapsed time and error identity on all three routes. Early timeout blocks release. |
  | Error identity | Deployed SQLState/vendor baseline pending AC89 | SEA SQLException mapping | Follow the agreeing baselines, otherwise warehouse SEA with recorded caller impact; AC89 proves each exposed failure class. |
  | Telemetry | Deployed event baseline pending AC85 | SEA operation, result, and error families | Match existing event families with APC compute/protocol attribution and redaction; AC85 compares observed events at each slice (R8). |

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
re-download payloads. No APC `/status` request is sent. An idle, partly read APC SEA ResultSet
must remain readable for the same observed interval as its paired APC Thrift cursor with
heartbeats enabled; a shorter lifetime is a documented server-limit exception requiring
measured caller impact and approval before release (AC136).
- R4: APC non-metadata execution sends `format=ARROW_BATCH`,
`disposition=INLINE_OR_EXTERNAL_LINKS`, and `session_id`, never `statements`.
Decode all bare batches in `chunk.arrow_batches` and complete Arrow IPC streams
at external links, retaining link headers and every row. `EnableQueryResultDownload=0`
keeps SEA and sets string-valued CreateSession
`session_confs["can_cloud_download"]="false"`; live results must remain inline
after T8/AC119 proves that this overrides the APC per-statement download
capability, or a server fix makes it so. Until then inline-only gates cannot pass.
An initial inline-size failure yields no successful ResultSet. A later fetch failure
propagates during iteration; rows already read remain delivered, and the cursor
must not report normal completion.
`EnableSQLExecHybridResults=0` cannot force external-only APC results, unlike
warehouse SEA when cloud download is enabled. APC still requests hybrid
disposition because Reyder offers no APC external-only contract; AC145 measures
inline bytes, first-row latency, and retained heap on paired routes before
exposing this setting. This can change download timing and memory for callers.
`UseBoundedSeaApi` changes neither APC request nor session behavior. Positive
`Statement.setMaxRows` sends `row_limit` and caps rows across pages/links;
zero is unlimited.

Successful no-result DDL, `SET AUTOCOMMIT = TRUE/FALSE`, `COMMIT`, `ROLLBACK`, and no-result `CALL`
have no terminal manifest or one without schema, chunks, links, continuation,
or data; a zero-column schema is not this shape. Direct and polled completion
expose no ResultSet, count zero then `-1` after `getMoreResults()`, and issue no
fetch. APC `Statement.execute("SET AUTOCOMMIT = ...")` returns `false`, unlike
existing APC Thrift and warehouse SEA's shared `SET` classifier. Preserve
existing-route classification and connection method side effects; AC52 measures
the caller impact.
Result-producing `SELECT`, `SHOW`, `VALUES`, unassigned `SET AUTOCOMMIT`, and
`NonRowcountQueryPrefixes` matches require a schema. Explicit
`total_row_count=0` without chunks, continuation, or payload is a typed empty
ResultSet. An absent count requires first-chunk fetch even with an empty list;
missing terminal evidence or fewer rows than the declared count is an error.
Counted `INSERT`, `UPDATE`,
`DELETE`, and `MERGE` require a `num_affected_rows` schema column and value,
including zero; missing count data is a contract error. A matching
`NonRowcountQueryPrefixes` command instead exposes the count ResultSet and update
count `-1`, and stops a Statement batch with prior counts and the existing error.
Direct and polled command live gates depend on T2.

APC advertises LZ4 decompression capability. `QueryResultCompressionType` and
its higher-precedence `EnableQueryResultLZ4Compression` alias use numeric `0`
(`NONE`) or `1` (`LZ4_FRAME`) for the request codec, but Reyder discards it;
decode inline and linked data from the manifest's
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
SEA forwards the metadata-operation, fully-sync, and require-native headers
to Reyder. Reyder honors fully-sync only for synchronous execution. T3/T7
deliver catalog Arrow/HIVE dispatch; T4–T6 extend both formats to the other
six operations. All metadata remains inline and link-free. AC16 gates each
public method on both live DBR formats.
On selected APC SEA, unset
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
`getExportedKeys` is outside the seven SHOW operations. APC Thrift sends a
parent-only `GetCrossReference` request, whose documented DBR metadata service
returns no rows. The metadata design contract defines no parent-only SEA wire
operation. APC SEA issues no request and returns the established typed empty
result, as warehouse SEA does. AC54 checks row and shape parity on the live DBR
service and records any deployed deviation before release.
A null table retains
warehouse SEA's `SQLException`, SQLState `42000`, and
`EXECUTE_STATEMENT_FAILED` (1003) vendor code before any request (AC54).
T19 delivers procedure SELECTs; T35 separately delivers key metadata calls
and their typed-empty behavior. Each has its own live gate before exposure.

The session-reachable async metadata API returns typed ResultSet handles for
the seven wire operations, with distinct imported-keys and cross-reference
calls. Async SHOW submits with `wait_timeout=0s`; await, timeout, cancel, and
conversion preserve synchronous outcomes. No negotiated V9 or saved-ID
capability is defined: a live delayed SHOW must return while RUNNING. Public
JDBC metadata remains synchronous, including `getColumns`; SQL async is separate.

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
`SQLException` warns and omits that catalog. A fan-out future timeout or
interruption aborts the whole call under the current warehouse SEA behavior;
it is not a partial-row result. Initial/enumeration/overall errors propagate
except established object-not-found or empty-pattern typed-empty cases. A
completed partial result leaves the connection usable (AC83). Null-catalog
`getTables` instead returns typed empty on `42601`
without fan-out, as warehouse SEA does, which may hide APC Thrift rows (AC105).
For null-catalog fan-out, each catalog future also retains the existing independent
90-second wait. A positive `MetadataOperationTimeout` bounds the entire call or await,
including fan-out, so the first deadline reached wins. Setting it to `0` disables only
that overall deadline; the 90-second per-future wait still applies. A future timeout or
interruption fails the whole call rather than returning partial rows (AC83/124).

- R6: APC CreateSession explicitly sends `execution_mode=DEFAULT` and leaves
`session_version` absent on later ExecuteStatement requests, matching Reyder's
absent version. `FAST` is reserved for sessions that return an initial version
and can echo the maximum observed version; the current warehouse FAST behavior
is unchanged. Capture APC mode, CreateSession response, and subsequent requests
at the first executing-statement slice (AC23), while the session slice proves
CreateSession mode and version absence (AC143); an APC `FAST` request without a version
cannot pass. CreateSession `negotiated_protocol_version` alone grants native scalar
parameters at V8 (42248) and opt-in `EnableNativeBatching=1` INSERT batches
at V10 (42250). It does not gate R5 async metadata. Reyder leaves
`session_version` unset; serialized IDs and missing versions grant no capability.
Retain the negotiated protocol version for later requests. Reject complex bindings
before any single or batch request with a tested driver-owned error name/code;
`toString()` interpolation is not lossless SQL. At `supportManyParameters=0`,
V8+ uses native scalar `parameters` and older/versionless sessions interpolate
supported scalars without unresolved markers; V8–V9 legacy batch sets may use
native scalars. At `supportManyParameters=1`, single and legacy batch sets
interpolate at every version and non-null `setBytes` is permitted; at `0`,
non-null `setBytes` remains rejected. Keep missing-version telemetry distinct.

T15 delivers legacy prepared INSERT batches; T36 separately delivers opt-in
native V10 prepared INSERT batches. Native V10 sends ordered nested
`parameter_sets`, no top-level `parameters`.
Its aggregate count yields `Statement.SUCCESS_NO_INFO` for each successful
input set; unlike warehouse native SEA, APC V10 cannot return per-set counts.
If execution fails, a `BatchUpdateException` marks every set
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

APC SEA's intended SQL-text limit is 16,777,216 UTF-8 bytes for single statements
and legacy generated chunks, without silent split or fallback; an oversized
request propagates the SEA size error. AC87 measures the deployed APC boundary
before dependent batch gates close. Warehouse SEA's character-count validator
is outside this project's change scope. `BatchInsertSize` can reduce interpolated
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
- R9: The first flag-on SELECT slice closes its open statement and live APC SEA session.
The session slice also makes public `getConnectionId()` return an ID encoding the selected protocol and
target without requiring a Thrift handle for an APC SEA session. Given
the original workspace endpoint (scheme, host, port) and usable credentials, new saved IDs
let `Driver.closeConnection` close the original session after a flag or compute-target change
in the supplied URL or properties. The ID does not encode the endpoint or credentials;
endpoint and credential changes are outside this guarantee. Public statement IDs remain raw.
Internal identities retain the compute target and reject malformed IDs without logging identifiers.
Preserve decoding of legacy warehouse SEA and Thrift IDs;
legacy warehouse Thrift IDs encoded as SEA by `origin/main` lack the Thrift secret and cannot
be repaired for saved close, so callers holding them may need to reopen. Recovered statements
use the current connection target and are outside the saved-session close guarantee. Only a
live CreateSession version grants capabilities.
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
The server sets a mask for every vector: bit 0 is the first row (least-significant-bit
first), and `1` means null. Null rows consume placeholder vector values; rows beyond
a short mask are non-null, and bits past the row count are ignored. Unknown vectors fail.
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
- R17: APC SEA follows warehouse SEA's retry, pool, and linked-download behavior
at either `UseBoundedSeaApi` value. `EnableInlineStreaming` and
`EnableStreamingChunkProvider` cannot change APC SEA rows, paging, or chunk prefetch
policy; APC continuation lacks reliable totals for the legacy linked behavior.
At `UseBoundedSeaApi=0`, APC keeps bounded linked streaming even when warehouse
SEA changes provider with the switch. APC also ignores `EnableInlineStreaming`
for inline buffering, unlike APC Thrift. AC39 measures first-row latency, heap,
and link-expiry impact before exposure.
`ThriftMaxBatchesInMemory` bounds queued APC inline response chunks (minimum two),
whereas APC Thrift bounds batches. A chunk can contain several batches, so the
same setting can use more memory; paging must avoid materializing the full result
(AC109).
Case-insensitive `cloudFetchThreadPoolSize` (default 16) bounds per-result
linked workers and buffered chunks; `LinkPrefetchWindow` (default 128) bounds
fetched-ahead links only. `ChunkReadyTimeoutSeconds` bounds linked-chunk wait;
`RowsFetchedPerBlock` does not tune SEA paging.

Presigned-link downloads use the existing Thrift HTTP pool and retry settings.
`RateLimitRetry` and `TemporarilyUnavailableRetry` control ordinary 429/503
link retries, which require `Retry-After`; their timeouts bound accumulated
delay. A code in `ApiRetriableHttpCodes` instead uses `ApiRetryTimeout` and can
retry without that header. `IdleHttpConnectionExpiry` applies to both the link
and SDK API pools. SEA SDK API calls follow warehouse SEA retries and ignore
these link-handler controls, so APC Thrift attempt count, delay, and final error
may differ. AC51 measures each exposed request class and caller impact.

SDK 0.118.0 may retry an `ExecuteStatement` POST after 429/503 even when a
write committed. Preserve warehouse SEA's ambiguous-response SQLException;
do not add APC replay or claim at-most-once execution. Release notes direct
callers to reconcile committed rows before resubmitting. No APC-only
concurrency cap or full-result buffer is allowed (AC30/46/51/84).
- R18: Each JDBC package exposes only its completed caller workflow under R1. Before
its live gate passes, an incomplete method or setting fails before network execution
when known; an unsupported response shape fails before false success. This includes
statement calls before T10, multi-page results before T11, download-disabled
sessions before T32/AC119, SQL async before T29, timeout/cancel before T33,
SQL-backed `isValid` before T34, single prepared INSERT/UPDATE/DELETE/MERGE
before T39/AC146, and IN-only `CallableStatement.execute`/`executeQuery`
before T38/AC147. Single prepared writes use R4's counted-DML contract and
preserve `executeUpdate` count and `execute` boolean/update count, including
zero. CALL results and server errors use the prepared execution path; a
result-producing CALL exposes its ResultSet, while a no-result CALL returns
`execute=false` with no ResultSet and a zero update count. Existing unsupported
OUT/named parameters and callable `executeUpdate` still reject before request.
`executeQuery` on a no-result CALL raises the existing result-set-expected error
after execution rather than returning a null ResultSet.
This no-result CALL outcome follows its actual APC terminal shape rather than
the base CALL classifier; AC147 records the caller difference. Also guard
nondefault row limit, direct-results,
response-compression, or other settings until their owning gate passes. Do not
fall back to Thrift after SEA selection. Scoped staging validation may start
after T1; production assignment waits for T27/T-val on the assembled stack
and post-merge smoke. Flag-off APC remains on Thrift.
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
A9: Yes, including JDBC partners allowed by `enableSqlExecForJdbc`. Deploy T1 before APC
assignment; partner enablement needs a separate rollout decision (R1, T1).
- Q10: How is repeated transfer avoided during `GetStatementResult` polling?
A10: At least three RUNNING polls of a large query transfer no result payload; terminal
response and fetch return all rows once (AC27).

## Acceptance Criteria

At first exposure, each public slice proves its completed call on scoped flag-ON APC staging
against APC Thrift and warehouse SEA where relevant. AC3/11/40/51/84/85 apply to
new request classes as relevant, AC30 to new pools, and AC10 to new DEBUG paths. Induced failures
use the T37 staging fault hook, which preserves the request and records the response mutation;
fixtures cover unavailable fault branches only where a criterion does not require
live proof. Ordinary and server-dependent gates require live deployment and proof. Retired AC IDs
(AC6/8/33/41/47/48/86/103/104/110/111/115/116/120/121/123/125/127/128/129/130/134/135/137) are not reused.

The shared paired-load gate (AC25/31/32/46/52/61/75/107/117/118) uses the same
driver build, staging region, warmed cluster, data shape, and one/32 callers on
APC SEA and Thrift. SEA/Thrift median throughput is at least 0.90, latency at most
1.10, and peak heap at most 1.20. AC31/32 also bound retained heap growth across
two data sizes to max(8 MiB, 5% of added bytes). Keep raw measurements, fixture
sizes, and error counts; the test plan owns sampling. For a proposed non-functional
exception, also measure warehouse SEA/Thrift on the same build and comparable
workload. Record both deltas, precedent or server limit, caller impact, and parity
matrix disposition. PECO/Perf Eng must approve measured exceptions before release.

- AC1: GIVEN scoped flag-ON/OFF APC staging WHEN `UseThriftClient` is unset, `0`, or `1` and `TreatMetadataCatalogNameAsPattern=1` varies THEN a live SELECT proves R1 precedence and no SEA fallback. At flag ON with no override, ordinary User-Agent uses SEA while partner and JDBC-allowlisted partner agents use APC Thrift and return the same rows; capture route and returned flag. Compare warehouse SEA routing. Fixtures cover Arrow-unavailable, legacy `cliservice`, and flag-lookup failure.
- AC2: GIVEN flag-on APC staging WHEN a cluster-path connection runs single-chunk, RUNNING, manifest-only, and explicit zero-row SELECTs with `UseBoundedSeaApi` and `EnableSQLExecHybridResults` each at `0`/`1` THEN live captures show APC-only CreateSession targeting and the same hybrid Arrow disposition at all four settings; rows, types, empties, and direct close match paired routes. A RUNNING response is polled to terminal; an absent zero count fetches chunk zero even with no listed chunks; missing expected rows fail before false success. Fixtures cover absent `arrow_schema` and unknown formats.
- AC3: GIVEN each exposed APC R2 operation WHEN it runs after a flag or target change THEN captures show R2 target placement, original statement target, exactly-once close, connection usability, and unchanged warehouse serialization.
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
- AC16: GIVEN each synchronous SHOW method at first exposure and T7's two repeatable staging DBR format conditions WHEN live direct and polled APC SEA captures `chunk.attachment` on the Arrow condition and `chunk.columnar_data` on the HIVE condition THEN nonempty, inline, link-free JDBC rows, columns, nulls, and errors match paired routes; typed empties retain their shape. Record the DBR version/configuration that produced each format for that method. Both live formats are required; R13 mask edge cases use fixtures.
- AC17: GIVEN live V10 APC and paired routes WHEN native prepared INSERT batches succeed or fail THEN ordered `parameter_sets` yield `SUCCESS_NO_INFO` per success or `EXECUTE_FAILED` for every set in a `BatchUpdateException` on execution failure; commit status is uncertain. A post-write count-response fault raises a distinct result error, clears the batch, and warns that rows may be committed. Apply the shared fault-hook and load rules.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. Compare elapsed time, SQLState/vendor code, target, and
post-timeout connection usability on flag-ON staging.
- AC22: GIVEN a live Statement INSERT batch with a failing middle command WHEN APC SEA
executes it THEN requests are sequential without `statements`, counts retain order, execution
stops on failure, and errors match APC Thrift. AC32/64/66 cover load and settings.
- AC23: GIVEN live and captured versionless CreateSession responses WHEN APC connects and executes its next statement THEN CreateSession sends `DEFAULT`, later requests omit `session_version`, and only `negotiated_protocol_version` grants R6 capabilities; neither a session version nor a saved ID does. Record the live negotiated version and missing-version diagnostic. Warehouse SEA still sends `FAST` and echoes its observed version.
- AC25: GIVEN new APC and warehouse SEA/Thrift IDs with the original endpoint and credentials WHEN `Driver.closeConnection` uses an ID after a flag or target change at one and 32 callers THEN only the original session closes without an APC Thrift handle; handles/threads return to baseline and close latency/errors pass the shared paired gate. Legacy IDs decode, malformed IDs fail without logging identifiers, statement IDs stay raw, and prechange warehouse Thrift IDs may require reopen. Saved IDs carry no statement identity; live statement close/cancel remain under AC3/107.
- AC27: GIVEN the first public SELECT slice and a live single-chunk APC query with three or more RUNNING polls WHEN transfer
is measured on explicit-override and flag-on routes THEN polls carry no result payload and
the terminal fetch delivers every row once; compare APC Thrift and warehouse SEA traffic.
- AC28: GIVEN the T1 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN flag-on APC staging and paired routes WHEN a complex binding is submitted to a single statement or an exposed prepared batch THEN APC SEA raises its named/numeric driver error before any request, keeps the connection usable, and records its identity against both baselines. The owning PR records an owner-verified taxonomy/dashboard update or maintainer handoff.
- AC30: GIVEN flag-on APC staging and paired APC Thrift/warehouse SEA under pool saturation WHEN `HttpConnectionPoolSize`, `HttpMaxConnectionsPerRoute`, and `HttpConnectionRequestTimeout` vary separately for each newly exposed API or linked-download class THEN measured in-flight count, wait, transport error identity, and throughput follow the applicable transport scope; SDK API acquisition ignores the Thrift HTTP wait setting.
- AC31: GIVEN each SELECT mode at its first public exposure WHEN one and 32 paired APC SEA/Thrift callers drain primitive, multi-page, or linked results as applicable THEN rows/errors match, SEA paging ignores `RowsFetchedPerBlock`, and the shared load gate passes. The release gate repeats the combined mode mix.
- AC32: GIVEN an exposed Statement, legacy prepared, or V10 native INSERT batch mode on flag-on live APC SEA and paired routes WHEN one and 32 callers use disjoint keys and applicable R6 settings THEN committed rows, counts, errors, and the shared load gate pass for that mode. Record negotiated version; captured V10 responses cannot close native correctness or load gates. Release repeats the combined modes.
- AC34: GIVEN scoped flag-ON staging and default-OFF configuration WHEN connector-service looks up the APC sub-flag for ordinary, partner, and allowlisted JDBC partner User-Agents THEN only the ordinary scoped agent receives true; default lookup and both partner lookups return false. Record flag version and values.
- AC36: GIVEN APC chunks with multiple Arrow batches and inline continuations WHEN JDBC drains them THEN rows appear once and `row_offset` includes all batch rows; a failed page cannot report success. If live staging lacks multiple batches, retain a fixture and leave live proof pending.
- AC39: GIVEN each inline or linked mode at first live exposure WHEN bounded, `EnableInlineStreaming`, and `EnableStreamingChunkProvider` switches vary for paired callers THEN R4/R17 rows, request patterns, buffer limits, and AC31 load bounds hold. APC inline paging and queue bounds do not change with `EnableInlineStreaming`; record first-row latency and heap against APC Thrift. For linked results at bounded `0`, compare APC's fixed provider against both warehouse switch values for first-row latency, throughput, heap, and link expiry. Disposition both caller impacts under the shared exception rule.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF or scoped ON THEN flag-OFF APC stays on Thrift, completed flag-ON calls pass their live gate, and incomplete calls fail before request or false success without fallback. Production assignment waits for T27/T-val.
- AC45: GIVEN linked or inline APC results WHEN `EnableSeaResponseCompression=0` and `1` run under the paired load gate THEN only `0` sends API `Accept-Encoding: identity`; links are unchanged. Rows, API bytes, request counts, throughput, latency, and heap are recorded for both settings and protocol deltas dispositioned.
- AC46: GIVEN paired linked results WHEN `cloudFetchThreadPoolSize`, `LinkPrefetchWindow`, chunk wait, and slow-link settings vary THEN workers/chunks remain within the per-result worker bound, fetched-ahead links within the window, and aggregate use scales with callers. Rows, duration, errors, and AC31 load bounds match baselines; inline pages stay unaffected.
- AC49: GIVEN each exposed live SELECT mode WHEN `setMaxRows` is zero, one, or a page boundary THEN positive limits send SEA `row_limit` and cap rows across continuation while zero remains unlimited; paired rows and request count match R4.
- AC50: GIVEN each typed SHOW handle at first exposure WHEN live delayed SHOW submits with `wait_timeout=0s` THEN it returns while RUNNING and another call completes before await; each handle passes AC16's two-format row gate and independently proves timeout, cancel, cleanup, R5 headers, and omission of the sync-only header at either `EnableSeaSyncMetadata` value. Procedure SELECTs reject async before request.
- AC51: GIVEN T37's live staging fault hook WHEN 429/503 with and without `Retry-After` affect each newly exposed SDK API request class and linked download, including a write before submission and after acceptance THEN APC SEA and warehouse SEA have the same SDK retry attempts, delays, and final SQLException identity; record committed rows and uncertain or duplicate write outcomes. Linked downloads obey R17's HTTP retry controls and budgets, including `ApiRetriableHttpCodes`, with attempts, delay, and exhausted errors compared to APC Thrift and warehouse SEA. The test plan owns the setting matrix; absent live injection leaves this gate open.
- AC52: GIVEN scoped flag-on staging after AC113 WHEN public JDBC executes no-result DDL, `SET AUTOCOMMIT = TRUE/FALSE`, `COMMIT`, `ROLLBACK`, and counted INSERT/UPDATE/DELETE/MERGE including zero through direct and polled SEA responses THEN wire captures show R4's shape; APC no-result `execute` is false with no ResultSet or fetch, count zero then `-1` after `getMoreResults()`, while DML counts/rows/errors match paired routes. Capture the intentional `SET AUTOCOMMIT = ...` boolean difference against APC Thrift and warehouse SEA and its caller impact; transaction side effects and errors retain parity. A zero-column schema or missing `num_affected_rows` fails without breaking the connection; fixtures alone cannot close either path.
- AC53: GIVEN live prepared scalars or an exposed fallback batch plus older/versionless fixtures WHEN `supportManyParameters` is 0 or 1 THEN R6's native/interpolated request choice has no unresolved markers, `setBytes` follows R6, and rows/counts match paired routes. Record fixture-only branches.
- AC54: GIVEN a referenced parent table on the documented DBR metadata service WHEN live `getExportedKeys` runs at both catalog settings THEN APC SEA makes no metadata request and returns the same empty rows and JDBC columns as APC Thrift and warehouse SEA. Capture the Thrift parent-only request and its empty response. Any nonempty deployed Thrift response is a parity gap requiring a measured caller-impact disposition before release. A null table fails before request with SQLState `42000` and vendor code 1003.
- AC55: GIVEN public APC SEA SQL async SELECT WHEN primitive and linked results complete
THEN `wait_timeout=0s`, targeting, rows/errors, cancel, close, and diagnostics follow R2/R10.
Apply AC31's one/32-caller load bounds to this public path before unguarding it.
- AC60: GIVEN a valid live query within its configured client wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Early timeout is a
correctness failure; only a measured latency difference may receive a documented exception.
- AC61: GIVEN a nonempty metadata method or typed handle at first exposure WHEN one and 32 paired APC SEA/Thrift callers run it with the same result shape THEN rows/errors and the shared load gate pass; null-catalog fan-out issues at most N+2 requests for N eligible catalogs. APC SEA CPU per 1,000 decoded cells and peak process RSS are each at most 1.20 times the paired APC Thrift value at both caller counts, or require a measured, approved warehouse-precedent/server-limit exception under the shared rule. Record both raw measurements; typed empties do not replace nonempty live proof.
- AC63: GIVEN valid delayed SHOW within its configured metadata wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Early timeout is a
correctness failure; only a measured latency difference may receive a documented exception.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching INSERT runs through public Statement THEN `execute` returns true, its ResultSet contains the affected-row value, and `getUpdateCount` is `-1`.
- AC65: GIVEN each synchronous APC SHOW at first exposure WHEN native, sync, hybrid, and bounded settings vary THEN request captures follow R5 and rows pass AC16. The warehouse unset-header exception does not excuse a row difference.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized `getProcedures` and `getProcedureColumns` WHEN each reads pages or links THEN header-free queries yield ordered paired JDBC rows and pass AC61. Query/fetch/decode failure logs and returns the typed empty result; older/versionless interpolation uses fixtures.
- AC82: GIVEN live `getFunctions` with explicit or null catalog WHEN `EnableShowCommandForGetFunctions` is 0 or 1 on paired routes THEN APC SEA uses SHOW at both values and follows warehouse SEA rows/errors; record APC Thrift's native/SHOW and null-catalog differences with caller impact.
- AC83: GIVEN live null-catalog `getSchemas` or `getColumns` with `42601` fan-out WHEN one catalog request raises `SQLException` THEN APC and warehouse SEA warn, omit that catalog, return other rows, and keep the connection usable. WHEN a catalog future exceeds its independent 90-second wait or is interrupted THEN the whole call fails without partial rows, including when `MetadataOperationTimeout=0` or exceeds 90 seconds. A shorter positive overall timeout wins first. Record APC Thrift row differences; initial object-not-found or empty-pattern yields typed empty, and other initial failures propagate.
- AC84: GIVEN each newly exposed live SDK API or linked-download request class WHEN socket, poll, and idle settings vary THEN `socketTimeout` bounds one request, `asyncexecpollinterval` changes later-poll delay, and `IdleHttpConnectionExpiry` evicts idle connections in each applicable pool. In-budget calls complete with paired timing/error identity and usable connections.
- AC85: GIVEN each exposed execution, metadata, download, batch, failure, or recovered-session path WHEN telemetry is enabled by `EnableTelemetry` and the server flag, or by `ForceEnableTelemetry`, THEN R8's redacted event families, operation/format/counts, and error identity match paired routes or have a measured exception. `TelemetryLogLevel=OFF`, or `EnableTelemetry=0` or server flag off without force, exports no new event. DEBUG and telemetry expose route/failure identity but no credential, presigned URL secret, raw APC target, or compound session/statement ID.
- AC87: GIVEN deployed APC SEA, APC Thrift, and warehouse SEA WHEN single-statement SQL crosses 16,777,216 UTF-8 bytes, including multibyte text THEN record each route’s acceptance and error identity. APC SEA accepts through the limit and rejects larger text with a size error and no fallback; any differing deployed boundary leaves the gate open with caller impact documented. Warehouse validation is unchanged.
- AC88: GIVEN two accessible catalogs and a metric view WHEN `enableMultipleCatalogSupport` and `EnableMetricViewMetadata` vary on all three live routes THEN catalog access, discovery, table types/rows, and APC session setting match R5; record any server-limited row difference and caller impact.
- AC89: GIVEN live invalid SQL, missing-object, or permission failure WHEN exposed execution methods surface it THEN SQLException class, SQLState, vendor code, and message category match agreeing baselines or warehouse SEA mapping with caller impact; no failed command reports success.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN an exposed live inline or linked APC result mode WHEN `QueryResultCompressionType` or its higher-precedence alias is `0` (`NONE`), `1` (`LZ4_FRAME`), or invalid text THEN decoding follows the manifest codec, including unmarked data and LZ4 despite `0`, while invalid text keeps the warehouse default. Rows and paired load pass for that mode; record requested/actual codec, transfer bytes, decompression CPU, and the `NONE` caller impact. Release repeats both modes.
- AC102: GIVEN an isolated live write table WHEN transaction and auto-commit settings vary THEN R19's state query, committed/rolled-back rows, counts, and errors match paired routes. APC SEA public assignments/commit/rollback have no ResultSet or fetch, count zero then `-1` after `getMoreResults()`; AC52 records the assignment boolean exception. Missing state data or failed commands cannot succeed.
- AC105: GIVEN live null-catalog `getTables` receiving `42601` WHEN synchronous APC SEA, APC Thrift, and warehouse SEA calls are compared THEN APC SEA and warehouse SEA return the same typed empty result without catalog fan-out; record omitted APC Thrift rows and caller impact.
- AC106: GIVEN a changed driver-owned APC SEA error WHEN it reaches JDBC THEN a test asserts its `DatabricksDriverErrorCode` name and unique number, and the PR records owner-verified telemetry taxonomy/dashboard updates or maintainer handoff and completes the telemetry-error template. Server errors retain server identity.
- AC107: GIVEN paired one- and 32-caller staging WHEN the first SELECT slice repeatedly opens, queries, leaves an operation open, and closes THEN original-target CloseStatement precedes TerminateSession, no live resources remain, handles/threads return to baseline, retained heap growth is at most 8 MiB, and median cycle latency is at most 1.10 times APC Thrift. Release repeats the assembled mix under the shared exception rule.
- AC108: GIVEN AC87 passed and live legacy APC INSERT batches WHEN batching and binding settings vary THEN R6 chunk limits, per-set counts, committed rows, errors, and AC32 paired load pass for each binding mode/version. Oversized chunks follow AC132.
- AC109: GIVEN concurrent live multi-page inline SELECTs WHEN `ThriftMaxBatchesInMemory` varies around two THEN queued response chunks obey R17 despite multiple batches per chunk, each row arrives once, and retained result memory and AC31 load bounds hold. Record peak queued chunks, Arrow batches, bytes, and heap against APC Thrift's batch-based setting.
- AC112: GIVEN AC119 passed and live single-chunk and multi-page APC SELECTs with download disabled at either bounded value WHEN rows are drained THEN CreateSession sends `session_confs["can_cloud_download"]="false"`, all chunks remain inline and appear once. Initial size failure prevents ResultSet success; later-page failure propagates during iteration without reporting normal completion.
- AC113: GIVEN T2 deployed on scoped APC staging WHEN direct and polled SEA calls complete no-result DDL, transaction commands, and counted DML including zero THEN captures show R4's no-result shape without a zero-column schema or fetch, and DML's `num_affected_rows` schema/value. Failed commands retain errors. The paired T14 JDBC command slice must then prove AC52 before the wire gate is considered delivered to callers; fixtures cannot close it.
- AC114: GIVEN a child with two parents and a second foreign table WHEN live `getImportedKeys` and `getCrossReference` run THEN each filters R5’s `GET_CROSS_REFERENCE` response to the requested rows and JDBC shape. Null foreign table returns typed empty before request; invalid table inputs fail before request; syntax/object-not-found returns typed empty. Compare each method with paired routes.
- AC119: GIVEN T8 completed on scoped APC staging WHEN CreateSession sets `can_cloud_download=false` and APC ExecuteStatement still advertises `can_download_result=true` THEN a single-chunk and multi-page query both return inline chunks only. Capture both wire values and the effective result disposition. If links appear or the override cannot be proved, fix the server contract and keep AC112 open; fixtures cannot close this gate.
- AC117: GIVEN paired live callers with a bound SELECT WHEN `PreparedStatement.getMetaData()` runs before execution THEN JDBC metadata matches both routes, bindings remain intact for the later SELECT, and request count, median metadata latency, workflow throughput, and peak heap pass the shared load gate. AC53 covers version/interpolation; fixtures cannot close this gate.
- AC118: GIVEN paired live callers with `EnableSQLValidationForIsValid=1` WHEN `isValid(timeout)` repeatedly runs `SELECT VERSION()` THEN healthy/failed booleans, timeout, connection usability, SELECT counts, median call latency, calls per second, and peak heap pass the shared load gate. Fixtures cannot close it.
- AC122: GIVEN live APC tables with imported keys from two parents and a second foreign table WHEN typed async `getImportedKeys` and `getCrossReference` handles complete THEN each returns its own filtered JDBC rows and typed empties under AC114's input/error rules, independent of the synchronous call. Compare both separately with paired routes and satisfy AC50/61 for each handle.
- AC124: GIVEN each exposed live synchronous SHOW or typed async handle WHEN `MetadataOperationTimeout` varies THEN a positive value bounds the complete call or await and targets cancel, while `0` disables only that overall bound. For null-catalog fan-out, AC83's independent 90-second per-future wait still applies and the first deadline wins. An in-budget operation completes despite a shorter SEA request timeout; the connection remains usable after timeout.
- AC126: GIVEN live null-catalog typed async `getTables` receiving `42601` WHEN its handle completes THEN it returns the same typed empty table result as synchronous APC and warehouse SEA without catalog fan-out, with any omitted Thrift rows recorded.
- AC131: GIVEN an exposed live APC Statement batch with `NonRowcountQueryPrefixes=INSERT` WHEN a matching command occurs after prior successful commands THEN the batch stops, reports preceding counts, and raises the same batch error identity as APC Thrift and warehouse SEA.
- AC132: GIVEN AC87 passed and a live legacy INSERT chunk at the UTF-8 SQL-size boundary WHEN an interpolated chunk or one oversized value exceeds 16,777,216 bytes THEN APC SEA raises AC87's size error without splitting or fallback; accepted chunks retain committed rows and per-set counts.
- AC133: GIVEN live typed async null-catalog `getSchemas` or `getColumns` with `42601` fan-out WHEN one catalog request raises `SQLException` THEN the handle warns, omits that catalog, preserves other rows and connection usability. A fan-out future timeout, interruption, or overall cancel fails the handle; compare with synchronous warehouse SEA under AC83.
- AC136: GIVEN the first public flag-on APC SELECT slice with `EnableHeartbeat=1` WHEN its partly read ResultSet stays idle across at least two heartbeat intervals and the server's configured idle-result expiry window alongside a paired APC Thrift cursor THEN zero APC `/status` requests occur and both cursors resume, drain, and close with identical rows. Record the observed survival duration and expiry setting before exposing SELECT; a shorter APC SEA lifetime needs the R3 exception approval.
- AC138: GIVEN T3 deployed on scoped APC staging WHEN direct and polled `GET_CATALOGS` reach Reyder THEN all three R5 headers are forwarded or omitted as sent, sync mode is honored, and a nonempty native-marked Arrow IPC result is inline and link-free. Capture received headers for the unset and explicit settings; fixtures cannot close this gate.
- AC139: GIVEN T4 deployed after AC138/142 WHEN direct and polled `GET_SCHEMAS` and `GET_TABLES` run on live Arrow/HIVE DBR conditions THEN each dispatches natively and satisfies R5's headers and inline, native-marked response shape; JDBC T17 closes AC16 before exposure.
- AC140: GIVEN T5 deployed after AC139 WHEN direct and polled `GET_COLUMNS` and `GET_FUNCTIONS` run on live Arrow/HIVE DBR conditions THEN each satisfies AC139's wire gate; JDBC T18 closes AC16 before exposure.
- AC141: GIVEN T6 deployed after AC140 WHEN direct and polled `GET_PRIMARY_KEYS` and `GET_CROSS_REFERENCE` run on live Arrow/HIVE DBR conditions THEN each satisfies AC139's wire gate; JDBC T35 closes AC16 before exposure.
- AC142: GIVEN T7 deployed after AC138 WHEN live `GET_CATALOGS` runs under the repeatable HIVE DBR condition THEN direct and polled responses contain nonempty native-marked, inline, link-free `columnar_data`; record its DBR version/configuration. T4–T6 repeat the live HIVE gate for their operations; fixtures cannot close it.
- AC143: GIVEN the first JDBC session slice after the APC flag deploys WHEN live flag-ON and flag-OFF cluster-path connections open, save an ID, and close without a statement THEN R1 route precedence, APC-only CreateSession with `execution_mode=DEFAULT` and no session version, original-target live and saved-ID TerminateSession, redacted route diagnostics, failure propagation, partner exclusion, and warehouse request invariance hold; incomplete statement methods fail before request. A server rejection leaves the close gate open.
- AC144: GIVEN T37's staging-only fault hook WHEN a scoped test injects 429/503 before a read request, after an accepted write, and on the post-write count response THEN the original request reaches the intended target, the selected response mutation and `Retry-After` are recorded, and isolation prevents unrelated callers from receiving the fault. A live demonstration on APC SEA and warehouse SEA is required before AC51/17 can pass.
- AC145: GIVEN cloud download enabled on flag-on APC SEA, APC Thrift, and warehouse SEA WHEN paired SELECT callers set `EnableSQLExecHybridResults=0` and `1` THEN captures show APC SEA requests hybrid at both values and warehouse SEA requests external-only at `0`; record inline and link bytes, first-row latency, throughput, and peak heap at one and 32 callers. The shared paired-load gate and approved exception rule disposition the APC difference before `0` is exposed.
- AC146: GIVEN T2/AC113 deployed and a live isolated write table WHEN single prepared INSERT, UPDATE, DELETE, and MERGE calls execute through `executeUpdate` and `execute` with scalar bindings, including zero affected rows THEN APC SEA emits one counted-DML request per call, returns the matching count and `execute=false`, exposes no ResultSet, and matches paired committed rows and error identity. Apply AC29/51/53/85/89 and the shared load gate before exposure; batch success does not close this gate.
- AC147: GIVEN live IN-only procedures that return rows, no result, or a server error WHEN APC SEA `CallableStatement.execute` or `executeQuery` runs THEN bound `CALL` uses APC targeting, returns actual rows or propagates the server SQLException; `execute` is true only with a ResultSet, while no-result `execute` is false with count zero and `executeQuery` raises the result-set-expected error. Compare APC Thrift and warehouse SEA rows, error identity, and boolean caller impact; unsupported OUT/named parameters and callable `executeUpdate` reject before request. Apply AC51/85 and the shared load gate before exposure.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The release-readiness JDBC package alone owns `docs/`
  and `NEXT_CHANGELOG.md` and records the complete gated feature. Earlier stacked PRs use
  `NO_CHANGELOG=true` only as internal implementation slices of that same unreleased feature:
  no driver release or production APC assignment may occur between slices. This basis must be
  stated in each PR; if a release is cut early, its visible slice needs a changelog update
  before release. Completed slices remain shippable behind the flag, with no partial ramp.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T1–T8 and staging-only T37; known gaps SC-256188 under SC-256186
  remain external prerequisites for affected dogfood checks. T8 is limited to
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

`plan.json` owns task-to-package mapping and same-repository PR ancestry.
Cross-repository `parent` is not an executable scheduler dependency, so each
JDBC package includes its deployed server wire criterion and a live caller
criterion as a blocking completion milestone: T1/T37 before session and first
transport exposure; T3/T7 before catalog SHOW; T4–T6 before later SHOWs; T2
before commands and single prepared writes; T8 before download-off. The tracked
termination binding fix gates live close proof. Catalog SHOW follows the first
SELECT foundation, before batch work. Independently gated linked transport,
SQL async, timeout/cancel, and SQL-backed `isValid` remain separate PRs. The
plan exceeds seven JDBC PRs to keep independently exposed workflows reviewable.
Sibling packages own disjoint lines; release validation waits for all siblings
to merge and runs on the assembled branch. Only that final package owns
docs/changelog. Production assignment follows T27/T-val and post-merge smoke.

### Affected Components

JDBC connection, statement, prepared-statement, metadata, SEA transport, and
telemetry; the named Reyder and connector-service contracts.

### Data Model Changes

Session state carries the APC target and negotiated capability version.

### API Changes

Existing JDBC methods keep their signatures, with APC no-result boolean
exceptions in R4/R18. The session-reachable async metadata API covers seven
SHOW operations; public connection IDs may encode APC SEA.

### Risks

Server deployment gates metadata and termination; AC13/Q7 block lost logical
types, AC27 blocks repeated poll payloads, and AC11/14 protect warehouse and
presigned-link boundaries.

---

## Tasks

Acceptance criteria define each task's live gate; `plan.json` owns package scope and ancestry.

- T1–T8: server APC flag; command wire; catalog Arrow dispatch; schema/table,
  column/function, and key dispatch; catalog HIVE conversion after T3; and
  inline-only session contract, respectively.
- T9–T13: session and saved-ID close; primitive SELECT with polling, first fetch,
  and heartbeat; multi-page inline SELECT; linked SELECT; and transport controls.
- T14–T19: commands; legacy prepared batches; catalog SHOW; schema/table
  SHOW; column/function SHOW; and procedure SELECT metadata, respectively.
- T20–T26: typed async catalog, schema, table, column, function, primary-key,
  and imported/cross-reference handles, respectively.
- T27–T34: release docs/changelog; prepared SELECT and pre-execution metadata;
  SQL async; transactions; Statement batches; download-off; timeout/cancel;
  and SQL-backed `isValid`, respectively.
- T35–T36: primary/imported/cross-reference/exported key metadata; opt-in
  native V10 prepared batches, respectively.
- T37–T39: isolated live staging fault hook; IN-only callable execution;
  single prepared INSERT/UPDATE/DELETE/MERGE, respectively.
- T-val: assembled live and release evidence.
