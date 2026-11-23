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
  SC-254478 covers per-statement `ExecuteStatement.session_confs`, not the CreateSession
  `can_cloud_download=false` override of APC `can_download_result=true` (T8/AC119).
  TerminateSession declares `body: "*"` (R2).
- Reyder's APC terminal translation currently requires an Arrow schema when metadata is
  present. T2 must prove direct and polled no-result DDL and counted DML before JDBC AC52;
  live manifests populate `arrow_schema`, so absent-field decoding needs a fixture.
- Reyder currently discards APC `result_compression`, advertises LZ4 decompression capability
  to DBR, and reports DBR's actual codec in the result manifest. Uncompressed results are valid;
  the JDBC codec setting cannot select APC's effective wire codec (R4, AC101).
- Reyder accepts APC `ARROW_BATCH` but currently rejects the contract's `ARROW_STREAM`
  metadata request, lacks native SHOW dispatch, and rejects HIVE results. SEA also does not
  forward the metadata-operation or fully-sync header to Reyder. T3–T7 close
  these server gaps before the corresponding JDBC method is exposed; JDBC retains `ARROW_STREAM`.
- Connector-service returns `databricks.partnerplatform.clientConfigsFeatureFlags`
  sub-flags at `/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`.
  `FeatureFlagsHandler.scala` remaps `enableSqlExecForJdbc` to
  `enableSqlExecForJdbc_v3.4.3` at ≥3.4.3 and may allowlist a JDBC partner;
  Q9 requires a blanket APC override.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata tabs). The intended SQL-text limit is 16 MiB
(16,777,216 UTF-8 bytes). The checked-out warehouse `ExecuteStatementOperation`
compares Scala string `.length` while its error says bytes; that warehouse validator
is not evidence of APC's effective boundary. AC87 checks the deployed APC boundary
and records warehouse behavior separately. This project does not change warehouse
SQL validation. The server rejects non-OSS-JDBC clients
and starts rollout opt-in through the dedicated flag.
- Driver baseline: `origin/main` @ `840f3a407` (spec #1809; bounded SEA default
  #1827). APC routes to Thrift; SEA assumes warehouse targeting and a `/status`
  heartbeat. Saved IDs can misroute close after flag or target changes. APC Arrow
  batches, async SHOW, and SEA columnar metadata need driver support. The native
  metadata property defaults to `1`, but unset warehouse SEA sends its header
  only when the warehouse flag is on; unset APC SEA will send it (R5).
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the prototype
baseline, not the design; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756)
is unchanged.
- Parity baseline for review and release (same driver build on all routes). `Parity` means the
  same caller outcome, `warehouse precedent` means APC SEA follows warehouse SEA where it
  differs from Thrift, and `exception` needs the cited server limit and measured caller impact.

  | Behavior | APC Thrift | Warehouse Thrift → SEA precedent | APC SEA | Disposition and caller impact |
  | --- | --- | --- | --- | --- |
  | Routing and close | Direct Thrift; live close | Explicit protocol and original-target close | Dedicated APC flag; saved IDs close sessions only | Warehouse precedent for protocol choice; parity for live close. A saved ID cannot close a statement (R1/R9). |
  | Result paging and codec | Arrow inline/links; requested codec | SEA pages/links and actual manifest codec | Hybrid batches, continuation, effective LZ4 or uncompressed | Warehouse precedent for paging; exception for APC codec because Reyder discards the request. `NONE` cannot eliminate decompression CPU/bytes (R4/R12). |
  | Idle ResultSet | Heartbeat preserves an idle cursor | Warehouse SEA has status heartbeats | No APC status RPC or heartbeat | Exception: Reyder has no APC status endpoint. An idle cursor may expire sooner; AC136 measures the paired survival window and blocks release on a shorter unapproved lifetime (R3). |
  | Timeouts and transport | Thrift request/retry controls | SEA SDK and linked HTTP pools differ | Warehouse SEA retry, pool, cancel and timeout policy | Warehouse precedent; API and link tuning have separate scopes. Measure latency, retry exhaustion, throughput and heap (R14/R17). |
  | SHOW metadata | Native RPC | SEA SHOW and native-result conversion | Inline Arrow IPC or HIVE per response | Warehouse precedent for SHOW; parity for JDBC rows except the documented cases below (R5/R13). |
  | Null-catalog tables | Thrift may return rows | SEA returns typed empty on `42601` | Typed empty, no fan-out | Warehouse precedent; callers may miss Thrift rows and must name a catalog (AC105). |
  | Exported keys | Parent-only RPC returns no rows on the documented DBR metadata service | SEA issues no request and returns typed empty | Typed empty, no request | Parity for rows and JDBC shape on the documented service; SEA avoids an RPC whose parent-only response is empty (AC54). |
  | Metadata headers and procedures | Native RPC, no SEA header | SEA native/sync headers and ordinary procedure SELECT | APC SHOW forwards headers; procedure SELECT omits them | Warehouse precedent where supported; APC header omission is a server-limited exception with the same typed rows (R5). |
  | Async metadata handles | Synchronous JDBC methods | Synchronous JDBC methods | Optional typed handles; JDBC methods stay synchronous | Additive API; await/cancel preserve synchronous results and errors (R5). |
  | Scalar and complex bindings | Scalar and complex values | SEA scalar binding rejects unsupported types | V8 scalars; complex binding fails before request | Warehouse precedent where warehouse SEA rejects the same types; otherwise exception due APC V8 wire support. Callers must bind supported scalars or use SQL casts; AC29 records exact delta (R6). |
  | Batch counts | Ordered per-set counts | Warehouse native SEA reads per-set `num_affected_rows` (and repeat counts) | APC V10 aggregate count becomes `SUCCESS_NO_INFO` per successful set | Exception: APC V10 response lacks per-set counts. Callers lose per-row update counts; legacy APC batches retain them (R6/AC17/32). |
  | `ThriftMaxBatchesInMemory` | Bounds queued Arrow batches | Warehouse bounded inline SEA bounds response chunks; a chunk may hold several batches | Bounds queued inline response chunks, minimum two | Warehouse SEA precedent, but changed APC tuning unit: memory can exceed the old batch-based expectation. AC109 measures chunk, batch, byte, and heap bounds. |
  | `EnableStreamingChunkProvider` | Selects a linked streaming provider | Warehouse SEA honors it with unbounded API and forces streaming with bounded API | APC SEA always uses bounded linked streaming | Exception: APC continuation cannot use legacy total-count behavior. At `0` with unbounded API, first-row latency, heap, and link-expiry risk may change (R17/AC39). |
  | `EnableInlineStreaming` | Changes inline prefetch and memory | Warehouse SEA behavior varies by bounded mode | Ignored for APC inline results | Exception: APC continuation has no reliable totals for the legacy path. At `0`, first-row latency and heap may differ; AC39 measures both deltas (R17). |
  | SQL size, errors, telemetry | Deployed APC boundary and Thrift errors/events | Warehouse SEA validator and errors/events | Intended 16 MiB UTF-8; SEA mapping | Warehouse precedent for errors, exception if APC boundary differs. AC87/85/89 record caller impact (R6/R8). |

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
`EnableSQLExecHybridResults=0` cannot force external-only APC results, and
`UseBoundedSeaApi` changes neither APC request nor session behavior. Positive
`Statement.setMaxRows` sends `row_limit` and caps rows across pages/links;
zero is unlimited.

Successful no-result DDL, `SET AUTOCOMMIT = TRUE/FALSE`, `COMMIT`, and `ROLLBACK`
have either no terminal manifest or a manifest without a schema, chunks, links,
continuation, or result data. A zero-column schema is not a no-result shape.
They expose no ResultSet
and update count zero, then `-1` after `getMoreResults()`. This holds for both direct
`ExecuteStatement` and polled terminal `GetStatementResult` completion; no fetch is issued.
Result-producing SQL, including `SELECT`, `SHOW`, `VALUES`, the unassigned
`SET AUTOCOMMIT` state query, and `NonRowcountQueryPrefixes` matches,
requires a result schema before public success. A terminal manifest with an
explicit `total_row_count=0`, no chunks or continuation, and no result payload
is a successful empty ResultSet with the declared columns. An absent count is
not zero-row evidence: a manifest-only initial response must fetch its first
chunk, even when its chunk list is empty (R12). If that fetch supplies neither
data nor a terminal empty indication, or fewer rows than an explicit count
arrive, fail rather than report empty.
Counted `INSERT`, `UPDATE`,
`DELETE`, and `MERGE` require a `num_affected_rows` schema column and value,
including zero; missing count data is a contract error. A matching
`NonRowcountQueryPrefixes` command instead exposes the count ResultSet and update
count `-1`, and stops a Statement batch with prior counts and the existing error.
Direct and polled command live gates depend on T2.

APC advertises LZ4 decompression capability. `QueryResultCompressionType` and
its higher-precedence `EnableQueryResultLZ4Compression` alias may set the request
codec, but Reyder discards it; decode inline and linked data from the manifest's
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
SEA must forward both the metadata-operation and fully-sync headers to Reyder. Reyder honors
the fully-sync header for synchronous execution and omits that mode for async requests.
T3 proves `GET_CATALOGS` native dispatch and native-marked Arrow IPC on the
production route; T4–T7 extend this to six more SHOW operations and HIVE
`columnar_data`. All metadata is inline and link-free despite the requested
hybrid disposition. T7/AC142 and client HIVE conversion gate the first public
SHOW; AC16 gates both formats for every subsequent method.
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
interruption fails the whole call rather than returning partial rows (AC83/124/127).

- R6: CreateSession `negotiated_protocol_version` alone grants native scalar
parameters at V8 (42248) and opt-in `EnableNativeBatching=1` INSERT batches
at V10 (42250). It does not gate R5 async metadata. Reyder leaves
`session_version` unset; serialized IDs and missing versions grant no capability.
Retain the negotiated version for later requests. Reject complex bindings
before any single or batch request with a tested driver-owned error name/code;
`toString()` interpolation is not lossless SQL. At `supportManyParameters=0`,
V8+ uses native scalar `parameters` and older/versionless sessions interpolate
supported scalars without unresolved markers; V8–V9 legacy batch sets may use
native scalars. At `supportManyParameters=1`, single and legacy batch sets
interpolate at every version and non-null `setBytes` is permitted; at `0`,
non-null `setBytes` remains rejected. Keep missing-version telemetry distinct.

Native V10 sends ordered nested `parameter_sets`, no top-level `parameters`.
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
The saved-ID close slice makes public `getConnectionId()` return an ID encoding the selected protocol and
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
At `UseBoundedSeaApi=0`, warehouse SEA changes linked-result behavior when this switch
changes, whereas APC keeps bounded streaming. This exception may
change first-row latency, retained memory, and presigned-link expiry risk;
AC39 compares both routes and switch values before linked results are exposed.
Unlike APC Thrift, APC SEA ignores `EnableInlineStreaming` for inline result
buffering. Callers may
observe different first-row latency and retained heap when migrating at `0`; AC39
measures and dispositions that difference.
`ThriftMaxBatchesInMemory` bounds queued APC inline response chunks (minimum two),
whereas APC Thrift bounds batches. A chunk can contain several batches, so the
same setting can use more memory; paging must avoid materializing the full result
(AC109).
Case-insensitive `cloudFetchThreadPoolSize` (default 16) bounds per-result linked
download workers and buffered chunks; `LinkPrefetchWindow` (default 128) bounds
fetched-ahead links, not workers, chunks, or inline pages. `ChunkReadyTimeoutSeconds`
bounds linked-chunk wait. `RowsFetchedPerBlock` does not tune SEA paging.
Thrift HTTP retry and connection-acquisition settings tune presigned-link downloads,
not SEA SDK API requests. `IdleHttpConnectionExpiry` applies to idle connections
in both the SDK API pool and the linked-download HTTP pool. Match warehouse SEA 429/503 and `Retry-After` handling by request class without
new write retries. `ApiRetriableHttpCodes` adds response codes to the existing
linked-download HTTP retry handler, including retries without `Retry-After`;
`ApiRetryTimeout` caps accumulated delay for those configured codes, including
429/503 when listed instead of using their ordinary retry budgets. The SEA SDK
API transport does not use these two link-handler settings. Preserve caller-visible
delay and exhausted-retry error identity (AC51). No APC-only concurrency cap or
full-result buffer is allowed (AC30/46/51/84).
- R18: Every approved JDBC slice exposes its completed public behavior when R1's default-off
APC flag is enabled or `UseThriftClient=0` is explicit. Incomplete public calls, including
`executeAsync`/`getExecutionResult` before T29, fail before network execution when known;
unsupported response shapes fail before false success. They never
silently switch to Thrift after SEA selection. After T1 deploy, a scoped staging
assignment may enable flag-on validation; production assignment or ramp waits for T27/T-val
on the assembled approved stack and a post-merge smoke check. Remove each guard when its
slice passes live staging, and keep ordinary flag-off APC connections on Thrift.
T9 exposes connection open and original-target session close while guarding
statement execution. T10 exposes primitive SELECT, original-target statement close,
and saved-ID session close. Both guard incomplete
statement workflows before network execution.
The download-control slice adds T11's download-disabled session setting after AC119; before
that slice, download-disabled APC SEA is guarded. Nondefault row-limit or query-timeout settings, `EnableDirectResults=0`,
`EnableSeaResponseCompression=1`, and SQL-backed `isValid` are guarded before
the affected connection or statement request until their owning slices pass
their live gates.
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
use a staging fault hook that preserves the request and records the response mutation;
if unavailable, fixtures close only that fault branch. Ordinary and server-dependent
gates require live deployment and proof. Retired AC IDs
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
- AC2: GIVEN flag-on APC staging WHEN a cluster-path connection runs single-chunk and zero-row SELECTs with `UseBoundedSeaApi` and `EnableSQLExecHybridResults` each at `0`/`1` THEN live captures show APC-only CreateSession targeting and the same hybrid Arrow disposition at all four settings; rows, types, empties, and direct close match paired routes. An absent zero count fetches chunk zero; missing expected rows fail before false success. Fixtures cover absent `arrow_schema` and unknown formats.
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
- AC16: GIVEN each synchronous SHOW method at first exposure after AC142 WHEN live direct and polled APC SEA returns nonempty Arrow IPC and HIVE responses THEN inline, link-free JDBC rows, columns, nulls, and errors match paired routes; typed empties retain their shape. Both live formats are required for each method; R13 mask edge cases use fixtures.
- AC17: GIVEN live V10 APC and paired routes WHEN native prepared INSERT batches succeed or fail THEN ordered `parameter_sets` yield `SUCCESS_NO_INFO` per success or `EXECUTE_FAILED` for every set in a `BatchUpdateException` on execution failure; commit status is uncertain. A post-write count-response fault raises a distinct result error, clears the batch, and warns that rows may be committed. Apply the shared fault-hook and load rules.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. Compare elapsed time, SQLState/vendor code, target, and
post-timeout connection usability on flag-ON staging.
- AC22: GIVEN a live Statement INSERT batch with a failing middle command WHEN APC SEA
executes it THEN requests are sequential without `statements`, counts retain order, execution
stops on failure, and errors match APC Thrift. AC32/64/66 cover load and settings.
- AC23: GIVEN live and captured versionless CreateSession responses WHEN APC connects THEN
only `negotiated_protocol_version` grants R6 capabilities; neither `session_version` nor a
saved ID does. Record the live version and missing-version diagnostic.
- AC25: GIVEN new APC and warehouse SEA/Thrift IDs with the original endpoint and credentials WHEN `Driver.closeConnection` uses an ID after a flag or target change at one and 32 callers THEN only the original session closes without an APC Thrift handle; handles/threads return to baseline and close latency/errors pass the shared paired gate. Legacy IDs decode, malformed IDs fail without logging identifiers, statement IDs stay raw, and prechange warehouse Thrift IDs may require reopen. Saved IDs carry no statement identity; live statement close/cancel remain under AC3/107.
- AC27: GIVEN a live single-chunk APC query with three or more RUNNING polls WHEN transfer
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
- AC50: GIVEN each typed SHOW handle at first exposure WHEN live delayed SHOW submits with `wait_timeout=0s` THEN it returns while RUNNING, another call completes before await, and both formats plus typed empties match synchronous and paired routes. Each handle proves timeout, cancel, cleanup, metadata/native headers, and no sync-only header at either `EnableSeaSyncMetadata` value; procedure SELECTs reject async before request.
- AC51: GIVEN live linked downloads and isolated 429/503 or one configured HTTP code, with and without `Retry-After`, WHEN retry settings vary THEN configured codes follow `ApiRetriableHttpCodes`/`ApiRetryTimeout`, SDK API requests keep their own policy, exhausted errors retain their class/SQLState/vendor code, and no write is replayed. Record attempts, delay, paired-route differences, and caller impact; induced faults follow the shared hook rule.
- AC52: GIVEN scoped flag-on staging after AC113 WHEN public JDBC executes no-result DDL, transactions, and counted INSERT/UPDATE/DELETE/MERGE including zero through direct and polled SEA responses THEN wire captures show R4's shape; no-result `execute` is false with no ResultSet or fetch, count zero then `-1` after `getMoreResults()`, while DML counts/rows/errors match paired routes. A zero-column schema or missing `num_affected_rows` fails without breaking the connection; fixtures alone cannot close either path.
- AC53: GIVEN live prepared scalars or an exposed fallback batch plus older/versionless fixtures WHEN `supportManyParameters` is 0 or 1 THEN R6's native/interpolated request choice has no unresolved markers, `setBytes` follows R6, and rows/counts match paired routes. Record fixture-only branches.
- AC54: GIVEN a referenced parent table on the documented DBR metadata service WHEN live `getExportedKeys` runs at both catalog settings THEN APC SEA makes no metadata request and returns the same empty rows and JDBC columns as APC Thrift and warehouse SEA. Capture the Thrift parent-only request and its empty response. Any nonempty deployed Thrift response is a parity gap requiring a measured caller-impact disposition before release. A null table fails before request with SQLState `42000` and vendor code 1003.
- AC55: GIVEN public APC SEA SQL async SELECT WHEN primitive and linked results complete
THEN `wait_timeout=0s`, targeting, rows/errors, cancel, close, and diagnostics follow R2/R10.
Apply AC31's one/32-caller load bounds to this public path before unguarding it.
- AC60: GIVEN a valid live query within its configured client wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Early timeout is a
correctness failure; only a measured latency difference may receive a documented exception.
- AC61: GIVEN a nonempty metadata method or typed handle at first exposure WHEN paired callers run it THEN rows/errors and the shared load gate pass; null-catalog fan-out issues at most N+2 requests for N eligible catalogs. For equal decoded cells, JDBC CPU per 1,000 cells is at most 110% and peak RSS at most 120% of a minimal Java 11 direct-SEA probe with the same SDK HTTP and Arrow libraries. Typed empties do not replace nonempty live proof.
- AC63: GIVEN valid delayed SHOW within its configured metadata wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Early timeout is a
correctness failure; only a measured latency difference may receive a documented exception.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching INSERT runs through public Statement THEN `execute` returns true, its ResultSet contains the affected-row value, and `getUpdateCount` is `-1`.
- AC65: GIVEN each synchronous APC SHOW at first exposure WHEN native, sync, hybrid, and bounded settings vary THEN headers, `ARROW_STREAM`, and disposition follow R5; rows match paired routes. The warehouse unset-header difference does not excuse a row difference.
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
- AC101: GIVEN an exposed live inline or linked APC result mode WHEN codec settings request `NONE`, `LZ4_FRAME`, or invalid text THEN decoding follows the manifest codec, including unmarked data and LZ4 despite `NONE`, while invalid text keeps the warehouse default. Rows and paired load pass for that mode; record requested/actual codec, transfer bytes, decompression CPU, and the `NONE` caller impact. Release repeats both modes.
- AC102: GIVEN an isolated live write table WHEN transaction and auto-commit settings vary THEN R19's state query, committed/rolled-back rows, counts, and errors match paired routes. Public assignments/commit/rollback have no ResultSet or fetch, count zero then `-1` after `getMoreResults()`; missing state data or failed commands cannot succeed.
- AC105: GIVEN live null-catalog `getTables` receiving `42601` WHEN synchronous APC SEA, APC Thrift, and warehouse SEA calls are compared THEN APC SEA and warehouse SEA return the same typed empty result without catalog fan-out; record omitted APC Thrift rows and caller impact.
- AC106: GIVEN a changed driver-owned APC SEA error WHEN it reaches JDBC THEN a test asserts its `DatabricksDriverErrorCode` name and unique number, and the PR records owner-verified telemetry taxonomy/dashboard updates or maintainer handoff and completes the telemetry-error template. Server errors retain server identity.
- AC107: GIVEN paired one- and 32-caller staging WHEN the first SELECT slice repeatedly opens, queries, leaves an operation open, and closes THEN original-target CloseStatement precedes TerminateSession, no live resources remain, handles/threads return to baseline, retained heap growth is at most 8 MiB, and median cycle latency is at most 1.10 times APC Thrift. Release repeats the assembled mix under the shared exception rule.
- AC108: GIVEN AC87 passed and live legacy APC INSERT batches WHEN batching and binding settings vary THEN R6 chunk limits, per-set counts, committed rows, errors, and AC32 paired load pass for each binding mode/version. Oversized chunks follow AC132.
- AC109: GIVEN concurrent live multi-page inline SELECTs WHEN `ThriftMaxBatchesInMemory` varies around two THEN queued response chunks obey R17 despite multiple batches per chunk, each row arrives once, and retained result memory and AC31 load bounds hold. Record peak queued chunks, Arrow batches, bytes, and heap against APC Thrift's batch-based setting.
- AC112: GIVEN AC119 passed and live single-chunk and multi-page APC SELECTs with download disabled at either bounded value WHEN rows are drained THEN CreateSession sends `session_confs["can_cloud_download"]="false"`, all chunks remain inline and appear once. Initial size failure prevents ResultSet success; later-page failure propagates during iteration without reporting normal completion.
- AC113: GIVEN T2 deployed on scoped APC staging WHEN direct and polled SEA calls complete no-result DDL, transaction commands, and counted DML including zero THEN captures show R4's no-result shape without a zero-column schema or fetch, and DML's `num_affected_rows` schema/value. Failed commands retain errors. This server-only gate precedes JDBC AC52; fixtures cannot close it.
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
- AC136: GIVEN a live flag-on APC SELECT with `EnableHeartbeat=1` WHEN its partly read ResultSet stays idle across at least two heartbeat intervals and the server's configured idle-result expiry window alongside a paired APC Thrift cursor THEN zero APC `/status` requests occur and both cursors resume, drain, and close with identical rows. Record the observed survival duration and expiry setting; a shorter APC SEA lifetime needs the R3 exception approval.
- AC138: GIVEN T3 deployed on scoped APC staging WHEN direct and polled `GET_CATALOGS` reach Reyder THEN SEA forwards metadata-operation and fully-sync headers, Reyder honors sync mode, accepts `ARROW_STREAM`, dispatches native RPC, and returns nonempty native-marked Arrow IPC inline without links. Capture sync-on and async-without-sync-header requests; fixtures cannot close this live gate.
- AC139: GIVEN AC138 and T4 deployed WHEN live direct and polled `GET_SCHEMAS`/`GET_TABLES` run THEN each passes AC138's wire gate.
- AC140: GIVEN AC139 and T5 deployed WHEN live direct and polled `GET_COLUMNS`/`GET_FUNCTIONS` run THEN each passes AC138's wire gate.
- AC141: GIVEN AC140 and T6 deployed WHEN live direct and polled `GET_PRIMARY_KEYS`/`GET_CROSS_REFERENCE` run THEN each passes AC138's wire gate.
- AC142: GIVEN T7 deployed after AC141 WHEN all seven SHOW operations return HIVE `columnar_data` on scoped APC staging THEN each has nonempty native-marked, inline, link-free direct and polled samples.
- AC143: GIVEN the first JDBC session slice after the APC flag deploys WHEN live flag-ON and flag-OFF cluster-path connections open and close without a statement THEN R1 route precedence, APC-only CreateSession and original-target TerminateSession, redacted route diagnostics, failure propagation, partner exclusion, and warehouse request invariance hold; incomplete statement methods fail before request. A server rejection leaves the close gate open.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The last JDBC package alone owns `docs/`
  and `NEXT_CHANGELOG.md` and records the complete gated feature. Earlier stacked PRs use
  `NO_CHANGELOG=true` only as internal implementation slices of that same unreleased feature:
  no driver release or production APC assignment may occur between slices. This basis must be
  stated in each PR; if a release is cut early, its visible slice needs a changelog update
  before release. Completed slices remain shippable behind the flag, with no partial ramp.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T1–T8; known gaps SC-256188 under SC-256186
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

`plan.json` owns each task once and names single-parent PR ancestry. JDBC
packages are live, flag-gated vertical slices in a linear stack so the release
package contains all approved changes. The 11 JDBC packages exceed the preferred
4–7 count because separating linked results, prepared SELECT, transport, SQL
lifecycle, commands, transactions, batches, and metadata keeps each review near
the 1,500-line cap. Universe branches may run in parallel where they edit disjoint
areas; deployed wire gates connect them to JDBC. T1 gates flag-on staging,
T2/AC113 commands, T7/AC142 SHOW formats, T8/AC119 download-off, and AC87
size-sensitive batches. The tracked termination binding fix gates close proof.
Only the release package owns docs/changelog and production assignment.

### Affected Components

JDBC connection, statement, prepared-statement, metadata, SEA transport, and
telemetry; the named Reyder and connector-service contracts.

### Data Model Changes

Session state carries the APC target and negotiated capability version.

### API Changes

Existing JDBC methods retain their contract. The session-reachable async metadata
API covers seven SHOW operations; public connection IDs may encode APC SEA.

### Risks

Server deployment gates metadata and termination; AC13/Q7 block lost logical
types, AC27 blocks repeated poll payloads, and AC11/14 protect warehouse and
presigned-link boundaries.

---

## Tasks

Acceptance criteria define each task's live gate; `plan.json` owns package scope and ancestry.

- T1–T8: server flag, command wire, seven SHOW wire operations, and inline-only session contract, in the order defined by AC28/34/113/138–142/119.
- T9–T12: APC session, primitive SELECT and saved close, inline continuation, and linked results.
- T13: transport/retry/download controls; T14: no-result and counted commands; T15: legacy and V10 prepared batches.
- T16–T19: synchronous catalog, schema/table, column/function, procedure, and key metadata.
- T20–T26: typed async metadata handles, from catalog through cross-reference.
- T27: sole docs/changelog and production-assignment gate; T28: prepared SELECT and pre-execution metadata; T29: SQL async, timeout/cancel, and SQL-backed `isValid`; T30: transactions; T31: Statement batches.
- T-val: assembled live and release evidence.
