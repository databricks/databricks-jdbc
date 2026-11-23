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

- In universe `cmdexec/sql-exec-api`, `SqlExecBackendRouter` routes APC
  `backend_compute` to Reyden behind `databricks.sqlexecapi.enable_apc_reyder_routing`.
  Reyder is the SEA service; Reyden is its runtime backend. APC lacks
  `GetStatementStatus`, so polling uses `GetStatementResult`. The current APC
  router calls Reyder outside `ApiFiltering`: its Scala SQL-length check and
  require-native normalization do not run. Metadata-operation and fully-sync
  headers reach SEA; the require-native header is not carried into APC native
  selection, and Reyder's downstream header allowlist excludes require-native
  and fully-sync. Server behavior at each hop needs a deployed gate (AC138).
- [SC-256188](https://databricks.atlassian.net/browse/SC-256188) under
  [SC-256186](https://databricks.atlassian.net/browse/SC-256186) tracks native
  metadata shape. [SC-258842](https://databricks.atlassian.net/browse/SC-258842)
  tracks terminal DDL; [SC-258787](https://databricks.atlassian.net/browse/SC-258787)
  tracks the CreateSession inline-only override. SC-254478 covers per-statement
  config, not that override. Termination binding remains a separate server gate.
- Connector-service returns sub-flags at
  `/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`;
  `FeatureFlagsHandler.scala` may allowlist JDBC partners, so Q9 requires an
  APC-wide exclusion. The dedicated APC flag is absent in the inspected
  connector-service source; T56/T57 track its definition and exclusion under
  [SC-256186](https://databricks.atlassian.net/browse/SC-256186).
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
  (API contract, Phase II, metadata). AC87 verifies the deployed SQL-size
  boundary; server rollout begins opt-in and rejects non-OSS-JDBC clients.
- Driver baseline: `origin/main` @ `840f3a407` (spec #1809; bounded SEA default
  #1827). APC uses Thrift; warehouse SEA uses warehouse paths, `FAST` sessions,
  `/status` heartbeats, and `ARROW_STREAM`/`JSON_ARRAY` without a HIVE
  `columnar_data` decoder. `SessionId.create()` assumes a Thrift APC handle;
  public statement IDs have no target. Ordinary session close suppresses delete
  failure, while `Driver.closeConnection()` propagates it. APC Reyder has no
  session version and validates SQL bytes on its direct route (R6). The current
  `CreateSessionResponse` model reads `session_id` and `session_version`, but not
  `negotiated_protocol_version`; T28 adds that capability field.
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the
  prototype baseline; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756) is unchanged.
- Parity uses one driver build on all routes. The shared gate below governs
  differences and exception approval.

  | Behavior | APC Thrift | Warehouse SEA on `origin/main` | APC SEA; reason and caller impact | Disposition |
  | --- | --- | --- | --- | --- |
  | Arrow unavailable | Thrift works | Explicit SEA may precede Arrow check | Select Thrift: APC needs Arrow batches, so explicit SEA cannot be honored. | APC format exception, AC1. |
  | Legacy `cliservice` URL | Thrift with `default` target | Concrete warehouse target | Stay on Thrift even with explicit SEA; no concrete APC ID exists. | Target exception, AC1. |
  | Close and saved IDs | Ordinary close suppresses delete failure; saved close propagates | Same, with warehouse ID | Connection and SELECT slices reject public saved-ID requests with `UNSUPPORTED_OPERATION` (1005); T44 restores saved close after inline results. | Temporary opt-in boundary, AC25/152/166. |
  | Paging | Blocking Thrift fetch | SEA continuation | Consume continuation without duplicate rows. | Follow SEA, AC14/31. |
  | Result codec | Honors requested codec | Forwards codec preference | Reyder sends LZ4 even for `NONE`; decode actual manifest codec. `NONE` cannot disable compression. | Server exception, AC101. |
  | Result tuning | `RowsFetchedPerBlock` tunes fetch; `ThriftMaxBatchesInMemory` limits batches | SEA paging ignores the former and bounds inline chunks with the latter | Follow SEA; multi-batch chunks can use more memory. API compression affects API bytes, not link bytes; inline streaming switch has no SEA Arrow-reader effect. | Warehouse precedent, AC31/45/109/165. |
  | Download/complex getters | Inline or linked typed values | Hybrid `0` external-only; download-off JSON or Arrow | Hybrid `0` remains hybrid (Reyder limit); download-off uses Arrow batches and representable typed getters. | Transfer exception; getter parity required, AC13/112/145. |
  | `UseBoundedSeaApi=0/1` | No SEA setting | Download-off format and linked provider vary | APC keeps `ARROW_BATCH`, hybrid targeting, and inline-only session fields at both values. Absent link totals require bounded continuation; transfer cost can differ. | Reyder format/total-count exception, AC39/112/145/165. |
  | INSERT tuning | Eligible combined batches | Legacy plus optional V10 sets | `EnableBatchedInserts` controls legacy only; `BatchInsertSize` bounds interpolated rows, not native-scalar chunks. | Warehouse precedent, AC17/108. |
  | Pools, retries and link diagnostics | Thrift HTTP controls | SDK API/link controls split | Follow SEA split; link settings do not govern SDK attempts; linked speed threshold affects diagnostics, not rows. | Warehouse precedent, AC30/46/51/84/165. |
  | Idle cursor | Thrift heartbeat | SEA `/status` heartbeat | Disable APC heartbeat: status RPC unsupported; shorter lifetime needs approval. | Server exception, AC136. |
  | Native metadata transport | Native RPC | Arrow/JSON, no SEA HIVE decoder | Decode Arrow/HIVE SHOW and preserve typed rows/nulls. | New decoder, AC16/61. |
  | Null-catalog `getTables` on `42601` | Thrift native RPC can return rows from multiple catalogs | SHOW returns typed empty without fan-out | Return typed empty; callers lose the Thrift rows from affected catalogs, including matching table types. Measure omitted rows before approval. | Warehouse precedent, AC105. |
  | Null-catalog `getFunctions` with SHOW switch | Native RPC can return functions; Thrift SHOW returns typed empty when catalog is null | SHOW resolves current catalog and can return its functions | Always use SHOW; callers with Thrift SHOW enabled gain current-catalog rows, while native-RPC callers can lose functions outside that catalog. Measure both deltas. | Warehouse precedent, AC82. |
  | Exported keys | Parent-only native RPC returns rows | Typed empty; no parent-only SHOW | Deliver rows through a supported parent-only lookup or hold release for a measured, approved server-limit exception; otherwise callers lose foreign-key relationships. | Server gap, AC54. |
  | Unset native-metadata setting | Thrift RPC/SHOW choice | Server flag decides | Require native on supported APC SHOW even when unset; response shape can differ. | APC contract exception, AC65. |
  | Partial fan-out | Compare live errors/rows | Omits failed schema/column catalog | Omit only failed catalog and keep connection usable; caller loses those rows. | Warehouse precedent, AC83. |
  | Procedure/function metadata | Query errors; RPC/SHOW switch | Query errors; SHOW | Propagate procedure errors and use SHOW for functions. | Warehouse precedent, AC75/82. |
  | Assignment `execute` | `SET` classifier exposes ResultSet | Same classifier | No-result assignment returns false; caller boolean changes. | Terminal-wire exception, AC52. |
  | V10 batch counts | Per-set counts | Native bindings | Reyder aggregate count yields `SUCCESS_NO_INFO` per successful set; exact counts lost. | Server exception, AC17. |
  | Session version | Thrift handle | `FAST` and version | `DEFAULT`, no absent version echo; capability handling differs. | Server contract, AC23. |
  | SQL text limit | Measure live boundary | Scala `ApiFiltering` checks `String.length` (UTF-16 units) on its route; Reyder also limits UTF-8 bytes | The current APC router bypasses Scala filtering, so Reyder's configured UTF-8 byte limit is the APC gate. Requests above Scala's threshold reach Reyder and may fail there; non-ASCII text may fail below that threshold. No split or fallback. | APC route exception; size/error impact, AC87/132/164. |
  | Intermediate explicit SEA | APC Thrift SQL, metadata and saved IDs work | Warehouse SEA equivalents work | Each JDBC package opens only its gated methods; other explicit-SEA calls reject before request until their owner lands. | Temporary opt-in boundary, AC40/151/166. |
  | `EnableStreamingChunkProvider` | Opt-in linked streaming | Opt-in, or forced by bounded SEA | Honor `1`; with absent APC link totals, use bounded streaming even at `0` so rows are not lost. `0` may therefore have different buffering and latency from APC Thrift. | APC total-count exception, AC39/165. |
  | Timeouts/accepted writes | Thrift waits | SDK timeout, JDBC waits, link wait | Follow SEA timing; an accepted write can have an ambiguous response. | Warehouse precedent, AC18/51/60/63/84/154. |
  | Error mapping | Thrift class/SQLState/vendor code | SEA mapping | Preserve fields that agree; otherwise follow SEA. Callers matching changed fields can break. | Map disagreement and approve impact, AC89. |
  | Telemetry | Thrift event families | SEA event families | Emit route, compute and error identity with redaction under existing toggles. | SEA precedent, AC10/85/106. |

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
normal SEA SHOW-command semantics (AC148). This applies to HTTP paths with a concrete
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

The dotted field is `backend_compute.all_purpose_cluster_compute.cluster_id`.
GET and CloseStatement DELETE have no body; polls omit `chunk_index` and chunk
fetches require it. JDBC `Connection.close()` attempts TerminateSession and
remains best effort on rejection, with local cleanup and a redacted warning;
after T44, `Driver.closeConnection()` propagates a rejected TerminateSession
for the saved ID. Presigned links and standard/Real-time warehouse serialization
stay unchanged; live dogfood termination requires the tracked server binding fix.

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
once the deployed CreateSession override passes AC119; until then
inline-only gates remain open.
Initial inline-size failure prevents ResultSet success; later fetch failure
interrupts iteration. Hybrid `0` still requests hybrid because Reyder lacks
external-only delivery (AC145). `UseBoundedSeaApi` changes no APC request or
session field. Positive `setMaxRows` sends `row_limit`; zero is unlimited.

Successful no-result DDL, `SET AUTOCOMMIT = TRUE/FALSE`, `COMMIT`, `ROLLBACK`,
and no-result `CALL` have no terminal schema, chunks, links, continuation, or
data (manifest may be absent), distinct from a zero-column schema. Direct and
polled completion expose no
ResultSet or fetch; update count is zero, then `-1` after `getMoreResults()`.
APC assignment `execute` returns `false` despite the existing `SET` classifier;
preserve side effects and measure caller impact under AC52.
Result-producing `SELECT`, `SHOW`, `VALUES`, unassigned `SET AUTOCOMMIT`, and
`NonRowcountQueryPrefixes` matches require a schema. Explicit
`total_row_count=0` without payload or continuation is a typed empty ResultSet.
Absent count requires first-chunk fetch even with an empty list; missing terminal
evidence or declared rows is an error. Counted `INSERT`, `UPDATE`, `DELETE`,
and `MERGE` require a `num_affected_rows` column and value, including zero. A matching
`NonRowcountQueryPrefixes` command exposes that ResultSet and update count `-1`;
in a Statement batch it stops with prior counts and the existing error.

APC advertises LZ4 decompression. `QueryResultCompressionType` and its
higher-precedence `EnableQueryResultLZ4Compression` alias request numeric `0`
(`NONE`) or `1` (`LZ4_FRAME`), but Reyder ignores the request. Decode the
manifest's actual codec; unmarked means uncompressed. `NONE` cannot force
uncompressed APC results, and invalid settings retain the warehouse LZ4 default
(AC101). For failed SQL,
keep SQLException class, SQLState, and vendor code where APC Thrift and warehouse
SEA agree; otherwise follow warehouse SEA and document caller impact (AC89).
- R5: Seven APC native metadata SHOW operations (`GET_CATALOGS`, `GET_SCHEMAS`,
`GET_TABLES`, `GET_COLUMNS`, `GET_FUNCTIONS`, `GET_PRIMARY_KEYS`,
`GET_CROSS_REFERENCE`) send `X-Databricks-Metadata-Operation-Type` with the
corresponding case-sensitive `GetCatalogs`, `GetSchemas`, `GetTables`,
`GetColumns`, `GetFunctions`, `GetPrimaryKeys`, or `GetCrossReference` value.
They request `ARROW_STREAM` and `INLINE_OR_EXTERNAL_LINKS` at either hybrid
setting. JDBC sends metadata-operation and fully-sync headers to SEA for
applicable calls, but the APC direct route bypasses `ApiFiltering`'s
require-native normalization. Before exposure, server work T58 must carry
native-selection intent to Reyder/Reyden and preserve the synchronous behavior
requested by the fully-sync header; forwarding HTTP headers unchanged at every
hop is not required. The current downstream allowlist excludes require-native
and fully-sync, so AC138 proves the effective behavior at each hop.
SHOW is inline/link-free; consume `next_chunk_index` through R12 if present,
though pagination is not promised (AC16/61).
On APC SEA, unset or explicit `EnableThriftNativeMetadata=1` sends
`X-Databricks-Require-Thrift-Native-Metadata: true` for supported SHOW; `0`
omits it. Unset warehouse SEA depends on its server flag, even when an explicit
override selects SEA.
Reyder still returns native-shaped metadata, so conversion follows
`manifest.is_native_metadata_result`, not the request setting. Synchronous
SHOW sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only at
`EnableSeaSyncMetadata=1`; async SHOW omits it. Decode each response as Arrow
IPC `chunk.attachment` or HIVE `chunk.columnar_data`/`COLUMN_BASED_SET`.
`getImportedKeys` and `getCrossReference` share `GET_CROSS_REFERENCE`; the latter
filters a foreign table's keys by parent. A null foreign table returns typed
empty before request; null/empty parent or empty foreign table is rejected.
Warehouse error behavior applies: catalog/function/primary-key syntax errors
propagate; null-catalog schema/column syntax errors fan out, while table syntax
errors return typed empty without fan-out; imported/cross-reference syntax
errors return typed empty. Object-not-found is typed empty except for catalog
errors. Other SQL failures propagate except existing table/column empty-pattern
and fan-out behavior (AC16/83/105/114).

`enableMultipleCatalogSupport=0` retains current-catalog filtering; `1`
permits eligible catalogs. `EnableMetricViewMetadata=1` adds
`spark.sql.thriftserver.metadata.metricview.enabled=true` to session config
and exposes `METRIC_VIEW` table types and eligible rows; `0` omits both.
`getExportedKeys` has no parent-only native SHOW. On `origin/main`, APC Thrift's
parent-only `TGetCrossReferenceReq` returns rows; warehouse SEA returns typed
empty. T47 must use a supported parent-only lookup for typed rows or record
measured loss, server limit, warehouse precedent, caller impact, and PECO
approval. An unapproved empty result cannot close the keys package or release.
Catalog filtering at `enableMultipleCatalogSupport=0` may first run
`SELECT CURRENT_CATALOG()`; at `1` it needs no lookup. A null table fails before
request with SQLState `42000` and `EXECUTE_STATEMENT_FAILED` (1003).
Session-reachable async metadata provides typed ResultSet handles for seven
SHOW operations, with separate imported/cross-reference calls. It submits at
`wait_timeout=0s`, returns while RUNNING, and preserves await, timeout, cancel,
and conversion. Version/saved IDs do not gate it; public JDBC metadata stays synchronous.

Internal catalog/schema and parameterized procedure SELECTs use hybrid
`ARROW_BATCH` without metadata headers; Reyder rejects procedure headers.
Procedure queries use R6 binding and propagate query/fetch/decode errors;
only the existing catalog-access filter returns typed empty (AC75).
`EnableShowCommandForGetFunctions` still selects native RPC or SHOW
on APC Thrift, whose SHOW path returns typed empty for null catalog; APC SEA,
like warehouse SEA, uses SHOW at either setting (AC82).

Async null-catalog schema/column `42601` fans out without sync headers.
Per-catalog `SQLException` warns and omits that catalog; timeout,
interruption, or initial/enumeration error fails the call. Partial results
leave the connection usable (AC83).
Positive `MetadataOperationTimeout` bounds call/await; `0` removes only that
deadline. Per-catalog collection retains its 90-second bound regardless of
earlier catalog completions (AC83/124/161).

- R6: APC CreateSession explicitly sends `execution_mode=DEFAULT` and leaves
`session_version` absent on later ExecuteStatement requests, matching Reyder's
absent version. `FAST` requires an initial version and echoes the maximum
observed; warehouse FAST stays unchanged. AC23 captures mode/version requests;
APC FAST without a version fails. Only CreateSession's
`negotiated_protocol_version` grants native scalar parameters at V8 (42248)
and opt-in `EnableNativeBatching=1` INSERT batches at V10 (42250); it does not
gate async metadata. Retain that version despite absent `session_version`;
saved IDs grant no capability. Reject complex bindings before request with a
tested driver error, since `toString()` loses SQL type. With
`supportManyParameters=0`, V8+ uses native scalar `parameters`; missing/pre-V8
interpolates supported scalars without unresolved markers, and V8–V9 legacy
batches may use native scalars. At `supportManyParameters=1`, single/legacy sets
always interpolate and permit non-null `setBytes`; at `0`, non-null `setBytes`
is rejected. Distinguish missing-version telemetry.

Opt-in V10 INSERT batches send ordered `parameter_sets`, no top-level
`parameters`, and return `Statement.SUCCESS_NO_INFO` per successful set because
Reyder provides only an aggregate count. On execution failure,
`BatchUpdateException` marks every set `Statement.EXECUTE_FAILED`, with uncertain
commit and no atomic rollback claim. If a successful write's counts cannot be
read, raise a distinct result error without failed-write counts, report possible
commit, and clear the batch to prevent duplicate retry. Below V10 or native-off,
`EnableBatchedInserts=1` combines eligible INSERT sets. For interpolated sets
(`supportManyParameters=1` at every version, or `0` before V8), `BatchInsertSize`
bounds rows per chunk; for native-scalar legacy sets (`supportManyParameters=0`
at V8–V9 or V10 with native batching off), the existing 256-parameter limit
bounds rows per chunk and `BatchInsertSize` has no effect. Ineligible sets
execute individually. Preserve ordered counts, stop-on-error, and the existing
uncertain-commit behavior when a prior chunk may have succeeded.

APC bypasses Scala `ApiFiltering`'s default 16,777,216-UTF-16-unit
`String.length` check; Reyder's configured UTF-8-byte limit (default
16,777,216) governs APC. Warehouse requests retain their Scala route. AC164
verifies byte rejection and bypass; a non-BMP point counts as two UTF-16 units
and four UTF-8 bytes. The driver never splits/falls back and propagates the
size error. AC87 gates dependent batches against APC Thrift/warehouse evidence.
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
families on each public APC path, with protocol and compute identity for correlation.
AC85 compares event fields and errors at exposure. Diagnostics and telemetry redact
credentials, org/cluster IDs, and compound session/statement IDs, including those
inside messages; raw targets never appear.
- R9: The connection slice attempts live APC SEA session termination
  and verifies server termination once the tracked binding fix is deployed; its
  `Connection.close()` remains best effort on rejection and closes an open statement
  before session termination. Until T44, public `getConnectionId()` on APC SEA
  raises `UNSUPPORTED_OPERATION` (1005) with a redacted explanation before
  serialization or another request; it never returns a Thrift-shaped or invalid
  ID. The connection slice retains the original target internally for ordinary close.
  The separate saved-close slice makes public
  `getConnectionId()` return an APC SEA ID with protocol and target, allowing
  `Driver.closeConnection` to terminate the original
  session after flag/target changes when endpoint and credentials stay the same.
  IDs contain neither endpoint nor credentials. Public statement IDs stay raw;
  recovered statements use the current connection target. Internal IDs retain the
  target and reject malformed values without logging them. Legacy warehouse
  SEA/Thrift IDs still decode, except `origin/main` warehouse Thrift IDs wrongly
  encoded as SEA cannot support saved close because they lack a secret. Only
  the live CreateSession `negotiated_protocol_version` grants parameter capabilities.
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
For inline `ARROW_BATCH`, enabled typed complex getters work when the value is
representable by the reconstructed logical type, as they do for warehouse Arrow
results; disabled support retains the established rejection. A getter exception
requires a demonstrated wire or type limit, the warehouse Arrow comparison, and
measured caller impact before release (AC13/112).
Download-off APC `ARROW_BATCH` and its transfer mode differ from warehouse SEA's
bounded-dependent `JSON_ARRAY`/`ARROW_STREAM` requests; AC112 measures caller impact
and the R17 load bounds at both settings.
- R12: Page APC results by `next_chunk_index` and `row_offset`, without requiring totals or
inline data in the initial response. When the initial manifest has no payload and
no explicit zero-row count, fetch chunk zero even if its chunk list is empty.
An empty fetched chunk is terminal zero-row evidence only with no continuation.
The next inline offset includes every batch in a chunk;
for linked results it follows the final link's end offset and continuation. Missing final-link
`next_chunk_index` ends paging.
- R13: Decode APC SEA `columnar_data` physical vectors and null masks using
`manifest.schema`; JDBC columns, values, and nulls match APC Thrift metadata.
Masks use least-significant-bit-first order and `1` for null. Null placeholders
consume positions; short/extra mask bits do not change row count. Unknown
vectors fail without partial success (AC157).
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
deserialize safely and fail clearly if decoded, without corrupting later responses
on the connection (AC158).
- R17: APC SEA follows warehouse SEA's retry, pool, and linked-download behavior
at either `UseBoundedSeaApi` value. APC links lack reliable continuation totals,
so linked delivery uses continuation rather than a required total count at both settings.
`EnableStreamingChunkProvider=1` selects streaming linked delivery; with
`UseBoundedSeaApi=1` warehouse SEA already forces it. At unbounded `0` with the
streaming switch `0`, APC also selects bounded streaming if link totals are absent,
instead of the total-dependent legacy provider. `EnableInlineStreaming` selects
Thrift inline readers on `origin/main` and has no effect on warehouse or APC SEA
Arrow readers. `CloudFetchSpeedThreshold` governs slow-link diagnostics on APC
links, not inline results. These switches may change first-row latency, buffered
chunks, and diagnostics but never row values or the R17 memory bounds (AC165).
`ThriftMaxBatchesInMemory` limits APC inline chunks (minimum two), while APC
Thrift limits batches. Since one chunk may contain several batches, compare
memory at the same setting and avoid whole-result buffering (AC109).
Case-insensitive `cloudFetchThreadPoolSize` (default 16) bounds per-result
linked workers and buffered chunks; `LinkPrefetchWindow` (default 128) bounds
fetched-ahead links only. `ChunkReadyTimeoutSeconds` bounds linked-chunk wait;
`RowsFetchedPerBlock` has no SEA paging effect.

Presigned links use the Thrift HTTP pool: ordinary 429/503 retries controlled by
`RateLimitRetry`/`TemporarilyUnavailableRetry` require `Retry-After`, while
`ApiRetriableHttpCodes` uses `ApiRetryTimeout` without that requirement.
`IdleHttpConnectionExpiry` applies to link and SDK API pools. API calls use
warehouse SEA's SDK retry policy, independent of link-handler controls; AC51
gates attempts, delay, final error, and caller impact per request class.

SDK 0.118.0 may retry an accepted 429/503 write POST. Surface warehouse SEA's
ambiguous-response SQLException without APC replay or at-most-once claims; AC151
warns callers to reconcile before resubmitting. No APC-only cap or full-result
buffer is allowed (AC30/46/51/84).
- R18: Each JDBC implementation package exposes a callable vertical slice with the APC flag OFF
by default. Explicit `UseThriftClient=0` opts into only workflows whose owning
package and prerequisites have merged; their live parity gates close before merge.
The Tasks table and `plan.json` name exposure boundaries. All other remote calls/settings
fail with a named driver error before their SEA request; local-only methods make
no extra request. Unsupported server-selected shapes fail before ResultSet
success, close the remote statement, and leave the connection usable. A later
page failure interrupts iteration without truncated success. AC40 verifies the
boundary at every slice; T44 and all parity gates close before default flag ramp.
Default flag-OFF callers stay on Thrift. Complex bindings and R18's unsupported
CALL forms remain rejected after release.

Counted prepared DML preserves
`executeUpdate`/`execute` outcomes, including zero (AC146). CALL with rows
exposes a ResultSet; no-result CALL returns `false` and count zero. OUT/named
parameters and callable `executeUpdate` reject before request; `executeQuery`
on a no-result CALL raises the established result-set-expected error (AC147).
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

- Q1: Reuse `UseThriftClient` or add `EnableApcSea`? A1: Reuse `UseThriftClient` after APC's Arrow prerequisite; it precedes catalog preference and the APC flag. Warehouse routing is unchanged (R1).
- Q2: Fall back after an APC SEA error? A2: No; propagate the SEA error (R7).
- Q3: How should Arrow IPC and HIVE metadata coexist? A3: Decode each response's Arrow IPC or HIVE `chunk.columnar_data`, including DD2/DD4 (R5, R13).
- Q4: Emulate batches below V10? A4: Yes. Legacy INSERT batching follows R6's chunk bounds; only opt-in V10 uses `parameter_sets`. Statement batches remain sequential (R15).
- Q5: Denylist govcloud/non-Reyden regions in the client? A5: No; keep the flag off there and propagate a selected SEA rejection.
- Q6: Add a special User-Agent or capability header? A6: No; ordinary User-Agent, path, and `backend_compute` identify APC.
- Q7: Is manifest `type_text`/`type_json` sufficient for Arrow types? A7: AC13 proves lossless reconstruction; otherwise block release for a Reyder `arrow_schema` contract and driver support. Do not coerce or fall back.
- Q8: Share `enableSqlExecForJdbc`? A8: No. Add default-off `databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc`; older drivers ignore it, and no connector-service version filter is needed.
- Q9: Exclude partners despite Reyder's OSS prefix allowlist? A9: Yes, including JDBC partners on the warehouse flag. Deploy the dedicated APC flag before assignment; partner enablement needs its own decision.
- Q10: Avoid transfer during result polling? A10: At least three RUNNING polls transfer no payload; terminal fetch returns rows once (AC27).
- Q11: Suppress procedure metadata failures? A11: No; propagate query, fetch, and decode SQLState/vendor code. This supersedes the earlier typed-empty-on-error wording.

## Acceptance Criteria

For every newly callable workflow, run scoped flag-ON APC staging and paired
APC Thrift/warehouse SEA before merge. AC40 covers its exposed and rejected
surface; AC11 protects its warehouse serialization; AC85 covers its telemetry;
AC89 covers its callable SQL/metadata
errors, not connection-only slices; AC106 applies in the PR that changes a
driver-owned error. AC144 applies only to the request/fault classes the slice
can issue. AC113/154 gate writes. Use fixtures only for cases staging cannot
produce. T-val checks the assembled stack. These common gates are inherited by
packages and need not be repeated in each `plan.json` acceptance list.

The shared paired-load gate uses one build, warmed cluster, identical data,
and one/32 APC SEA and Thrift callers, all completing or returning bounded
errors. At 32 callers, SEA/Thrift throughput is at least 0.90;
median/p95/slowest latency at most 1.10/1.20/1.50, peak heap at most 1.20.
Inline/linked SELECT CPU seconds/GiB and peak RSS are at most 1.20 at both
loads; record idle and absolute RSS at 8/64 MiB inline and 64/128 MiB linked
per caller. AC31 drains at least 8 MiB per single-chunk caller and 64 MiB per
multi-page/linked caller at 0.90 of Thrift sustained bytes/s. AC32 inserts
1,024 disjoint rows per caller over at least two requests/chunks/sets at 0.90
of Thrift committed rows/s. Retained heap growth across two sizes is at most
max(8 MiB, 5% of added bytes). Record rows, bytes, duration, errors, CPU and
memory. Exceptions need same-build warehouse SEA/Thrift evidence, a server
limit/precedent, caller impact, and PECO/Perf Eng approval before release.

AC138 checks each native SHOW on raw scoped APC staging before its JDBC method
is exposed. Capture the applicable metadata-operation, require-native, and
fully-sync headers at JDBC and SEA ingress; capture the server's derived native
selection and sync decision on the APC route, and verify the resulting Reyden
request/response without assuming all three headers cross the downstream
allowlist. Native mark, inline/link-free rows/nulls, and any continuation must
match R5 under direct and polled execution and both hybrid settings. Arrow IPC
and HIVE `columnar_data` need a repeatable catalog/table null-cell sample.
Exposure adds AC16/50 paired JDBC behavior and AC61 load per method/handle.

- AC1: GIVEN scoped flag-ON/OFF APC staging WHEN R1 overrides and metadata preference vary THEN protocol, target, flag, partner exclusion, and no-fallback match R1 and paired warehouse routing. Fixtures cover Arrow absence, legacy `cliservice`, and lookup failure; AC2 proves rows.
- AC2: GIVEN flag-on/explicit-SEA APC at bounded/hybrid `1` WHEN single-chunk, RUNNING, manifest-only, and empty SELECTs run THEN targeted Arrow matches paired rows/types. A premature link/continuation rejects before ResultSet success with remote cleanup; AC145 gates alternate settings.
- AC3: GIVEN a live APC statement WHEN it polls, fetches, and closes THEN R2 targeting, exactly-once close, connection usability, and unchanged warehouse serialization are captured.
- AC9: GIVEN live APC SEA CreateSession rejection or APC Thrift OpenSession KP001
WHEN connecting on the affected route THEN the SEA error and failed-route event
propagate without fallback, and warehouse-only KP001 recovery never selects SEA for APC.
- AC10: GIVEN successful and failed opens on four APC/warehouse SEA/Thrift routes WHEN DEBUG logs and telemetry are captured THEN compute, protocol, reason, outcome, and error identity appear where enabled; R8 toggles suppress export and secrets/targets/IDs are redacted.
- AC11: GIVEN standard and Real-time warehouse connections WHEN each slice's R2
operation class runs (T9: session open/close) THEN its JDBC outcome and
URL/body/header serialization, including `realtimeSession`, match `origin/main`.
- AC12: GIVEN live synchronous SELECT at both direct-result settings WHEN paired callers drain rows THEN `wait_timeout` is absent or `0s` respectively, no `statements` field appears, rows/errors and AC31 load pass, and first-row latency/request count are recorded.
- AC13: GIVEN R11 primitive/logical values WHEN inline/linked getters run at both complex/geospatial settings THEN types/values match paired routes, representable typed getters work when enabled, and disabled support rejects as before. Exceptions need R11 evidence; unavailable live types stay pending.
- AC14: GIVEN live APC multi-link results with absent totals after T42 controls pass WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN AC138 for a native SHOW operation WHEN its synchronous JDBC method runs THEN rows, types, `wasNull()`, and errors match paired routes except AC105/114's documented differences; AC61 covers load.
- AC17: GIVEN live V10 batches on paired routes WHEN success or failure occurs THEN ordered `parameter_sets`, R6 counts, and committed rows are verified; a post-write count fault raises the distinct result error, clears the batch, and reports uncertain commit under AC154/load.
- AC18: GIVEN an APC statement past `setQueryTimeout(n)` WHEN it times out THEN targeted CancelStatement raises the paired timeout SQLException within the expected elapsed time and leaves the connection usable.
- AC22: GIVEN a live Statement batch with middle failure or non-rowcount-prefix match WHEN APC executes THEN requests remain sequential without `statements`, prior counts retain order, and the paired batch error stops execution.
- AC23: GIVEN live APC CreateSession without `session_version` WHEN the next statement runs THEN `DEFAULT`, absent `session_version`, captured `negotiated_protocol_version` (if supplied), and missing-session-version diagnostics match R6 while warehouse FAST/version behavior stays intact.
- AC25: GIVEN saved APC/warehouse IDs WHEN flag or target changes before live `Driver.closeConnection()` THEN only the original session is targeted; termination rejection propagates, legacy IDs decode, malformed IDs fail without leakage, and R9's irreparable IDs are recorded.
- AC27: GIVEN a live single-chunk APC query with at least three RUNNING polls WHEN transfer is captured on flag-on and explicit SEA routes THEN polls transfer no payload and terminal fetch returns each row once.
- AC28: GIVEN T56's deployed APC flag SAFE configuration WHEN its definition and served
connector-service response are inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags`, defaults OFF, and is
independent of the warehouse flag.
- AC29: GIVEN flag-on APC and paired routes WHEN complex binding enters a single statement, legacy batch, or V10 batch THEN each has live named/numeric driver error before ExecuteStatement, no accepted write, and a usable connection; record taxonomy/dashboard ownership or handoff.
- AC30: GIVEN paired routes under API or link-pool saturation WHEN HTTP pool sizes and acquisition wait vary THEN in-flight count, wait, errors, and per-caller completion pass the shared load gate; SDK API acquisition ignores Thrift HTTP wait.
- AC31: GIVEN each SELECT mode at exposure WHEN paired callers drain it THEN rows, errors, and resource measures meet the shared paired-load gate; varying `RowsFetchedPerBlock` does not alter SEA paging.
- AC32: GIVEN each batch mode WHEN paired callers insert disjoint keys THEN ordered counts and committed rows meet the shared batch-load gate.
- AC34: GIVEN T57's deployed partner exclusion and scoped ON/default OFF states WHEN
connector-service serves ordinary, partner, and allowlisted JDBC partner agents THEN only the
ordinary scoped agent gets true; record the flag version and T56/T57 deployment evidence.
- AC36: GIVEN multi-batch Arrow chunks and inline continuation WHEN JDBC drains them THEN rows occur once, `row_offset` counts every batch row, and failed pages never succeed; if staging lacks multi-batch chunks, live proof stays pending.
- AC39: GIVEN each exposed result mode WHEN bounded and streaming switches vary on paired callers THEN requests and rows follow R17, with the shared load gate or its approved exception.
- AC40: GIVEN each JDBC slice under scoped flag ON/OFF and explicit SEA WHEN completed and incomplete calls/settings run THEN completed calls pass paired gates; incomplete calls reject before request, unsupported server shapes reject before ResultSet success with cleanup, and a later SELECT works. Capture requests and error name/code; OFF follows R1 without fallback or truncated success.
- AC45: GIVEN live APC/warehouse SEA inline or linked results WHEN `EnableSeaResponseCompression` is `0`/`1` THEN only `0` sends API `Accept-Encoding: identity`; link headers stay unchanged and paired rows, API bytes, decode CPU, and throughput pass the shared load gate.
- AC46: GIVEN linked results at one/32 callers WHEN worker, prefetch, and chunk-wait settings vary THEN workers/buffered chunks and fetched-ahead links respect R17 bounds; all callers finish or return bounded errors under the shared load gate.
- AC49: GIVEN live SELECT pages WHEN `setMaxRows` is zero, one, or a page boundary THEN positive limits send `row_limit` and cap all pages; zero is unlimited.
- AC50: GIVEN each typed native SHOW handle WHEN delayed live SHOW submits at `wait_timeout=0s` THEN it returns while RUNNING and passes shared metadata, await, timeout, cancel, cleanup, and header gates.
- AC51: GIVEN AC144's SDK or presigned-link fault path WHEN scoped 429/503 faults with/without `Retry-After` affect an exposed request class THEN attempts, delay, errors, and caller outcome follow R17 and paired routes. Writes also require AC154.
- AC52: GIVEN AC113 WHEN direct/polled no-result and counted commands, including zero, run live THEN R4 JDBC shapes and paired rows/errors hold except the documented assignment boolean; missing terminal evidence fails.
- AC53: GIVEN live scalar binding with negotiated V8+ and fixtures with missing/pre-V8 `negotiated_protocol_version` WHEN `supportManyParameters` varies THEN native/interpolated requests, `setBytes`, rows, and counts follow R6 without unresolved markers; absent `session_version` alone never downgrades a V8+ binding.
- AC54: GIVEN a known parent with nonempty exported keys and both catalog settings WHEN three live routes call `getExportedKeys` THEN capture Thrift's parent-only rows and APC's matching typed rows through a supported lookup. Otherwise the keys package stays open until measured row loss, server limit, warehouse precedent, caller impact, and PECO approval are linked; an empty sample cannot pass. Current-catalog lookup occurs only if needed; null table fails before request with SQLState `42000`/code 1003.
- AC55: GIVEN live async SQL SELECT with primitive and linked results WHEN it completes or cancels THEN `wait_timeout=0s`, target, rows/errors, close, diagnostics, and AC31 load pass.
- AC60: GIVEN AC144's scoped hook delays an accepted `GetStatementResult` beyond SEA request timeout but within `setQueryTimeout` WHEN each live SELECT mode polls THEN the same statement ID returns every row once without duplicate execution or premature JDBC timeout; record timeouts, elapsed time, and paired outcomes.
- AC61: GIVEN each nonempty metadata method or typed handle WHEN paired callers run THEN rows, types, nulls, errors, and load pass the shared gates; null-catalog requests stay at most N+2, and decode CPU per 1,000 cells plus peak RSS stay at most 1.20 times Thrift absent approved exception.
- AC63: GIVEN AC144's scoped read-delay hook and synchronous SHOW with `EnableDirectResults=0` WHEN one `GetStatementResult` response exceeds the configured SEA request timeout but stays inside `MetadataOperationTimeout` THEN polling succeeds with paired rows and a usable connection; timeout `0` removes only the overall bound.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching INSERT runs through public Statement THEN `execute` returns true, its ResultSet contains the affected-row value, and `getUpdateCount` is `-1`.
- AC65: GIVEN AC138 and each live synchronous SHOW WHEN native, sync, hybrid, and bounded settings vary THEN R5 header presence, effective native selection, and AC16 rows hold, including the warehouse-default and AC148 override differences.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized procedure metadata WHEN pages/links or query/fetch/decode errors occur THEN header-free rows pass AC61 or errors propagate with paired identity; failure never becomes typed empty.
- AC82: GIVEN live `getFunctions(null, ...)` at both SHOW-switch settings with current-catalog and other-catalog functions WHEN paired routes run THEN APC SEA uses SHOW and returns warehouse SEA's current-catalog rows; record current-catalog rows gained over Thrift SHOW, any Thrift-native rows omitted outside that catalog, counts, and caller impact. Explicit-catalog rows/errors follow the paired gate.
- AC83: GIVEN synchronous or async null-catalog schema/column `42601` fan-out WHEN one live catalog raises `SQLException` THEN APC and warehouse SEA omit only its rows, warn, and remain usable; record Thrift delta, while timeout/interruption fails the call.
- AC84: GIVEN each new live SDK or link request class WHEN socket, poll, and idle settings vary THEN one-request timeout, later-poll delay, and applicable idle eviction follow R17, with paired timing/errors and usable connections.
- AC85: GIVEN each new path WHEN telemetry toggles vary THEN R8 event/error families match paired routes or approved exceptions, disabled export suppresses them, and logs/events redact secrets, targets, and IDs.
- AC87: GIVEN AC164 WHEN JDBC sends ASCII and multibyte text on both sides of Reyder's APC UTF-8-byte limit, including one request above Scala's UTF-16-unit threshold, THEN acceptance and size errors match the measured APC byte gate without split/fallback. Record both lengths, a non-BMP case, and paired APC Thrift/warehouse SEA outcomes before batch exposure.
- AC88: GIVEN two catalogs and a metric view WHEN catalog and metric-view settings vary on three live routes THEN discovery, table types/rows, and APC session config follow R5; record server-limited impact.
- AC89: GIVEN live invalid SQL, missing object, or permission failure on three routes WHEN each exposed method runs THEN one release table records SQLException class, SQLState, vendor code, and origin. Equal baseline fields stay equal; differences follow warehouse SEA with recorded limit/precedent, caller impact, and PECO approval. Failure never becomes success.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN live inline/linked results WHEN codec setting or alias is `0`, `1`, or invalid THEN manifest codec governs lossless decode, invalid keeps warehouse default, and paired bytes/CPU/load are recorded.
- AC102: GIVEN an isolated live table WHEN transaction settings vary THEN R19 state, commit/rollback rows, counts, and errors match paired routes; no-result commands fetch nothing and fail on missing state.
- AC105: GIVEN live synchronous or typed async null-catalog `getTables` receiving `42601` WHEN APC SEA, APC Thrift, and warehouse SEA are compared THEN APC returns warehouse SEA’s typed empty result without fan-out; record omitted Thrift row counts/types and caller impact.
- AC106: GIVEN a changed driver-owned error WHEN emitted THEN a test asserts its code name/number, the PR records taxonomy/dashboard ownership or handoff, and the telemetry-error template is complete; server errors retain server identity.
- AC107: GIVEN one/32 paired callers WHEN SELECT opens, leaves an operation open, and closes repeatedly THEN close order, baseline resource recovery, at most 8 MiB retained heap growth, and cycle-latency gate pass.
- AC108: GIVEN AC87 WHEN live legacy INSERT batching settings vary THEN R6 chunk limits, counts, committed rows, errors, AC32 load, and AC132 oversize behavior pass each binding mode.
- AC109: GIVEN concurrent live multi-page inline SELECTs WHEN `ThriftMaxBatchesInMemory` varies around two THEN all rows arrive once without whole-result buffering, AC31 memory/load bounds pass, and observed chunks, batches, bytes, and heap explain the APC Thrift difference.
- AC112: GIVEN AC119 WHEN live APC and warehouse download-off SELECTs drain primitive
and complex values at both bounded/hybrid settings THEN APC remains targeted,
`ARROW_BATCH`, inline and lossless; enabled representable getters match, while
wire fields, transfer cost and paired load meet R4/R11/R17 and AC31/39. Initial
size failure blocks ResultSet success; later failure interrupts iteration.
- AC113: GIVEN the deployed terminal/count contract WHEN direct/polled no-result and counted commands run on scoped staging THEN raw captures prove R4 terminal shapes/errors, including zero, before T14 caller exposure.
- AC114: GIVEN two-parent and second-foreign-table keys WHEN live imported/cross-reference methods run THEN each returns its filtered paired JDBC rows; null foreign table is typed empty and invalid input fails before request.
- AC119: GIVEN the deployed CreateSession override WHEN session disables cloud download but statement advertises it THEN live single/multi-page results remain inline; capture both wire values before AC112.
- AC117: GIVEN paired bound SELECT callers WHEN pre-execution `PreparedStatement.getMetaData()` runs THEN metadata matches, bindings survive, and request/latency/throughput/heap load passes.
- AC118: GIVEN live `EnableSQLValidationForIsValid=1` callers WHEN `isValid(timeout)` runs `SELECT VERSION()` repeatedly THEN booleans, timeout, usability, request count, latency, call-rate, and heap pass paired load.
- AC122: GIVEN live two-parent and second-foreign-table keys WHEN typed async imported-key and cross-reference handles complete THEN each independently passes AC114 and the per-handle AC50/61 gates.
- AC124: GIVEN synchronous SHOW or typed async metadata WHEN `MetadataOperationTimeout` varies THEN positive values bound full call/await and cancel; `0` removes only the overall bound, in-budget calls survive shorter request timeouts, and timed-out connections remain usable.
- AC132: GIVEN AC87 and a live legacy INSERT chunk near the deployed APC byte limit WHEN ASCII or multibyte text crosses that limit THEN in-limit chunks succeed and over-limit chunks fail at Reyder without client split/fallback; committed rows and counts match captured requests.
- AC136: GIVEN live APC and Thrift cursors with unread continuation/link data and heartbeat enabled WHEN one row is read, both idle beyond two heartbeat intervals and configured expiry, then drain THEN APC sends no `/status`, remaining rows match, and survival time is recorded. Shorter APC lifetime requires R3 exception approval.
- AC138: GIVEN deployed T58 APC native SHOW routing WHEN raw synchronous/async catalog,
schema, table, column, function, primary-key, and cross-reference operations run THEN the
shared per-hop server-readiness gate above passes before each JDBC method is exposed; record
DBR version/configuration and the HIVE null-cell sample.
- AC144: GIVEN scoped staging fault hooks WHEN 429/503 with and without `Retry-After` affect session/read/link requests, accepted-write responses fail after commit, and one accepted `GetStatementResult` response for SELECT or synchronous SHOW is delayed past the configured SEA request timeout THEN captures identify target, fault point, committed rows for writes, and unrelated-caller isolation. A readiness probe must prove the delayed response stays inside the JDBC/metadata deadline with a stable statement ID before AC60/63; independently scoped fan-out delays gate AC161.
- AC145: GIVEN linked SELECT with hybrid and bounded settings at `0`/`1` WHEN paired routes run THEN APC stays targeted/hybrid with paired rows while warehouse hybrid `0` is external-only; bytes, latency, throughput, and heap pass before exposure.
- AC146: GIVEN AC113 and isolated table WHEN live prepared INSERT/UPDATE/DELETE/MERGE run, including zero count THEN each yields one counted request, paired counts/rows/errors, `execute=false`, and no ResultSet under AC154/load.
- AC147: GIVEN live IN-only CALL with rows, no result, or failure WHEN execute/executeQuery runs THEN R18 JDBC shapes and paired rows/errors hold; unsupported OUT/named/update forms reject before request.
- AC148: GIVEN flag-on APC with explicit SEA and catalog-pattern preference WHEN live `getCatalogs` and `getTables` run THEN each captures SEA SHOW, paired rows, and ignored-pattern diagnostic at first exposure and release.
- AC151: GIVEN each slice's live gates close before merge WHEN an intermediate flag-OFF build is distributed THEN completed explicit-SEA workflows pass and incomplete calls reject under AC40; its newly visible behavior has a changelog entry. Final docs/`NEXT_CHANGELOG.md` explain complete behavior and accepted-write uncertainty before ramp; internal PRs may use `NO_CHANGELOG=true`.
- AC152: GIVEN saved APC SEA session and raw statement IDs WHEN a connection flag or target changes THEN saved close uses the original session target and recovered statement uses the current connection; captures prove both outcomes without ID leakage.
- AC154: GIVEN AC113 and AC144's post-acceptance write/count fault WHEN APC and warehouse SEA writes commit before their response is faulted THEN no APC-only replay occurs; missing counts fail and accepted writes report uncertain commit before each write exposure.
- AC155: GIVEN AC168 on scoped ON/OFF APC and warehouse routes WHEN callers open/close without SQL THEN ON terminates its session, OFF follows R1, and paired load passes. Rejected termination leaves ordinary close locally successful with a redacted warning; AC25 covers saved IDs.
- AC157: GIVEN HIVE null-mask and unknown-vector fixtures WHEN decoded THEN null positions and row count follow R13, unknown vectors fail before partial success, and a later valid result works; AC16 proves live HIVE parity.
- AC158: GIVEN unknown numeric SEA format fixture WHEN deserialized and decoded THEN decoding raises a named driver error before ResultSet success and a later valid SELECT still works.
- AC159: GIVEN single-chunk APC SELECT with no unread server page and heartbeat enabled WHEN a partly read ResultSet idles beyond two heartbeat intervals THEN no APC `/status` request is sent; cursor survival remains gated by AC136 once unread server data exists.
- AC161: GIVEN scoped, independently delayed catalogs in null-catalog schema/column calls WHEN overall metadata timeout is disabled THEN each catalog completing within its 90-second collection allowance returns despite earlier completions; over-budget work fails within that limit. Positive overall timeout can end sooner. Compare paired rows, elapsed times, and connection usability.
- AC164: GIVEN the deployed APC backend route WHEN raw scoped syntactically valid SELECTs padded with ASCII/multibyte comments straddle Reyder's configured UTF-8-byte limit THEN captures show acceptance below and Reyder rejection above it, including a non-BMP case and an above-Scala-threshold request rejected by Reyder rather than Scala. Record UTF-16-unit/UTF-8-byte counts, fault origin, and other-route outcomes before AC87.
- AC165: GIVEN T12/T32/T42 expose linked and bounded modes WHEN streaming switches and CloudFetch speed thresholds vary on live inline/linked SELECT THEN R17 rows and diagnostics hold under AC39, including absent-total multi-link delivery at unbounded/streaming-off within its memory bound.
- AC166: GIVEN live APC SEA before T44 WHEN public `getConnectionId()` follows
open (T9) or successful SELECT (T10) THEN each slice's callable case raises
`UNSUPPORTED_OPERATION` (1005) without ID or extra request; ordinary close
uses the original cluster and a later connection works. APC Thrift/warehouse
SEA IDs remain valid. T44 replaces this boundary after AC25/152.
- AC168: GIVEN the tracked server termination binding fix is deployed WHEN a raw APC CreateSession and TerminateSession use the same cluster target THEN the created session is removed; a rejected termination remains identifiable for JDBC AC155.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- The final release package alone edits `docs/` and `NEXT_CHANGELOG.md`.
  Intermediate internal PRs may use `NO_CHANGELOG=true`; a separately
  distributed intermediate build requires its own changelog and AC151 gate.
- Each JDBC slice closes its callable workflow and the applicable common gates
  defined above. Keep each PR below roughly 1,500 changed lines; revise the
  package plan before implementation if a scoped slice cannot meet that limit.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server-side Reyder, Reyden, connector-service, SAFE definition, and staging
  fault-hook implementation are outside JDBC delivery. T56–T62 and their
  universe packages track these prerequisites and their deployment evidence.
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

`plan.json` assigns each task once to a callable JDBC slice or a tracked universe
prerequisite. Each repository has its own parent tree; deployed universe outputs
are explicit JDBC gates rather than cross-repository branch parents. Connection
and first SELECT establish common transport; independent
SQL and metadata branches then proceed as siblings with disjoint owned edits.
Shared transport has one owner, and each branch closes its own live gate. The
final package integrates all branches, validates the assembled stack, and alone
edits docs and `NEXT_CHANGELOG.md`. A branch must be split again if its review
diff approaches 1,500 changed lines. The larger-than-target package count keeps
the linked, write, metadata, and server gates independently reviewable.

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

Server deployment gates metadata and termination; Reyder's deployed byte limit
must pass AC87 before dependent batches. AC13/Q7 block lost logical types, AC27 blocks
repeated poll payloads, and AC11/14 protect warehouse and presigned-link
boundaries.

---

## Tasks

The task IDs below define the work packages in `plan.json`. Requirements and
acceptance criteria above carry the wire and live-validation detail. Universe
tasks own server changes and deployment evidence; their branch heads are
integration prerequisites, not JDBC code changes.

| Workflow | Task outcomes |
| --- | --- |
| Connection | T9 open and close APC SEA sessions. |
| First SELECT | T10 single-chunk polling, fetch, and cancel; T40 direct/polled error identity; T39 `isValid`. |
| Inline and saved close | T11 inline continuation; T13 logical getters; T41 codec/API compression; T44 saved session close. |
| Linked SELECT | T12 linked continuation; T29 async SELECT; T32 SELECT settings and limits; T42 linked resource controls; T43 download-off. |
| Commands and transactions | T14 terminal commands; T30 transactions; T33 CALL; T34 Statement batches. |
| Prepared scalars | T28 negotiated scalar binding; T31 prepared DML; T38 prepared metadata. |
| INSERT batches | T15 legacy INSERT batches; T27 V10 native batches. |
| Metadata core | T16 catalogs; T17 schemas and the shared header/format contract. |
| Metadata discovery | T18 columns; T48 tables; T49 functions. |
| Metadata keys | T35 primary keys; T47 exported keys; T55 imported/cross-reference keys. |
| Procedure metadata | T19 parameterized procedure SELECT after scalar binding and metadata transport. |
| Async metadata discovery | T20 catalogs; T21 schemas; T22 tables; T23 columns; T24 functions. |
| Async metadata keys | T25 primary keys; T26 imported/cross-reference keys. |
| Release | T36 docs/changelog; T-val assembled live gates and integration handoff. |
| Server flag | T56 Partner Platform defines the default-off APC client-config sub-flag; T57 connector-service excludes all partners, including allowlisted JDBC agents (AC28/34). Track both under [SC-256186](https://databricks.atlassian.net/browse/SC-256186); the flag and exclusion packages own the deliverables, and the connection slice requires their deployed evidence. |
| Server metadata | T58 SEA/Reyder/Reyden carries effective native and sync intent and returns native Arrow/HIVE metadata (AC138); [SC-256188](https://databricks.atlassian.net/browse/SC-256188). |
| Server result and session | T59 Reyder honors CreateSession inline-only override (AC119; [SC-258787](https://databricks.atlassian.net/browse/SC-258787)); T60 SEA/Reyder fixes APC TerminateSession target binding (AC168); T62 Reyder returns terminal/count command shapes (AC113; [SC-258842](https://databricks.atlassian.net/browse/SC-258842)). |
| Staging faults | T61 SEA staging owns scoped request delays, 429/503 and accepted-write faults (AC144). |
