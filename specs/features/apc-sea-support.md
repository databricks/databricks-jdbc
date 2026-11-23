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
  `GetStatementStatus`, so polling uses `GetStatementResult`.
- SC-256188/SC-256186 gate live metadata and termination; SC-254478 covers
  per-statement config, not T8's CreateSession override. T2–T7 verify or complete
  Reyder command and native SHOW behavior before JDBC exposure.
- Connector-service returns sub-flags at
  `/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`;
  `FeatureFlagsHandler.scala` may allowlist JDBC partners, so Q9 requires an
  APC-wide exclusion.
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
  | Close and saved IDs | Ordinary close suppresses delete failure; saved close propagates | Same, with warehouse ID | An internal SELECT slice rejects public saved-ID requests with `UNSUPPORTED_OPERATION` (1005); T44 restores saved close before distribution. Callers cannot use saved IDs in a partial build. | Internal integration boundary; no release exception, AC25/152/166. |
  | Paging | Blocking Thrift fetch | SEA continuation | Consume continuation without duplicate rows. | Follow SEA, AC14/31. |
  | Result codec | Honors requested codec | Forwards codec preference | Reyder sends LZ4 even for `NONE`; decode actual manifest codec. `NONE` cannot disable compression. | Server exception, AC101. |
  | SEA API HTTP compression | No SEA API setting | `EnableSeaResponseCompression=0` sends `Accept-Encoding: identity`; links separate | Follow warehouse setting; bytes and CPU can change, rows cannot. | Warehouse precedent, AC45. |
  | `RowsFetchedPerBlock` | Tunes fetch | No SEA paging effect | Ignored for APC SEA paging. | Warehouse precedent, AC31. |
  | `ThriftMaxBatchesInMemory` | Limits batches | Limits inline chunks | Limits chunks, each potentially several batches; memory can rise. | Warehouse precedent, AC109. |
  | Download/complex getters | Inline or linked typed values | Hybrid `0` external-only; download-off JSON or Arrow | Hybrid `0` remains hybrid (Reyder limit); download-off uses Arrow batches and representable typed getters. | Transfer exception; getter parity required, AC13/112/145/156. |
  | `UseBoundedSeaApi=0/1` | No SEA setting; Thrift fetch unchanged | Download-off `0` uses `JSON_ARRAY`/inline; `1` uses `ARROW_STREAM`/hybrid inline Arrow, while linked `1` forces streaming and `0` can use the total-dependent provider | Both values retain APC `ARROW_BATCH`, hybrid request and inline-only session fields; link totals may be absent, so both drain by continuation with bounded buffering. `0` can transfer more bytes and change first-row latency, CPU and heap versus warehouse SEA or APC Thrift. | Reyder format/total-count exception, AC39/112/145/156/165/167. |
  | Legacy INSERT tuning | Eligible combined batches | Same, with optional V10 native sets | `EnableBatchedInserts` affects legacy only; `BatchInsertSize` bounds interpolated rows, not native-scalar chunks. | Warehouse precedent; caller tuning changes, AC17/108. |
  | Pools and retries | Thrift HTTP controls | SDK API and link controls split | Follow SEA split; link properties do not govern SDK attempts. | Warehouse precedent, AC30/46/51/84. |
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
  | SQL text limit | Measure live boundary | Scala validates character length before routing; Reyder also limits UTF-8 bytes | ASCII over Scala's default 16 MiB character limit fails before Reyder; multibyte text below that character limit can reach Reyder yet exceed its byte limit. Callers can see a size error at fewer characters. | Server limit; no split/fallback, AC87/132/164. |
  | Download resource use | Thrift pool/batches | SDK pool, linked workers, inline chunk limits | Follow SEA bounds; compare throughput, heap, RSS, CPU and caller impact. | Warehouse precedent/load gate, AC31/46/61/109. |
  | `EnableStreamingChunkProvider` | Opt-in linked streaming | Opt-in, or forced by bounded SEA | Honor `1`; with absent APC link totals, use bounded streaming even at `0` so rows are not lost. `0` may therefore have different buffering and latency from APC Thrift. | APC total-count exception, AC39/165. |
  | `EnableInlineStreaming` | Chooses streaming or lazy Thrift inline reader | Does not select the SEA Arrow reader | APC SEA follows warehouse SEA: the switch has no effect on inline Arrow prefetch. Caller memory and first-row timing may differ from APC Thrift. | Warehouse precedent, AC165. |
  | `CloudFetchSpeedThreshold` | Linked-download slow-speed threshold | Linked-download slow-speed threshold | Apply to APC links at both bounded settings; inline results have no CloudFetch speed check. Threshold changes diagnostics, not row delivery. | Match linked behavior, AC165. |
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
once T8/AC119 proves it overrides the APC per-statement capability; until then
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
setting. SEA's forward allowlist must include `X-Databricks-Metadata-Operation-Type`
and `X-Databricks-Sea-Can-Run-Fully-Sync`: forward each present value unchanged
to Reyder and omit it when absent. Reyder honors fully-sync only for synchronous
execution; APC require-native stays at the JDBC-to-SEA hop. These operations
are inline/link-free. If a response provides `next_chunk_index`, consume it through
R12; the current metadata contract does not promise paginated SHOW results (AC16/61).
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
Error outcomes follow the warehouse method: catalog syntax errors propagate;
null-catalog schema and column syntax errors trigger catalog fan-out;
null-catalog table syntax errors return typed empty without fan-out;
function and primary-key syntax errors propagate; imported-key and
cross-reference syntax errors return typed empty. Object-not-found returns
typed empty for schemas, tables, columns, functions, primary keys, imported
keys, and cross-reference; catalog errors propagate. Other SQL failures
propagate except existing table/column empty-pattern and fan-out behavior
(AC16/83/105/114).

`enableMultipleCatalogSupport=0` retains current-catalog filtering; `1`
permits eligible catalogs. `EnableMetricViewMetadata=1` adds
`spark.sql.thriftserver.metadata.metricview.enabled=true` to session config
and exposes `METRIC_VIEW` table types and eligible rows; `0` omits both.
`getExportedKeys` is outside native SHOW: the SEA SHOW contract lacks parent-only
lookup. On `origin/main`, APC Thrift sends a parent-only `TGetCrossReferenceReq`
and returns its rows, while warehouse SEA returns typed empty. T47 delivers a
supported parent-only metadata lookup with typed exported-key rows for a known
parent. If no supported lookup can return those rows, T47 instead records the
measured loss, server limit, warehouse precedent, caller impact, and PECO approval;
the keys package and release cannot close with an unapproved empty result.
Catalog filtering at `enableMultipleCatalogSupport=0` may first run
`SELECT CURRENT_CATALOG()`; at `1` it needs no lookup. A null table fails before
request with SQLState `42000` and `EXECUTE_STATEMENT_FAILED` (1003).
Session-reachable async metadata has typed ResultSet handles for seven SHOW
operations, with separate imported-key and cross-reference calls. It submits
at `wait_timeout=0s`, returns while RUNNING, and preserves synchronous await,
timeout, cancel, and conversion outcomes. Version and saved IDs do not gate it;
public JDBC metadata stays synchronous.

Internal catalog/schema and parameterized procedure SELECTs use hybrid
`ARROW_BATCH` without metadata headers; Reyder rejects warehouse procedure
headers. Procedure queries use R6 binding, not async SHOW, and propagate query,
fetch, and decode errors. Only the existing catalog-access filter returns typed
empty (AC75).
`EnableShowCommandForGetFunctions` still selects native RPC or SHOW
on APC Thrift, whose SHOW path returns typed empty for null catalog; APC SEA,
like warehouse SEA, uses SHOW at either setting (AC82).

Async null-catalog schema/column `42601` fans out without sync headers.
Per-catalog `SQLException` warns and omits that catalog; timeout,
interruption, or initial/enumeration error fails the call. Partial results
leave the connection usable (AC83).
Positive `MetadataOperationTimeout` bounds the whole call/await; `0` disables
only that overall deadline. Per-catalog collection retains its 90-second bound, and a catalog
that completes within its collection budget is not lost solely because another
catalog finished earlier (AC83/124/161).

- R6: APC CreateSession explicitly sends `execution_mode=DEFAULT` and leaves
`session_version` absent on later ExecuteStatement requests, matching Reyder's
absent version. `FAST` is reserved for sessions that return an initial version
and can echo the maximum observed version; the current warehouse FAST behavior
is unchanged. AC23 captures session mode, response, and later requests; APC `FAST` without
a version fails the gate. CreateSession `negotiated_protocol_version` alone grants native scalar
parameters at V8 (42248) and opt-in `EnableNativeBatching=1` INSERT batches
at V10 (42250). It does not gate R5 async metadata. Reyder leaves
`session_version` unset; that absence does not reduce a negotiated capability,
and saved IDs grant none. Retain negotiated protocol version. Reject complex bindings
before request with a tested driver error; `toString()` is not lossless SQL. At `supportManyParameters=0`,
a negotiated V8+ uses native scalar `parameters`; missing or pre-V8
`negotiated_protocol_version` interpolates
supported scalars without unresolved markers; V8–V9 legacy batch sets may use
native scalars. At `supportManyParameters=1`, single and legacy batch sets
interpolate at every version and non-null `setBytes` is permitted; at `0`,
non-null `setBytes` remains rejected. Keep missing-version telemetry distinct.

Opt-in native V10 INSERT batches send ordered nested `parameter_sets` and no
top-level `parameters`.
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

Scala SEA checks statement character length before routing (default 16,777,216
characters); Reyder checks UTF-8 bytes at its configured limit (default
16,777,216 bytes) only for requests that pass Scala. T2 verifies both deployed
boundaries (AC164) and changes only missing terminal command or count behavior.
The driver sends text without silent split or fallback and propagates the size
error. AC87 proves the JDBC boundary before dependent batch gates close;
warehouse and APC Thrift outcomes are captured separately.
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
- R9: The first SELECT slice attempts live APC SEA session termination
  and verifies server termination once the tracked binding fix is deployed; its
  `Connection.close()` remains best effort on rejection and closes an open statement
  before session termination. Until T44, public `getConnectionId()` on APC SEA
  raises `UNSUPPORTED_OPERATION` (1005) with a redacted explanation before
  serialization or another request; it never returns a Thrift-shaped or invalid
  ID. The first slice retains the original target internally for ordinary close.
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
measured caller impact before release (AC13/156).
Download-off APC `ARROW_BATCH` and its transfer mode differ from warehouse SEA's
bounded-dependent `JSON_ARRAY`/`ARROW_STREAM` requests; AC156 measures caller impact
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
- R18: JDBC packages are internal integration slices. Explicit
`UseThriftClient=0` reaches incomplete surfaces, so no partial stack is
distributed. The complete stack is released after all parity gates pass (AC151).
Client-selected incomplete methods and settings fail with a named driver error
before their SEA request; local-only methods make no extra request. A supported
request with an unsupported server-selected shape fails before ResultSet success,
closes its remote statement, and leaves the connection usable. A later-page
failure interrupts iteration; no call reports truncated success.
The first slice supports primitive, single-chunk inline `Statement` SELECT,
polling, fetch, timeout/cancel, and ordinary close. Saved IDs follow R9; writes,
prepared/async SQL, remote metadata, `isValid` SQL validation, download-off,
unbounded/hybrid-off settings, and positive `setMaxRows` remain incomplete.
Each subsequent slice enables only its task's modes and proves the incomplete
boundary and newly enabled setting values under AC40. T44 and every parity gate
close before distribution. Counted prepared DML preserves
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
- Q9: Exclude partners despite Reyder's OSS prefix allowlist? A9: Yes, including JDBC partners on the warehouse flag. Deploy T1 before assignment; partner enablement needs its own decision.
- Q10: Avoid transfer during result polling? A10: At least three RUNNING polls transfer no payload; terminal fetch returns rows once (AC27).
- Q11: Suppress procedure metadata failures? A11: No; propagate query, fetch, and decode SQLState/vendor code. This supersedes the earlier typed-empty-on-error wording.

## Acceptance Criteria

At first exposure, each public slice proves its completed call on scoped flag-ON
APC staging against APC Thrift and warehouse SEA where relevant. Common routing,
transport, telemetry, error, and load gates apply to each new request class.
Use fixtures only for cases that live staging cannot produce. AC144/160 gate
read/link fault injection; AC113/154 gate writes.
Every JDBC slice that exposes a public workflow closes the common AC40,
AC85, and AC89 gates for that workflow. Only the complete stack is releasable.
AC106 applies where a driver-owned error changes.
When one AC names several callers, a package closes only its newly exposed
caller subset; T-val repeats the assembled check after all callers exist.

The shared paired-load gate uses one build, a warmed cluster, identical data,
and one/32 APC SEA and Thrift callers. Every caller completes or returns its
expected bounded error. At 32 callers, SEA/Thrift aggregate throughput is
at least 0.90; median/p95/slowest-caller latency is at most 1.10/1.20/1.50,
and peak heap at most 1.20. Inline and linked SELECT CPU seconds/GiB and
peak RSS are at most 1.20 at one and 32 callers; record the idle RSS baseline
and absolute values at 8/64 MiB inline and 64/128 MiB linked per caller.
AC31 drains at least 8 MiB per caller through repeated first-slice SELECTs and
64 MiB for multi-page/linked modes, at least 0.90 of Thrift sustained bytes/s.
AC32 inserts at least 1,024 rows per caller over two or more requests/chunks/
sets, at least 0.90 of Thrift committed rows/s. Retained heap growth across
two data sizes is at most max(8 MiB, 5% of added bytes). Record rows, bytes,
duration, errors, CPU and memory. An exception needs same-build warehouse
SEA/Thrift evidence, server limit or precedent, caller impact, and PECO/Perf
Eng approval before release.

For each native SHOW operation, the server-readiness gate uses raw scoped APC
staging requests at both hybrid settings and direct/polled execution for the
Arrow IPC or HIVE `columnar_data` format named by its task. Check the native
mark, forwarded headers, inline/link-free delivery, schema and row values,
and continuation if supplied. AC138–142 and AC169–172 close the operation's
formats without JDBC callers; catalog/table HIVE gates require a repeatable
null cell with its vector value and mask. At exposure, AC16/50 add paired JDBC
rows/types, `wasNull()`, errors, and AC61 load for each method or typed handle.

- AC1: GIVEN scoped flag-ON/OFF APC staging WHEN R1 overrides and metadata preference vary at connection open THEN selected protocol, target, flag value, partner exclusion, and no-fallback match R1 and paired warehouse routing. Fixtures cover Arrow absence, legacy `cliservice`, and lookup failure; AC2 proves SELECT rows after exposure.
- AC2: GIVEN flag-on and explicit-SEA APC staging at bounded/hybrid `1` WHEN single-chunk, RUNNING, manifest-only, and zero-row SELECTs run THEN targeted Arrow yields paired rows/types/empties. A server-selected link or continuation in this slice fails before ResultSet success and closes the remote statement; AC145 gates alternate settings.
- AC3: GIVEN a live APC statement WHEN it polls, fetches, and closes THEN R2 targeting, exactly-once close, connection usability, and unchanged warehouse serialization are captured.
- AC9: GIVEN live APC SEA CreateSession rejection or APC Thrift OpenSession KP001
WHEN connecting on the affected route THEN the SEA error and failed-route event
propagate without fallback, and warehouse-only KP001 recovery never selects SEA for APC.
- AC10: GIVEN successful and failed opens on four APC/warehouse SEA/Thrift routes WHEN DEBUG logs and telemetry are captured THEN compute, protocol, reason, outcome, and error identity appear where enabled; R8 toggles suppress export and secrets/targets/IDs are redacted.
- AC11: GIVEN standard and Real-time warehouse connections WHEN equivalent R2 operations run THEN JDBC outcomes and URL/body/header serialization, including `realtimeSession`, match `origin/main`.
- AC12: GIVEN live synchronous SELECT at both direct-result settings WHEN paired callers drain rows THEN `wait_timeout` is absent or `0s` respectively, no `statements` field appears, rows/errors and AC31 load pass, and first-row latency/request count are recorded.
- AC13: GIVEN R11 primitive/logical values WHEN inline and linked getters run at both complex/geospatial settings THEN types and values match paired routes; enabled typed getters work for representable inline Arrow and linked values, while disabled support retains established rejection. A remaining inline exception requires the R11 evidence. Unavailable live types stay pending.
- AC14: GIVEN live APC multi-link results with absent totals after T42 controls pass WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN a deployed native SHOW operation WHEN its synchronous JDBC method runs THEN it passes the shared JDBC metadata gate against warehouse SEA and applicable APC Thrift behavior; AC105 and AC114 govern documented differences.
- AC17: GIVEN live V10 batches on paired routes WHEN success or failure occurs THEN ordered `parameter_sets`, R6 counts, and committed rows are verified; a post-write count fault raises the distinct result error, clears the batch, and reports uncertain commit under AC154/load.
- AC18: GIVEN an APC statement past `setQueryTimeout(n)` WHEN it times out THEN targeted CancelStatement raises the paired timeout SQLException within the expected elapsed time and leaves the connection usable.
- AC22: GIVEN a live Statement batch with middle failure or non-rowcount-prefix match WHEN APC executes THEN requests remain sequential without `statements`, prior counts retain order, and the paired batch error stops execution.
- AC23: GIVEN live APC CreateSession without `session_version` WHEN the next statement runs THEN `DEFAULT`, absent `session_version`, captured `negotiated_protocol_version` (if supplied), and missing-session-version diagnostics match R6 while warehouse FAST/version behavior stays intact.
- AC25: GIVEN the saved-close slice and APC/warehouse saved IDs WHEN flag or target changes before live `Driver.closeConnection()` THEN only the original session is targeted, a TerminateSession rejection propagates, legacy IDs decode, malformed IDs fail without leakage, and R9’s irreparable legacy Thrift IDs are recorded.
- AC27: GIVEN a live single-chunk APC query with at least three RUNNING polls WHEN transfer is captured on flag-on and explicit SEA routes THEN polls transfer no payload and terminal fetch returns each row once.
- AC28: GIVEN the T1 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN flag-on APC and paired routes WHEN a complex binding enters a single bound statement, legacy prepared batch, or V10 prepared batch THEN each exposed form raises a named/numeric driver error before ExecuteStatement, leaves the connection usable, and records taxonomy/dashboard ownership or maintainer handoff.
- AC30: GIVEN paired routes under API or link-pool saturation WHEN HTTP pool sizes and acquisition wait vary THEN in-flight count, wait, errors, and per-caller completion pass the shared load gate; SDK API acquisition ignores Thrift HTTP wait.
- AC31: GIVEN each SELECT mode at exposure WHEN paired callers drain it THEN rows, errors, and resource measures meet the shared paired-load gate; `RowsFetchedPerBlock` has no SEA paging effect.
- AC32: GIVEN each batch mode WHEN paired callers insert disjoint keys THEN ordered counts and committed rows meet the shared batch-load gate.
- AC34: GIVEN scoped ON and default OFF flag states WHEN connector-service serves ordinary, partner, and allowlisted JDBC partner agents THEN only the ordinary scoped agent gets true; record the flag version.
- AC36: GIVEN multi-batch Arrow chunks and inline continuation WHEN JDBC drains them THEN rows occur once, `row_offset` counts every batch row, and failed pages never succeed; if staging lacks multi-batch chunks, live proof stays pending.
- AC39: GIVEN each exposed result mode WHEN bounded and streaming switches vary on paired callers THEN APC requests follow R17, rows match, and first-row latency, heap, link expiry, and sustained download rate pass the shared load gate or have an approved exception.
- AC40: GIVEN each internal JDBC slice under scoped flag ON/OFF and explicit SEA WHEN its completed workflows, incomplete calls, and newly selected settings run THEN completed calls pass their paired gate; incomplete calls fail before request, and unsupported server-selected shapes fail before ResultSet success with remote cleanup. Capture request count and error name/code; a later SELECT works. OFF follows R1, with no SEA fallback or truncated success.
- AC45: GIVEN live linked or inline results on APC SEA and warehouse SEA WHEN `EnableSeaResponseCompression` is `0` or `1` THEN only `0` sends API `Accept-Encoding: identity`, presigned-link headers remain unchanged, rows match APC Thrift, and API wire bytes, decode CPU, and throughput are recorded against both paired routes under the shared load gate.
- AC46: GIVEN linked results at one/32 callers WHEN worker, prefetch, and chunk-wait settings vary THEN workers/buffered chunks and fetched-ahead links respect R17 bounds; all callers finish or return bounded errors under the shared load gate.
- AC49: GIVEN live SELECT pages WHEN `setMaxRows` is zero, one, or a page boundary THEN positive limits send `row_limit` and cap all pages; zero is unlimited.
- AC50: GIVEN each typed native SHOW handle WHEN delayed live SHOW submits at `wait_timeout=0s` THEN it returns while RUNNING and passes shared metadata, await, timeout, cancel, cleanup, and header gates.
- AC51: GIVEN AC144's SDK fault path or AC160's presigned-link path WHEN scoped 429/503 faults with/without `Retry-After` affect an exposed request class THEN attempts, delay, errors, and caller outcome follow R17 and paired routes. Writes also require AC154.
- AC52: GIVEN AC113 WHEN direct/polled no-result and counted commands, including zero, run live THEN R4 JDBC shapes and paired rows/errors hold except the documented assignment boolean; missing terminal evidence fails.
- AC53: GIVEN live scalar binding with negotiated V8+ and fixtures with missing/pre-V8 `negotiated_protocol_version` WHEN `supportManyParameters` varies THEN native/interpolated requests, `setBytes`, rows, and counts follow R6 without unresolved markers; absent `session_version` alone never downgrades a V8+ binding.
- AC54: GIVEN a known parent with nonempty exported keys and both catalog settings WHEN live APC SEA, APC Thrift, and warehouse SEA call `getExportedKeys` THEN capture Thrift's parent-only request and rows; APC SEA returns matching typed rows through T47's supported parent-only lookup without unsupported SHOW. If no supported lookup exists, the keys package and release remain open until measured row loss, the server limit, warehouse precedent, caller impact, and PECO approval are linked as an explicit exception; an empty sample or unapproved typed-empty result cannot pass. Current-catalog lookup occurs only if needed, and null table fails before request with SQLState `42000`/code 1003.
- AC55: GIVEN live async SQL SELECT with primitive and linked results WHEN it completes or cancels THEN `wait_timeout=0s`, target, rows/errors, close, diagnostics, and AC31 load pass.
- AC60: GIVEN a valid live query within its client wait WHEN it exceeds one SEA request timeout THEN polling completes without premature failure; only measured latency may receive an exception.
- AC61: GIVEN each nonempty metadata method or typed handle WHEN paired callers run THEN rows, types, nulls, errors, and load pass the shared gates; null-catalog requests stay at most N+2, and decode CPU per 1,000 cells plus peak RSS stay at most 1.20 times Thrift absent approved exception.
- AC63: GIVEN delayed live synchronous SHOW within its metadata deadline WHEN it exceeds one SEA request timeout THEN polling succeeds and the connection remains usable; deadline `0` disables only the overall bound, retaining AC83 future waits.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching INSERT runs through public Statement THEN `execute` returns true, its ResultSet contains the affected-row value, and `getUpdateCount` is `-1`.
- AC65: GIVEN each live synchronous SHOW WHEN native, sync, hybrid, and bounded settings vary THEN R5 headers and AC16 Arrow/HIVE rows pass, including warehouse default and AC148 override differences.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized procedure metadata WHEN pages/links or query/fetch/decode errors occur THEN header-free rows pass AC61 or errors propagate with paired identity; failure never becomes typed empty.
- AC82: GIVEN live `getFunctions(null, ...)` at both SHOW-switch settings with current-catalog and other-catalog functions WHEN paired routes run THEN APC SEA uses SHOW and returns warehouse SEA's current-catalog rows; record current-catalog rows gained over Thrift SHOW, any Thrift-native rows omitted outside that catalog, counts, and caller impact. Explicit-catalog rows/errors follow the paired gate.
- AC83: GIVEN synchronous or async null-catalog schema/column `42601` fan-out WHEN one live catalog raises `SQLException` THEN APC and warehouse SEA omit only its rows, warn, and remain usable; record Thrift delta, while timeout/interruption fails the call.
- AC84: GIVEN each new live SDK or link request class WHEN socket, poll, and idle settings vary THEN one-request timeout, later-poll delay, and applicable idle eviction follow R17, with paired timing/errors and usable connections.
- AC85: GIVEN each new path WHEN telemetry toggles vary THEN R8 event/error families match paired routes or approved exceptions, disabled export suppresses them, and logs/events redact secrets, targets, and IDs.
- AC87: GIVEN AC164 WHEN JDBC sends valid ASCII text on each side of Scala's character limit and multibyte text below that limit on each side of Reyder's byte limit THEN outcomes match the measured routing boundary and size error without split or fallback. Record APC Thrift and warehouse SEA outcomes; a different boundary blocks dependent batch gates.
- AC88: GIVEN two catalogs and a metric view WHEN catalog and metric-view settings vary on three live routes THEN discovery, table types/rows, and APC session config follow R5; record server-limited impact.
- AC89: GIVEN live invalid SQL, missing object, or permission failure on APC SEA, APC Thrift, and warehouse SEA WHEN each exposed method runs THEN record its SQLException class, SQLState, vendor code, and server/driver origin in one release mapping table. Matching baseline fields stay equal; every differing field follows warehouse SEA only with a recorded server limit or warehouse precedent, affected caller behavior, and PECO approval before release. Failure never becomes success.
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
- AC112: GIVEN AC119 WHEN download-off SELECT drains at both bounded settings THEN all pages remain inline and lossless; initial size failure blocks ResultSet success and later failure interrupts iteration.
- AC113: GIVEN deployed T2 WHEN direct/polled no-result and counted commands run on scoped staging THEN raw captures prove R4 terminal shapes/errors, including zero, before T14 caller exposure.
- AC114: GIVEN two-parent and second-foreign-table keys WHEN live imported/cross-reference methods run THEN each returns its filtered paired JDBC rows; null foreign table is typed empty and invalid input fails before request.
- AC119: GIVEN deployed T8 WHEN session disables cloud download but statement advertises it THEN live single/multi-page results remain inline; capture both wire values before AC112.
- AC117: GIVEN paired bound SELECT callers WHEN pre-execution `PreparedStatement.getMetaData()` runs THEN metadata matches, bindings survive, and request/latency/throughput/heap load passes.
- AC118: GIVEN live `EnableSQLValidationForIsValid=1` callers WHEN `isValid(timeout)` runs `SELECT VERSION()` repeatedly THEN booleans, timeout, usability, request count, latency, call-rate, and heap pass paired load.
- AC122: GIVEN live two-parent and second-foreign-table keys WHEN typed async imported-key and cross-reference handles complete THEN each independently passes AC114 and the per-handle AC50/61 gates.
- AC124: GIVEN synchronous SHOW or typed async metadata WHEN `MetadataOperationTimeout` varies THEN positive values bound full call/await and cancel; `0` removes only the overall bound, in-budget calls survive shorter request timeouts, and timed-out connections remain usable.
- AC132: GIVEN AC87 and a live legacy INSERT chunk near the deployed limits WHEN ASCII text crosses Scala's character limit or multibyte text below that limit crosses Reyder's UTF-8 byte limit THEN each in-limit chunk succeeds and each over-limit chunk fails at the measured gate without client split/fallback; committed rows and counts match captured requests.
- AC136: GIVEN a live APC result with unread server continuation or linked data and heartbeat enabled WHEN one row is read, the cursor idles beyond two heartbeat intervals and the configured expiry window beside APC Thrift, and both cursors drain THEN no APC `/status` request occurs, all remaining rows match, and survival time is recorded. A shorter APC lifetime requires R3 exception approval.
- AC138: GIVEN deployed T3 WHEN raw synchronous/async `GET_CATALOGS` and an ordinary SELECT run THEN the shared server-readiness gate proves header forwarding and native Arrow IPC.
- AC139: GIVEN deployed T4 WHEN raw `GET_SCHEMAS` Arrow/HIVE runs THEN it passes the shared server-readiness gate before JDBC schema exposure.
- AC140: GIVEN deployed T5 WHEN raw `GET_COLUMNS` Arrow/HIVE runs THEN it passes the shared server-readiness gate before JDBC column exposure.
- AC141: GIVEN deployed T6 WHEN raw `GET_PRIMARY_KEYS` Arrow/HIVE runs THEN it passes the shared server-readiness gate before JDBC primary-key exposure.
- AC142: GIVEN deployed T7 WHEN raw HIVE `GET_CATALOGS` with a repeatable null cell runs THEN it passes the shared server-readiness gate, including its null mask; record DBR version/configuration.
- AC144: GIVEN T37's scoped staging hook WHEN 429/503 with and without `Retry-After` fault session/read requests and read-only ExecuteStatement on APC/warehouse SEA THEN captures prove original target, fault point, mutation, and unrelated-caller isolation before the corresponding JDBC AC51 gate. Accepted writes use T46/AC163.
- AC145: GIVEN linked SELECT with hybrid and bounded settings at `0`/`1` WHEN paired routes run THEN APC stays targeted/hybrid with paired rows while warehouse hybrid `0` is external-only; bytes, latency, throughput, and heap pass before exposure.
- AC146: GIVEN AC113 and isolated table WHEN live prepared INSERT/UPDATE/DELETE/MERGE run, including zero count THEN each yields one counted request, paired counts/rows/errors, `execute=false`, and no ResultSet under AC154/load.
- AC147: GIVEN live IN-only CALL with rows, no result, or failure WHEN execute/executeQuery runs THEN R18 JDBC shapes and paired rows/errors hold; unsupported OUT/named/update forms reject before request.
- AC148: GIVEN flag-on APC with explicit SEA and catalog-pattern preference WHEN live `getCatalogs` and `getTables` run THEN each captures SEA SHOW, paired rows, and ignored-pattern diagnostic at first exposure and release.
- AC151: GIVEN all JDBC slices are live-gated WHEN the final release PR updates docs and `NEXT_CHANGELOG.md` THEN they describe the complete delivered behavior and accepted-write uncertainty. The stack, including T44, remains undistributed until all parity gates close. Intermediate PRs use `NO_CHANGELOG=true` only while no partial build is published; any independent release needs its own changelog and a revised parity plan.
- AC152: GIVEN saved APC SEA session and raw statement IDs WHEN a connection flag or target changes THEN saved close uses the original session target and recovered statement uses the current connection; captures prove both outcomes without ID leakage.
- AC154: GIVEN AC113 and T46's scoped post-acceptance write/count fault hook WHEN APC and warehouse SEA writes commit before their response is faulted THEN captures prove target, committed rows, mutation, unrelated-caller isolation, and no APC-only replay; missing counts fail and accepted writes report uncertain commit before each write exposure.
- AC155: GIVEN AC168 and the connection slice on scoped ON/OFF APC and warehouse routes WHEN paired callers open and close without SQL THEN ON terminates its created session, OFF follows R1, and the shared load gate passes. Rejected TerminateSession leaves ordinary close locally successful with a redacted warning. AC25 covers saved IDs separately.
- AC156: GIVEN T43 exposes download-off at both bounded settings WHEN primitive/complex results drain THEN APC `ARROW_BATCH` matches warehouse JSON/Arrow values, enabled representable typed getters work, and transfer/getter differences pass AC31's paired load gate.
- AC157: GIVEN HIVE null-mask and unknown-vector fixtures WHEN decoded THEN null positions and row count follow R13, unknown vectors fail before partial success, and a later valid result works; AC16 proves live HIVE parity.
- AC158: GIVEN unknown numeric SEA format fixture WHEN deserialized and decoded THEN decoding raises a named driver error before ResultSet success and a later valid SELECT still works.
- AC159: GIVEN first-slice single-chunk APC SELECT with no unread server page and heartbeat enabled WHEN a partly read ResultSet idles beyond two heartbeat intervals THEN no APC `/status` request is sent; cursor survival remains gated by AC136 once unread server data exists.
- AC160: GIVEN T45's scoped link hook WHEN 429/503 with and without `Retry-After` fault direct APC and warehouse SEA presigned-link fetches THEN captures identify external URL, original target, mutation, and unrelated-caller isolation before linked JDBC AC51.
- AC161: GIVEN two null-catalog schema/column requests with independently delayed catalogs WHEN the overall metadata timeout is disabled THEN a catalog available within its 90-second collection allowance is returned even if another completed earlier; an over-budget catalog fails within that limit. A positive overall timeout ends the call earlier when applicable, and the connection remains usable. Compare APC and warehouse SEA rows and elapsed times.
- AC162: GIVEN T54's scoped fan-out delay hook WHEN two ordered metadata requests and an unrelated caller run THEN captures prove independent delays, original APC/warehouse targets, and isolation before JDBC schema/column exposure.
- AC163: GIVEN T46's scoped staging post-acceptance write/count hook WHEN raw APC and warehouse SEA writes are faulted after acceptance THEN captures identify target, committed rows, response mutation, and unrelated-caller isolation before JDBC AC154 closes.
- AC164: GIVEN deployed T2 WHEN raw scoped ASCII requests straddle Scala's character limit THEN the over-limit request fails before Reyder and the in-limit request reaches it. WHEN multibyte requests remain below Scala's character limit but straddle Reyder's UTF-8 byte limit THEN captures show Reyder acceptance and rejection respectively. Record character/byte counts, fault origin, and other-route outcomes before JDBC AC87.
- AC165: GIVEN T12/T32/T42 expose linked and bounded modes WHEN both streaming switches and CloudFetch speed thresholds vary on live inline and linked SELECT THEN rows match, link expiry and slow-link diagnostics follow R17, and AC39's paired load gate passes. An absent-total multi-link response at unbounded/streaming-off drains fully within the same memory bound.
- AC166: GIVEN the first live APC SEA SELECT slice before T44 WHEN public `getConnectionId()` is called after opening and after a successful SELECT THEN both calls raise `UNSUPPORTED_OPERATION` (1005) without an ID or another network request; ordinary close targets the original cluster and a later valid connection remains usable. APC Thrift and warehouse SEA IDs still work. T44 replaces this boundary only after AC25/152 pass.
- AC167: GIVEN T43 exposes download-off WHEN live APC and warehouse SEA SELECTs run at both bounded, cloud-download, and hybrid settings THEN request/session fields and formats follow R4/R11/R17, rows match, and AC31/39's paired transfer/load gate passes at one/32 callers without truncated success.
- AC168: GIVEN the tracked server termination binding fix is deployed WHEN a raw APC CreateSession and TerminateSession use the same cluster target THEN the created session is removed; a rejected termination remains identifiable for JDBC AC155.
- AC169: GIVEN deployed T50 WHEN raw `GET_TABLES` Arrow runs THEN it passes the shared server-readiness gate before JDBC table exposure.
- AC170: GIVEN deployed T53 WHEN raw HIVE `GET_TABLES` with a repeatable null cell runs THEN it passes the shared server-readiness gate, including its null mask; record DBR version/configuration.
- AC171: GIVEN deployed T51 WHEN raw `GET_FUNCTIONS` Arrow/HIVE runs THEN it passes the shared server-readiness gate before JDBC function exposure.
- AC172: GIVEN deployed T52 WHEN raw `GET_CROSS_REFERENCE` Arrow/HIVE runs THEN it passes the shared server-readiness gate before JDBC cross-reference exposure.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- The final release package alone edits `docs/` and `NEXT_CHANGELOG.md`.
  Intermediate stacked PRs are internal integration changes and use
  `NO_CHANGELOG=true` only while they remain undistributed (AC151).
- Each JDBC slice closes a live paired workflow gate and AC40/85/89 for its
  exposed callers. Keep each
  PR below roughly 1,500 changed lines and split an implementation package if
  that review limit would be exceeded.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T1–T8, T50–T53, and staging-only T37/T45/T46/T54; known gaps SC-256188 under SC-256186
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

`plan.json` assigns each task once. JDBC and universe each have a repository-local
stacked tree because a PR can branch only from a parent in its own repository.
The plan names required deployed universe gates in each dependent JDBC package;
those gates must pass before that JDBC package is considered complete. Server
contract and scoped fault packages precede the first JDBC workflow they gate; each
JDBC package closes a live paired workflow. Metadata packages pair synchronous
and async forms of one SHOW operation. Each tree serializes some independent
work so release inherits every prerequisite. It exceeds the four-to-seven JDBC
package aim because the approved surface spans distinct result, SQL, and
metadata workflows that cannot fit reviewable diffs. The partial stack is
internal only under R18/AC151. The keys package closes AC54 only with rows or
an approved exception. The final JDBC package alone edits docs and
`NEXT_CHANGELOG.md` and hands post-merge smoke to integration.

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

Each task has one package in `plan.json`; each JDBC package closes its named
deployed server and live paired gates before its caller is exposed.

### Server contracts and validation prerequisites

- T1: Add the default-OFF APC client-config sub-flag with partner exclusion.
- T2: Establish APC terminal command/count responses and verify Scala's character and Reyder's UTF-8 byte limits on raw deployed APC requests.
- T3: Forward native SHOW headers and return GET_CATALOGS Arrow.
- T4: Return GET_SCHEMAS Arrow/HIVE.
- T5: Return GET_COLUMNS Arrow/HIVE.
- T6: Return GET_PRIMARY_KEYS Arrow/HIVE.
- T7: Return GET_CATALOGS HIVE with repeatable null cells.
- T8: Make CreateSession download-off override APC per-statement cloud capability.
- T50: Return GET_TABLES Arrow.
- T51: Return GET_FUNCTIONS Arrow/HIVE.
- T52: Return GET_CROSS_REFERENCE Arrow/HIVE.
- T53: Return GET_TABLES HIVE with repeatable null cells.
- T37: Provide scoped session/read API faults in staging and verify raw APC termination after the tracked server binding fix.
- T45: Provide scoped presigned-link fetch faults in staging.
- T46: Provide scoped post-acceptance write/count faults in staging.
- T54: Provide scoped delayed metadata fan-out in staging.

### JDBC connection and result workflows

- T9: Open and close flag-selected APC SEA connections with a concrete cluster target; reject public saved IDs with the R9 error until T44.
- T10: In the first JDBC package, run a single-chunk inline APC SELECT through polling, fetch, timeout/cancel, and close.
- T11: Drain multi-batch inline chunks and server continuation without duplicate rows.
- T12: Drain linked Arrow IPC chunks through final-link continuation.
- T13: Preserve primitive, logical, complex, and geospatial getters across result modes.
- T32: Apply direct, bounded, hybrid, and max-row settings to exposed SELECT modes.
- T41: Honor result codec and SEA HTTP response-compression settings.
- T42: Complete linked worker, prefetch, pool, retry, and wait bounds before T12 exposes links; retain existing warehouse SEA controls where they pass the live gate.
- T43: Keep download-off SELECT results inline across pages and bounded settings.
- T44: In a separate saved-close slice, encode APC SEA saved session targets and close them through `Driver.closeConnection()`.

### JDBC SQL, metadata, and release workflows

- T14: Expose terminal no-result and counted command shapes without a false ResultSet.
- T15: Execute legacy prepared INSERT batches with ordered chunks and counts.
- T16: Decode and expose synchronous catalog SHOW, including HIVE null masks.
- T17: Expose synchronous schema SHOW with catalog filtering.
- T18: Expose synchronous column SHOW with paired errors.
- T19: Run parameterized procedure metadata through a header-free bound SELECT.
- T20: Expose a typed async catalog SHOW handle.
- T21: Expose a typed async schema SHOW handle with fan-out and deadlines.
- T22: Expose a typed async table SHOW handle.
- T23: Expose a typed async column SHOW handle with fan-out and deadlines.
- T24: Expose a typed async function SHOW handle.
- T48: Expose synchronous table SHOW with catalog filtering.
- T49: Expose synchronous function SHOW with paired errors.
- T25: Expose a typed async primary-key SHOW handle.
- T26: Expose typed async imported-key and cross-reference SHOW handles.
- T27: Execute opt-in V10 native prepared INSERT batches with uncertain-count handling.
- T28: Parse and retain CreateSession's negotiated protocol capability; bind scalar SELECTs at V8+ and missing/pre-V8 capabilities.
- T29: Expose public async SQL SELECT, await, and cancel.
- T30: Preserve transaction and autocommit state and errors.
- T31: Execute counted prepared DML with zero-count behavior.
- T33: Execute supported CALL forms and reject unsupported forms before request.
- T34: Execute sequential Statement batches with ordered failure counts.
- T35: Expose synchronous primary keys with a paired live gate.
- T55: Expose synchronous imported and cross-reference keys with paired live gates.
- T47: Deliver exported-key rows through a supported parent-only lookup, or link measured server-limit exception and PECO approval before the keys package closes.
- T36: Publish delivered behavior and accepted-write guidance in docs and changelog.
- T38: Return pre-execution prepared SELECT metadata without consuming bindings.
- T39: Validate live `isValid(timeout)` through APC-targeted SQL.
- T40: Preserve command error identity and telemetry across direct and polled completion.
- T-val: Repeat assembled pre-integration flag, parity, resource, and fault gates;
  hand the smoke checklist to integration for execution after merge.
