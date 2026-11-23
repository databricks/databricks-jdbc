---
service: databricks-jdbc
created: 2026-10-28
---

# Add All-Purpose Cluster (APC) support over SEA in the JDBC driver

## Intent

Let the OSS JDBC driver run all-purpose cluster (APC) workloads through the SQL Execution API
(SEA), which routes APC requests through Reyder to DPR and DBR. This brings APC connections the same
Arrow/CloudFetch result path, telemetry, and future feature work as SQL warehouses, and unblocks
retiring the direct-Thrift APC path. Routing is controlled by a dedicated APC server flag so APC
can ramp and roll back independently of warehouses, with an explicit client setting taking
precedence so customers can force either protocol.

## Context

- SEA routes APC `backend_compute` through Reyder directly to DPR behind
  `databricks.sqlexecapi.enable_apc_reyder_routing`. APC polling uses
  `GetStatementResult`; native metadata intent, synchronous behavior, and
  per-hop result shape require deployed server proof (AC138).
- [SC-256188](https://databricks.atlassian.net/browse/SC-256188) under
  [SC-256186](https://databricks.atlassian.net/browse/SC-256186) tracks native
  metadata shape. [SC-258842](https://databricks.atlassian.net/browse/SC-258842)
  tracks terminal DDL; [SC-258787](https://databricks.atlassian.net/browse/SC-258787)
  tracks the CreateSession inline-only override. SC-254478 covers per-statement
  config, not that override. Termination binding remains a separate server gate.
- Connector-service returns sub-flags at
  `/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`. The dedicated
  APC flag is absent on the inspected server branch; T56/T57 track its
  definition and partner exclusion from flag-selected APC routing under
  [SC-256186](https://databricks.atlassian.net/browse/SC-256186).
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
  (API contract, Phase II, metadata). AC164 verifies the deployed SQL-size
  boundary; server rollout begins opt-in and rejects non-OSS-JDBC clients.
- Driver baseline: `origin/main` @ `840f3a407` (spec PR #1809; bounded SEA default
  PR #1827). APC selects Thrift before the explicit client override. Warehouse
  SEA is warehouse-targeted and supports `ARROW_STREAM`/`JSON_ARRAY` results;
  its result dispatch has no `ARROW_BATCH` or HIVE `columnar_data` path. Public
  APC session IDs currently assume Thrift, and ordinary serialized statement
  IDs carry no APC target; internal Real-time warehouse statement IDs can carry
  a warehouse ID. The SEA CreateSession response model does not read
  `negotiated_protocol_version`. These are implementation gaps for the tasks
  below, not a change to the server contract. `DatabricksConnection` currently
  implements `setCatalog` with `SET CATALOG` when multiple-catalog support is
  enabled, `setSchema` with `USE SCHEMA`, and uncached `getCatalog`/`getSchema`
  with `SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()`. Statement and
  PreparedStatement share the `DatabricksResultSet` update-count path.
  Public `DatabricksDatabaseMetaData.getFunctions`, `getProcedures`, and
  `getProcedureColumns` currently catch initial query errors and return typed
  empty results; procedure iteration can still fail after return. T19 changes
  the two procedure entry points for APC SEA to honor Q11.
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the
  prototype baseline; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756) is unchanged.
- Parity uses one driver build on all routes. The shared gate below governs
  differences and exception approval.

  | Behavior | APC Thrift | Warehouse SEA on `origin/main` | APC SEA; reason and caller impact | Disposition |
  | --- | --- | --- | --- | --- |
  | Arrow unavailable | Thrift works | Explicit SEA may precede Arrow check | Select Thrift: APC needs Arrow batches, so explicit SEA cannot be honored. | APC format exception, AC1. |
  | Legacy `cliservice` URL | Thrift with `default` target | Concrete warehouse target | Stay on Thrift even with explicit SEA; no concrete APC ID exists. | Target exception, AC1. |
  | Close and saved IDs | Ordinary close is best effort; saved close propagates | Warehouse ID | Reject APC saved IDs until T44. | Temporary opt-in boundary, AC25/166. |
  | Result codec | Honors requested codec | Forwards codec preference | Reyder sends LZ4 even for `NONE`; decode actual manifest codec. `NONE` cannot disable compression. | Server exception, AC101. |
  | Download/complex getters | Typed values | Hybrid `0` external-only; download-off JSON or Arrow | Hybrid `0` remains hybrid; use Arrow batches and representable getters. | Reyder transfer exception, AC13/112/145/171. |
  | Absent link totals with `UseBoundedSeaApi=0`, `EnableStreamingChunkProvider=0` | No SEA setting | Uses the non-streaming link provider when both switches are `0`; bounded `1` forces streaming | Use bounded streaming even with both switches `0`: Reyder may omit totals. Callers retain complete rows but may see different prefetch, memory, and throughput. | APC continuation exception, AC14/39/46/165. |
  | `UseBoundedSeaApi=0/1` and download off | No SEA setting | Changes `JSON_ARRAY`/`ARROW_STREAM` format and delivery | Keep APC `ARROW_BATCH` and hybrid targeting at both settings; download off requests inline-only via session config. Callers may see different transfer cost and getter behavior. | Reyder format exception, AC39/112/145. |
  | SEA API response compression | Thrift HTTP compression setting | `EnableSeaResponseCompression=0` sends `Accept-Encoding: identity`; `1` permits API compression, while link headers are separate | Follow warehouse SEA for API responses and preserve link headers. Callers may see changed network bytes, decode CPU, or throughput. | Warehouse precedent, AC45/31. |
  | `RowsFetchedPerBlock` | Controls Thrift fetch size | SEA pages by server continuation | Does not change APC SEA paging; callers lose this page-size control. | Warehouse precedent, AC31/39. |
  | `EnableInlineStreaming` | Selects Thrift inline reader | No SEA effect | Has no APC SEA effect; callers cannot use it to change inline memory behavior. | Warehouse precedent, AC39/109. |
  | `ThriftMaxBatchesInMemory` | Bounds Thrift batches | Bounds SEA inline chunks | Bounds APC inline chunks, each possibly containing multiple batches; peak memory may rise. | Warehouse precedent and APC chunk shape, AC109. |
  | API and link retry controls | Thrift requests use HTTP retry settings | SDK API retry is separate from presigned-link retry | APC API calls use SDK policy; link `Retry-After`, `ApiRetriableHttpCodes`, and pool controls govern links. Callers may see different retry counts, delay, or an uncertain write. | Warehouse precedent, AC51/154. |
  | Other transport controls | Thrift HTTP pool and heartbeat | SDK/API pool, link HTTP pool, `/status` heartbeat | APC uses separate API/link pools and suppresses heartbeat; waits, peak memory, and cursor lifetime may differ. | Warehouse precedent and APC status limit, AC30/46/84/136/165. |
  | Null-catalog `getTables` on `42601` | Native RPC may return multiple catalogs | SHOW returns typed empty | Return typed empty; measure omitted Thrift rows and table types. | Warehouse precedent, AC105. |
  | Null-catalog `getFunctions` | Native RPC/Thrift SHOW differ | SHOW resolves current catalog | Use SHOW; measure gained current-catalog and lost other-catalog rows. | Warehouse precedent, AC82. |
  | Exported keys | Sends parent-only `TGetCrossReferenceReq`; the metadata design reports empty DBR results, so nonempty rows are not an established baseline | Typed empty; no parent-only SHOW | Return typed empty when the paired DBR version does; if Thrift returns rows, require a supported lookup or an approved, measured loss. | Versioned server gap, AC54. |
  | Native-metadata setting `unset`/`0`/`1` | RPC/SHOW choice | Unset follows server flag; explicit setting controls header | APC SHOW is always native. Unset/`1` sends the header and `0` omits it, but Reyder ignores that header for APC; callers cannot use the setting to change APC metadata routing. | APC contract exception, AC65/138. |
  | Partial fan-out | Native metadata RPC has no client-side per-catalog fan-out | Omits a catalog whose SHOW fails with `SQLException` | Omit only that catalog; callers may lose its rows. | Warehouse precedent, AC83. |
  | Transactions and connection catalog/schema methods | `IgnoreTransactions` and catalog support settings govern commands; cached getters are local | Uses the same connection methods and command/result path | Preserve state, commit/rollback and cache updates on success; failed commands leave state unchanged. | Match, AC102/172/173. |
  | Async SELECT and Statement batches | Async statement lifecycle; sequential batch SQL with stop-on-error | Async SEA lifecycle; sequential batch SQL with stop-on-error | Target APC on each request and preserve rows, counts, cancellation and error order. | Match, AC22/55/66. |
  | Query limits and timeouts | `setMaxRows`, query timeout and cancellation apply | SEA `row_limit`, client wait and cancel apply | Preserve row cap and caller timeout; APC polls and cancellation carry the cluster target. | Match, AC18/49/60. |
  | Procedure/function metadata | RPC/SHOW switch; public wrappers can return typed empty on initial failure | Public wrappers return typed empty on initial failure | APC SEA propagates initial and later procedure errors under Q11; public `getFunctions` retains typed empty on initial failure, while later iteration can fail. Procedure callers gain visible errors. | APC procedure exception; function warehouse precedent, AC75/82/89. |
  | Assignment `execute` | `SET` classifier exposes ResultSet | Same classifier | No-result assignment returns false; caller boolean changes. | Terminal-wire exception, AC52. |
  | Session version | Thrift handle | `FAST` and version | `DEFAULT`, no absent version echo; capability handling differs. | Server contract, AC23. |
  | Parameter binding and INSERT tuning | Scalar interpolation and eligible combined INSERTs | Native scalar at V8 and opt-in native sets at V10 | Negotiate separately from session version; reject complex bindings before request. Native-scalar legacy chunks ignore `BatchInsertSize`; aggregate V10 counts become `SUCCESS_NO_INFO`. Callers using complex values, chunk tuning, or exact counts see a changed outcome. | Server contract and safe binding exception, AC17/29/53/108. |
  | Catalog-pattern preference with explicit SEA | Thrift native catalog pattern | Explicit SEA ignores this Thrift-only setting | Explicit SEA wins on APC; SHOW uses normal catalog semantics and logs the ignored setting. Callers relying on pattern matching must select Thrift. | Warehouse precedent, AC1/148. |
  | SQL text limit | Direct Thrift bypasses the Scala and Reyder SEA checks; its own request-size outcome is captured live | Scala UTF-16 check applies to warehouse SEA | APC bypasses Scala; Reyder's UTF-8 byte limit governs, changing non-ASCII acceptance. | APC route exception, AC164. |
  | Accepted writes | Thrift waits and errors | SDK and JDBC waits | Follow R14/R17; an accepted write may have an ambiguous response, so callers must reconcile before retry. | Warehouse precedent, AC154. |
  | Error mapping | Thrift class/SQLState/vendor code | SEA mapping | Preserve fields that agree; otherwise follow SEA. Callers matching changed fields can break. | Map disagreement and approve impact, AC89. |
  | Telemetry | Thrift event families | SEA event families | Follow R8 route and error identity with redaction; caller dashboards can distinguish paths. | SEA precedent, AC10/85/106. |

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
T57 excludes partner agents only from the dedicated flag's automatic assignment.
An explicit `UseThriftClient=0` still selects APC SEA for a partner agent when
the other R1 prerequisites hold; it is not a partner authorization check.
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

Successful no-result DDL, `SET CATALOG`, `USE SCHEMA`, `SET AUTOCOMMIT = TRUE/FALSE`,
`COMMIT`, `ROLLBACK`, and no-result `CALL` have no terminal schema, chunks, links,
continuation, or data (manifest may be absent), distinct from a zero-column schema.
A failed `SET CATALOG` or `USE SCHEMA` returns a terminal error, never no-result
success; the driver preserves the previous catalog/schema cache. Direct and
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
(AC101). The first live SELECT slice must decode Reyder's manifest-marked LZ4
inline result before exposing SELECT; T41 covers codec settings, aliases and API
response compression, and T12 covers linked-result variation. For failed SQL,
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
metadata-operation and fully-sync intent through Reyder to DPR and preserve the
requested synchronous behavior; forwarding HTTP headers unchanged at every hop
is not required. Reyder selects native Thrift metadata unconditionally for these
APC SHOW operations: it ignores `X-Databricks-Require-Thrift-Native-Metadata`
for APC, including when the client omits that header. The setting cannot switch
APC metadata to a non-native route. The current downstream allowlist excludes
require-native and fully-sync, so AC138 proves the effective native and sync
decisions at each hop. T58 must also admit `ARROW_STREAM` on APC metadata requests
while retaining `ARROW_BATCH` for ordinary APC SQL; current APC normalization
accepts only `ARROW_BATCH`.
SHOW is inline/link-free; consume `next_chunk_index` through R12 if present,
though pagination is not promised (AC16/61).
On APC SEA, unset or explicit `EnableThriftNativeMetadata=1` sends
`X-Databricks-Require-Thrift-Native-Metadata: true` for supported SHOW; `0`
omits it. Unset warehouse SEA depends on its server flag, even when an explicit
override selects SEA.
Reyder always returns native-shaped APC SHOW metadata, so conversion follows
`manifest.is_native_metadata_result`, not the request setting. Synchronous
SHOW sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only at
`EnableSeaSyncMetadata=1`; async SHOW omits it. Decode each response as Arrow
IPC `chunk.attachment` or HIVE `chunk.columnar_data`/`COLUMN_BASED_SET`.
`getImportedKeys` and `getCrossReference` share `GET_CROSS_REFERENCE`; the latter
filters a foreign table's keys by parent. A null foreign table returns typed
empty before request; null/empty parent or empty foreign table is rejected.
At the public JDBC entry points, initial `getFunctions` errors return typed
empty results as on warehouse SEA; errors while consuming a returned result
still propagate. Catalog/primary-key syntax errors propagate; null-catalog
schema/column syntax errors fan out, while table syntax
errors return typed empty without fan-out; imported/cross-reference syntax
errors return typed empty. Object-not-found is typed empty except for catalog
errors. Other SQL failures propagate except initial public `getFunctions` errors
and existing table/column empty-pattern and fan-out behavior (AC16/83/105/114).

`enableMultipleCatalogSupport=0` retains current-catalog filtering; `1`
permits eligible catalogs. `EnableMetricViewMetadata=1` adds
`spark.sql.thriftserver.metadata.metricview.enabled=true` to session config
and exposes `METRIC_VIEW` table types and eligible rows; `0` omits both.
`getExportedKeys` has no parent-only native SHOW. On `origin/main`, APC Thrift
sends a parent-only `TGetCrossReferenceReq`; that client request does not prove
DBR returns rows. The metadata design reports an empty DBR result for this
request, and warehouse SEA returns typed empty. T47 returns typed empty when
the paired, versioned APC Thrift result is empty. If a supported DBR version
returns rows, T47 must use a supported parent-only lookup or record measured
loss, server limit, warehouse precedent, caller impact, and PECO approval before
release. An unapproved loss of observed rows cannot close the keys package.
Catalog filtering at `enableMultipleCatalogSupport=0` may first run
`SELECT CURRENT_CATALOG()`; at `1` it needs no lookup. A null table fails before
request with SQLState `42000` and `EXECUTE_STATEMENT_FAILED` (1003).
Session-reachable async metadata provides typed ResultSet handles for seven
SHOW operations, with separate imported/cross-reference calls. It submits at
`wait_timeout=0s`, returns while RUNNING, and preserves await, timeout, cancel,
and conversion. Version/saved IDs do not gate it; public JDBC metadata stays synchronous.

Internal catalog/schema and parameterized procedure SELECTs use hybrid
`ARROW_BATCH` without metadata headers; Reyder rejects procedure headers.
Procedure queries use R6 binding. For APC SEA, public `getProcedures` and
`getProcedureColumns` propagate initial query errors as well as subsequent
fetch/decode errors, with their SQLState/vendor code; only the existing
catalog-access filter returns typed empty (AC75). This is a deliberate APC
exception to the current public warehouse SEA wrappers, which swallow initial
errors; callers see failures that were previously empty results.
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
size error. AC164 gates dependent batches against APC Thrift/warehouse evidence.
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
measured caller impact before release (AC13/112/171).
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
- R17: APC SEA follows warehouse SEA's retry, pool, and linked-download behavior.
APC links use continuation because totals may be absent. `EnableStreamingChunkProvider=1`
selects streaming links; `UseBoundedSeaApi=1` already forces it on warehouse SEA.
At both bounded settings, APC uses bounded streaming when link totals are absent,
including when the streaming switch is `0`; warehouse SEA uses its non-streaming
provider when both switches are `0`. `EnableInlineStreaming` affects Thrift
inline readers only; `CloudFetchSpeedThreshold` affects link diagnostics only.
Neither changes rows (AC39/165). `ThriftMaxBatchesInMemory` limits inline chunks
(minimum two), versus batches on APC Thrift; a chunk may contain multiple batches.
Avoid whole-result buffering (AC109). Case-insensitive `cloudFetchThreadPoolSize`
(default 16) bounds per-result link workers and buffered chunks;
`LinkPrefetchWindow` (default 128) bounds fetched-ahead links.
`ChunkReadyTimeoutSeconds` bounds linked-chunk wait; `RowsFetchedPerBlock` does
not control SEA paging.

Presigned links use the Thrift HTTP pool. `RateLimitRetry` and
`TemporarilyUnavailableRetry` require `Retry-After` for ordinary 429/503 retries;
`ApiRetriableHttpCodes` uses `ApiRetryTimeout` without it. API calls use the
warehouse SDK retry policy, independent of link controls;
`IdleHttpConnectionExpiry` applies to both pools. SDK 0.118.0 may retry an
accepted 429/503 write POST. Surface its ambiguous-response SQLException without
APC replay or at-most-once claims. Callers must reconcile before resubmitting
(AC51/154). No APC-only cap or full-result buffer is allowed (AC30/46/84).
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

On APC SEA before T10, `Connection.getCatalog` and `getSchema` return an already
cached session value locally; if uncached, they reject before requesting current
catalog/schema. T10 exposes the uncached lookup through a targeted SELECT and
updates both cached values only after a valid row. Before T14,
`Connection.setCatalog` with `enableMultipleCatalogSupport=1` and `setSchema`
reject before their commands; disabled `setCatalog` remains the existing local
no-op. T14 exposes enabled `setCatalog` and `setSchema` through the terminal
command path and updates the cache only after success. Failures retain the
previous cache and leave the connection usable (AC172/173). These boundaries apply
to explicit SEA on every intermediate build and to flag-selected SEA at release.

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
- Q5: Denylist govcloud/unsupported APC SEA regions in the client? A5: No; keep the flag off there and propagate a selected SEA rejection.
- Q6: Add a special User-Agent or capability header? A6: No; ordinary User-Agent, path, and `backend_compute` identify APC.
- Q7: Is manifest `type_text`/`type_json` sufficient for Arrow types? A7: AC13 proves lossless reconstruction; otherwise block release for a Reyder `arrow_schema` contract and driver support. Do not coerce or fall back.
- Q8: Share `enableSqlExecForJdbc`? A8: No. Add default-off `databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc`; older drivers ignore it, and no connector-service version filter is needed.
- Q9: Exclude partners despite Reyder's OSS prefix allowlist? A9: Exclude partners, including allowlisted JDBC agents and those on the warehouse flag, from automatic assignment of the dedicated APC flag. Deploy that gate before assignment; explicit `UseThriftClient=0` keeps R1 precedence, and partner enablement through the flag needs its own decision.
- Q10: Avoid transfer during result polling? A10: At least three RUNNING polls transfer no payload; terminal fetch returns rows once (AC27).
- Q11: Suppress procedure metadata failures? A11: No; propagate query, fetch, and decode SQLState/vendor code. This supersedes the earlier typed-empty-on-error wording.

## Acceptance Criteria

For every newly callable workflow, run scoped flag-ON APC staging and paired
APC Thrift/warehouse SEA before merge. AC40 covers its exposed and rejected
surface; AC11 protects its warehouse serialization; AC85 covers its telemetry;
AC89 covers its callable SQL/metadata
errors, not connection-only slices; AC106 applies in the PR that changes a
driver-owned error. AC144 applies only to the request/fault classes the slice
can issue. AC113/154 gate writes. Use deterministic client-response fixtures
for `42601` and a single failing fan-out catalog when scoped staging cannot
produce those server faults; retain live paired row/type/error and
connection-usability checks on ordinary SHOW responses. T-val checks the
assembled stack. These common gates are inherited by packages and need not be
repeated in each `plan.json` acceptance list.

The shared paired-load gate uses one build, warmed cluster, identical data,
and one/32 APC SEA and Thrift callers, all completing or returning bounded
errors. At 32 callers, SEA/Thrift completed operations per second are at least 0.90;
median/p95/slowest latency at most 1.10/1.20/1.50, peak heap at most 1.20.
For SELECT, record both received HTTP body bytes and decoded logical bytes;
download throughput uses decoded logical bytes per second for identical rows
and values; received bytes per second is diagnostic because APC LZ4 and Thrift
`NONE` can have different wire sizes. CPU per 1,000 rows and peak RSS are at
most 1.20 at both loads. Use a live, single-chunk inline result with a captured 0.5–2 MiB
response below the server's 5 MiB large-result threshold for the first SELECT
gate; record
its body size, rows, and completed calls per second without requiring an
8 MiB transfer. Multi-page inline uses an equally feasible below-threshold
result. The sustained download gate starts with linked delivery: each caller
drains an identical logical result of at least 64 MiB decoded data at 0.90 of
Thrift decoded bytes/s (or at most 1.10 times its drain time),
with 64/128 MiB decoded-result memory samples. AC32 inserts
1,024 disjoint rows per caller over at least two requests/chunks/sets at 0.90
of Thrift committed rows/s. Retained heap growth across two sizes is at most
max(8 MiB, 5% of added bytes). Record rows, bytes, duration, errors, CPU and
memory. Exceptions need same-build warehouse SEA/Thrift evidence, a server
limit/precedent, caller impact, and PECO/Perf Eng approval before release.

AC138 is the server-readiness gate for each SHOW before its JDBC method is
exposed: capture JDBC/SEA ingress headers, unconditional APC native selection
with require-native header present and absent, derived sync decision,
Reyder-to-DPR request and DPR response, native mark, and Arrow/HIVE typed rows
under direct and polled execution. Prove APC metadata accepts `ARROW_STREAM`
while ordinary APC SQL still accepts `ARROW_BATCH`. Header identity across hops
is not required.
Each exposed JDBC method also closes AC16 or AC50 and AC61.

- AC1: GIVEN flag-ON/OFF APC staging WHEN R1 overrides vary THEN protocol, target, partner flag-assignment exclusion, explicit partner override and no-fallback match R1; cover Arrow absence, legacy `cliservice` and flag lookup failure. AC2 proves rows.
- AC2: GIVEN flag-on/explicit-SEA APC WHEN single-chunk, RUNNING, manifest-only and empty SELECTs with nullable INT, BOOLEAN, and STRING run THEN the first slice decodes inline `ARROW_BATCH` schema, manifest-marked LZ4 payload and values into a usable ResultSet with paired rows/types; premature link/continuation or deferred logical type fails before ResultSet success with cleanup. AC145 covers alternate settings; AC89 closes invalid SQL, missing-object and permission failures before SELECT exposure.
- AC3: GIVEN a live APC statement WHEN it polls, fetches and closes THEN R2 target, exactly-once close, connection usability and warehouse serialization hold.
- AC9: GIVEN APC SEA CreateSession rejection or APC Thrift KP001 WHEN opening THEN the selected-route error/event propagates without APC fallback; warehouse-only KP001 recovery stays warehouse-only.
- AC10: GIVEN successful/failed opens on APC and warehouse SEA/Thrift routes WHEN logs and telemetry are captured THEN R8 identity, routing and toggle behavior hold with secrets and IDs redacted.
- AC11: GIVEN standard/Real-time warehouse connections WHEN a slice’s R2 operation class runs THEN JDBC outcome and URL/body/header serialization, including `realtimeSession`, match `origin/main`.
- AC12: GIVEN live SELECT at both direct-result settings WHEN callers drain it THEN `wait_timeout` is absent or `0s`, `statements` is absent, and paired rows, first-row latency, requests and AC31 load pass.
- AC13: GIVEN R11 values WHEN inline getters run with complex/geospatial support on and off THEN paired types/values and representable typed getters hold; disabled getters reject. Unavailable live types stay pending; linked getters close AC171 after T12.
- AC14: GIVEN live absent-total multi-link results WHEN JDBC drains them THEN R12 continuation returns each row once; Arrow IPC and presigned headers match paired link behavior.
- AC16: GIVEN AC138 for an exposed native SHOW WHEN its synchronous JDBC method runs THEN paired rows, types, nulls and errors hold except the route-specific AC82/105/114 outcomes; AC61 gates load.
- AC17: GIVEN live V10 batch success, execution failure or post-write count fault WHEN compared with paired routes THEN ordered `parameter_sets`, R6 counts, committed rows, batch clearing and uncertain-commit error hold.
- AC18: GIVEN an APC statement past `setQueryTimeout(n)` WHEN it times out THEN targeted CancelStatement raises the paired timeout SQLException within the expected elapsed time and leaves the connection usable.
- AC22: GIVEN a live Statement batch with middle failure or non-rowcount-prefix match WHEN APC executes THEN requests remain sequential without `statements`, prior counts retain order, and the paired batch error stops execution.
- AC23: GIVEN live APC CreateSession without `session_version` WHEN a prepared statement runs THEN `DEFAULT`, absent `session_version`, captured `negotiated_protocol_version` (if supplied), and missing-session-version diagnostics match R6 while warehouse FAST/version behavior stays intact.
- AC25: GIVEN saved APC/warehouse IDs WHEN flag or target changes before live `Driver.closeConnection()` THEN only the original session is targeted; termination rejection propagates, legacy IDs decode, malformed IDs fail without leakage, and R9's irreparable IDs are recorded.
- AC27: GIVEN a live single-chunk APC query with at least three RUNNING polls WHEN transfer is captured on flag-on and explicit SEA routes THEN polls transfer no payload and terminal fetch returns each row once.
- AC28: GIVEN T56's deployed APC flag SAFE configuration WHEN its definition and served
connector-service response are inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags`, defaults OFF, and is
independent of the warehouse flag.
- AC29: GIVEN flag-on APC and paired routes WHEN complex binding enters a single statement, legacy batch, or V10 batch THEN each has live named/numeric driver error before ExecuteStatement, no accepted write, and a usable connection; record taxonomy/dashboard ownership or handoff.
- AC30: GIVEN paired API/link-pool saturation WHEN pool size and acquisition wait vary THEN in-flight requests, waits, errors and completion meet the shared load gate; SDK acquisition ignores Thrift HTTP wait.
- AC31: GIVEN a below-threshold inline SELECT or a linked SELECT of at least 64 MiB decoded logical data per caller WHEN paired callers drain it THEN rows, errors, mode-specific throughput, CPU, heap, and RSS meet the shared paired-load gate; received and decoded bytes are reported separately, and varying `RowsFetchedPerBlock` does not alter SEA paging.
- AC32: GIVEN each batch mode WHEN paired callers insert disjoint keys THEN ordered counts and committed rows meet the shared batch-load gate.
- AC34: GIVEN T57's deployed flag-assignment exclusion and scoped ON/default OFF states WHEN
connector-service serves ordinary, partner, and allowlisted JDBC partner agents THEN only the
ordinary scoped agent gets true. A partner with explicit `UseThriftClient=0` still takes the
APC SEA route when R1 prerequisites hold; record both cases, flag version and T56/T57 evidence.
- AC36: GIVEN multi-batch Arrow chunks and inline continuation WHEN JDBC drains them THEN rows occur once, `row_offset` counts every batch row, and failed pages never succeed; if staging lacks multi-batch chunks, live proof stays pending.
- AC39: GIVEN each exposed result mode WHEN bounded and streaming switches vary on paired callers THEN requests and rows follow R17, with the shared load gate or its approved exception.
- AC40: GIVEN each slice under flag ON/OFF and explicit SEA WHEN exposed and unexposed calls run THEN exposed calls pass paired gates, unexposed calls reject before request, failed server shapes clean up without false success, and the connection remains usable; capture error name/code.
- AC45: GIVEN live APC/warehouse SEA inline or linked results WHEN `EnableSeaResponseCompression` is `0`/`1` THEN only `0` sends API `Accept-Encoding: identity`; link headers stay unchanged and paired rows, API bytes, decode CPU, and throughput pass the shared load gate.
- AC46: GIVEN linked results at one/32 callers WHEN worker, prefetch, and chunk-wait settings vary THEN workers/buffered chunks and fetched-ahead links respect R17 bounds; all callers finish or return bounded errors under the shared load gate.
- AC49: GIVEN live SELECT pages WHEN `setMaxRows` is zero, one, or a page boundary THEN positive limits send `row_limit` and cap all pages; zero is unlimited.
- AC50: GIVEN an exposed typed native SHOW handle WHEN delayed live SHOW submits at `wait_timeout=0s` THEN it returns while RUNNING and passes shared metadata, await, timeout, cancel, cleanup, and header gates before that handle merges.
- AC51: GIVEN AC144’s SDK/link 429/503 faults with and without `Retry-After` WHEN an exposed class retries THEN attempts, delay, final error and caller outcome follow R17 and paired routes; writes also pass AC154.
- AC52: GIVEN AC113 WHEN direct/polled no-result and counted commands, including zero, run live THEN R4 JDBC shapes and paired rows/errors hold except the documented assignment boolean; missing terminal evidence fails.
- AC53: GIVEN live scalar binding with negotiated V8+ and fixtures with missing/pre-V8 `negotiated_protocol_version` WHEN `supportManyParameters` varies THEN native/interpolated requests, `setBytes`, rows, and counts follow R6 without unresolved markers; absent `session_version` alone never downgrades a V8+ binding.
- AC54: GIVEN a parent referenced by a child foreign key on the current staging DBR and one earlier supported DBR WHEN paired APC Thrift/APC SEA `getExportedKeys` runs at both catalog settings THEN record DBR versions and returned rows. An empty Thrift result yields a typed empty APC SEA result; observed nonempty Thrift rows require a supported lookup or measured loss, server limit, warehouse precedent, caller impact and PECO approval before release. Null table fails before request with SQLState `42000`/code 1003.
- AC55: GIVEN live async SQL SELECT with primitive and linked results WHEN it completes or cancels THEN `wait_timeout=0s`, target, rows/errors, close, diagnostics, and AC31 load pass.
- AC60: GIVEN AC144 delays accepted `GetStatementResult` beyond request timeout but within JDBC timeout WHEN an exposed SELECT polls THEN the same ID returns every row once without re-execution or early timeout; record elapsed time and paired outcome.
- AC61: GIVEN an exposed nonempty metadata method or typed handle WHEN paired callers run THEN rows, types, nulls and errors follow AC16/50 and their documented route exceptions, and the shared load gate passes; null-catalog requests stay at most N+2, and decode CPU per 1,000 cells plus peak RSS stay at most 1.20 times Thrift absent approved exception.
- AC63: GIVEN AC144's scoped read-delay hook and synchronous SHOW with `EnableDirectResults=0` WHEN one `GetStatementResult` response exceeds the configured SEA request timeout but stays inside `MetadataOperationTimeout` THEN polling succeeds with paired rows and a usable connection; timeout `0` removes only the overall bound.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching INSERT runs through public Statement THEN `execute` returns true, its ResultSet contains the affected-row value, and `getUpdateCount` is `-1`.
- AC65: GIVEN AC138 and an exposed live synchronous SHOW WHEN native setting is unset, `0`, or `1` and sync, hybrid, and bounded settings vary THEN R5 header presence and unconditional APC native selection, plus AC16 rows, hold; AC148 closes the catalog-pattern override after `getTables` lands.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized `getProcedures` and `getProcedureColumns` WHEN initial query, later fetch or decode fails THEN APC SEA raises the underlying SQLException at the public call or iteration point with SQLState/vendor code; header-free successful rows pass AC61 and the catalog-access filter alone may return typed empty. Record the warehouse SEA public-wrapper difference and caller impact.
- AC82: GIVEN current/other-catalog functions and both SHOW-switch values WHEN live `getFunctions(null, ...)` runs THEN APC SEA matches warehouse SEA current-catalog rows; record rows gained over Thrift SHOW and omitted from Thrift native RPC. Explicit-catalog rows and the public typed-empty outcome for an initial failure pass paired gates; later iteration errors propagate.
- AC83: GIVEN paired live two-catalog schema/column rows and a deterministic client-response fixture returning `42601` for the null-catalog SHOW and `SQLException` for one named catalog WHEN synchronous fan-out is first exposed THEN APC and warehouse SEA omit only the failed catalog's rows, warn, and remain usable; timeout/interruption fixtures fail the call. Repeat the same gate for each async schema/column handle before its exposure. Record live paired rows/types/errors and the Thrift row delta at each stage; run the fault case live as well if scoped staging can produce it.
- AC84: GIVEN each new live SDK or link request class WHEN socket, poll, and idle settings vary THEN one-request timeout, later-poll delay, and applicable idle eviction follow R17, with paired timing/errors and usable connections.
- AC85: GIVEN each new path WHEN telemetry toggles vary THEN R8 event/error families match paired routes or approved exceptions, disabled export suppresses them, and logs/events redact secrets, targets, and IDs.
- AC88: GIVEN two catalogs and a metric view WHEN catalog and metric-view settings vary on three live routes THEN discovery, table types/rows, and APC session config follow R5; record server-limited impact.
- AC89: GIVEN invalid SQL, missing object and permission failures WHEN exposed SQL methods and metadata entry points run on three routes THEN record each public outcome (error or typed empty), SQLException class/SQLState/code and origin where raised. Equal baseline fields stay equal; deviations need warehouse precedent or the Q11 procedure exception, caller impact and PECO approval. `getFunctions` initial failures follow the public warehouse typed-empty behavior; AC75 gates public procedure errors.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN a live inline SELECT in T10 THEN a manifest-marked LZ4 payload decodes losslessly before SELECT exposure. When T41 adds codec setting or alias values `0`, `1`, or invalid, repeat the inline gate; when T12 exposes linked results, repeat for links. At each stage manifest codec governs decode, invalid keeps warehouse default, and paired bytes/CPU/load are recorded.
- AC102: GIVEN an isolated live table WHEN transaction settings vary THEN R19 state, commit/rollback rows, counts, and errors match paired routes; no-result commands fetch nothing and fail on missing state.
- AC105: GIVEN paired live null-catalog `getTables` rows/types and a deterministic client-response fixture returning SQLState `42601` for its SHOW WHEN APC SEA and warehouse SEA handle that response THEN both return a typed empty result without fan-out; compare with live APC Thrift rows/types and record omitted rows and caller impact before T48 merges. Repeat the fault case live if scoped staging can produce it.
- AC106: GIVEN a changed driver-owned error WHEN emitted THEN a test asserts its code name/number, the PR records taxonomy/dashboard ownership or handoff, and the telemetry-error template is complete; server errors retain server identity.
- AC107: GIVEN one/32 paired APC SEA and APC Thrift callers WHEN at least 100 open, partly read SELECT, and close cycles complete and resources quiesce THEN end-minus-start retained SEA heap is at most 8 MiB, paired cycle latency meets the shared load gate, and a subsequent connection succeeds. Record the cycle count, memory samples, and recovery.
- AC108: GIVEN AC164 WHEN live legacy INSERT batching settings vary THEN R6 chunk limits, counts, committed rows, errors, AC32 load, and AC132 oversize behavior pass each binding mode.
- AC109: GIVEN concurrent live multi-page inline SELECTs WHEN `ThriftMaxBatchesInMemory` varies around two THEN rows are complete, no whole-result buffer appears, and AC31 memory/load bounds pass; record chunk/batch counts and heap.
- AC112: GIVEN AC119 WHEN live APC and warehouse download-off SELECTs drain
primitive and complex values at both bounded/hybrid settings THEN R4/R11 format,
target, getters and failure timing hold. At one/32 paired callers, drain a
multi-page inline result of at least 16 MiB decoded logical data per caller,
or the largest result the deployed inline-only server limit permits. Apply the
shared throughput, CPU, heap and RSS ratios to the attainable size; if the
server limit prevents 16 MiB, capture its rejection boundary, maximum
successful size and caller impact and obtain the shared-gate exception approval.
Record received/decoded bytes, elapsed time and AC31/39 load.
- AC113: GIVEN the deployed terminal/count contract WHEN direct/polled no-result commands (including `SET CATALOG` and `USE SCHEMA`) and counted commands run on scoped staging THEN raw captures prove R4 success shapes, count zero, and terminal errors before T14 caller exposure; AC173 proves JDBC cache behavior.
- AC114: GIVEN two-parent and second-foreign-table keys WHEN live imported/cross-reference methods run THEN each returns its filtered paired JDBC rows; null foreign table is typed empty and invalid input fails before request.
- AC119: GIVEN the deployed CreateSession override WHEN session disables cloud download but statement advertises it THEN live single/multi-page results remain inline; capture both wire values before AC112.
- AC117: GIVEN paired bound SELECT callers WHEN pre-execution `PreparedStatement.getMetaData()` runs THEN metadata matches, bindings survive, and request/latency/throughput/heap load passes.
- AC118: GIVEN live `EnableSQLValidationForIsValid=1` callers WHEN `isValid(timeout)` runs `SELECT VERSION()` repeatedly THEN booleans, timeout, usability, request count, latency, call-rate, and heap pass paired load.
- AC122: GIVEN live two-parent and second-foreign-table keys WHEN typed async imported-key and cross-reference handles complete THEN each independently passes AC114 and the per-handle AC50/61 gates.
- AC124: GIVEN an exposed synchronous SHOW or typed async metadata handle WHEN `MetadataOperationTimeout` varies THEN positive values bound full call/await and cancel; `0` removes only the overall bound, in-budget calls survive shorter request timeouts, and timed-out connections remain usable.
- AC132: GIVEN AC164 and a live legacy INSERT chunk near the deployed APC byte limit WHEN ASCII or multibyte text crosses that limit THEN in-limit chunks succeed and over-limit chunks fail at Reyder without client split/fallback; committed rows and counts match captured requests.
- AC136: GIVEN live APC and Thrift cursors with unread continuation/link data and heartbeat enabled WHEN one row is read, both idle beyond two heartbeat intervals and configured expiry, then drain THEN APC sends no `/status`, remaining rows match, and survival time is recorded. Shorter APC lifetime requires R3 exception approval.
- AC138: GIVEN deployed T58 APC native SHOW routing WHEN raw synchronous/async catalog,
schema, table, column, function, primary-key, and cross-reference operations run THEN the
shared per-hop server-readiness gate above passes before each JDBC method is exposed; record
DBR version/configuration and the HIVE null-cell sample.
- AC144: GIVEN scoped staging faults WHEN 429/503, accepted-write response loss and delayed SELECT/SHOW polls are injected THEN captures prove target, fault point, committed rows, stable statement ID and caller isolation. Delay stays inside JDBC deadline; scoped fan-out delay gates AC161.
- AC145: GIVEN linked SELECT with hybrid and bounded settings at `0`/`1` WHEN paired routes run THEN APC stays targeted/hybrid with paired rows while warehouse hybrid `0` is external-only; bytes, latency, throughput, and heap pass before exposure.
- AC146: GIVEN AC113 and isolated table WHEN live prepared INSERT/UPDATE/DELETE/MERGE run, including zero count THEN each yields one counted request, paired counts/rows/errors, `execute=false`, and no ResultSet under AC154/load.
- AC147: GIVEN live IN-only CALL with rows, no result, or failure WHEN execute/executeQuery runs THEN R18 JDBC shapes and paired rows/errors hold; unsupported OUT/named/update forms reject before request.
- AC148: GIVEN flag-on APC with explicit SEA and catalog-pattern preference WHEN live `getCatalogs` and `getTables` run THEN each captures SEA SHOW, paired rows, and ignored-pattern diagnostic at first exposure and release.
- AC151: GIVEN each slice's live gates close before merge WHEN an intermediate flag-OFF build is tested internally THEN completed explicit-SEA workflows pass and incomplete calls reject under AC40. Intermediate builds are not distributed. Before any distribution, the release package's `NEXT_CHANGELOG.md` and docs cover every exposed slice and accepted-write uncertainty; only undistributed internal PRs may use `NO_CHANGELOG=true`.
- AC152: GIVEN saved APC SEA session and raw statement IDs WHEN a connection flag or target changes THEN saved close uses the original session target and recovered statement uses the current connection; captures prove both outcomes without ID leakage.
- AC154: GIVEN AC113 and AC144's post-acceptance write/count fault WHEN APC and warehouse SEA writes commit before their response is faulted THEN no APC-only replay occurs; missing counts fail and accepted writes report uncertain commit before each write exposure.
- AC155: GIVEN AC168 on scoped ON/OFF APC and warehouse routes WHEN callers open/close without SQL THEN ON terminates its session, OFF follows R1, and paired load passes. Rejected termination leaves ordinary close locally successful with a redacted warning; AC25 covers saved IDs.
- AC157: GIVEN HIVE null-mask and unknown-vector fixtures WHEN decoded THEN null positions and row count follow R13, unknown vectors fail before partial success, and a later valid result works; AC16 proves live HIVE parity.
- AC158: GIVEN unknown numeric SEA format fixture WHEN deserialized and decoded THEN decoding raises a named driver error before ResultSet success and a later valid SELECT still works.
- AC159: GIVEN single-chunk APC SELECT with no unread server page and heartbeat enabled WHEN a partly read ResultSet idles beyond two heartbeat intervals THEN no APC `/status` request is sent; cursor survival remains gated by AC136 once unread server data exists.
- AC161: GIVEN an exposed null-catalog schema/column call with independently delayed catalogs WHEN overall metadata timeout is disabled THEN each in-budget catalog returns despite earlier completions and over-budget work fails within its 90-second collection bound. A positive overall timeout may end sooner; compare paired rows and connection usability.
- AC164: GIVEN deployed APC routing WHEN valid ASCII/multibyte SELECTs straddle Reyder’s UTF-8 byte limit THEN raw captures prove acceptance below, Reyder rejection above, a non-BMP case and bypass of Scala’s UTF-16 limit; record lengths, origin and paired outcomes.
- AC165: GIVEN T12/T32/T42 expose linked and bounded modes WHEN streaming switches and CloudFetch speed thresholds vary on live inline/linked SELECT THEN R17 rows and diagnostics hold under AC39, including absent-total multi-link delivery at unbounded/streaming-off within its memory bound.
- AC166: GIVEN live APC SEA before T44 WHEN public `getConnectionId()` follows
open (T9) or successful SELECT (T10) THEN each slice's callable case raises
`UNSUPPORTED_OPERATION` (1005) without ID or extra request; ordinary close
uses the original cluster and a later connection works. APC Thrift/warehouse
SEA IDs remain valid. T44 replaces this boundary after AC25/152.
- AC168: GIVEN the tracked server termination binding fix is deployed WHEN a raw APC CreateSession and TerminateSession use the same cluster target THEN the created session is removed; a rejected termination remains identifiable for JDBC AC155.
- AC169: GIVEN T22's typed async null-catalog `getTables` and the AC105 `42601` client-response fixture WHEN the handle completes THEN it returns the same typed empty result without fan-out; paired live rows/types and AC50 delayed RUNNING, await, timeout, and cancel pass before T22 merges. Repeat the fault case live if scoped staging can produce it.
- AC171: GIVEN T12 exposes linked results WHEN R11 complex/geospatial getters run with support on and off THEN paired types/values and representable typed getters hold, disabled getters reject, and unsupported live values remain pending as in AC13 before T12 merges.
- AC172: GIVEN live APC SEA and paired APC Thrift/warehouse SEA connections WHEN `getCatalog` and `getSchema` run before and after T10 THEN cached reads remain local; uncached pre-T10 reads reject before a SEA request with a named error/code; post-T10 uncached reads issue one targeted `SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()`, cache both valid values, and match paired results. Capture flag-ON, flag-OFF, and explicit-SEA traces and connection usability.
- AC173: GIVEN live APC SEA and paired APC Thrift/warehouse SEA connections WHEN `setCatalog` and `setSchema` run before and after T14 THEN disabled `setCatalog` remains a local no-op; enabled `setCatalog` and `setSchema` reject before a SEA request with a named error/code until T14; after T14 they issue `SET CATALOG` and `USE SCHEMA`, update the cache only on success, retain prior values after failure, and match paired results. Capture flag-ON, flag-OFF, and explicit-SEA traces and connection usability.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- The final release package alone edits `docs/` and `NEXT_CHANGELOG.md`.
  Intermediate builds are internal and must not be distributed. They may use
  `NO_CHANGELOG=true`; distribution waits for AC151 on the integrated release
  commit.
- Each JDBC slice closes its callable workflow and the applicable common gates
  defined above. Keep each PR below roughly 1,500 changed lines; revise the
  package plan before implementation if a scoped slice cannot meet that limit.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server-side SEA, Reyder, DPR, connector-service, SAFE definition, and staging
  fault-hook implementation are outside JDBC delivery. T56–T62 and their
  universe packages track these prerequisites and their deployment evidence.
- Ramping the APC flag in SAFE (an operational step after the driver ships, owned by PECO).
- Benchmark suites (`MultiJdbcDriverBenchmark` APC variant, Quicksilver `dust-cluster-benchmark`
parameter sets).
- ODBC, Python, Go, and Node.js connectors.
- Govcloud / unsupported APC SEA region enablement beyond keeping the APC flag off there; server errors
propagate without fallback.
- Removing the direct-Thrift APC code path.

---

## Plan

### Approach

`plan.json` assigns each task once to a callable JDBC slice or a tracked universe
prerequisite. Each repository has its own rooted package tree; parent links encode
same-repository branch ancestry. The JDBC root has no Git parent and requires
deployed server-flag and termination evidence as entry gates, since Git cannot
branch across repositories. Siblings may
proceed in parallel only where their owned lines do not overlap. A JDBC agent
must verify the named universe package's deployed acceptance evidence before
starting dependent implementation or live validation, and block its package if
that evidence is absent. Branch completion alone does not satisfy this gate.
The connection slice requires AC28/34/168; metadata slices require their
operation's AC138; download-off requires AC119; terminal/transaction/CALL/batch
and prepared-write slices require AC113; delayed-read, retry, and write slices
require the applicable AC144 fault evidence. A dependency outside a package's
parent chain is also an entry gate recorded in its notes. Each slice closes its
own live gate before merge; linked SELECT includes its worker, prefetch, pool,
retry, and timeout gates before callers can use links. The release package
alone edits docs and `NEXT_CHANGELOG.md`. Integrate each independent sibling
slice into a descendant that also exposes a new callable workflow. Rerun its
live gate and affected shared gates on the integration HEAD, and close conflicts
before starting descendants. T16's shared metadata transport precedes T19's procedure work;
T31 owns and records AC164 on live APC and paired routes before T15 starts.
Run T-val on the exact integrated release HEAD; any subsequent commit reruns
T-val and its affected slice gates. The tested HEAD is the release candidate.
No intermediate build is distributed.
Split a slice before implementation if its review diff approaches 1,500 changed
lines. The plan uses more than seven JDBC packages because connection, inline,
linked, metadata, and write workflows have separate exposure gates; consolidation
must not defer one of those gates. Each JDBC package has an end-to-end callable outcome.

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
must pass AC164 before dependent batches. AC13/Q7 block lost logical types, AC27 blocks
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
| First SELECT | T10 single-chunk polling, fetch, cancel, primitive inline `ARROW_BATCH` schema/value and manifest-marked LZ4 decoding, and uncached `Connection.getCatalog`/`getSchema`; T40 direct/polled invalid SQL, missing-object and permission error identity before SELECT exposure; T39 `isValid`. |
| Results and saved close | T11 inline continuation; T13 logical getters; T41 codec/API compression; T12 linked continuation; T32 SELECT settings and limits; T42 linked resource controls, completed before linked SELECT exposure; T29 async SELECT; T43 download-off; T44 saved session close at release. |
| Commands and transactions | T14 shared terminal-result handling, Statement commands, enabled `Connection.setCatalog` and `setSchema`; T30 transactions; T33 CALL; T34 Statement batches. |
| Prepared scalars | T28 negotiated scalar binding and prepared SELECT; T31 prepared DML; T38 prepared metadata. |
| INSERT batches | T15 legacy INSERT batches; T27 V10 native batches. |
| Metadata core | T16 catalogs and the shared header/format contract; T17 schemas and fan-out. |
| Metadata discovery | T48 tables; T18 columns; T49 functions, each exposed only after its live server gate. |
| Metadata keys | T35 primary keys; T47 exported keys; T55 imported/cross-reference keys. |
| Procedure metadata | T19 parameterized procedure SELECT after scalar binding and metadata transport. |
| Async metadata discovery | T20 catalogs and first typed async lifecycle; T21 schemas; T22 tables; T23 columns; T24 functions. |
| Async metadata keys | T25 primary keys; T26 imported/cross-reference keys. |
| Release | T36 docs/changelog; T-val live gates on the exact integrated release HEAD containing every sibling slice. |
| Server flag | T56 Partner Platform defines the default-off APC client-config sub-flag; T57 connector-service excludes all partners, including allowlisted JDBC agents, from automatic APC flag assignment while preserving explicit-client precedence (AC28/34). Track both under [SC-256186](https://databricks.atlassian.net/browse/SC-256186); the flag package owns the deliverables, and the connection slice requires their deployed evidence. |
| Server metadata | T58 SEA/Reyder/DPR admits APC `ARROW_STREAM` metadata while preserving `ARROW_BATCH` SQL, selects native SHOW unconditionally, carries sync intent, and returns native Arrow/HIVE metadata (AC138); [SC-256188](https://databricks.atlassian.net/browse/SC-256188). |
| Server result and session | T59 Reyder honors CreateSession inline-only override (AC119; [SC-258787](https://databricks.atlassian.net/browse/SC-258787)); T60 SEA/Reyder fixes APC TerminateSession target binding (AC168); T62 Reyder returns terminal/count command shapes (AC113; [SC-258842](https://databricks.atlassian.net/browse/SC-258842)). |
| Staging faults | T61 SEA staging owns scoped request delays, 429/503 and accepted-write faults (AC144); deterministic client-response fixtures cover unavailable `42601` and one-catalog `SQLException` cases under AC83/105/169. |
