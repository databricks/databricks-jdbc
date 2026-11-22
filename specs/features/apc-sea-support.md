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
  SC-254478). Driver behavior and fake-service tests assume those fixes. Affected live staging
  metadata, inline-only, and session-termination gates remain pending until the fixes deploy.
  The TerminateSession REST binding declares `body: "*"` (R2).
- Reyder's current APC `GetStatementResult` path fetches missing metadata after a successful
  poll, then requires an Arrow schema to build a manifest. No-result DDL and counted DML
  therefore need T32's server proof before live JDBC gates pass (R4, AC52). Live result
  manifests currently populate `arrow_schema`; an absent-field decode has a fixture gate.
- Reyder currently discards APC `result_compression`, advertises LZ4 decompression capability
  to DBR, and reports DBR's actual codec in the result manifest. Uncompressed results are valid;
  the JDBC codec setting cannot select APC's effective wire codec (R4, AC101).
- Client-config flags come from connector-service
(`/api/2.0/connector-service/feature-flags/OSS_JDBC/{version}`), which returns sub-flags of
`databricks.partnerplatform.clientConfigsFeatureFlags`
(`connector-service/src/featureflags/FeatureFlagsHandler.scala`). It version-maps
`enableSqlExecForJdbc` to `enableSqlExecForJdbc_v3.4.3` for ≥ 3.4.3. For partner User-Agents,
that existing flag may remain true for an allowlisted JDBC partner; other sub-flags currently
pass through unchanged. Q9 requires a separate blanket APC override.
- Design contract: ["All-Purpose Clusters in SEA"](https://docs.google.com/document/d/1usi5sa3AgX6p6XpXhNsOiY-ZE9xG7Nykawddal_6P0s/edit?tab=t.h8jgor38k2bp)
(API contract, Phase II, and metadata tabs). R1–R19 are the authoritative driver contract;
the matrix and acceptance criteria summarize parity and observable outcomes. The
server also limits SQL text to 16 MiB (16,777,216 UTF-8 bytes), rejects non-OSS-JDBC clients, and starts rollout opt-in
before defaulting APC to Reyder through the dedicated flag.
- Driver baseline verified against `origin/main` @ `840f3a407` (merged spec #1809;
  `UseBoundedSeaApi` default #1827): APC routes to Thrift before the warehouse flag logic.
  `DatabricksSdkClient` assumes `Warehouse` for SEA session/statement requests, polls with
  `GET /statements/{id}`, and heartbeats through `/status`. Its session close and
  `getConnectionId()` paths infer protocol from compute type, not the selected client:
  `SessionId.create` encodes warehouse sessions as SEA IDs and APC sessions as Thrift IDs.
  `Driver.closeConnection` selects a client from the supplied URL and current flags before
  deserializing the saved ID, so a flag or target change can misroute close. The SEA response path lacks
  APC `ARROW_BATCH`, async SHOW, and SEA `columnar_data` decoding; the Thrift decoder already
  handles `COLUMN_BASED_SET`. Public SQL `executeAsync` is separate from metadata SHOW async.
- Draft [#1771](https://github.com/databricks/databricks-jdbc/pull/1771) is the prototype
baseline, not the design; frozen benchmark [#1756](https://github.com/databricks/databricks-jdbc/pull/1756)
is unchanged.
- Parity baseline for review and release ("warehouse" means SQL warehouse with the same driver
  build). Document each allowed difference and its client impact.

  | Behavior | APC Thrift today | Warehouse SEA today | APC SEA | Disposition, reason, caller impact |
  | --- | --- | --- | --- | --- |
  | Route and failures | Default Thrift | Flag/override, no SEA fallback | Dedicated flag, no fallback | Warehouse precedent except APC Arrow-first prerequisite; SEA failures reach callers (R1, R7). |
  | SELECT, pages, row limit | Inline/CloudFetch | Inline/links, `row_limit` | Same rows/limit; APC continuation | Match; extra `next_chunk_index`/`row_offset` requests may add cost (R4, R12; AC31). |
  | Download and streaming switches | Thrift providers/links | SEA provider controls | APC hybrid and bounded providers; download-off stays SEA | APC has no external-only disposition or reliable totals; link/inline choices and `UseBoundedSeaApi=0` cannot force legacy behavior. Thrift-only switches retain their Thrift effect (R4, R17; AC8, AC39). |
  | DDL and single DML | DDL zero, DML count | Same | Same counts | Match correctness and one/32-caller throughput, latency, concurrency, and heap; missing DML count is an error (R4; AC33, AC48, AC103, AC104). |
  | Metadata SHOW and exported keys | Native RPC; parent-only exported keys empty | SHOW; typed-empty exported keys | SHOW/native-shaped; typed-empty exported keys | Warehouse/DBR precedent; callers retain empty exported keys, subject to live contract proof (R5; AC54). |
  | Null-catalog metadata recovery | Native RPC, no fan-out | `42601` fan-out may omit failed catalog | Same fan-out | Warehouse precedent; warning and partial rows can reach callers (R5; AC83). |
  | Metadata settings and procedures | Catalog/metric-view settings; native or SHOW | SHOW; procedure headers | Same visible scope; APC procedures omit unsupported headers | Server contract difference; measure procedure latency, resources, and telemetry, and retain complete JDBC rows (R5; AC65, AC75, AC82, AC88). |
  | Typed async metadata | JDBC waits | JDBC waits | V9+ handles overlap; older waits | Optional API; synchronous JDBC stays unchanged (R5; AC50, AC61). |
  | Bindings and batches | Scalars/complex, prepared/Statement batches | Same, native per-set counts | Native V8+ scalars/V10 batches; fallback otherwise | Match supported scalars/counts; complex interpolation cannot be lossless, so callers get an early coded error (R6, R15; AC17, AC29, AC32). |
  | SQL over 16 MiB | Backend boundary | SEA rejects above 16,777,216 UTF-8 bytes | Same SEA boundary | Warehouse precedent; callers shorten SQL or legacy chunks and reconcile prior commits (R6; AC87). |
  | Arrow and heartbeat | No Arrow prerequisite; Thrift heartbeat | Explicit override before Arrow check; `/status` | Arrow-first, no `/status` | APC status RPC unsupported; an open ResultSet has no heartbeat (R1, R3; AC1, AC3). |
  | Logical/complex getters | Thrift value settings | Inline typed complex getters reject | Same SEA getters | Warehouse precedent; typed complex callers need links (R11; AC13). |
  | Fetch and CloudFetch tuning | Workers/chunks and streaming link lookahead | Same linked scope; no SEA page tuning | `cloudFetchThreadPoolSize` bounds linked workers/chunks; `LinkPrefetchWindow` bounds fetched-ahead links, not inline pages | Match APC Thrift and warehouse SEA linked queue, heap, and timeout behavior at one/32 callers; `RowsFetchedPerBlock` has no SEA effect (R17; AC31, AC46). |
  | Result codec and API compression | Thrift/result setting | Requested result codec; separate API compression | Reyder ignores APC codec request, advertises LZ4; manifest controls decode | Server exception: `NONE` cannot force uncompressed APC results, affecting bytes, CPU, and latency. Selectable compression requires a server change (R4; AC45, AC101). |
  | SQL async, waits, direct results | Thrift async/timeouts | SEA polling and direct-results choice | APC-targeted SEA polling and direct-results choice | Warehouse precedent; extra polls can increase first-row latency (R10, R14; AC55, AC60, AC12). |
  | Saved IDs and close | APC ID encodes Thrift; changed flag can misroute close | Warehouse ID encodes SEA even on Thrift | New IDs encode selected protocol/target | New saved IDs close correctly; old warehouse Thrift IDs lack a recoverable secret and may require reopen (R9; AC25). |
  | Transactions | Ignore or execute SQL commands | Same calls | Same state, rows, errors | Match; server-fetched auto-commit requires a state row (R19; AC102). |
  | Errors, retries, pools | Thrift error/HTTP controls | SEA mapping, SDK API pool; link HTTP pool | Same SEA request-class scopes | Warehouse precedent may change retry/error identity; no new write replay or APC-only cap (R4, R17; AC30, AC51, AC89). |
  | Telemetry and resources | Connection/operation/error events | Same families | Same families; measured SEA use | Match visible events and bounded heap; record any exception (R8, R17; AC85, AC31). |

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
re-download payloads. No APC `/status` request is sent.
- R4: APC non-metadata execution sends `format=ARROW_BATCH`, `disposition=INLINE_OR_EXTERNAL_LINKS`,
and a `session_id`, never sends `statements`, and handles bare Arrow batches
in `chunk.arrow_batches` (including multiple batches per chunk). External links are complete
Arrow IPC streams, not bare batches; preserve framing and headers and drain every link.
`EnableQueryResultDownload=0` keeps SEA selected and sets CreateSession
`session_confs["can_cloud_download"]="false"` (a string-valued map entry, not a top-level
field); after the tracked server fix, results remain inline despite the
hybrid disposition. An inline-size error propagates without a successful partial ResultSet.
`EnableSQLExecHybridResults=0` cannot request external-only APC results because this APC
format has only hybrid disposition; callers may observe inline data instead of links.
Result completeness, request count, and heap at either value are bounded by AC31 and AC39.
`UseBoundedSeaApi` does not change this APC request or session behavior.
Positive `Statement.setMaxRows` sends SEA `row_limit` and caps exposed rows across pages/links;
zero is unlimited, matching APC Thrift.
Successful no-result commands such as DDL may omit a manifest or use zero columns; they
expose no ResultSet and update count zero, then `getMoreResults()` changes it to `-1`.
Sequential batches record zero. Result-producing commands, including `SELECT`, `SHOW`,
`VALUES`, `SET AUTOCOMMIT` state queries, and a `NonRowcountQueryPrefixes` match, require their schema and data before
public success. Counted `INSERT`, `UPDATE`, `DELETE`, and `MERGE` require a
`num_affected_rows` schema column and row, including zero; missing count data is a
contract error. The reported affected-row count matches APC Thrift. A configured
`NonRowcountQueryPrefixes` match instead exposes the affected-rows ResultSet and update
count `-1`; a Statement batch stops there with preceding counts and the existing error.
Server failures propagate; live polled DDL and DML gates depend on T32. APC advertises LZ4
decompression capability. The driver may send `result_compression` from
`QueryResultCompressionType` or its higher-precedence `EnableQueryResultLZ4Compression` alias,
but Reyder currently discards it and DBR chooses the effective APC codec. Decode inline and
linked payloads from the manifest codec; an unmarked response is uncompressed. Selecting
`NONE` does not guarantee uncompressed APC results. AC101 measures the caller impact;
selectable APC result compression requires a server prerequisite, not a driver-only change.
For ordinary failed statements,
preserve the public SQLException class, SQLState (including established transient remapping),
and vendor code where APC Thrift and warehouse SEA agree. Where their mappings differ, follow
warehouse SEA and record the precise caller-visible difference (AC89).
- R5: Seven APC metadata SHOW operations (`GET_CATALOGS`, `GET_SCHEMAS`, `GET_TABLES`,
`GET_COLUMNS`, `GET_FUNCTIONS`, `GET_PRIMARY_KEYS`, `GET_CROSS_REFERENCE`) send
`X-Databricks-Metadata-Operation-Type` using the existing case-sensitive wire values
`GetCatalogs`, `GetSchemas`, `GetTables`, `GetColumns`, `GetFunctions`, `GetPrimaryKeys`,
and `GetCrossReference`, respectively. They request `ARROW_STREAM` with
`INLINE_OR_EXTERNAL_LINKS` regardless of `EnableSQLExecHybridResults`. On APC the default
`EnableThriftNativeMetadata` resolves false: explicit `1` sends
`X-Databricks-Require-Thrift-Native-Metadata: true` for supported SHOW operations, while
explicit `0` or an unset value omits it. Reyder's APC response remains native-shaped at both
values, so JDBC conversion uses the response marker rather than the request setting. Synchronous SHOW adds
`X-Databricks-Sea-Can-Run-Fully-Sync: true` only with `EnableSeaSyncMetadata=1`; async SHOW
omits it. Reyder returns native-shaped metadata in Arrow IPC `chunk.attachment` or HIVE
`chunk.columnar_data`/`COLUMN_BASED_SET`; decode per response and retain
`manifest.is_native_metadata_result=true` for JDBC conversion.
`enableMultipleCatalogSupport=0` retains the current-catalog access and metadata filtering
used by both existing routes; `1` permits eligible catalogs. `EnableMetricViewMetadata=1`
adds `spark.sql.thriftserver.metadata.metricview.enabled=true` to session configuration and
exposes `METRIC_VIEW` in table types and eligible table rows; `0` omits that configuration and
type. APC SEA follows the same observable results (AC88).

`DatabaseMetaData.getExportedKeys` is outside those seven operations. The design contract's
Metadata Queries tab says DBR returns an empty result for parent-only `GetCrossReference`;
warehouse SEA also returns typed empty. APC SEA returns the same typed empty result without a
server request. Compare all three routes on a table with a referencing foreign key; issuing the
Thrift request alone is not evidence of populated rows.

The session-reachable async metadata API accepts the seven SHOW operations and JDBC inputs
and returns typed ResultSet handles. At V9+ (42249), APC SEA submits without blocking,
polls within the metadata timeout, and preserves sync conversion, empty, error, and cancel
behavior. `DatabaseMetaData.getColumns` consumes its handle without changing its synchronous
JDBC contract. APC Thrift and warehouse SEA handles preserve their existing requests and
public results; only V9+ APC SEA handles overlap other work. Older/versionless APC handles
complete synchronously before returning. Both cross-reference conversions remain distinct;
SQL async behavior is unchanged.

APC internal catalog/schema SELECTs and parameterized `getProcedures`/`getProcedureColumns`
SELECTs are ordinary `ARROW_BATCH` queries with hybrid disposition and no metadata headers;
convert their JDBC metadata after ordinary query decoding. Current warehouse SEA sends the
`GetProcedures`/`GetProcedureColumns` header values, but Reyder does not accept them for APC.
Procedure SELECTs use R6's scalar binding rule; async SHOW rejects them before request.
Warehouse requests stay unchanged.
`EnableShowCommandForGetFunctions` still controls the APC Thrift native-RPC versus SHOW
choice; APC SEA follows warehouse SEA and uses SHOW at both settings. Compare catalog-filtered
and null-catalog results live at each setting (AC82), including the Thrift SHOW path's
null-catalog empty result.

For V9+ async `GET_SCHEMAS`/`GET_COLUMNS` with null catalog, SQLState `42601` from
`SHOW ... IN ALL CATALOGS` triggers async catalog enumeration and one SHOW per nonempty
catalog. Keep the operation header, omit the sync header, and combine rows using sync
filtering/empty semantics. Match the current warehouse SEA synchronous fan-out: warn and omit a catalog
on *any* per-catalog `SQLException`, including a timeout, cancellation, or null SQLState;
propagate catalog-enumeration and initial SHOW errors except the existing object-not-found
and applicable empty-pattern cases that return typed empty. A timeout or cancellation of the
overall metadata operation still fails the call. This partial-row behavior is observable
on warehouse SEA and APC SEA, while APC Thrift's native metadata RPC has no client fan-out.
Record skipped catalogs and preserve a usable connection (AC83).

- R6: APC CreateSession `negotiated_protocol_version` is the capability source for native scalar parameters at V8
(42248), async metadata at V9 (42249), and opt-in `EnableNativeBatching=1` INSERT batches
at V10 (42250). Reyder leaves `session_version` unset on this response; that field and
serialized IDs grant no capability. Missing `negotiated_protocol_version` grants none.
The driver must deserialize the negotiated field and retain its version ID for later requests.
Reject complex
bindings (ARRAY/MAP/STRUCT and other complex logical types) before every request, including
fallback batches, with a tested driver-owned error name/code and taxonomy; interpolation's
`toString()` is not lossless SQL. Below V8 or without a live version,
`supportManyParameters=0` interpolates supported scalars for single executions and legacy
batch sets; unresolved markers must not reach the server. The missing-version telemetry
reason remains distinct. Explicit
`supportManyParameters=1` selects interpolation for single executions and legacy batch sets at
every version and permits non-null `setBytes`. With `supportManyParameters=0`, non-null
`setBytes` remains rejected; V8–V9 fallback sets use native scalar `parameters`.
Native V10 uses ordered nested `parameter_sets`, no top-level `parameters`, and
preserves the ordered per-set affected-row counts returned by SEA, including repeated counts.
A successful response without complete per-set counts is a result-contract error rather than
fabricated `SUCCESS_NO_INFO`. On failure, report only a server-confirmed successful prefix;
otherwise expose empty update counts and an error that leaves commit status uncertain. Never
claim atomic rollback without server evidence. All other
batches use the shared legacy executor: eligible INSERT sets combine into multi-row SQL when
`EnableBatchedInserts=1`; `BatchInsertSize` limits interpolated chunks and the existing
parameter limit caps bound chunks;
ineligible sets execute individually. Preserve ordered counts and stop-on-error.
SEA documents a 16 MiB (16,777,216 UTF-8 byte) SQL-text limit, the same as warehouse SEA. As on warehouse SEA, APC SEA does not
silently split or reroute an oversized single statement or generated legacy INSERT chunk;
it propagates the server error. The shared legacy executor currently marks all entries
failed for some chunk exceptions even if an earlier chunk committed, matching APC Thrift
and warehouse SEA behavior but leaving commit status ambiguous to the caller.
`BatchInsertSize` remains the caller's control for smaller chunks.
APC Thrift's backend boundary and error identity are measured, not assumed equal.
Pre-execution `PreparedStatement.getMetaData()` uses binding-free
`DESCRIBE QUERY` without losing bindings. Warehouse behavior stays unchanged.
- R7: R1 selects the protocol before connecting; `UseBoundedSeaApi` does not affect that
decision. After SEA is selected, server errors, including unsupported regions, propagate
without Thrift fallback. Warehouse-only KP001 Thrift-to-SEA recovery does not change APC routing.
- R8: Every APC and warehouse SEA/Thrift connection attempt emits compute type, selected
protocol, routing reason, and success/failure, including failed opens. Log when explicit
SEA selection ignores `TreatMetadataCatalogNameAsPattern=1`. Preserve the existing
operation-latency, result-format, total-chunk, chunk-download-latency, and error telemetry
families for APC SEA execution, metadata, linked downloads, batches, and failures, with the
selected protocol and compute identity available for correlation. Compare event presence,
operation type, format, counts, error name/code, SQLState, and vendor code at each public
slice with APC Thrift and warehouse SEA (AC85). APC diagnostics and telemetry redact
org/cluster ids and compound session/statement ids, including execute and session logs;
no event leaks a raw target through a message or serialized identity.
- R9: New public `getConnectionId()` values encode selected protocol and target so
`Driver.closeConnection` can close the original session after a flag or URL change; public
statement IDs remain raw. Internal identities retain the compute target and reject malformed
IDs without logging identifiers. Preserve decoding of legacy warehouse SEA and Thrift IDs;
legacy warehouse Thrift IDs encoded as SEA by `origin/main` lack the Thrift secret and cannot
be repaired for saved close, so callers holding them may need to reopen. Recovered statements
use the current connection target. Only a live CreateSession version grants capabilities.
- R10: APC ExecuteStatement keeps SEA `wait_timeout` unset for synchronous direct results and
uses `0s` for async or `EnableDirectResults=0` (alias `EnableSQLExecDirectResults=0`). Public SQL `executeAsync` and
`getExecutionResult` use APC targeting, polling, and decoding separately from async metadata.
Measure server request timeouts separately from JDBC waits, including the extra
`GetStatementResult` round trip (AC60, AC63).
- R11: Decode inline `ARROW_BATCH` from `manifest.schema` (`type_text`/`type_json`, including
nested types), without requiring `manifest.arrow_schema`; external links remain full Arrow IPC
streams. A deferred logical type fails before false ResultSet success. Preserve
`EnableComplexDatatypeSupport` and `EnableGeoSpatialSupport` effects on JDBC metadata and
`getObject`: disabled support returns established strings and rejects typed complex getters;
enabled support permits linked typed getters and native geospatial values where applicable.
Inline typed complex getters still reject, so callers needing them require linked results.
- R12: Page APC results by `next_chunk_index` and `row_offset`, without requiring totals or
inline data in the initial response. The next inline offset includes every batch in a chunk;
for linked results it follows the final link's end offset and continuation. Missing final-link
`next_chunk_index` ends paging.
- R13: For APC metadata, decode the emitted SEA `columnar_data` physical vectors (boolean,
byte, short, int, long, double, string, binary) using `manifest.schema` and least-significant-bit
first null masks. The server always sets a mask; short masks leave unrepresented rows non-null and
long masks ignore excess bits. The decoded JDBC columns and values match the same metadata
operation over APC Thrift. An unrecognized physical vector fails clearly.
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
(wire value 6) for APC metadata while preserving existing warehouse wire values. Unknown future
response formats deserialize safely and fail clearly if decoded; requested `ARROW_STREAM` must
not prevent decoding a `COLUMN_BASED_SET` metadata response.
- R17: APC SEA follows warehouse SEA's SDK retry/pool and streaming CloudFetch behavior.
APC SEA uses bounded inline and linked providers at either `UseBoundedSeaApi` value.
`EnableInlineStreaming` selects only Thrift inline providers, so changing it does not alter
APC SEA rows, paging, or memory. `EnableStreamingChunkProvider=0` cannot select the legacy
linked provider on APC because APC continuation has no reliable totals; `1` selects the same
streaming provider. `ThriftMaxBatchesInMemory` still bounds APC inline buffering (minimum two).
The case-insensitive `cloudFetchThreadPoolSize` URL property (also spelled
`CloudFetchThreadPoolSize`, default 16) bounds per-result linked download workers and
in-memory chunks. `LinkPrefetchWindow` (default 128) bounds how far streaming fetches
links ahead of consumption; it does not raise the download-worker or chunk-memory limit.
Both settings retain their APC Thrift and warehouse SEA linked-result scopes on APC SEA;
neither tunes inline continuation. `ChunkReadyTimeoutSeconds` bounds wait for a linked chunk.
Thrift HTTP retry settings do not tune SEA API requests, but continue to tune presigned-link
downloads through the shared HTTP client; `HttpConnectionRequestTimeout` controls Thrift and
linked-download acquisition wait, not SEA SDK API acquisition. `RowsFetchedPerBlock` does not
tune SEA paging. Match warehouse SEA 429/503 and `Retry-After` handling separately for API
requests and linked downloads without new write retries. No APC-only
concurrency cap or full-result buffer is allowed. AC30/46/51/84 test each transport
setting against its API or linked-download request class.
- R18: Every approved JDBC slice exposes its completed public behavior when R1's default-off
APC flag is enabled or `UseThriftClient=0` is explicit. Incomplete public calls, including
`executeAsync`/`getExecutionResult` before T39, fail before network execution when known;
unsupported response shapes fail before false success. They never silently switch to Thrift
after SEA selection. After T17 and T35 deploy, a scoped staging
assignment may enable flag-on validation; production assignment or ramp waits for T18/T-val
on the assembled approved stack and a post-merge smoke check. Remove each guard when its
slice passes live staging, and keep
ordinary flag-off APC connections on Thrift.
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
`BatchInsertSize` and the parameter limit retain their scopes. Only opt-in V10 uses native
`parameter_sets`. Statement SQL batches stay sequential without `statements` (R6, R15).
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
A9: Yes, including JDBC partners allowed by `enableSqlExecForJdbc`. Deploy T35 before APC
assignment; partner enablement needs a separate rollout decision (R1, T17).
- Q10: How is repeated transfer avoided during `GetStatementResult` polling?
A10: At least three RUNNING polls of a large query transfer no result payload; terminal
response and fetch return all rows once (AC27).

## Acceptance Criteria

At first exposure, each public slice proves its completed call on scoped flag-ON APC staging
against APC Thrift and warehouse SEA where relevant. AC3, AC11, AC40, AC51, AC84, and AC85
apply to newly exposed request classes once per slice; AC30 applies when a new request pool
is exposed. Server-dependent outcomes remain
pending until deployed and tested. Captured fixtures may cover only protocol versions or wire
shapes unavailable live; they do not replace available live proof. AC identifiers retain prior
review references, so retired duplicate gates leave gaps.

For paired loads (AC31/32/46/61/75/103/104), hold driver build, compute, data, and
network fixed per route pair; run three repetitions. Record rows, errors, request and
byte counts, throughput, latency, peak and retained heap. Results, batches, procedures,
and CloudFetch tuning cover one and 32 callers; SHOW covers 32. AC61 also uses a direct-SEA
server-resource reference. Isolate a request class and target for fault injection;
use a real server failure to prove its error identity. Each first exposure also runs
AC3/11/40/51/84/85; AC30 applies to a newly used pool, and AC10 to a newly exposed
DEBUG-log path. For AC31/32/75/103/104, paired median throughput must be at
least 90% of APC Thrift and latency at most 110%; peak heap at most 120%.
For doubled result/input size in AC31/32, retained heap may grow by at most
max(8 MiB, 5% of added bytes). A measured server limitation or warehouse-SEA
precedent needs caller impact and PECO/Perf Eng approval; unavailable live
versions remain pending. These shared gates need one evidence record per slice.
- AC1: GIVEN APC/warehouse routes and Arrow/legacy-path fixtures WHEN the flag, `UseThriftClient`, and catalog-pattern setting vary THEN R1 precedence, partner exclusion, fail-closed lookup, and unchanged warehouse routing hold; explicit SEA logs ignored Thrift-only settings and never falls back.
- AC2: GIVEN flag-on APC staging WHEN a cluster-path connection opens, runs a primitive SELECT, and closes THEN CreateSession carries that cluster ID without `warehouse_id`, returns correct rows, and targeted TerminateSession succeeds after its server fix; both baseline routes still work.
- AC3: GIVEN each new APC R2 operation WHEN it runs THEN R2 target/body placement and R3 no-status behavior hold, running statements close once, and the connection remains usable; warehouse serialization is unchanged.
- AC6: GIVEN live APC prepared SQL with bound scalars WHEN `getMetaData()` runs before
execution THEN it returns JDBC metadata without losing bindings; subsequent execution
matches APC Thrift and warehouse SEA. AC53 covers the version and interpolation matrix.
- AC8: GIVEN APC SELECT with `EnableQueryResultDownload=0` WHEN single and multi-page results run THEN CreateSession sends `session_confs["can_cloud_download"]="false"`, all pages stay inline, and an inline-size error fails before partial success at either `UseBoundedSeaApi` value; the server fix gates this.
- AC9: GIVEN live APC SEA CreateSession rejection WHEN connecting THEN the error and failed
route event propagate without Thrift fallback. GIVEN APC Thrift OpenSession returns KP001 WHEN
connecting THEN warehouse-only recovery does not select SEA.
- AC10: GIVEN successful and failed opens or statements WHEN DEBUG logs and telemetry are captured THEN route/outcome/compute/protocol appear without credentials or raw APC IDs; recheck linked and recovered paths when exposed.
- AC11: GIVEN standard and Real-time warehouse connections WHEN equivalent R2 operations run THEN JDBC outcomes and URL/body/header serialization, including `realtimeSession`, match pre-change `origin/main`; each slice checks affected classes.
- AC12: GIVEN APC synchronous SELECT WHEN `EnableDirectResults` is 1 or 0 THEN `wait_timeout` is respectively absent or `0s`, `statements` is absent, and rows/errors match both baselines; record first-row latency and requests.
- AC13: GIVEN primitive, decimal, array, map, struct, interval, timestamp,
timestamp_ntz, geography, geometry, and variant values WHEN inline and linked
getters run with R11 settings at 0/1 THEN types and values match baselines;
inline typed complex getters reject, linked enabled getters work, and disabled
settings retain strings. Unavailable types remain pending with fixtures.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN each synchronous APC SHOW in emitted Arrow IPC or columnar format WHEN read THEN JDBC columns, values, nulls, and errors match both routes, including null catalogs and distinct key conversions; fixtures cover R13 vectors/masks and format switching, and AC61 gates load.
- AC17: GIVEN live or captured V10 APC WHEN a native prepared INSERT batch succeeds or fails THEN ordered `parameter_sets` produce one count per successful set, only confirmed prefix counts on failure, and empty counts for uncertain failure. Older/native-off modes retain legacy behavior; fixture-only V10 leaves live load pending.
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
- AC25: GIVEN new APC and warehouse SEA/Thrift IDs WHEN `Driver.closeConnection` receives a changed flag or target THEN the original protocol/target closes only its own session; legacy IDs still decode, malformed IDs fail without logging IDs, and statement IDs stay raw. Prechange warehouse Thrift IDs lack a recoverable secret and may require reopen.
- AC27: GIVEN a live single-chunk APC query with three or more RUNNING polls WHEN transfer
is measured on explicit-override and flag-on routes THEN polls carry no result payload and
the terminal fetch delivers every row once; compare APC Thrift and warehouse SEA traffic.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN complex scalar or batch bindings WHEN APC execution starts THEN a driver-owned
named/numeric error occurs before any request and the connection remains usable. Record route
comparisons and owner-verified telemetry taxonomy/dashboard updates, or a maintainer handoff, in the
PR.
- AC30: GIVEN pool saturation for newly exposed API and linked-download classes WHEN `HttpConnectionPoolSize`, `HttpMaxConnectionsPerRoute`, and `HttpConnectionRequestTimeout` vary separately THEN in-flight count, wait, error, and throughput follow each transport scope; SDK API acquisition ignores the wait setting.
- AC31: GIVEN paired APC SEA/Thrift primitive, multi-page inline, and linked
SELECTs at first exposure WHEN one and 32 callers run 1/2 MiB inline or
256/512 MiB linked results with `RowsFetchedPerBlock` varied THEN rows/errors
match, SEA paging ignores that property, and the paired load limits above pass.
Record request bytes and retries.
- AC32: GIVEN paired APC SEA/Thrift 1,000-row INSERT batches with disjoint keys
WHEN each legacy/native prepared or sequential Statement mode runs at one and
32 callers THEN counts, committed rows, errors, and paired load limits pass.
Vary R6 batch settings and record negotiated version; a V10 fixture does not
close an unavailable live V10 performance gate.
- AC33: GIVEN successful live APC DDL with absent or zero-column manifest WHEN public JDBC executes it
THEN no ResultSet/fetch appears, execute is false, count is zero, and getMoreResults makes it -1.
Repeated close is safe; failed DDL preserves its error, while result-producing SQL without schema
fails before success.
- AC34: GIVEN the deployed default-off APC flag WHEN partner and non-partner User-Agents
request client flags THEN connector-service returns false for every partner, including JDBC
warehouse-allowlisted partners, and retains the ordinary APC value for non-partners. Record the
service version and region before any APC assignment or ramp.
- AC36: GIVEN live APC chunks with multiple Arrow batches and inline continuations WHEN JDBC drains
them THEN every row appears once and row_offset sums all batch rows. Compare values to APC Thrift; if
staging cannot emit multiple batches, retain a fixture and leave that live gate pending. Failed pages
cannot report partial success.
- AC39: GIVEN live inline and linked results at each mode's first exposure WHEN hybrid,
bounded-SEA, and streaming switches vary THEN R17's provider and buffer behavior holds,
rows remain complete, and AC31's request/heap bounds pass.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF THEN APC uses Thrift; on scoped
flag-ON staging completed calls pass their live parity gate. Known incomplete calls fail
before SEA requests without Thrift fallback; public SQL async stays guarded until T39 and
production assignment waits for T18/T-val.
- AC41: GIVEN an executable statement or saved session after a flag change WHEN live
fetch/cancel/close runs THEN R9's current or saved target/protocol is used and the connection
remains usable; compare APC Thrift and warehouse SEA after AC2 passes.
- AC45: GIVEN live LZ4-marked and unmarked APC results WHEN
`EnableSeaResponseCompression` changes between 0 and 1 THEN `0` sends
`Accept-Encoding: identity` on SEA API requests and `1` permits compressed API responses as
on warehouse SEA, while the manifest-selected result codec, decoded values, and
presigned-link compression do not change because of that setting. AC101 owns caller result-codec
selection; compare API bytes and latency with the two baselines.
- AC46: GIVEN paired linked results at one and 32 callers WHEN `cloudFetchThreadPoolSize`, `LinkPrefetchWindow`, chunk wait, and slow-link settings vary independently THEN active downloads and buffered chunks stay within the per-result worker limit, fetched-ahead links within the window, and the aggregate limits scale by caller count. Rows, queue depth, timeouts, warnings, duration, and AC31 load bounds match the baselines; inline pages are unaffected.
- AC47: GIVEN live single-chunk APC inline SELECT WHEN public JDBC reads it THEN rows,
order, and types match both baselines. Fixtures prove R16 wire values, absent
`arrow_schema`, and clear failure for an unknown format.
- AC48: GIVEN live APC counted `INSERT`, `UPDATE`, `DELETE`, or `MERGE` WHEN public
update methods run THEN counts, including zero, match APC Thrift after T32. A missing
`num_affected_rows` column, row, or value produces a contract error, not successful zero.
- AC49: GIVEN live inline and linked SELECTs at each mode's first exposure WHEN
`setMaxRows(0)`, `1`, and page/link-boundary limits run THEN positive limits send SEA
`row_limit`, zero is unlimited, and rows never exceed the limit. Compare rows and request
count with APC Thrift and warehouse SEA; fixtures cover unavailable boundaries.
- AC50: GIVEN live V9+ delayed APC SHOW WHEN each typed handle submits THEN it returns while RUNNING and another call completes before await; a blocking submit fails this gate. Awaited rows/errors, timeout, cancel, cleanup, and both cross-reference conversions match synchronous APC and both baselines. Older/versionless handles complete synchronously, `getColumns` consumes its handle, and procedure SELECTs reject async before request.
- AC51: GIVEN isolated live 429/503 failures with and without `Retry-After` WHEN each new SEA API or linked-download class retries or exhausts under R17 settings THEN attempts, delay, exception, SQLState, and vendor code match warehouse SEA without write replay. Exhausted CreateSession is covered; Thrift HTTP settings affect links, not SDK API requests.
- AC52: GIVEN Reyder's APC `GetStatementResult` route after RUNNING polls WHEN no-result DDL
or counted DML completes THEN direct server contract evidence returns, respectively, success
with absent or zero-column manifest and no data, or a schema and affected-rows data including
zero. Failed statements retain their errors. AC33 and AC48 own public JDBC proof.
- AC53: GIVEN live APC prepared scalars/fallback batches and older/versionless fixtures WHEN `supportManyParameters` is 0 or 1 THEN V8+ at 0 sends native scalars, while older/versionless at 0 and every version at 1 interpolate without unresolved markers; `setBytes` follows R6. Rows/counts and request shape match both baselines; identify fixture-only branches.
- AC54: GIVEN a live APC table with a referencing foreign key WHEN public
`getExportedKeys` runs on scoped flag-ON APC SEA, APC Thrift, and warehouse SEA THEN each
returns a typed empty result, APC SEA issues no exported-key query, and Thrift's parent-only
`GetCrossReference` is observed. If DBR later returns rows on Thrift, record that contract
change and keep APC SEA parity unresolved rather than claiming this gate passed.
- AC55: GIVEN public APC SEA SQL async SELECT WHEN primitive and linked results complete
THEN `wait_timeout=0s`, targeting, rows/errors, cancel, close, and diagnostics follow R2/R10.
Apply AC31's one/32-caller load bounds to this public path before unguarding it.
- AC60: GIVEN a valid live query within its configured client wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Record timing,
error identity, and connection usability against both baselines; an exception needs caller
impact and PECO approval.
- AC61: GIVEN four catalogs with 25 tables and 16 columns each WHEN each
synchronous method or typed async handle runs at 32 callers THEN rows/errors
match both routes, recovery takes at most N+2 requests, and median latency
is at most 110% of APC Thrift. Against a fixed direct-SEA trace on the same
compute/Reyder build, CPU per 1,000 cells is at most 110% and peak RSS above
idle at most 120%. The combined seven-handle mix passes too; record trace
hash, requests, and raw measures. Exceptions need caller impact and PECO/Perf
Eng approval.
- AC63: GIVEN valid delayed SHOW within its configured metadata wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Record timing,
error identity, and usability against both baselines; an exception needs caller impact and
PECO approval.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on live APC SEA, APC Thrift, and warehouse SEA
WHEN a matching INSERT runs THEN `execute` returns true, its ResultSet contains the affected
rows value, and `getUpdateCount` returns `-1`; a Statement batch stops at that command with
the same preceding counts and error identity on all three routes.
- AC65: GIVEN live APC SHOW with `EnableThriftNativeMetadata` unset, `0`, and `1` and
`EnableSeaSyncMetadata` at `0` and `1` WHEN a supported synchronous method runs THEN the
native request header appears only at native setting `1`, the sync header only at sync
setting `1`, and native-shaped JDBC columns/values match APC Thrift and warehouse SEA at
all settings. Async SHOW omits the sync header. A changed Reyder response shape leaves
this gate pending for contract review.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized `getProcedures` and `getProcedureColumns`
WHEN each first runs THEN header-free R5/R6 queries return complete, ordered
JDBC rows across inline pages and links, matching APC Thrift and warehouse
SEA. At one and 32 callers, the paired load limits pass and CPU per 1,000
cells and peak RSS meet AC61's direct-SEA bounds. Record operation/error
telemetry and impact of omitted warehouse procedure headers. Fixtures cover
older/versionless interpolation and `supportManyParameters=1`.
- AC82: GIVEN live `getFunctions` with an explicit catalog and with null catalog WHEN
`EnableShowCommandForGetFunctions` is `0` and `1` on APC SEA, APC Thrift, and warehouse SEA
THEN APC SEA sends the R5 SHOW request at both values, rows and errors follow the documented
warehouse SEA precedent, and the APC Thrift native/SHOW choice and null-catalog difference are
recorded with client impact.
- AC83: GIVEN live null-catalog getSchemas/getColumns with 42601 fan-out WHEN one catalog errors,
times out, or cancels THEN APC and warehouse SEA warn, omit only that catalog, and keep the connection
usable; record lost rows against APC Thrift. Initial object-not-found/empty-pattern yields typed
empty, while other initial or overall failures propagate.
- AC84: GIVEN each newly exposed live request class WHEN metadata wait, socket timeout,
poll interval, and idle expiry vary THEN their R17 scopes, timing, errors, and connection
usability hold, including zero metadata timeout.
- AC85: GIVEN each live execution, metadata, download, batch, and failure path WHEN
telemetry exports THEN R8's redacted event families, operation/format/counts, and error
identity match both baselines or document a measured exception.
- AC86: GIVEN live APC with `EnableSQLValidationForIsValid=1` WHEN
`isValid(timeout)` runs its `SELECT VERSION()` check THEN healthy/failed results, timeout,
and connection usability match APC Thrift and warehouse SEA.
- AC87: GIVEN SQL at 16,777,215/16,777,216/16,777,217 UTF-8 bytes WHEN a single statement or legacy
INSERT chunk executes THEN APC SEA accepts up to the SEA limit and propagates the warehouse SEA
oversize error above it without fallback. Record bytes, counts, committed rows, and APC Thrift
differences; BatchInsertSize can reduce chunk size.
- AC88: GIVEN live APC SEA, APC Thrift, and warehouse SEA metadata with two accessible
catalogs and a metric view WHEN `enableMultipleCatalogSupport` and
`EnableMetricViewMetadata` each change between 0 and 1 THEN `setCatalog`, explicit and
null-catalog discovery, `getTableTypes`, and metric-view table rows have the same visible
scope on all routes. Capture the APC CreateSession metric-view session setting only at 1;
record any server-limited row difference and caller impact.
- AC89: GIVEN live invalid SQL, missing-object, and permission failures WHEN applicable execute
methods surface them THEN record SQLException class, SQLState, vendor code, and message category
across APC SEA/Thrift and warehouse SEA. APC SEA matches agreement between baselines, otherwise
measured warehouse SEA mapping with the caller impact; no failed command reports success.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success. With either hybrid setting, the
APC request stays hybrid; LZ4-marked and unmarked fixtures decode by the manifest codec.
- AC101: GIVEN live APC SEA/Thrift and warehouse SEA inline/linked results WHEN `QueryResultCompressionType` and its higher-precedence LZ4 alias choose `NONE`, `LZ4`, or conflicting values THEN request selection matches warehouse SEA, decoding follows the manifest codec, and unmarked data is uncompressed. At one/32 callers record requested/actual codec, bytes, CPU, throughput, and latency, including `NONE` yielding LZ4; quantify caller impact and get PECO/Perf Eng approval. Selectable APC codec requires a deployed Reyder change.
- AC102: GIVEN live APC SEA/Thrift and warehouse SEA with an isolated write table WHEN
IgnoreTransactions and FetchAutoCommitFromServer vary during set/get auto-commit, insert, commit, and
rollback THEN state, rows, no-ops, and errors match R19. Include missing state-row and failed-command
cases; record exception/cause, SQLState/vendor code, and connection usability.
- AC103: GIVEN paired live APC SEA/Thrift no-result DDL on isolated schemas
WHEN one and 32 callers create and drop distinct tables THEN AC33's
results/errors and the paired command load limits pass.
- AC104: GIVEN paired live APC SEA/Thrift single INSERT, UPDATE, DELETE, and
MERGE on disjoint keys WHEN each runs at one and 32 callers THEN AC48's
counts/errors and the paired command load limits pass.

## Constraints

- APC default protocol follows R1. No change to warehouse requests or routing.
- No new runtime dependencies; reuse the existing SDK client, Arrow, and LZ4 stack.
- Protocol selection is final before CreateSession; never switch from SEA to Thrift after an error.
- New error paths use `DatabricksDriverErrorCode` values per the repo telemetry rules.
- Commits signed off (`git commit -s`). The final JDBC package alone owns `docs/` and
  `NEXT_CHANGELOG.md` and records the complete gated feature. Earlier stacked PRs use
  `NO_CHANGELOG=true` only as internal implementation slices of that same unreleased feature:
  no driver release or production APC assignment may occur between slices. This basis must be
  stated in each PR; if a release is cut early, its visible slice needs a changelog update
  before release. Completed slices remain shippable behind the flag, with no partial ramp.
- Java 11 compatible; formatted with `mvn spotless:apply`.

## Out of Scope

- Server changes beyond T32 and T35; known gaps SC-256188 under SC-256186 and SC-254478 remain
  external prerequisites for affected dogfood checks.
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

`plan.json` is the stacked-PR tree; each JDBC child builds on its parent. T5's
negotiated capability state ships in the first SELECT slice, before V8 binding,
V10 batches, or V9 metadata. Inline continuation precedes linked results;
connection controls precede linked and async SQL. DDL, counted writes, prepared
batches, metadata discovery, key/procedure metadata, and typed async handles
ship as smaller public slices. Procedure SELECTs follow complete-result and
scalar-binding support. Sibling packages own disjoint edit areas; each package
passes its first-exposure live parity, error, telemetry, and resource gates before
review. The larger package count is intentional to keep review diffs below about
1,500 changed lines while retaining these dependencies.

T17/T35 precede scoped flag-on staging; T32 precedes public DDL and counted DML.
The tracked termination fix gates the first SELECT slice, and the inline-only and
metadata fixes gate only their affected slices. T18/T-val run on the assembled
stack after sibling merges and a post-merge smoke check; unresolved live or
performance gates block release and production assignment. T16 owns the sole
JDBC docs/changelog edit in the last functional slice.

### Affected Components

- Public JDBC connection, statement, prepared-statement, and metadata APIs.
- SEA request targeting, session and statement identity, result decoding and paging.
- Driver diagnostics and error taxonomy; Reyder and connector-service contracts in Universe.

### Data Model Changes

- APC target and live capability version in session state.

### API Changes

Public JDBC methods remain the same. Add a session-reachable asynchronous metadata API for
the seven SHOW operations. Public connection IDs may encode APC SEA; statement IDs stay raw.

### Risks

- Tracked server fixes gate live metadata, inline-only, and termination checks; fixtures
  protect unavailable wire shapes before deployment.
- AC13/Q7 block release on lost logical types; AC27 blocks repeated poll payload transfer.
- AC11/AC14 protect warehouse request and presigned-link boundaries.

---

## Tasks

`plan.json` assigns each task once. The common first-exposure gates in the Acceptance
Criteria introduction apply to every package with a new public request path.

### Universe flag and result contract

- [ ] T17: Define the default-off APC flag.
- [ ] T35: Exclude partner User-Agents from the APC flag.
- [ ] T32: Prove or fix no-result and counted-DML server responses.

### Primitive SELECT and capability state (`apc-select-core`)

- [ ] T1: Carry the APC target through SEA.
- [ ] T2: Open and terminate an APC SEA session.
- [ ] T4: Select protocol by R1.
- [ ] T5: Retain the negotiated capability version.
- [ ] T6: Propagate route errors.
- [ ] T7: Disable APC status heartbeat.
- [ ] T8: Expose single-chunk SELECT and SQL-backed isValid.
- [ ] T10: Decode bare Arrow batches.
- [ ] T27: Close APC sessions safely.
- [ ] T45: Target statement execute, poll, cancel, and close.
- [ ] T3: Emit redacted route diagnostics.
- [ ] T47: Send the APC hybrid query shape.
- [ ] T57: Emit first-operation telemetry.

### Inline continuation (`apc-inline-pages`)

- [ ] T11: Continue inline pages.
- [ ] T25: Fetch manifest-only first chunks.
- [ ] T34: Preserve page-boundary max rows.
- [ ] T59: Send and enforce first-chunk row limits.
- [ ] T62: Expose synchronous direct-results-off polling.

### Inline-only mode (`apc-inline-only`)

- [ ] T58: Expose download-off after the inline-only server fix.

### Connection controls (`apc-connection-controls`)

- [ ] T12: Enforce query timeout and targeted cancellation.
- [ ] T26: Check API retries and pool behavior.
- [ ] T19: Round-trip protocol-aware connection IDs.
- [ ] T29: Close saved sessions after route changes.
- [ ] T44: Target recovered statements and redact their diagnostics.

### Linked SELECT (`apc-linked-select`)

- [ ] T15: Redact linked-download diagnostics.
- [ ] T20: Preserve logical result types and getters.
- [ ] T33: Drain complete Arrow IPC links.
- [ ] T36: Bound linked transfer and retain CloudFetch controls.
- [ ] T46: Preserve link-boundary max rows.
- [ ] T61: Decode the actual result codec.

### SQL async (`apc-sql-async`)

- [ ] T39: Expose public SQL async and its result lifecycle.

### No-result DDL (`apc-ddl`)

- [ ] T28: Classify no-result commands.
- [ ] T31: Expose DDL after T32.

### Single DML and scalar binding (`apc-single-writes`)

- [ ] T9: Bind single prepared scalar SQL.
- [ ] T22: Reject unsupported complex bindings.
- [ ] T38: Expose counted DML and non-rowcount override.
- [ ] T60: Preserve transaction calls and errors.

### Prepared and Statement batches (`apc-batches`)

- [ ] T21: Expose native and legacy prepared batches.
- [ ] T30: Execute Statement batches sequentially.

### Catalog and table metadata (`apc-metadata-discovery`)

- [ ] T14: Expose synchronous getCatalogs.
- [ ] T43: Expose synchronous getSchemas.
- [ ] T48: Expose synchronous getTables.
- [ ] T49: Expose synchronous getColumns.

### Keys, functions, and procedures (`apc-metadata-relations`)

- [ ] T13: Expose synchronous getFunctions.
- [ ] T37: Expose synchronous getPrimaryKeys.
- [ ] T54: Expose imported and cross-reference keys.
- [ ] T55: Expose typed-empty exported keys.
- [ ] T42: Expose parameterized getProcedures.
- [ ] T56: Expose parameterized getProcedureColumns.

### Initial typed async metadata (`apc-metadata-async-discovery`)

- [ ] T24: Coordinate async SHOW submit and await.
- [ ] T23: Expose typed async getColumns.
- [ ] T40: Expose typed async getCatalogs.
- [ ] T50: Expose typed async getSchemas.
- [ ] T51: Expose typed async getTables.

### Remaining typed async metadata and documentation (`apc-metadata-async-relations`)

- [ ] T41: Expose typed async getFunctions.
- [ ] T52: Expose typed async getPrimaryKeys.
- [ ] T53: Expose typed async cross-reference conversions.
- [ ] T16: Document the gated feature and own the sole JDBC changelog entry.
- [ ] T18: Run integrated local and live checks.
- [ ] T-val: Record every AC outcome and block open gates.
