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
- The earlier always-LZ4 APC assumption is narrower in the current server contract: Reyder
  marks LZ4 only when DBR reports compression; AC45 covers uncompressed responses.
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
  | Routing and failure | Default Thrift | Flag/override; SEA errors propagate | Dedicated APC flag; no fallback | Warehouse SEA precedent; an explicit SEA caller receives the SEA error (R1, R7). |
  | SELECT values, paging, `setMaxRows` | Inline/CloudFetch; row limit | Inline/links; `row_limit` | Same rows and limit; APC continuation | Match; APC wire uses `next_chunk_index`/`row_offset`, invisible except request cost (R4, R12; AC31, AC67). |
  | Download and hybrid toggles | Inline or CloudFetch | Can require links or choose Thrift | Hybrid always; download-off stays SEA | APC `ARROW_BATCH` has only hybrid disposition; caller may get inline data or an inline-size error (R4; AC8). |
  | DDL and DML update counts | DDL zero; DML affected rows | Same | Same, including zero | Match; missing DML count is a contract error rather than a false zero (R4; AC33, AC48). |
  | Metadata and exported keys | Native RPC; parent-only cross-reference empty | SHOW; exported keys typed empty | SHOW/native-shaped; exported keys typed empty | Warehouse/DBR precedent; no populated exported-key rows today, so callers retain empty results (R5; AC54). |
  | Null-catalog recovery | Native RPC, no fan-out | `42601` fan-out may omit faulty catalog | Same fan-out | Warehouse SEA precedent; callers may miss one catalog's rows and receive a warning (R5; AC83). |
  | Typed async metadata | Server async; JDBC call waits | SHOW; JDBC call waits | V9+ handles overlap; older handles wait | New optional API; no change to synchronous JDBC, while V9+ handle callers can overlap work (R5; AC50). |
  | Catalog/metric-view tuning | Current-catalog filter; metric-view setting | Same | Same visible rows/types | Match (R5; AC88). |
  | Metadata command tuning | Native/SHOW functions | SHOW; native and sync headers | SHOW functions; native-shaped response | Warehouse SEA precedent; function rows retain its catalog semantics (R5; AC65, AC82). |
  | Procedure metadata | Native RPC/SELECT | SELECT with metadata headers | SELECT without APC headers | Reyder rejects these APC headers; callers retain JDBC columns and rows (R5; AC75, AC76). |
  | Scalar and complex bindings | Scalars and some complex values | Same | V8+ native scalars; older/versionless scalar interpolation; unsupported complex rejected | Preserve scalar fallback; APC SEA cannot losslessly interpolate complex values, so affected callers get an early coded error (R6; AC29, AC53). |
  | Prepared/Statement batches | Legacy/native prepared; sequential Statement | Same; native per-set counts | Same counts and limits | Match; incomplete native count data blocks success, uncertain failures require reconciliation (R6, R15; AC32, AC66). |
  | SQL text over 16 MiB | Backend limit measured | SEA rejects above 16,777,216 bytes | Same SEA rejection | Warehouse precedent; callers must shorten SQL and reconcile earlier committed chunks (R6; AC87). |
  | Arrow prerequisite and heartbeat | No Arrow prerequisite; status heartbeat | Explicit override before Arrow check; `/status` | Arrow-first; no heartbeat | APC status RPC unsupported; callers without Arrow use Thrift and open ResultSets get no heartbeat (R1, R3; AC1, AC3). |
  | `UseBoundedSeaApi` and memory | Provider selection; `ThriftMaxBatchesInMemory` | Bounded inline streaming when enabled | Bounded APC inline and linked streaming at either setting | APC paging cannot use a totals-dependent provider; `UseBoundedSeaApi=0` cannot opt APC out, while `ThriftMaxBatchesInMemory` still bounds APC inline buffering (R12, R17; AC39). |
  | `EnableInlineStreaming` | Selects streaming versus lazy Thrift inline providers | No effect on SEA providers | No effect on APC SEA providers | Warehouse SEA precedent: this switch selects only Thrift providers in `ExecutionResultFactory`; APC callers retain bounded SEA inline streaming at either value (AC39). |
  | `EnableStreamingChunkProvider` | Selects streaming versus legacy Thrift links | Selects linked SEA provider unless `UseBoundedSeaApi=1` forces streaming | Streaming linked provider at either value | APC links require continuation without totals, so `0` cannot select the legacy provider; worker and prefetch tuning still applies (R12, R17; AC39, AC46). |
  | Complex/geospatial result settings | Controls values/getters | Same; inline typed getters reject | Same | Warehouse precedent; typed complex getters need linked results (R11; AC13). |
  | `RowsFetchedPerBlock` | Tunes Thrift fetch | Does not tune SEA paging | Does not tune paging | Warehouse precedent; existing fetch-size tuning has no SEA effect (R17; AC31). |
  | API compression and result codec | Thrift compression; result codec setting | Caller selects result codec; API compression is separate | Send selected result codec; decode actual manifest codec | Match request semantics; a Reyder override needs measured impact. API compression cannot change result or link codec (R4; AC45, AC101). |
  | CloudFetch workers/prefetch/wait | Configurable links | Same | Same for links | Match linked scope; inline paging is unaffected (R17; AC46). |
  | Query/metadata waits and idle expiry | HTTP and metadata waits | SDK/link timeouts, polling, idle expiry | Same SEA scopes | Warehouse precedent; caller timeout settings affect their matching request classes (R10, R14, R17; AC84). |
  | Public SQL async | Thrift async result retrieval | SEA polling/result retrieval | APC-targeted SEA polling | Match public rows, lifecycle and load bounds; polling adds SEA request cost (R10; AC55). |
  | Synchronous direct results | Thrift may return first results with execute | `EnableDirectResults=0` requires result polling | Same SEA choice and polling when disabled | Warehouse precedent; callers may see extra round trips and longer first-row latency (R10; AC12, AC60). |
  | Public connection ID and saved close | APC ID encodes Thrift handle, but `closeConnection` reselects the current client | Warehouse ID encodes SEA even on a Thrift route; changed flags can misroute close | New IDs encode selected protocol and APC target; statement ID stays raw | New saved IDs close correctly. Legacy warehouse Thrift IDs lack the secret needed to recover the route and may still fail close; callers must reopen (R9; AC25, AC41). |
  | Autocommit and transactions | `IgnoreTransactions=1` makes set/commit/rollback no-ops; at `0` SQL commands run | Same driver calls and SEA SQL path | Same public state, committed/rolled-back rows, and errors | Match; `FetchAutoCommitFromServer=1` requires a returned state even when transaction calls are ignored (R19; AC102). |
  | Statement errors | Thrift SQLState/code | SEA transient remapping | Same SEA mapping | Warehouse precedent where routes differ; caller retry classification may change (R4; AC89). |
  | 429/503 and `Retry-After` | Thrift HTTP controls | SDK API; HTTP-client links | Same per request class | Warehouse precedent; caller may see different retry delay/error, with no new write replay (R17; AC51). |
  | HTTP pools | Thrift pool/wait | SDK API pool; separate link client | Same per request class | Warehouse precedent; Thrift pool wait setting does not bound SDK acquisition (R17; AC30). |
  | Telemetry/resources | Connection, operation, error events | Same families | Same families; measured SEA resources | Match event visibility; no new caller-facing telemetry gap (R8, R17; AC61, AC85). |

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
decompression capability and sends the result codec selected by `QueryResultCompressionType`
or its `EnableQueryResultLZ4Compression` alias, whose explicit value takes precedence. It decodes
inline and linked payloads according to the response manifest's codec; an unmarked response
is uncompressed. A server override of the requested codec needs AC101's measured exception.
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

For paired client loads (AC31/32/61), use the same driver build, compute, data, and network
within each route pair. Results and batches cover one and 32 callers; metadata covers 32.
Record rows, errors, requests, bytes, throughput, latency, and peak/retained heap; apply limits
to repeated paired medians. AC61 defines the separate server-resource reference. Isolate one
request class and target for fault injection; use a real server failure to prove server error identity.
- AC1: GIVEN live APC/warehouse routes and Arrow-unavailable or legacy-path fixtures WHEN the APC
flag, explicit UseThriftClient, and catalog-pattern setting vary THEN R1's precedence holds, including
override, partner exclusion, fail-closed lookup, and unchanged warehouse routing. Explicit SEA logs
the ignored Thrift-only setting; no route falls back after selection.
- AC2: GIVEN scoped flag-ON APC staging and a cluster HTTP path WHEN a public connection opens,
runs a primitive single-chunk SELECT, and closes THEN CreateSession has the URL's cluster id in
`backend_compute`, no `warehouse_id`, the SELECT returns correct rows, and one APC-targeted
TerminateSession succeeds after the tracked binding fix. Compare APC Thrift and unchanged
warehouse SEA opening, SELECT, and close.
- AC3: GIVEN each newly exposed live APC R2 operation WHEN it runs THEN target placement,
bodylessness, cancel/close, and no-status-heartbeat behavior match R2/R3. A running statement closes
once and leaves the connection usable; warehouse request serialization stays unchanged. Session
termination waits for its tracked server fix.
- AC6: GIVEN live APC prepared SQL with bound scalars WHEN `getMetaData()` runs before
execution THEN it returns JDBC metadata without losing bindings; subsequent execution
matches APC Thrift and warehouse SEA. AC53 covers the version and interpolation matrix.
- AC8: GIVEN live single-chunk and later multi-page APC SELECTs with
`EnableQueryResultDownload=0` WHEN they run on flag-ON staging THEN CreateSession sends
`session_confs["can_cloud_download"]="false"`, every result page stays inline, and an
inline-size error propagates before partial success. `UseBoundedSeaApi` does not change
this; the tracked server fix gates first exposure.
- AC9: GIVEN live APC SEA CreateSession rejection WHEN connecting THEN the error and failed
route event propagate without Thrift fallback. GIVEN APC Thrift OpenSession returns KP001 WHEN
connecting THEN warehouse-only recovery does not select SEA.
- AC10: GIVEN successful and rejected opens and first statements WHEN telemetry and DEBUG logs are
captured THEN route, outcome, compute, and protocol appear without credentials or raw APC IDs. Recheck
execute/poll, linked, and recovered paths as each becomes public.
- AC11: GIVEN standard and Real-time warehouse connections on live staging WHEN R2's
equivalent operations run THEN JDBC outcomes and request bodies, URLs, and headers match
pre-change `origin/main`, including `realtimeSession`. Each slice checks affected classes;
fixtures protect exact serialization and T18 closes the full route matrix.
- AC12: GIVEN live APC SEA synchronous primitive single-chunk SELECT with
`EnableDirectResults=1` WHEN requests and first-row time are captured THEN `wait_timeout`
and `statements` are absent, and rows/errors match APC Thrift and warehouse SEA.
AC93 covers the direct-results-off variant; AC55 owns public SQL async.
- AC13: GIVEN live primitive, decimal, array, map, struct, interval, timestamp,
timestamp_ntz, geography, geometry, and variant values WHEN APC reads inline
and linked results with R11 settings at 0 and 1 THEN metadata/getters match both baselines; inline
typed complex getters reject, linked enabled getters work, and disabled settings retain strings.
Unavailable types remain pending with fixtures.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN live APC getCatalogs/getSchemas WHEN either emitted metadata format arrives THEN JDBC
columns, values, nulls, and errors match both baselines. Captured fixtures cover R13's eight vectors,
null-mask order and lengths, and a columnar-to-Arrow response switch.
- AC17: GIVEN live V10 APC or a captured V10 response WHEN a native prepared INSERT batch succeeds or
fails THEN ordered parameter_sets has one count per set on success and only confirmed prefix counts on
failure; uncertain failure reports empty counts. Legacy behavior remains at native-off/older versions;
unavailable V10 live performance stays pending.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. Compare elapsed time, SQLState/vendor code, target, and
post-timeout connection usability on flag-ON staging.
- AC22: GIVEN a live Statement INSERT batch with a successful and a failing middle command
WHEN `executeBatch` runs THEN APC SEA sends sequential requests without `statements`, returns
ordered counts, stops on failure, and matches APC Thrift errors. AC32 gates its throughput;
AC64 and AC66 cover configured ResultSet and size-limit cases.
- AC23: GIVEN a live APC CreateSession and captured older/versionless variants WHEN a
connection opens THEN capability state comes only from `negotiated_protocol_version`;
neither `session_version` nor an encoded ID grants a capability. Record the live value,
missing-version diagnostic, and unchanged warehouse behavior. AC53 owns execution choices.
- AC25: GIVEN newly issued APC SEA/Thrift and warehouse SEA/Thrift connection IDs WHEN
`Driver.closeConnection` receives a URL with a changed flag or target THEN the encoded
protocol and target close only the original session. Legacy warehouse SEA/Thrift IDs retain
their existing decoding; record that prechange warehouse Thrift IDs lack a recoverable secret
and may fail saved close. Malformed IDs fail without logging identifiers; statement IDs stay raw.
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
- AC30: GIVEN live pool saturation for each newly exposed API or linked-download request
class on all three routes, starting with CreateSession, WHEN
`HttpConnectionPoolSize`, `HttpMaxConnectionsPerRoute`, and
`HttpConnectionRequestTimeout` vary independently THEN in-flight requests, wait, error,
and throughput follow the matrix's pool scopes; SDK API acquisition ignores the wait setting.
- AC31: GIVEN paired APC SEA/Thrift 1/2 MiB inline and 256/512 MiB linked SELECTs WHEN one and 32
callers run with RowsFetchedPerBlock varied THEN rows/errors match, SEA paging ignores that property,
throughput/latency regress by at most 10%, peak heap by at most 20%, and retained heap for doubled
data adds at most max(8 MiB, 5% of added bytes). Record requests, bytes, retries, and three medians;
server-limited exceptions need caller impact and PECO/Perf Eng approval.
- AC32: GIVEN paired APC SEA/Thrift 1,000-row INSERT batches with disjoint keys WHEN each
newly exposed legacy or native prepared mode or sequential Statement mode runs at one and
32 callers THEN counts, committed rows, and errors match its route baseline and median
throughput/latency regress by at most 10%. Vary R6's relevant batch settings and record
requests, version, and three paired medians. A server-limited exception needs caller impact
and PECO/Perf Eng approval; unavailable live V10 performance stays pending after AC17's fixture.
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
- AC39: GIVEN live inline pages and links WHEN hybrid, bounded-SEA, and streaming switches vary
independently THEN APC retains hybrid disposition and complete, bounded results. Thrift streaming
switches retain their Thrift effect; ThriftMaxBatchesInMemory bounds APC inline buffering at minimum
two. Capture provider, transfer, requests, and peak heap under AC31.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF THEN APC uses Thrift; on scoped
flag-ON staging completed calls pass their live parity gate. Known incomplete calls fail
before SEA requests without Thrift fallback; public SQL async stays guarded until T39 and
production assignment waits for T18/T-val.
- AC41: GIVEN an already executable APC statement or saved APC session after a flag change
WHEN fetch, cancel, close, or termination runs on live staging THEN the current or saved
target/protocol follows R9 and connection usability matches APC Thrift or warehouse SEA.
The SELECT path and successful AC2 session close must pass before this gate is exposed.
- AC45: GIVEN live LZ4-marked and unmarked APC results WHEN
`EnableSeaResponseCompression` changes between 0 and 1 THEN `0` sends
`Accept-Encoding: identity` on SEA API requests and `1` permits compressed API responses as
on warehouse SEA, while the manifest-selected result codec, decoded values, and
presigned-link compression do not change because of that setting. AC101 owns caller result-codec
selection; compare API bytes and latency with the two baselines.
- AC46: GIVEN live linked and delayed inline results WHEN R17's CloudFetch worker,
prefetch, wait, and slow-link settings vary independently THEN rows, worker/queue bounds,
timeouts, warnings, and transfer duration follow the matrix. Prefetch and slow-link settings
do not change inline pages; AC31 gates throughput and heap.
- AC47: GIVEN live primitive APC single-chunk inline SELECT WHEN public JDBC consumes it
THEN rows, order, types, and R16 wire values match APC Thrift and warehouse SEA. An
absent-`arrow_schema` fixture decodes from `manifest.schema`; an unknown-format fixture
deserializes safely and fails clearly before false success.
- AC48: GIVEN live APC counted `INSERT`, `UPDATE`, `DELETE`, or `MERGE` WHEN public
update methods run THEN counts, including zero, match APC Thrift after T32. A missing
`num_affected_rows` column, row, or value produces a contract error, not successful zero.
- AC49: GIVEN live linked APC queries with `setMaxRows(0)`, `1`, and a link-boundary limit
WHEN results are read THEN positive limits send SEA `row_limit`, zero is unlimited, and rows
never exceed the limit; compare rows and request count with APC Thrift and warehouse SEA.
A boundary unavailable live remains pending with a contract fixture. AC67 covers inline pages.
- AC50: GIVEN live V9+ APC delayed SHOW WHEN each newly exposed typed handle submits THEN
it returns while SHOW is RUNNING and another operation finishes before await. Awaited rows,
empty/error results, timeout/cancel, and cleanup match synchronous APC, APC Thrift, and
warehouse SEA; both cross-reference conversions stay distinct. Older/versionless handles
complete synchronously; `getColumns` consumes its handle, and procedure SELECTs reject async
before request. A blocking V9+ submit fails even if final rows match.
- AC51: GIVEN isolated live 429/503 failures with and without Retry-After WHEN each newly exposed SEA
API or linked-download request class retries or exhausts under its relevant R17 settings THEN attempt
count, delay, exception, SQLState, and vendor code match warehouse SEA, without new write replay.
Include exhausted CreateSession 429/503 and show Thrift HTTP settings affect links, not SDK API
requests. Vary `RateLimitRetry`, `RateLimitRetryTimeout`, `TemporarilyUnavailableRetry`,
`TemporarilyUnavailableRetryTimeout`, `ApiRetriableHttpCodes`, and `ApiRetryTimeout` independently.
- AC52: GIVEN Reyder's APC `GetStatementResult` route after RUNNING polls WHEN no-result DDL
or counted DML completes THEN direct server contract evidence returns, respectively, success
with absent or zero-column manifest and no data, or a schema and affected-rows data including
zero. Failed statements retain their errors. AC33 and AC48 own public JDBC proof.
- AC53: GIVEN live APC prepared scalars and fallback batches at available versions, plus
captured older/versionless CreateSession fixtures WHEN `supportManyParameters` is 0 or 1
THEN V8+ setting 0 sends native scalar bindings; older/versionless setting 0 and setting 1
at any version interpolate scalars without unresolved markers. `setBytes` follows R6.
Compare rows/counts and request shape with APC Thrift and warehouse SEA; identify fixture-only branches.
- AC54: GIVEN a live APC table with a referencing foreign key WHEN public
`getExportedKeys` runs on scoped flag-ON APC SEA, APC Thrift, and warehouse SEA THEN each
returns a typed empty result, APC SEA issues no exported-key query, and Thrift's parent-only
`GetCrossReference` is observed. If DBR later returns rows on Thrift, record that contract
change and keep APC SEA parity unresolved rather than claiming this gate passed.
- AC55: GIVEN public APC SEA `executeAsync` WHEN `getExecutionResult` polls and consumes
primitive inline and linked results THEN `wait_timeout=0s`, rows/errors, targeting,
cancellation, close, and diagnostics follow R2/R10 and route baselines. The paired live load
gate applies AC31's throughput, latency, one/32-caller concurrency, transfer, and heap bounds
to this public path, including polling requests. Both methods stay guarded until it passes.
- AC60: GIVEN a valid long-running APC query within its configured client wait WHEN it crosses the
measured
single-request timeout THEN APC SEA completes, including result polling, without premature server timeout.
Record duration, retry/final error identity, and connection usability against APC Thrift and warehouse SEA;
a server limit remains pending without cause, caller impact, and PECO approval.
- AC61: GIVEN four catalogs with 25 tables and 16 columns each WHEN each exposed metadata method runs
at 32 callers THEN rows/errors match both route pairs, recovery uses at most N+2 requests, and median
latency regresses by at most 10%. Against a fixed direct-SEA trace on the same compute/Reyder build,
CPU per 1,000 cells regresses by at most 10% and peak RSS above idle by at most 20%. Record trace
hash, requests, raw measures, and three medians; exceptions need caller impact and PECO/Perf Eng
approval.
- AC63: GIVEN valid delayed APC SHOW within its configured metadata wait WHEN it crosses the measured
single-request timeout THEN APC SEA completes, including polling, without premature server timeout. Record
duration, retry/final error identity, and connection usability against APC Thrift and warehouse SEA; a
server limit remains pending without cause, caller impact, and PECO approval.
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
- AC67: GIVEN live inline APC queries with `setMaxRows(0)`, `1`, and a page-boundary limit
WHEN results are read THEN positive limits send SEA `row_limit`, zero is unlimited, and rows
never exceed the limit; compare rows and request count with APC Thrift and warehouse SEA.
This gate includes the single-chunk zero/one subset and the page boundary.
AC73–AC77 apply R5's request, format, JDBC value, error, and two-route parity checks to each
public synchronous metadata method at its first exposure; AC61 applies per-method load gates.
- AC73: GIVEN live APC `getTables` and `getColumns`, including null catalog for columns,
WHEN each method's emitted Arrow IPC and columnar responses arrive THEN the shared checks
hold for each method, including column request count and vector/null shapes. AC16 owns
the eight-vector fixture proof.
- AC75: GIVEN live APC `getProcedures` WHEN its parameterized SELECT runs THEN R5's
header-free ordinary query and R6's versioned scalar policy return the same JDBC columns
and rows as APC Thrift and warehouse SEA. Older/versionless fixtures prove interpolation
at setting `0`; setting `1` interpolates at every version before exposure.
- AC76: GIVEN live APC `getProcedureColumns` WHEN its parameterized SELECT runs THEN
R5's header-free format, R6's scalar policy, and JDBC conversion match the two route
baselines. Verify older/versionless setting `0` and all-version setting `1` before exposure.
- AC77: GIVEN live APC `getPrimaryKeys`, `getImportedKeys`, and `getCrossReference` WHEN
each SHOW runs THEN the shared checks hold for each method, including both distinct
cross-reference JDBC conversions.
- AC82: GIVEN live `getFunctions` with an explicit catalog and with null catalog WHEN
`EnableShowCommandForGetFunctions` is `0` and `1` on APC SEA, APC Thrift, and warehouse SEA
THEN APC SEA sends the R5 SHOW request at both values, rows and errors follow the documented
warehouse SEA precedent, and the APC Thrift native/SHOW choice and null-catalog difference are
recorded with client impact.
- AC83: GIVEN live null-catalog getSchemas/getColumns with 42601 fan-out WHEN one catalog errors,
times out, or cancels THEN APC and warehouse SEA warn, omit only that catalog, and keep the connection
usable; record lost rows against APC Thrift. Initial object-not-found/empty-pattern yields typed
empty, while other initial or overall failures propagate.
- AC84: GIVEN each exposed live request class WHEN MetadataOperationTimeout, socketTimeout,
AsyncExecPollInterval, and IdleHttpConnectionExpiry vary independently THEN metadata wait/cancel,
SDK/link timeout, poll spacing, and idle expiry follow their R17 scopes. Record timing, error,
usability, and zero-metadata-timeout behavior.
- AC85: GIVEN each newly exposed live execution, metadata, download, batch, or failure path
WHEN its public slice exports telemetry THEN applicable operation, format, chunk/download, and
error families include correct type, count, name/code, SQLState, and vendor code. Compare
event presence and redacted route correlation with both baselines; document exceptions.
- AC86: GIVEN a live APC SEA connection with
`EnableSQLValidationForIsValid=1` WHEN `Connection.isValid(timeout)` executes its
`SELECT VERSION()` check THEN it returns true for a healthy connection, false for a failed
check, honors the supplied query timeout, and leaves the connection usable; compare both
outcomes with APC Thrift and warehouse SEA at this call's first exposure.
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
- AC93: GIVEN live synchronous APC SELECT with `EnableDirectResults=0` WHEN requests and
first-row time are captured THEN `wait_timeout=0s`, GetStatementResult polling yields the
same rows/errors as AC12, and no `statements` field is sent. Compare the extra round trip
with APC Thrift and warehouse SEA.
- AC94: GIVEN paired live APC SEA/Thrift single-chunk 1/2 MiB inline SELECTs at one and
32 callers WHEN the first slice runs THEN rows/errors match, throughput and median latency
regress by at most 10%, peak heap by at most 20%, and no APC-only cap appears. Record
requests, bytes, and three paired medians; AC31 owns multi-page and linked loads.
- AC99: GIVEN AC61's fixed workload and direct-SEA reference WHEN each newly exposed typed
SHOW handle runs at 32 callers THEN its rows/errors, recovery bound, latency, and Reyder
CPU/memory satisfy AC61. After all seven handles ship, run the combined mix under the same
limits; AC50 separately proves delayed-SHOW overlap.
- AC101: GIVEN live APC and warehouse SEA results with QueryResultCompressionType and its LZ4 alias,
including conflicting values, WHEN NONE or LZ4 is selected THEN APC sends the warehouse-equivalent
result_compression and decodes actual inline/link manifest codecs. Record codec, bytes, CPU, and
latency at one/32 callers against both baselines; a Reyder override needs server evidence, caller
impact, and approval.
- AC102: GIVEN live APC SEA/Thrift and warehouse SEA with an isolated write table WHEN
IgnoreTransactions and FetchAutoCommitFromServer vary during set/get auto-commit, insert, commit, and
rollback THEN state, rows, no-ops, and errors match R19. Include missing state-row and failed-command
cases; record exception/cause, SQLState/vendor code, and connection usability.
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

`plan.json` is the authoritative stacked-PR plan. `parent` is the Git ancestry edge;
`depends_on` names explicit cross-repo deployment gates. The first JDBC slice opens an APC
SEA connection, runs a primitive single-chunk SELECT, and closes its statement and session.
Later slices add inline controls, linked and SQL-async results, writes, synchronous metadata
methods, and finally typed async metadata. Each slice exposes its completed public calls
behind the default-off flag and passes its live gate before approval. The JDBC packages
form one ancestry chain so integrated release evidence inherits every slice. Independent
public gates are separate PRs to keep each review diff below about 1,500 lines; this
requires more packages than the 4–7 target.

Universe T17/T35 must deploy before flag-on JDBC approval; T32 and tracked server fixes gate
their affected DDL/DML, termination, inline-only, and metadata outcomes. T18/T-val audits the final stack with deployed server prerequisites and one result per AC.
Unresolved live or performance gates block release and production flag assignment; the
post-merge smoke must also pass.

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

`plan.json` defines the stacked PR tree. Each task below has one package and one observable
outcome; a package passes its listed live gate before review. Shared AC3/11/40/51/84/85
apply to each newly exposed request class under the Acceptance Criteria preamble.

### Default-off APC flag and partner exclusion (`apc-flag-partner`)

- [ ] T17: Define the default-off APC SAFE sub-flag without assignment or ramp.
- [ ] T35: Force APC flag false for partner User-Agents without changing warehouse allowlist.

### No-result and counted-DML server contract (`apc-result-contract`)

- [ ] T32: Prove or fix Reyder no-result DDL and counted-DML result contracts.

### Connection, primitive SELECT, and close (`apc-select-core`)

- [ ] T1: Carry APC compute identity through SEA without changing warehouse targeting.
- [ ] T2: Open and terminate an APC SEA session on the public connection path.
- [ ] T3: Emit redacted connection route and outcome diagnostics, including failed opens.
- [ ] T4: Select APC protocol by Arrow prerequisite, explicit override, catalog preference, then APC flag.
- [ ] T5: Capture CreateSession negotiated capabilities independently of serialized IDs.
- [ ] T6: Propagate APC connection failures without SEA-to-Thrift or KP001 route switches.
- [ ] T7: Suppress APC status heartbeats while retaining result polling.
- [ ] T8: Run a primitive single-chunk SELECT and SQL-backed `Connection.isValid`.
- [ ] T10: Decode marked LZ4 and unmarked inline bare Arrow batches.
- [ ] T26: Verify first-session SDK retry, rate-limit, timeout, and pool behavior.
- [ ] T27: Terminate APC sessions and make repeated close safe.
- [ ] T45: Target execution, poll, cancel, and close; preserve statement errors.
- [ ] T47: Send APC hybrid SELECT shape; guard inline-only mode until its slice.
- [ ] T57: Emit redacted first-statement execute, poll, and close telemetry.

### Inline pages, controls, and saved sessions (`apc-inline-controls`)

- [ ] T11: Drain inline pages by `next_chunk_index` and summed `row_offset`.
- [ ] T12: Apply query timeout and targeted cancellation without premature request expiry.
- [ ] T19: Round-trip APC session IDs while retaining raw statement IDs and legacy decoding.
- [ ] T25: Fetch APC result chunks with bodyless, targeted GetResultData requests.
- [ ] T29: Close saved sessions using saved protocol and target after flag/URL changes.
- [ ] T34: Retain `setMaxRows` across inline page boundaries.
- [ ] T44: Use the current connection target for recovered statements.
- [ ] T58: Enable inline-only/direct-results-off modes and fetch manifest-only first chunks.
- [ ] T59: Send positive `row_limit` and cap first-chunk rows.

### Linked SELECT, logical types, and result codec (`apc-linked-select`)

- [ ] T15: Redact linked-result and recovered-statement diagnostics.
- [ ] T20: Prove logical-type metadata and getter parity without silent coercion.
- [ ] T33: Stream complete Arrow IPC links and continue linked results.
- [ ] T36: Preserve CloudFetch controls and bounded linked transfer and heap.
- [ ] T46: Retain `setMaxRows` across external-link boundaries.
- [ ] T61: Honor result codec settings and decode actual inline and linked manifest codecs.

### Public SQL async (`apc-sql-async`)

- [ ] T39: Expose public SQL `executeAsync` and `getExecutionResult` lifecycle.

### No-result DDL (`apc-no-result-ddl`)

- [ ] T28: Classify successful no-result DDL without fetching a ResultSet.
- [ ] T31: Expose no-result DDL after server contract proof.

### Single prepared writes and transactions (`apc-single-writes`)

- [ ] T9: Run single prepared scalar DML with versioned binding and binding-free `getMetaData`.
- [ ] T22: Reject unsupported complex bindings before a request with a coded error.
- [ ] T38: Expose counted DML, missing-count errors, and `NonRowcountQueryPrefixes`.
- [ ] T60: Preserve autocommit, commit, rollback, state-fetch, and transaction errors.

### Prepared batches (`apc-prepared-batches`)

- [ ] T21: Expose native V10 and legacy prepared batches with ordered per-set counts.

### Sequential Statement batches (`apc-statement-batches`)

- [ ] T30: Expose sequential Statement batches with stop and size-limit behavior.

### Synchronous catalogs and schemas (`apc-catalog-schema-sync`)

- [ ] T14: Expose synchronous `getCatalogs` with Arrow IPC and columnar decoding.
- [ ] T43: Expose synchronous `getSchemas`, including null catalog recovery.

### Synchronous tables, columns, and procedures (`apc-table-column-procedures`)

- [ ] T42: Expose parameterized `getProcedures` without APC metadata headers.
- [ ] T48: Expose synchronous `getTables` and catalog/metric-view tuning.
- [ ] T49: Expose synchronous `getColumns`, including null catalog recovery.
- [ ] T56: Expose parameterized `getProcedureColumns` without APC metadata headers.

### Synchronous functions and keys (`apc-function-key-sync`)

- [ ] T13: Expose synchronous `getFunctions` at both SHOW tuning values.
- [ ] T37: Expose synchronous `getPrimaryKeys`.
- [ ] T54: Expose synchronous imported-key and cross-reference conversions.
- [ ] T55: Expose typed-empty `getExportedKeys`.

### First typed async SHOW handles (`apc-typed-metadata-core`)

- [ ] T23: Expose typed async `GET_COLUMNS` and consume its handle from synchronous JDBC.
- [ ] T24: Coordinate async SHOW submit, await, timeout, cancel, and older-version fallback.
- [ ] T40: Expose typed async `GET_CATALOGS`.
- [ ] T50: Expose typed async `GET_SCHEMAS`, including null catalog.

### Remaining typed async SHOW handles (`apc-typed-metadata-rest`)

- [ ] T41: Expose typed async `GET_FUNCTIONS`.
- [ ] T51: Expose typed async `GET_TABLES`.
- [ ] T52: Expose typed async `GET_PRIMARY_KEYS`.
- [ ] T53: Expose typed async `GET_CROSS_REFERENCE` with both conversions.

### Documentation and integrated release evidence (`apc-release-evidence`)

- [ ] T16: Document the complete gated feature and add the sole JDBC changelog entry.
- [ ] T18: Run local and live integrated validation against deployed prerequisites.
- [ ] T-val: Record each AC's pass, failure, or justified unavailable-version fixture; block release on other open gates.
