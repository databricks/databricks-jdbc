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
(API contract, Phase II, and metadata tabs). R1–R18 are the authoritative driver contract;
the matrix and acceptance criteria summarize parity and observable outcomes. The
server also limits SQL text to 16 MiB (16,777,216 UTF-8 bytes), rejects non-OSS-JDBC clients, and starts rollout opt-in
before defaulting APC to Reyder through the dedicated flag.
- Driver baseline verified against `origin/main` @ `840f3a407` (merged spec #1809;
  `UseBoundedSeaApi` default #1827): APC routes to Thrift before the warehouse flag logic.
  `DatabricksSdkClient` assumes `Warehouse` for SEA session/statement requests, polls with
  `GET /statements/{id}`, and heartbeats through `/status`. Its session close and
  `getConnectionId()` paths assume warehouse or Thrift identities. SEA lacks APC
  `ARROW_BATCH`, async SHOW, and `columnar_data` support. Public SQL `executeAsync` is separate
  from metadata SHOW async.
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
  | Metadata command tuning | Native/SHOW functions | SHOW; native and sync headers | SHOW functions; native-shaped response | Warehouse SEA precedent; function rows retain its catalog semantics (R5; AC43, AC65, AC82). |
  | Procedure metadata | Native RPC/SELECT | SELECT with metadata headers | SELECT without APC headers | Reyder rejects these APC headers; callers retain JDBC columns and rows (R5; AC75, AC76). |
  | Scalar and complex bindings | Scalars and some complex values | Same | Scalars; unsupported complex rejected | APC SEA wire cannot losslessly interpolate complex values; affected callers get an early coded error (R6; AC29, AC53). |
  | Prepared/Statement batches | Legacy/native prepared; sequential Statement | Same; native per-set counts | Same counts and limits | Match; incomplete native count data blocks success, uncertain failures require reconciliation (R6, R15; AC32, AC66). |
  | SQL text over 16 MiB | Backend limit measured | SEA rejects above 16,777,216 bytes | Same SEA rejection | Warehouse precedent; callers must shorten SQL and reconcile earlier committed chunks (R6; AC87). |
  | Arrow prerequisite and heartbeat | No Arrow prerequisite; status heartbeat | Explicit override before Arrow check; `/status` | Arrow-first; no heartbeat | APC status RPC unsupported; callers without Arrow use Thrift and open ResultSets get no heartbeat (R1, R3; AC7, AC24). |
  | `UseBoundedSeaApi` and memory | Provider selection; `ThriftMaxBatchesInMemory` | Bounded inline streaming | Forced APC streaming | APC paging requires bounded streaming; caller setting cannot select legacy provider (R12, R17; AC39). |
  | Complex/geospatial result settings | Controls values/getters | Same; inline typed getters reject | Same | Warehouse precedent; typed complex getters need linked results (R11; AC13). |
  | `RowsFetchedPerBlock` | Tunes Thrift fetch | Does not tune SEA paging | Does not tune paging | Warehouse precedent; existing fetch-size tuning has no SEA effect (R17; AC31). |
  | API compression and result codec | Thrift compression | API compression separate from result codec | Separate API compression and manifest codec | Warehouse SEA precedent; API setting does not change LZ4 decoding or presigned links (R4, R17; AC45). |
  | CloudFetch workers/prefetch/wait | Configurable links | Same | Same for links | Match linked scope; inline paging is unaffected (R17; AC46). |
  | Query/metadata waits and idle expiry | HTTP and metadata waits | SDK/link timeouts, polling, idle expiry | Same SEA scopes | Warehouse precedent; caller timeout settings affect their matching request classes (R10, R14, R17; AC84). |
  | Public SQL async | Thrift async result retrieval | SEA polling/result retrieval | APC-targeted SEA polling | Match public rows, lifecycle and load bounds; polling adds SEA request cost (R10; AC55). |
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

- R3: For APC over SEA, execution polling uses `GetStatementResult`. Result heartbeats are
disabled for APC even when `EnableHeartbeat=1`, because `GetStatementStatus` is unsupported and
using full `GetStatementResult` after completion can re-download result payloads. The driver logs
that heartbeat is unsupported and never schedules `checkStatementAlive` or calls `/status`.
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
APC compute type, not `isBoundedSeaApiEnabled`, selects this request and session behavior.
Positive `Statement.setMaxRows` sends SEA `row_limit` and caps exposed rows across pages/links;
zero is unlimited, matching APC Thrift.
Successful no-result commands such as DDL may omit a manifest or use zero columns; they
expose no ResultSet and update count zero, then `getMoreResults()` changes it to `-1`.
Sequential batches record zero. Result-producing commands, including `SELECT`, `SHOW`,
`VALUES`, and a `NonRowcountQueryPrefixes` match, require their schema and data before
public success. Counted `INSERT`, `UPDATE`, `DELETE`, and `MERGE` require a
`num_affected_rows` schema column and row, including zero; missing count data is a
contract error. The driver fetches and sums counts as on APC Thrift. A configured
`NonRowcountQueryPrefixes` match instead exposes the affected-rows ResultSet and update
count `-1`; a Statement batch stops there with preceding counts and the existing error.
Server failures propagate; live polled DDL and DML gates depend on T32. APC advertises LZ4
decompression capability, but decodes inline payloads according to the response manifest's
compression codec; an unmarked response is uncompressed. For ordinary failed statements,
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
`supportManyParameters=0` sends the original SQL without bindings, so unresolved markers fail
as on APC Thrift; the missing-version telemetry reason remains distinct. Explicit
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
- R9: Public `getConnectionId()` returns a durable APC SEA session ID that
`Driver.closeConnection` can use after a flag change; public statement IDs remain raw server
IDs. Internal session and statement identities retain the compute target, reject malformed
IDs without logging identifiers, and preserve existing warehouse/raw SEA ID compatibility.
Recovered statements use the current connection target; saved-session close uses the saved
protocol and target. Only a live CreateSession version grants capabilities, not an ID value.
- R10: APC ExecuteStatement keeps SEA `wait_timeout` unset for synchronous direct results and
uses `0s` for async or disabled direct results. Public SQL `executeAsync` and
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
inline data in the initial response. Advance inline offset by the sum of batch row counts;
for multiple links, use the final link's continuation and end offset. Missing final-link
`next_chunk_index` ends paging.
- R13: For APC metadata, decode the emitted SEA `columnar_data` physical vectors (boolean,
byte, short, int, long, double, string, binary) using `manifest.schema` and least-significant-bit
first null masks. The server always sets a mask; short masks leave unrepresented rows non-null and
long masks ignore excess bits. The decoded JDBC columns and values match the same metadata
operation over APC Thrift. An unrecognized physical vector fails clearly.
- R14: APC query timeouts use the warehouse `TimeoutHandler` and targeted CancelStatement path.
A valid query or metadata call within its configured client wait must not fail early from the
Reyder/SEA request timeout (AC60, AC63).
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
Thrift HTTP retry settings do not tune SEA API requests, but continue to tune presigned-link
downloads through the shared HTTP client; `HttpConnectionRequestTimeout` controls Thrift and
linked-download acquisition wait, not SEA SDK API acquisition. `RowsFetchedPerBlock` does not
tune SEA paging. Match warehouse SEA 429/503 and `Retry-After` handling separately for API
requests and linked downloads without new write retries. No APC-only
concurrency cap or full-result buffer is allowed. Record APC Thrift differences and impact
under AC30–AC32, AC45–AC46, AC51, and AC61. The matrix and AC30/46/51/84 assign
the pool, CloudFetch, retry, and timeout settings to their request classes; vary each
setting and observe the corresponding wait, polling, concurrency, transfer, warning,
expiry, or retry outcome.
- R18: Every approved JDBC slice exposes its completed public behavior when R1's default-off
APC flag is enabled or `UseThriftClient=0` is explicit. Incomplete public calls, including
`executeAsync`/`getExecutionResult` before T39, fail before network execution when known;
unsupported response shapes fail before false success. They never silently switch to Thrift
after SEA selection. After T17 and T35 deploy, a scoped staging
assignment may enable flag-on validation; production assignment or ramp waits for T18/T-val
on the assembled approved stack and a post-merge smoke check. Remove each guard when its
slice passes live staging, and keep
ordinary flag-off APC connections on Thrift.

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
against APC Thrift and warehouse SEA where relevant. AC3, AC11, AC30, AC40, AC51, AC84, and
AC85 apply to newly exposed request classes once per slice. Server-dependent outcomes remain
pending until deployed and tested. Captured fixtures may cover only protocol versions or wire
shapes unavailable live; they do not replace available live proof. AC identifiers retain prior
review references, so retired duplicate gates leave gaps.

AC31, AC32, AC61, and AC90 compare paired live loads on the same build, cluster, data, and
network. Result and batch gates cover one and 32 callers; metadata covers 32. Record rows,
errors, request and transfer counts, throughput, latency, peak and retained heap, and normalized
Reyder CPU/peak memory. Apply the stated limits to repeated paired medians. Controlled failures
must isolate one request class and target; transport injection proves retry behavior, while
server-origin error identity requires a real server failure.
- AC1: GIVEN a concrete APC HTTP path and no explicit route override WHEN the APC flag is
OFF, missing, lookup-failed, or partner-forced false THEN public JDBC uses Thrift independently
of the warehouse flag; an ON flag with Arrow available selects SEA. Legacy `cliservice` stays
on Thrift. Verify both routes on staging.
- AC2: GIVEN scoped flag-ON APC staging and a cluster HTTP path WHEN a public connection opens
THEN CreateSession contains `backend_compute.all_purpose_cluster_compute.cluster_id` equal to
the URL's cluster id and no `warehouse_id`; compare the resulting connection with APC Thrift and
the unchanged warehouse SEA CreateSession shape.
- AC3: GIVEN live APC SEA requests WHEN each exposed R2 operation runs THEN its target
appears once in the specified body or query field, GET and statement DELETE are bodyless,
session DELETE has its target body, and no `/status` call occurs. Warehouse fixtures retain
their wire shape; termination waits for its server fix.
- AC4: GIVEN live APC linked results under either hybrid-results setting WHEN public JDBC
drains them THEN primitive rows, metadata, errors, and R4's request shape match the baselines.
- AC5: GIVEN live APC function SHOW metadata WHEN Arrow IPC or `columnar_data` arrives
THEN JDBC columns, values, errors, and request headers match APC Thrift and warehouse SEA.
- AC6: GIVEN live APC prepared SQL with bound scalars WHEN `getMetaData()` runs before
execution THEN it returns JDBC metadata without losing bindings; subsequent execution
matches APC Thrift and warehouse SEA. AC53 covers the version and interpolation matrix.
- AC7: GIVEN `UseThriftClient=0` and the APC flag ON on AIX/PowerPC with Arrow unavailable WHEN a
connection is opened THEN the Arrow prerequisite takes precedence, the pre-connection routing
decision selects Thrift, and the driver logs the routing reason.
- AC8: GIVEN live multi-page APC SELECT with `EnableQueryResultDownload=0` after inline
continuation is public WHEN it runs on flag-ON staging THEN every page stays inline, and an
inline-size error propagates without partial success. `UseBoundedSeaApi` does not change
this; the tracked inline-only server fix gates exposure. AC92 gates the first single-chunk slice.
- AC9: GIVEN live APC SEA CreateSession rejection WHEN connecting THEN the error and failed
route event propagate without Thrift fallback. GIVEN APC Thrift OpenSession returns KP001 WHEN
connecting THEN warehouse-only recovery does not select SEA.
- AC10: GIVEN successful or failed APC/warehouse SEA/Thrift connections WHEN telemetry and
DEBUG logs are captured on live staging THEN each attempt identifies compute, protocol, reason,
and outcome without exposing credentials or APC identifiers. The first public statement also
keeps its execute and poll diagnostics redacted; T15 covers later linked and recovered paths.
- AC11: GIVEN standard and Real-time warehouse connections on live staging WHEN R2's
equivalent operations run THEN JDBC outcomes and request bodies, URLs, and headers match
pre-change `origin/main`, including `realtimeSession`. Each slice checks affected classes;
fixtures protect exact serialization and T18 closes the full route matrix.
- AC12: GIVEN live APC SEA synchronous direct-result and no-direct-result executions WHEN
requests are captured THEN `wait_timeout` is respectively absent and `0s`, with no
`statements` field; public outcomes match APC Thrift and warehouse SEA precedent. AC55 owns
public SQL asynchronous execution and result retrieval.
- AC13: GIVEN live primitive, decimal, array, map, struct, interval, geography, geometry,
timestamp, timestamp_ntz, and variant results WHEN APC reads inline and linked results
with both R11 settings at 0 and 1
THEN metadata and getters match the route baselines. Inline typed complex getters reject;
enabled linked getters work and disabled settings retain strings. Unavailable live types
remain pending with fixtures; silent coercion fails.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN live flag-ON APC `getCatalogs` WHEN Arrow IPC or `columnar_data` physical
vectors and least-significant-bit null masks are decoded THEN JDBC rows, metadata, and errors
match APC Thrift and warehouse SEA; short/long masks retain fixtures, and a following Arrow
IPC response decodes correctly. AC72–AC74 cover the other public table-discovery methods.
- AC17: GIVEN live V10 APC, or a captured V10 contract fixture when unavailable live, WHEN
enabled native prepared INSERT batches succeed or fail THEN one ordered `parameter_sets`
request returns every per-set count on success, or only server-confirmed prefix counts on
failure; without such evidence the error has empty counts and uncertain commit status.
Native off or unavailable retains legacy behavior and APC Thrift counts; live V10
performance remains pending until available.
- AC18: GIVEN an APC-SEA statement with `setQueryTimeout(n)` WHEN it runs longer than `n`
seconds THEN the driver cancels it via CancelStatement with `backend_compute` and raises the
standard timeout SQLException. Compare elapsed time, SQLState/vendor code, target, and
post-timeout connection usability on flag-ON staging.
- AC19: GIVEN Arrow available and a cluster HTTP path on scoped flag-ON APC staging WHEN
`UseThriftClient=1` opens a connection THEN it uses Thrift with APC Thrift behavior; with
`UseThriftClient=0` and the flag OFF it uses SEA and matches the flag-ON APC outcome. The
client setting takes precedence after the Arrow prerequisite.
- AC20: GIVEN a warehouse HTTP path on live staging WHEN the APC flag is ON or OFF THEN
public warehouse JDBC calls and captured routing match the `origin/main` warehouse SEA or
Thrift baseline selected by `enableSqlExecForJdbc`.
- AC21: GIVEN live APC routing with `TreatMetadataCatalogNameAsPattern=1` and the flag ON
WHEN no `UseThriftClient` is explicit THEN Thrift retains catalog-pattern semantics. With
explicit `UseThriftClient=0`, SEA wins and logs that the Thrift-only setting has no effect.
- AC22: GIVEN a live Statement INSERT batch with a successful and a failing middle command
WHEN `executeBatch` runs THEN APC SEA sends sequential requests without `statements`, returns
ordered counts, stops on failure, and matches APC Thrift errors. AC32 gates its throughput;
AC64 and AC66 cover configured ResultSet and size-limit cases.
- AC23: GIVEN live or captured APC CreateSession without `negotiated_protocol_version`
WHEN prepared SQL or a batch runs THEN neither an encoded ID nor `session_version` grants
capability. R6's original-SQL failure, interpolation alternative, and missing-version
telemetry hold; compare available versioned live behavior and record fixture-only branches.
- AC24: GIVEN a live APC SEA ResultSet with `EnableHeartbeat=1` WHEN it stays open past
two heartbeat intervals THEN no result heartbeat or `/status` request occurs; execution
polling still works.
- AC25: GIVEN a live APC SEA connection WHEN `getConnectionId()` feeds
`Driver.closeConnection` after the flag or URL target changes THEN R9's saved protocol and
target close the correct session. Internal statement IDs, malformed-ID rejection,
warehouse/Thrift compatibility, and no capability from encoded versions also hold.
- AC26: GIVEN live APC query/metadata and warehouse SEA responses WHEN their emitted wire
formats are decoded on staging THEN JDBC results and physical formats follow R16. A separate
fixture with an unknown future format must deserialize safely and fail clearly on decode; it
does not require an unavailable live server response.
- AC27: GIVEN a live single-chunk APC query with three or more RUNNING polls WHEN transfer
is measured on explicit-override and flag-on routes THEN polls carry no result payload and
the terminal fetch delivers every row once; compare APC Thrift and warehouse SEA traffic.
- AC28: GIVEN the T17 SAFE configuration WHEN the flag definition is inspected THEN
`databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` exists as a
sub-flag of `databricks.partnerplatform.clientConfigsFeatureFlags` with default OFF.
- AC29: GIVEN complex scalar or batch bindings, including `Types.ARRAY` and `Types.STRUCT`,
WHEN APC execution starts THEN a tested driver-owned error name/code occurs before request
and the connection stays usable. Compare live route outcomes; fixture unavailable versions.
The PR completes the telemetry-error template and links the owner-verified dashboard and
automation taxonomy update, or requests a maintainer handoff if access is missing.
- AC30: GIVEN live API and linked-download pool saturation on all three routes WHEN
`HttpConnectionPoolSize`, `HttpMaxConnectionsPerRoute`, and
`HttpConnectionRequestTimeout` vary independently THEN in-flight requests, wait, error,
and throughput follow the matrix's pool scopes; SDK API acquisition ignores the wait setting.
- AC31: GIVEN paired APC SEA/Thrift 1/2 MiB inline and 256/512 MiB linked SELECTs WHEN
loads run at one and 32 callers with `RowsFetchedPerBlock` varied THEN rows and errors match,
SEA page size and request count do not change with that property, no APC-only cap appears,
and throughput/latency and peak heap regress by at most 10%/20%. At doubled result size,
median added retained heap is at most `max(8 MiB, 5% of added result bytes)` after resources
close. Record bytes, requests, and retries against APC Thrift and warehouse SEA; Thrift fetch
size must respond to the property. Any exception requires a server limit, warehouse precedent,
caller impact, and PECO/Perf Eng approval; unavailable inline-only proof stays pending.
- AC32: GIVEN paired APC SEA/Thrift 1,000-row INSERT batches with disjoint keys WHEN
legacy interpolated and bound prepared batches, native V10 prepared batches, and sequential
Statement batches run at one and 32 callers THEN per-set counts, committed rows, and errors
match the applicable route baseline and each
mode's throughput/latency regress by at most 10%. Vary `EnableBatchedInserts`,
`BatchInsertSize`, `supportManyParameters`, and `EnableNativeBatching` in their R6 scopes;
record requests and version. An exception requires a server limit, warehouse precedent,
caller impact, and PECO/Perf Eng approval. If V10 is unavailable, AC17's fixture proves
request/count shape and live throughput stays pending until V10 is available.
- AC33: GIVEN successful APC SEA DDL with absent or zero-column manifest and no data WHEN
public JDBC executes it on live staging THEN no ResultSet or fetch occurs, `execute` is false,
update count is zero, and `getMoreResults()` changes the count to `-1`. Failed DDL keeps its
server error; AC48 owns counted DML. A successful fixture response to `SELECT` or `SHOW`
with the same missing schema fails with a contract error before reporting success.
- AC34: GIVEN the deployed default-off APC flag WHEN partner and non-partner User-Agents
request client flags THEN connector-service returns false for every partner, including JDBC
warehouse-allowlisted partners, and retains the ordinary APC value for non-partners. Record the
service version and region before any APC assignment or ramp.
- AC36: GIVEN a manifest-only start or multiple inline bare-batch continuations on live staging
WHEN public JDBC drains them THEN rows arrive once using each page's summed `row_count` and
`next_chunk_index`; errors propagate without a partial successful ResultSet. Compare APC Thrift
rows and warehouse SEA paging. AC14 covers linked continuation.
- AC37: GIVEN T32 and the TerminateSession binding fix deployed WHEN a successful APC no-result
DDL is closed or reused after polling on scoped flag-ON staging THEN no result fetch or
`/status` call occurs, repeated statement and connection close are safe, one APC-targeted
TerminateSession succeeds on the server, and an unclosed connection remains usable. Failed
DDL retains its error; without the binding fix, the first public slice stays gated.
- AC39: GIVEN live inline pages and external links WHEN hybrid and streaming toggles and
`ThriftMaxBatchesInMemory` vary THEN rows/errors and request disposition follow R4, APC uses
no legacy provider, and buffered inline batches obey the configured bound (minimum two)
and AC31's heap gate. Record request and transfer bytes against both route baselines.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF THEN APC uses Thrift; on scoped flag-ON staging
completed calls pass their live parity gate. Known incomplete calls fail before SEA requests without Thrift
fallback; public SQL async stays guarded until T39 and production assignment waits for T18/T-val.
- AC41: GIVEN a recovered statement or saved APC session after a flag change WHEN fetch,
cancel, close, or termination runs on live staging THEN the current or saved target/protocol
follows R9 and connection usability matches APC Thrift or warehouse SEA. Termination proof waits
for the server binding fix.
- AC42: GIVEN a RUNNING APC statement WHEN public JDBC closes it on live staging THEN one
close request, no `/status` call, and a usable connection match APC Thrift and warehouse SEA.
AC3 covers request targeting.
- AC43: GIVEN live synchronous APC SHOW WHEN `EnableSeaSyncMetadata` changes between 0 and 1
THEN only R5's sync header changes; rows, errors, and JDBC values match APC Thrift and
warehouse SEA. AC61 owns the common metadata load and latency gate.
- AC44: GIVEN live legacy and native prepared INSERT batches WHEN `EnableBatchedInserts`,
`BatchInsertSize`, `supportManyParameters`, and `EnableNativeBatching` vary within R6's scopes
THEN eligible legacy requests honor multi-row and chunk limits, native V10 uses ordered
`parameter_sets`, and counts/errors match APC Thrift. AC32 gates each mode's throughput and latency.
- AC45: GIVEN live LZ4-marked and unmarked APC results WHEN
`EnableSeaResponseCompression` changes between 0 and 1 THEN `0` sends
`Accept-Encoding: identity` on SEA API requests and `1` permits compressed API responses as
on warehouse SEA, while the manifest-selected result codec, decoded values, and
presigned-link compression do not change because of that setting. Compare API and result/link
bytes, CPU, and latency with APC Thrift and warehouse SEA; an unavailable codec keeps a
fixture gate.
- AC46: GIVEN live linked and delayed inline results WHEN R17's CloudFetch worker,
prefetch, wait, and slow-link settings vary independently THEN rows, worker/queue bounds,
timeouts, warnings, and transfer duration follow the matrix. Prefetch and slow-link settings
do not change inline pages; AC31 gates throughput and heap.
- AC47: GIVEN live primitive APC inline results, including a manifest-only initial response,
WHEN public JDBC consumes the first chunk THEN rows, order, types, and errors match APC Thrift;
the manifest-only response triggers a bodyless targeted chunk GET. Both hybrid settings retain
the APC request. LZ4/unmarked and absent-`arrow_schema` fixtures pass; links stay guarded until
their slice passes.
- AC48: GIVEN live APC counted `INSERT`, `UPDATE`, `DELETE`, or `MERGE` WHEN public
update methods run THEN counts, including zero, match APC Thrift after T32. A missing
`num_affected_rows` column, row, or value produces a contract error, not successful zero.
- AC49: GIVEN live linked APC queries with `setMaxRows(0)`, `1`, and a link-boundary limit
WHEN results are read THEN positive limits send SEA `row_limit`, zero is unlimited, and rows
never exceed the limit; compare rows and request count with APC Thrift and warehouse SEA.
A boundary unavailable live remains pending with a contract fixture. AC67 covers inline pages.
- AC50: GIVEN live V9+ APC and a delayed SHOW WHEN each typed handle submits THEN it
returns while SHOW is RUNNING and another operation completes before await. Awaited
values, errors, timeout/cancel, cleanup, and both cross-reference conversions match R5 and
the route baselines. Older/versionless handles complete synchronously; procedure SELECTs
reject async. A blocking submit fails even if its final rows are correct.
- AC51: GIVEN healthy live calls and isolated 429/503 failures with and without `Retry-After`
for each exposed SEA API request class and linked download WHEN retries succeed or exhaust
under each relevant API or HTTP-client retry
setting THEN attempt count, delay, exception class, SQLState, and vendor code match
warehouse SEA for the same request class; no write is replayed beyond existing SDK behavior.
The Thrift HTTP controls affect links but leave SEA API calls unchanged. Include exhausted
CreateSession 429 and a 503 during open. Vary
`RateLimitRetry`, `RateLimitRetryTimeout`, `TemporarilyUnavailableRetry`,
`TemporarilyUnavailableRetryTimeout`, `ApiRetriableHttpCodes`, and `ApiRetryTimeout`
independently; compare APC Thrift's link outcome separately.
- AC52: GIVEN Reyder's APC `GetStatementResult` route after RUNNING polls WHEN no-result DDL
or counted DML completes THEN direct server contract evidence returns, respectively, success
with absent or zero-column manifest and no data, or a schema and affected-rows data including
zero. Failed statements retain their errors. AC33, AC37, and AC48 own public JDBC proof.
- AC53: GIVEN live APC prepared scalars and fallback batches at available versions, plus
older-version CreateSession fixtures derived from a captured response when staging cannot emit them,
WHEN `supportManyParameters` is 0 or 1 THEN R6's bindings, original-SQL failure below V8
with setting 0, interpolation with setting 1, and `setBytes` behavior match APC Thrift and
warehouse SEA where live comparison is available. Record fixture-only version branches.
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
AC57–AC59 and AC68–AC71 apply R5's version, conversion, error, timeout, cancel, and live
delayed-SHOW overlap checks at each handle's first exposure. Older/versionless fallback
may use a captured fixture when unavailable live. Compare live awaited results with
synchronous APC, APC Thrift, and warehouse SEA;
AC50 closes after all seven handles work together.
- AC57: GIVEN live V9+ APC and an older/versionless session fixture WHEN public `getColumns` consumes its typed handle
THEN the shared checks hold, including R5's null-catalog partial-row rule and AC91's submission bound.
- AC58: GIVEN live V9+ APC and an older/versionless session fixture WHEN a catalog typed handle is consumed THEN the shared checks hold.
- AC59: GIVEN live V9+ APC and an older/versionless session fixture WHEN a function typed handle is consumed THEN the shared checks hold.
- AC60: GIVEN a valid long-running APC query within its configured client wait WHEN it crosses the measured
single-request timeout THEN APC SEA completes, including result polling, without premature server timeout.
Record duration, retry/final error identity, and connection usability against APC Thrift and warehouse SEA;
a server limit remains pending without cause, caller impact, and PECO approval.
- AC61: GIVEN paired APC SEA/Thrift and warehouse Thrift/SEA metadata loads across four
nonempty catalogs, 25 tables each, and 16 columns each WHEN synchronous and typed async
SHOW run at 32 callers THEN rows and errors match, `42601` recovery uses at most N+2
SHOW/metadata submissions for N catalogs, and latency and normalized Reyder CPU/peak memory regress by at
most 10% and 20% respectively. An exception requires a server limit, caller impact, and
PECO/Perf Eng approval.
- AC63: GIVEN valid delayed APC SHOW within its configured metadata wait WHEN it crosses the measured
single-request timeout THEN APC SEA completes, including polling, without premature server timeout. Record
duration, retry/final error identity, and connection usability against APC Thrift and warehouse SEA; a
server limit remains pending without cause, caller impact, and PECO approval.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on live APC SEA, APC Thrift, and warehouse SEA
WHEN a matching INSERT runs THEN `execute` returns true, its ResultSet contains the affected
rows value, and `getUpdateCount` returns `-1`; a Statement batch stops at that command with
the same preceding counts and error identity on all three routes.
- AC65: GIVEN live APC SHOW with `EnableThriftNativeMetadata` unset, `0`, and `1` WHEN a
supported metadata method runs THEN capture the native request header only at `1` and verify
the same native-shaped JDBC columns and values at all three settings against APC Thrift and
warehouse SEA. If Reyder changes response shape, leave this gate pending for contract review.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC67: GIVEN live inline APC queries with `setMaxRows(0)`, `1`, and a page-boundary limit
WHEN results are read THEN positive limits send SEA `row_limit`, zero is unlimited, and rows
never exceed the limit; compare rows and request count with APC Thrift and warehouse SEA.
- AC68: GIVEN live V9+ APC and an older/versionless session fixture WHEN a schema typed handle is consumed THEN the shared checks hold, including
null-catalog recovery and AC91's N+2 submission bound.
- AC69: GIVEN live V9+ APC and an older/versionless session fixture WHEN a table typed handle is consumed THEN the shared checks hold.
- AC70: GIVEN live V9+ APC and an older/versionless session fixture WHEN a primary-key typed handle is consumed THEN the shared checks hold.
- AC71: GIVEN live V9+ APC and an older/versionless session fixture WHEN a cross-reference typed handle is consumed THEN the shared checks hold
for both distinct JDBC conversions.
AC72–AC78 apply R5's request, format, JDBC value, error, and two-route parity checks to each
public synchronous metadata method at its first exposure; AC91 applies per-method load gates.
- AC72: GIVEN live APC `getSchemas` including null catalog WHEN both response formats arrive
THEN the shared checks hold, including AC63's delayed-call timeout.
- AC73: GIVEN live APC `getTables` WHEN both response formats arrive THEN the shared checks hold.
- AC74: GIVEN live APC `getColumns` including null catalog WHEN both response formats arrive
THEN the shared checks hold, including request count and short/long null-mask fixtures.
- AC75: GIVEN live APC `getProcedures` WHEN its SELECT runs THEN the shared checks hold without
an unsupported APC metadata header.
- AC76: GIVEN live APC `getProcedureColumns` WHEN its SELECT runs THEN the shared checks hold
without an unsupported APC metadata header.
- AC77: GIVEN live APC `getPrimaryKeys` WHEN its SHOW runs THEN the shared checks hold.
- AC78: GIVEN live APC `getImportedKeys` and `getCrossReference` WHEN their SHOW runs THEN the
shared checks hold for both distinct JDBC conversions.
- AC82: GIVEN live `getFunctions` with an explicit catalog and with null catalog WHEN
`EnableShowCommandForGetFunctions` is `0` and `1` on APC SEA, APC Thrift, and warehouse SEA
THEN APC SEA sends the R5 SHOW request at both values, rows and errors follow the documented
warehouse SEA precedent, and the APC Thrift native/SHOW choice and null-catalog difference are
recorded with client impact.
- AC83: GIVEN live null-catalog `getSchemas` and `getColumns` on APC SEA and warehouse SEA
with `42601` recovery and isolated SQLException, timeout, and cancellation for one catalog
WHEN synchronous calls and V9+ handles finish THEN
each route warns with the skipped catalog, returns only healthy rows, and leaves the
connection usable. Record the lost rows against APC Thrift. Initial SHOW object-not-found
and applicable empty-pattern cases return typed empty; other initial or overall failures
propagate. The typed handle also passes delayed-SHOW overlap.
- AC84: GIVEN live APC SEA, APC Thrift, and warehouse SEA calls WHEN
`MetadataOperationTimeout`, `socketTimeout`, `AsyncExecPollInterval`, and
`IdleHttpConnectionExpiry` vary separately, including zero where valid, THEN metadata
wait/cancel, SDK and link timeout, poll spacing, and idle reuse/expiry follow the matrix.
Record timestamps, final error, and usability; zero metadata timeout permits a delayed
SHOW while configured query timeout still applies.
- AC85: GIVEN live execution, metadata, downloads, batches, and controlled failures WHEN
each public slice exports telemetry THEN applicable operation, format, chunk/download, and
error families include correct type, count, name/code, SQLState, and vendor code. Compare
event presence and redacted route correlation with both baselines; document exceptions.
- AC86: GIVEN a live first-slice APC SEA connection with
`EnableSQLValidationForIsValid=1` WHEN `Connection.isValid(timeout)` executes its
`SELECT VERSION()` check THEN it returns true for a healthy connection, false for a failed
check, honors the supplied query timeout, and leaves the connection usable; compare both
outcomes with APC Thrift and warehouse SEA before the primitive SELECT slice is approved.
- AC87: GIVEN paired SQL text at 16,777,215, 16,777,216, and 16,777,217 UTF-8 bytes WHEN
a single statement and legacy prepared INSERT chunk execute THEN APC SEA accepts text up to
SEA's 16 MiB limit and follows warehouse SEA's oversize error and shared batch-count
semantics above it, without fallback. Record UTF-8 bytes,
error identity, counts, and earlier committed rows on all routes; lowering
`BatchInsertSize` permits equivalent rows. Document APC Thrift differences and the
caller need to reconcile ambiguous earlier commits.
- AC88: GIVEN live APC SEA, APC Thrift, and warehouse SEA metadata with two accessible
catalogs and a metric view WHEN `enableMultipleCatalogSupport` and
`EnableMetricViewMetadata` each change between 0 and 1 THEN `setCatalog`, explicit and
null-catalog discovery, `getTableTypes`, and metric-view table rows have the same visible
scope on all routes. Capture the APC CreateSession metric-view session setting only at 1;
record any server-limited row difference and caller impact.
- AC89: GIVEN live invalid SQL, missing-object, and permission-denied statement failures
on APC SEA, APC Thrift, and warehouse SEA WHEN JDBC `execute`, `executeQuery`, and
`executeUpdate` surface each applicable failure THEN record SQLException class, SQLState,
vendor code, and message category. APC SEA matches APC Thrift where both baseline routes
agree; otherwise it follows the measured warehouse SEA mapping and documents the Thrift
delta and caller impact. No failed statement reports a successful ResultSet or count.
- AC90: GIVEN paired live single-chunk inline APC SEA/Thrift SELECTs at 1 and 2 MiB WHEN
loads run at one and 32 callers THEN rows, errors, bytes, request
counts, throughput, latency, and peak/retained heap meet AC31's inline bounds. This first
slice gate excludes continuation and links; AC31 closes after the linked-result slice.
- AC91: GIVEN four accessible catalogs WHEN each newly exposed synchronous metadata call
runs at 32 callers THEN rows and errors match the route baselines and latency and
normalized Reyder resources meet AC61's thresholds. `getCatalogs` uses one SHOW submission;
null-catalog `getSchemas` recovery uses at most N+2 submissions for N nonempty catalogs. Apply the
same bound where later methods fan out; AC61 closes after all typed async methods exist.
- AC92: GIVEN the first live single-chunk APC SELECT with
`EnableQueryResultDownload=0` WHEN its session opens and the result is read THEN
CreateSession sends `session_confs["can_cloud_download"]="false"`, the complete result
stays inline, and an oversized inline response fails before public partial success. The
tracked inline-only server fix gates this first exposure; AC8 owns later pages.
- AC93: GIVEN successful and rejected live APC and warehouse connection opens WHEN route
telemetry and DEBUG logs are captured before any statement runs THEN compute, protocol,
routing reason, and outcome are present without credentials or raw APC identifiers.
AC10 closes once first-statement diagnostics are exposed.
- AC94: GIVEN a live APC CreateSession and captured older/versionless variants WHEN a
connection opens THEN its internal capability ID comes only from
`negotiated_protocol_version`; an unset field grants none even when `session_version` or
the serialized connection ID contains a value. Warehouse capability handling stays intact.
- AC95: GIVEN a live single-chunk APC SELECT with `setMaxRows(0)` and `1` WHEN rows are
read THEN zero is unlimited, one sends SEA `row_limit` and exposes at most one row, and
both match APC Thrift. AC67 owns the later inline page boundary.
- AC96: GIVEN live APC and warehouse SEA CreateSession calls under SDK pool saturation
and scoped 429/503 fault injection WHEN pool and API retry settings vary THEN open
latency, request count, retry delay, public error, and no-fallback behavior match the
warehouse SEA baseline. AC30/51 close once linked downloads and all request classes exist.

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

`plan.json` is the authoritative stacked-PR tree. JDBC connection and single-chunk SELECT
precede separate inline continuation, DDL, linked results, logical types, public SQL async,
prepared single execution, batches, and metadata slices. Each package exposes its public call
behind the default-off flag and passes its live gate before approval. Siblings own disjoint
areas; the prepared/batch branch merges before the final metadata branch rebases for release.

Universe T17/T35 must deploy before flag-on JDBC approval; T32 and tracked server fixes gate
their affected DDL/DML, termination, inline-only, and metadata outcomes. T18/T-val audits a
temporary composite of approved JDBC tips and deployed server prerequisites, with one evidence
result per AC. Unresolved live or performance gates block release and production flag assignment;
the post-merge smoke must also pass.

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

Each task appears in one `plan.json` package. AC3, AC11, AC30, AC40, AC51, AC84, and AC85
apply to newly exposed calls; AC91 applies to each new synchronous metadata method, while
AC61 closes after all metadata handles. Each planned PR stays below about 1,500 changed lines.

### JDBC task inventory

- [ ] T1: Carry APC compute and session identity through SEA.
- [ ] T2: Open and terminate APC SEA sessions from public connections.
- [ ] T3: Redact connection diagnostics and emit route/outcome telemetry.
- [ ] T4: Route APC by Arrow, explicit settings, and dedicated flag precedence.
- [ ] T5: Gate capabilities on negotiated CreateSession version.
- [ ] T6: Preserve protocol selection after SEA and KP001 errors.
- [ ] T19: Round-trip APC session IDs while retaining raw statement IDs.
- [ ] T26: Verify first-session SDK retry, timeout, and pool parity.
- [ ] T27: Complete targeted termination and repeated-close lifecycle.
- [ ] T29: Close saved sessions by saved protocol and target.

- [ ] T7: Suppress APC result heartbeats; retain execution polling.
- [ ] T8: Expose primitive SELECT and SQL-backed isValid.
- [ ] T10: Decode marked LZ4 and unmarked bare Arrow batches.
- [ ] T12: Preserve query timeout and targeted cancellation.
- [ ] T25: Send bodyless, APC-targeted result-chunk GETs.
- [ ] T45: Target execution/poll/cancel/close and classify statement failures.
- [ ] T47: Set hybrid/inline-only request behavior and bound first-chunk memory.
- [ ] T57: Redact first-statement diagnostics and emit telemetry.
- [ ] T58: Fetch manifest-only first chunks; guard unsupported continuation.
- [ ] T59: Apply first-chunk row limit and unlimited zero.

- [ ] T11: Drain inline continuation by chunk index and row offset.
- [ ] T34: Retain row limits across inline pages.
- [ ] T28: Classify no-result DDL without result fetch.
- [ ] T31: Expose DDL after server contract proof.

- [ ] T33: Stream complete external-link Arrow IPC and continuation.
- [ ] T46: Retain row limits across links.
- [ ] T15: Redact linked-result and recovered-statement diagnostics.
- [ ] T44: Use current connection target for recovered statements.
- [ ] T36: Preserve CloudFetch controls and bounded transfer/heap.

- [ ] T20: Expose logical-type metadata and getters without coercion.
- [ ] T39: Expose public SQL async execute, poll, result and cleanup.

- [ ] T9: Preserve scalar binding, interpolation, bytes and pre-execution metadata.
- [ ] T22: Reject complex bindings early with coded telemetry error.
- [ ] T38: Expose counted DML, missing-count errors and prefix results.

- [ ] T21: Expose V10 native and legacy prepared batches with ordered counts.
- [ ] T30: Expose sequential Statement batches and size-limit rejection.

- [ ] T14: Expose getCatalogs through the shared SHOW response contract.
- [ ] T43: Expose getSchemas, including null catalog.
- [ ] T48: Expose getTables with catalog and metric-view tuning.
- [ ] T49: Expose getColumns, including null catalog.
- [ ] T13: Expose getFunctions at both command-tuning values.

- [ ] T37: Expose getPrimaryKeys.
- [ ] T54: Expose imported-key and cross-reference conversions.
- [ ] T55: Expose typed-empty getExportedKeys.
- [ ] T42: Expose parameterized getProcedures without APC metadata header.
- [ ] T56: Expose parameterized getProcedureColumns without APC metadata header.

- [ ] T24: Add async SHOW coordination, timeout, cancel and fallback.
- [ ] T23: Expose and consume typed async getColumns.

- [ ] T40: Expose typed async GET_CATALOGS.
- [ ] T50: Expose typed async GET_SCHEMAS with null-catalog recovery.
- [ ] T51: Expose typed async GET_TABLES.
- [ ] T41: Expose typed async GET_FUNCTIONS.

- [ ] T52: Expose typed async GET_PRIMARY_KEYS.
- [ ] T53: Expose typed async GET_CROSS_REFERENCE conversions.
- [ ] T16: Document the gated feature and add the JDBC changelog entry.
- [ ] T18: Verify integrated local and live gates on the approved composite.
- [ ] T-val: Record every AC result and post-merge smoke before release.

### Universe task inventory

- [ ] T17: Define the default-off APC SAFE sub-flag without ramp.
- [ ] T32: Prove or fix server DDL and counted-DML result contracts.
- [ ] T35: Force partner APC flag false without changing warehouse allowlist.
