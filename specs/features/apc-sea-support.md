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
- Reyder's current APC terminal-result translation requires an Arrow schema when metadata is
  present. Both direct `ExecuteStatement` results (the default with `wait_timeout` unset) and
  terminal `GetStatementResult` polls need T32's server proof for no-result DDL and counted DML
  before live JDBC gates pass (R4, AC52). Live result
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
  `UseBoundedSeaApi` default #1827): APC routes to Thrift before warehouse flag logic.
  `DatabricksSdkClient` assumes `Warehouse` in SEA requests and uses result polls and `/status`;
  `SessionId.create`, `getConnectionId()`, and `Driver.closeConnection` infer or reselect
  protocol from compute type/current flags, so a changed flag or target can misroute saved-ID
  close. SEA lacks APC `ARROW_BATCH`, async SHOW, and `columnar_data` decoding. The public
  procedure metadata methods catch failures into typed empty results. Public SQL
  `executeAsync` is separate from metadata SHOW async.
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
  | DDL and single DML | DDL zero, DML count | Same | Same counts | Match correctness and the shared load gate; missing DML count is an error (R4; AC33, AC48, AC103, AC104). |
  | Metadata SHOW and exported keys | Native RPC; parent-only exported keys empty for a valid table | SHOW; typed-empty exported keys for a valid table; null table is a coded error | SHOW/native-shaped; same valid-table empty and null-table error | Warehouse/DBR precedent; invalid arguments retain the existing SQLState and vendor code (R5; AC54). |
  | Null-catalog schemas/columns recovery | Native RPC, no fan-out | `42601` fan-out may omit failed catalog | Same fan-out | Warehouse precedent; warning and partial rows can reach callers (R5; AC83). |
  | Null-catalog `getTables` on `42601` | Native metadata request | Typed empty result | Typed empty result | Warehouse SEA precedent: no table fan-out on this error; callers can see no tables even when APC Thrift finds some (R5; AC105). |
  | `getFunctions` setting | `EnableShowCommandForGetFunctions=0` uses native RPC; `1` uses SHOW and returns typed empty for null catalog | SHOW at either setting; null catalog resolves to current/fallback catalog | SHOW at either setting, including null catalog | Warehouse SEA precedent: setting cannot select native RPC on SEA. A caller switching from APC Thrift may see functions for null catalog instead of an empty result, and may see SHOW rather than native-RPC errors/latency (R5; AC82). |
  | Native metadata default and procedures | Catalog/metric-view settings; procedure failures become empty | Native header may be enabled by the client default and server flag; procedure headers and empty-on-error | Unset `EnableThriftNativeMetadata` sends no native header; only explicit `1` sends it; APC omits procedure headers | APC's native-shaped responses do not require the header and Reyder does not support procedure headers. An unset setting may change request headers and server work versus warehouse SEA, but public metadata shape and procedure empty-on-error behavior remain (R5; AC65, AC75). |
  | Typed async metadata | JDBC waits | JDBC waits | V9+ handles overlap; older waits | Optional API; synchronous JDBC stays unchanged (R5; AC50, AC61). |
  | Bindings and batches | Scalars/complex, prepared/Statement batches | Same; warehouse native batches may return per-set counts | Native V8+ scalars/V10 batches; fallback otherwise | APC native response has only an aggregate count, so each successful set reports `SUCCESS_NO_INFO`; complex interpolation raises an early coded error (R6, R15; AC17, AC29, AC32). |
  | SQL over 16 MiB | Backend boundary | SEA rejects above 16,777,216 UTF-8 bytes | Same SEA boundary | Warehouse precedent; callers shorten SQL or legacy chunks and reconcile prior commits (R6; AC87). |
  | Arrow and heartbeat | No Arrow prerequisite; Thrift heartbeat | Explicit override before Arrow check; `/status` | Arrow-first, no `/status` | APC status RPC unsupported; an open ResultSet has no heartbeat (R1, R3; AC1, AC3). |
  | Logical/complex getters | Thrift value settings | Inline typed complex getters reject | Same SEA getters | Warehouse precedent; typed complex callers need links (R11; AC13). |
  | Fetch and CloudFetch tuning | Workers/chunks and streaming link lookahead | Same linked scope; no SEA page tuning | `cloudFetchThreadPoolSize` bounds linked workers/chunks; `LinkPrefetchWindow` bounds fetched-ahead links, not inline pages | Match APC Thrift and warehouse SEA linked queue, heap, and timeout behavior at one/32 callers; `RowsFetchedPerBlock` has no SEA effect (R17; AC31, AC46). |
  | Result codec and API compression | Thrift/result setting | Requested result codec; separate API compression | Manifest codec governs APC decode | Server exception: `NONE` cannot force uncompressed APC results; measure caller cost (R4; AC101). |
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
- R4: APC non-metadata execution sends `format=ARROW_BATCH`,
`disposition=INLINE_OR_EXTERNAL_LINKS`, and `session_id`, never `statements`.
Decode all bare batches in `chunk.arrow_batches` and complete Arrow IPC streams
at external links, retaining link headers and every row. `EnableQueryResultDownload=0`
keeps SEA and sets string-valued CreateSession
`session_confs["can_cloud_download"]="false"`; after the tracked server fix,
results remain inline. An inline-size failure yields no successful partial ResultSet.
`EnableSQLExecHybridResults=0` cannot force external-only APC results, and
`UseBoundedSeaApi` changes neither APC request nor session behavior. Positive
`Statement.setMaxRows` sends `row_limit` and caps rows across pages/links;
zero is unlimited.

Successful no-result DDL has no ResultSet and update count zero, then `-1`
after `getMoreResults()`. Result-producing SQL, including `SELECT`, `SHOW`,
`VALUES`, `SET AUTOCOMMIT` state queries and `NonRowcountQueryPrefixes` matches,
requires schema and data before public success. Counted `INSERT`, `UPDATE`,
`DELETE`, and `MERGE` require a `num_affected_rows` schema column and value,
including zero; missing count data is a contract error. A matching
`NonRowcountQueryPrefixes` command instead exposes the count ResultSet and update
count `-1`, and stops a Statement batch with prior counts and the existing error.
Direct and polled DDL/DML live gates depend on T32.

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
On APC, unset `EnableThriftNativeMetadata` acts as false; only explicit `1`
sends `X-Databricks-Require-Thrift-Native-Metadata: true` for supported SHOW.
Reyder still returns native-shaped metadata, so conversion follows
`manifest.is_native_metadata_result`, not the request setting. Synchronous
SHOW sends `X-Databricks-Sea-Can-Run-Fully-Sync: true` only at
`EnableSeaSyncMetadata=1`; async SHOW omits it. Decode each response as Arrow
IPC `chunk.attachment` or HIVE `chunk.columnar_data`/`COLUMN_BASED_SET`.

`enableMultipleCatalogSupport=0` retains current-catalog filtering; `1`
permits eligible catalogs. `EnableMetricViewMetadata=1` adds
`spark.sql.thriftserver.metadata.metricview.enabled=true` to session config
and exposes `METRIC_VIEW` table types and eligible rows; `0` omits both.
`getExportedKeys` is outside the seven SHOW operations: the DBR parent-only
`GetCrossReference` contract and warehouse SEA return typed empty for a valid
table, so APC SEA returns typed empty without a request for valid inputs. A null
table retains warehouse SEA's `SQLException`, SQLState `42000`, and
`EXECUTE_STATEMENT_FAILED` (1003) vendor code before any request (AC54).

The session-reachable async metadata API accepts those seven operations and
JDBC inputs and returns typed ResultSet handles. V9+ (42249) APC SEA submits
without blocking, then awaits within the metadata timeout with synchronous
conversion, empty, error, and cancel behavior. Older/versionless APC handles
complete before return; APC Thrift and warehouse SEA retain existing results.
Public `getColumns` remains synchronous, and SQL async is separate.

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

For V9+ async null-catalog `GET_SCHEMAS`/`GET_COLUMNS`, initial SQLState `42601`
triggers catalog enumeration and per-catalog SHOW with the operation header
and no sync header. As on synchronous warehouse SEA, a per-catalog
`SQLException` warns and omits that catalog; initial/enumeration/overall errors
propagate except established object-not-found or empty-pattern typed-empty
cases. Overall timeout/cancel fails; partial rows and a usable connection remain
(AC83). Null-catalog `getTables` instead returns typed empty on `42601`
without fan-out, as warehouse SEA does, which may hide APC Thrift rows (AC105).

- R6: CreateSession `negotiated_protocol_version` alone grants native scalar
parameters at V8 (42248), async metadata at V9 (42249), and opt-in
`EnableNativeBatching=1` INSERT batches at V10 (42250). Reyder leaves
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
input set; on failure every set is `Statement.EXECUTE_FAILED`, with commit
status uncertain and no atomic rollback claim. Below V10 or native-off,
`EnableBatchedInserts=1` combines eligible INSERT sets; `BatchInsertSize`
and the existing parameter limit bound chunks, while ineligible sets execute
individually. Preserve ordered counts, stop-on-error, and the existing
uncertain-commit behavior when a prior chunk may have succeeded.

SEA's 16,777,216 UTF-8 byte SQL-text limit applies to single statements and
legacy generated chunks without silent split or fallback; an oversized request
propagates the warehouse SEA error. `BatchInsertSize` can reduce chunk size.
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
- R9: From the first flag-on connection slice, public `getConnectionId()` returns a usable
ID encoding the selected protocol and target; `SessionId.create()` must not require a
Thrift handle for an APC SEA session. New saved IDs let
`Driver.closeConnection` close the original session after a flag or URL change; public
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
- R13: Decode APC SEA `columnar_data` physical vectors and null masks using
`manifest.schema`; JDBC columns, values, and nulls match APC Thrift metadata.
An unrecognized vector fails clearly. Test-plan fixtures cover emitted vectors and
mask boundaries.
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
- R17: APC SEA follows warehouse SEA's SDK retry/pool and streaming CloudFetch behavior
at either `UseBoundedSeaApi` value. `EnableInlineStreaming` and
`EnableStreamingChunkProvider` cannot change APC SEA rows, paging, or memory; APC
continuation lacks reliable totals for the legacy linked behavior.
`ThriftMaxBatchesInMemory` still bounds inline buffering (minimum two).
Case-insensitive `cloudFetchThreadPoolSize` (default 16) bounds per-result linked
download workers and buffered chunks; `LinkPrefetchWindow` (default 128) bounds
fetched-ahead links, not workers, chunks, or inline pages. `ChunkReadyTimeoutSeconds`
bounds linked-chunk wait. `RowsFetchedPerBlock` does not tune SEA paging.
Thrift HTTP retry and connection-acquisition settings tune presigned-link downloads,
not SEA SDK API requests. Match warehouse SEA 429/503 and `Retry-After` handling
by request class without new write retries. No APC-only concurrency cap or
full-result buffer is allowed (AC30/46/51/84).
- R18: Every approved JDBC slice exposes its completed public behavior when R1's default-off
APC flag is enabled or `UseThriftClient=0` is explicit. Incomplete public calls, including
`executeAsync`/`getExecutionResult` before T39, fail before network execution when known;
unsupported response shapes fail before false success. They never
silently switch to Thrift after SEA selection. After T17 and T35 deploy, a scoped staging
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
against APC Thrift and warehouse SEA where relevant. AC3/11/40/51/84/85 apply to each new
request class; AC30 applies to a new pool and AC10 to a new DEBUG path. Fixtures cover wire
shapes or versions unavailable live, but cannot close a live gate. Server-dependent gates stay
pending until deployment and live proof. AC identifiers retain prior review references.

For the shared paired-load gate (AC31/32/46/61/75/103/104/107), compare the same
driver build, staging region, warmed cluster, data shape, and one/32-caller workload
on APC SEA and APC Thrift. Median SEA/Thrift throughput must be at least 0.90,
latency at most 1.10, and peak heap at most 1.20. AC31/32 additionally bound
retained heap growth across two data sizes to max(8 MiB, 5% of added data bytes).
Keep raw measurements, fixture sizes, and error counts as release evidence.
The test plan owns rounds, sampling, and fixture matrices. A measured exception
requires caller impact and PECO/Perf Eng approval.

- AC1: GIVEN scoped flag-ON and flag-OFF APC staging WHEN `UseThriftClient` is unset, `0`, or `1` and `TreatMetadataCatalogNameAsPattern=1` is varied THEN live routing proves flag default, forced Thrift with flag ON, forced SEA with flag OFF, and explicit SEA precedence over catalog-pattern Thrift. Compare APC Thrift and warehouse SEA; fixtures cover Arrow-unavailable, legacy `cliservice`, lookup failure, and partner exclusion. Selected SEA errors never fall back.
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
timestamp_ntz, geography, geometry, and variant values WHEN inline and linked getters
run with complex/geospatial settings at 0/1 THEN types and values match both baselines;
inline typed complex getters reject, enabled linked getters work, and disabled modes
return established strings. Unavailable live types remain pending despite fixtures.
- AC14: GIVEN live APC multi-link results with absent totals WHEN public JDBC drains them
THEN rows arrive once using R12's final-link continuation; complete Arrow IPC streams and
presigned headers match APC Thrift values and warehouse SEA link handling.
- AC16: GIVEN each synchronous APC SHOW when introduced in emitted Arrow IPC or columnar format WHEN read on live staging THEN JDBC columns, values, nulls, and errors match the applicable baseline and R5; fixture-only formats remain pending live proof.
- AC17: GIVEN flag-on live V10 APC and paired APC Thrift/warehouse SEA routes WHEN a native prepared INSERT batch succeeds or fails through either batch API THEN ordered `parameter_sets` yield one `SUCCESS_NO_INFO` per successful input, never a copied aggregate; failure yields `EXECUTE_FAILED` for every set with uncertain commit status. Compare committed rows, counts, and error identity with both baselines. Older/native-off modes retain legacy counts; captured V10 responses supplement but cannot close this live gate.
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
- AC29: GIVEN flag-on APC staging and the paired APC Thrift/warehouse SEA routes WHEN complex scalar or batch bindings are submitted THEN APC SEA raises its named/numeric driver error before any request, keeps the connection usable, and records the route's error identity against both baselines. Record the owner-verified taxonomy/dashboard update or maintainer handoff in the PR.
- AC30: GIVEN flag-on APC staging and paired APC Thrift/warehouse SEA under pool saturation WHEN `HttpConnectionPoolSize`, `HttpMaxConnectionsPerRoute`, and `HttpConnectionRequestTimeout` vary separately for each newly exposed API or linked-download class THEN measured in-flight count, wait, transport error identity, and throughput follow the applicable transport scope; SDK API acquisition ignores the Thrift HTTP wait setting.
- AC31: GIVEN each SELECT mode at its first public exposure WHEN one and 32 paired APC SEA/Thrift callers drain primitive, multi-page, or linked results as applicable THEN rows/errors match, SEA paging ignores `RowsFetchedPerBlock`, and the shared load gate passes. The release gate repeats the combined mode mix.
- AC32: GIVEN flag-on live APC SEA and paired APC Thrift/warehouse SEA legacy, V10 native prepared, and Statement INSERT batches WHEN one and 32 callers use disjoint keys and R6 settings THEN committed rows, counts, errors, and the shared load gate pass for each exposed mode. Record negotiated version; captured V10 responses cannot close native correctness or load gates.
- AC33: GIVEN successful live APC DDL with absent or zero-column manifest WHEN public JDBC executes it
THEN no ResultSet/fetch appears, execute is false, count is zero, and getMoreResults makes it -1.
Repeated close is safe; failed DDL preserves its error, while result-producing SQL without schema
fails before success.
- AC34: GIVEN the deployed default-off APC flag WHEN partner and non-partner User-Agents
request client flags THEN connector-service returns false for every partner, including JDBC
warehouse-allowlisted partners, and retains the ordinary APC value for non-partners. Record the
service version and region before any APC assignment or ramp.
- AC36: GIVEN APC chunks with multiple Arrow batches and inline continuations WHEN JDBC drains them THEN rows appear once and `row_offset` includes all batch rows; a failed page cannot report success. If live staging lacks multiple batches, retain a fixture and leave live proof pending.
- AC39: GIVEN each inline or linked mode at its first flag-on live exposure WHEN hybrid, bounded-SEA, and streaming switches vary at one and 32 callers THEN R4/R17 rows, requests, and AC31 load bounds hold against APC Thrift and warehouse SEA; switches that cannot select an APC wire mode do not alter returned rows or bypass buffering limits.
- AC40: GIVEN any merged JDBC slice WHEN the APC flag is OFF THEN APC uses Thrift; on scoped
flag-ON staging completed calls pass their live parity gate. Known incomplete calls fail
before SEA requests without Thrift fallback; public SQL async stays guarded until T39 and
production assignment waits for T18/T-val.
- AC41: GIVEN an executable statement or saved session after a flag change WHEN live
fetch/cancel/close runs THEN R9's current or saved target/protocol is used and the connection
remains usable; compare APC Thrift and warehouse SEA after AC2 passes.
- AC45: GIVEN APC results WHEN `EnableSeaResponseCompression` changes THEN `0` sends `Accept-Encoding: identity`, `1` permits API compression, and neither alters presigned links; compare API bytes and latency with both baselines.
- AC46: GIVEN paired linked results at one and 32 callers WHEN `cloudFetchThreadPoolSize`, `LinkPrefetchWindow`, chunk wait, and slow-link settings vary independently THEN active downloads and buffered chunks stay within the per-result worker limit, fetched-ahead links within the window, and the aggregate limits scale by caller count. Rows, queue depth, timeouts, warnings, duration, and AC31 load bounds match the baselines; inline pages are unaffected.
- AC47: GIVEN live single-chunk APC inline SELECT WHEN public JDBC reads it THEN rows,
order, and types match both baselines. Fixtures prove R16 wire values, absent
`arrow_schema`, and clear failure for an unknown format.
- AC48: GIVEN live APC counted `INSERT`, `UPDATE`, `DELETE`, or `MERGE` WHEN public
update methods run THEN counts, including zero, match APC Thrift after T32. A missing
`num_affected_rows` column, row, or value produces a contract error, not successful zero.
- AC49: GIVEN each live SELECT mode at its first exposure WHEN
`setMaxRows(0)`, `1`, and boundaries applicable to that mode run THEN positive limits send SEA
`row_limit`, zero is unlimited, and rows never exceed the limit. Compare rows and request
count with APC Thrift and warehouse SEA; fixtures cover unavailable boundaries.
- AC50: GIVEN each typed SHOW handle when introduced on live V9+ APC WHEN it submits a delayed operation THEN it returns while RUNNING and another call completes before await. Awaited rows/errors, timeout, cancel, and cleanup match synchronous APC and both baselines for that operation; older/versionless handles complete synchronously. Procedure SELECTs reject async before request. Cross-reference conversions are gated when their handles are introduced.
- AC51: GIVEN isolated live 429/503 failures with and without `Retry-After` WHEN each new SEA API or linked-download class retries or exhausts under R17 settings THEN attempts, delay, exception, SQLState, and vendor code match warehouse SEA without write replay. Exhausted CreateSession is covered; Thrift HTTP settings affect links, not SDK API requests.
- AC52: GIVEN terminal direct ExecuteStatement and polled GetStatementResult for each APC DDL or DML command when introduced WHEN it completes THEN server evidence shows no-result DDL success or a counted DML schema and row, including zero; failures keep errors. Each path gates its public AC33 or AC48 outcome.
- AC53: GIVEN live APC prepared scalars or fallback batch mode when introduced, plus older/versionless fixtures WHEN `supportManyParameters` is 0 or 1 THEN V8+ at 0 sends native scalars, while older/versionless at 0 and every version at 1 interpolate without unresolved markers; `setBytes` follows R6. Rows/counts and request shape match both baselines for the exposed mode; identify fixture-only branches.
- AC54: GIVEN a valid table with a referencing foreign key on all three live routes WHEN `getExportedKeys` runs THEN each returns typed empty, APC SEA makes no request, and APC Thrift sends parent-only GetCrossReference. GIVEN a null table WHEN the method runs THEN APC SEA raises warehouse SEA's `SQLException`, SQLState `42000`, and `EXECUTE_STATEMENT_FAILED` (1003) before any request. If Thrift gains rows, parity remains unresolved.
- AC55: GIVEN public APC SEA SQL async SELECT WHEN primitive and linked results complete
THEN `wait_timeout=0s`, targeting, rows/errors, cancel, close, and diagnostics follow R2/R10.
Apply AC31's one/32-caller load bounds to this public path before unguarding it.
- AC60: GIVEN a valid live query within its configured client wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Record timing,
error identity, and connection usability against both baselines; an exception needs caller
impact and PECO approval.
- AC61: GIVEN each nonempty metadata operation or typed handle at first exposure WHEN one and 32 callers run it THEN rows/errors and the shared load gate pass; null-catalog fan-out makes at most N+2 requests for N eligible catalogs. For that operation, CPU per 1,000 JDBC cells is at most 110% and peak RSS above idle at most 120% of a paired direct-SEA trace. The release gate repeats the combined seven-SHOW, handle, and procedure mix; typed-empty correctness cannot replace nonempty samples.
- AC63: GIVEN valid delayed SHOW within its configured metadata wait WHEN it exceeds one
SEA request timeout THEN polling completes without premature failure. Record timing,
error identity, and usability against both baselines; an exception needs caller impact and
PECO approval.
- AC64: GIVEN `NonRowcountQueryPrefixes=INSERT` on all three live routes WHEN a matching
INSERT runs THEN `execute` returns true, its ResultSet contains the affected-row value,
and `getUpdateCount` returns `-1`. Once Statement batches are exposed, that command
stops the batch with preceding counts and the baseline error identity.
- AC65: GIVEN each exposed live APC SHOW with `EnableThriftNativeMetadata` unset, `0`, and `1` and
`EnableSeaSyncMetadata` at `0` and `1` WHEN a supported synchronous method runs THEN the
native request header appears only at native setting `1`, the sync header only at sync
setting `1`, and native-shaped JDBC columns/values match APC Thrift and warehouse SEA at
all settings. Async SHOW omits the sync header when typed handles are exposed. A changed Reyder response shape leaves
this gate pending for contract review.
- AC66: GIVEN `MaxBatchSize=2` on live APC SEA, APC Thrift, and warehouse SEA WHEN a third
`Statement.addBatch` is attempted THEN it fails with the same validation error before any
batch execution, and the two accepted commands remain executable in order.
- AC75: GIVEN live parameterized `getProcedures` and `getProcedureColumns` WHEN each runs across pages/links THEN header-free R5/R6 queries return ordered JDBC rows matching baselines and pass AC61. On query/fetch/decode failure, public JDBC returns the same typed empty result as both baselines and logs the failure; zero matches are also typed empty. Older/versionless interpolation needs fixtures.
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
poll interval, and idle expiry vary THEN `MetadataOperationTimeout` bounds the overall
metadata poll wait, raises the metadata timeout error and targets cancellation on expiry;
`0` disables that client metadata deadline while valid delayed SHOW completes. `socketTimeout`
bounds one SDK API or linked HTTP request, not the entire polling operation, and a stalled
request raises the route's transport error without changing the selected protocol.
`asyncexecpollinterval` changes the delay between polls after the first, not the first poll
or query deadline. `IdleHttpConnectionExpiry` evicts idle SDK connections before reuse, not
active requests or the metadata deadline. Record configured and observed timing, request
counts, error identity, and usability against warehouse SEA and APC Thrift for the applicable
transport; no setting makes a valid in-budget operation fail early.
- AC85: GIVEN each live execution, metadata, download, batch, and failure path WHEN
telemetry exports THEN R8's redacted event families, operation/format/counts, and error
identity match both baselines or document a measured exception.
- AC86: GIVEN live APC with `EnableSQLValidationForIsValid=1` WHEN
`isValid(timeout)` runs its `SELECT VERSION()` check THEN healthy/failed results, timeout,
and connection usability match APC Thrift and warehouse SEA.
- AC87: GIVEN SQL at 16,777,215/16,777,216/16,777,217 UTF-8 bytes WHEN an exposed
single statement or legacy INSERT chunk executes THEN APC SEA accepts up to the SEA limit
and propagates the warehouse SEA oversize error above it without fallback. Record bytes,
counts, committed rows, and APC Thrift differences; BatchInsertSize can reduce chunk size.
- AC88: GIVEN two accessible catalogs and a metric view WHEN `enableMultipleCatalogSupport` and `EnableMetricViewMetadata` vary on all three live routes THEN catalog access, discovery, table types/rows, and APC session setting match R5; record any server-limited row difference and caller impact.
- AC89: GIVEN live invalid SQL, missing-object, and permission failures WHEN applicable execute
methods surface them THEN record SQLException class, SQLState, vendor code, and message category
across APC SEA/Thrift and warehouse SEA. APC SEA matches agreement between baselines, otherwise
measured warehouse SEA mapping with the caller impact; no failed command reports success.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN each APC inline or linked mode when introduced WHEN result-codec settings select `NONE` or `LZ4_FRAME` THEN APC decodes the actual manifest codec, including unmarked data and LZ4 despite requested `NONE`, and returns baseline rows; invalid text retains R4's default. At one/32 callers record requested/actual codec, bytes, CPU, throughput, latency, and caller impact for PECO/Perf Eng approval. Selectable APC codec requires a server change.
- AC102: GIVEN an isolated write table WHEN transaction and auto-commit settings vary on all three live routes THEN state, rows, no-ops, and errors match R19, including missing state-row and failed-command behavior; record cause, SQLState/vendor code, and usability.
- AC103: GIVEN paired live APC SEA/Thrift DDL on isolated schemas WHEN one and 32 callers create/drop distinct tables THEN AC33 and the shared command load gate pass.
- AC104: GIVEN paired live APC SEA/Thrift DML on disjoint keys WHEN one and 32 callers run INSERT, UPDATE, DELETE, and MERGE THEN AC48 and the shared command load gate pass.
- AC105: GIVEN live null-catalog `getTables` that receives `42601` WHEN each exposed
synchronous call or V9+ typed async handle is compared across APC SEA, APC Thrift, and
warehouse SEA THEN APC SEA and warehouse SEA return the same typed empty table result
without catalog fan-out; record any APC Thrift rows omitted and the caller impact.
- AC106: GIVEN a changed driver-owned APC SEA error WHEN it reaches JDBC THEN a test asserts its `DatabricksDriverErrorCode` name and unique number, and the PR records owner-verified telemetry taxonomy/dashboard updates or maintainer handoff and completes the telemetry-error template. Server errors retain server identity.
- AC107: GIVEN one and 32 concurrent callers on paired APC SEA and APC Thrift staging
WHEN each repeatedly opens, executes, cancels, and closes a statement/session, including
saved-ID close after a flag change, THEN no live session or statement remains; server
active counts and client handles/threads return to baseline, retained heap growth is at
most 8 MiB, and median SEA/Thrift cycle latency is at most 1.10. The test plan owns
cycle counts and sampling; record cleanup delay or exceptions with caller impact and
PECO/Perf Eng approval.
- AC108: GIVEN flag-on live APC legacy prepared INSERT batches with `EnableBatchedInserts=1` and paired APC Thrift/warehouse SEA WHEN `BatchInsertSize` and the existing parameter limit vary THEN eligible sets combine into observable bounded request groups, ineligible sets remain individual, and committed rows, ordered counts, errors, request counts, and throughput follow the baseline scope. With batching disabled, eligible sets no longer combine. No generated chunk exceeds the SEA SQL-text limit.
- AC109: GIVEN concurrent flag-on live APC multi-page inline SELECTs and paired APC Thrift/warehouse SEA WHEN `ThriftMaxBatchesInMemory` varies below two and above two THEN the effective per-result cap is at least two and no greater than the configured value above two, peak buffered batches and heap stay within that cap, all rows arrive once, and measured throughput and AC31 load bounds hold. The test plan supplies values and sampling.
- AC110: GIVEN the first flag-on APC connection slice WHEN a caller obtains `getConnectionId()` and closes that saved ID after changing the flag or URL THEN the ID is usable without a Thrift handle, only the original targeted session closes, and the live connection lifecycle and error identity match APC Thrift and warehouse SEA.

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

`plan.json` has repository-local PR trees; a `parent` is the branch point in its
repository. The JDBC lifecycle slice proves open, public ID, saved-ID close,
and ordinary close before the first SELECT slice exposes SQL. The latter includes
its routing, retry, timeout, cancel, telemetry, and paired-load gates. Paging,
links, commands, and metadata then add separately gated public behavior. The
command and metadata branches may develop in parallel because their public
implementations own disjoint areas; an explicit integration package merges the
command head into the metadata head before full validation. Splitting these
end-to-end outcomes exceeds the preferred 4–7 JDBC packages but keeps each PR
near the 1,500-line review cap; no slice is approved solely on fixtures. The
flag/partner server package deploys before scoped flag-on validation, the
TerminateSession fix gates lifecycle approval, the command-result server package
gates DDL/DML, and SC-254478 gates inline-only SELECT. The last JDBC package
alone owns docs/changelog. Open live or performance gates block release and ramp.

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

Each task has one owner in `plan.json`. The outcome is observable through a public call,
wire capture, server response, or release evidence.

| ID | Area | Outcome |
| --- | --- | --- |
| T17 | Server | Define default-off dedicated APC client-config sub-flag. |
| T35 | Server | Exclude every partner User-Agent from APC flag assignment. |
| T32 | Server | Return correct direct and polled DDL/DML terminal results. |
| T1 | Connection | Represent APC compute targeting in SEA requests. |
| T2 | Connection | Open and terminate an APC-targeted SEA session. |
| T3 | Connection | Emit redacted route and failure diagnostics. |
| T4 | Connection | Honor explicit protocol override and APC Arrow prerequisite. |
| T6 | Connection | Honor APC flag, catalog-pattern preference, and no fallback. |
| T19 | Connection | Serialize selected protocol and target in new connection IDs before SELECT exposure. |
| T29 | Connection | Close saved sessions after flag or URL change. |
| T44 | Connection | Preserve legacy ID decoding and raw statement IDs. |
| T26 | Connection | Apply SDK API retry, pool, and socket timeout scopes. |
| T7 | First SELECT | Poll results without APC status heartbeats. |
| T8 | First SELECT | Request APC Arrow batch and hybrid disposition. |
| T10 | First SELECT | Return a primitive single-chunk SELECT through public JDBC. |
| T12 | First SELECT | Apply direct-result and query timeout controls. |
| T27 | First SELECT | Cancel and close running statements without leaks. |
| T45 | First SELECT | Target every first-slice R2 operation without warehouse fields. |
| T47 | First SELECT | Decode first inline batch from manifest schema. |
| T57 | First SELECT | Emit correlated operation, format, and error telemetry. |
| T59 | SELECT options | Enforce positive row limits in each exposed SELECT mode. |
| T63 | SELECT options | Run SQL-backed isValid through the selected protocol. |
| T61 | First SELECT | Decode result data using the actual manifest codec. |
| T5 | Inline pages | Retain negotiated session version without inferring capabilities. |
| T11 | Inline pages | Fetch inline continuation until terminal page. |
| T25 | Inline pages | Fetch the first chunk from a manifest-only response. |
| T34 | Inline pages | Advance row offsets across all batches in a chunk. |
| T62 | Inline pages | Return every row from multiple Arrow batches. |
| T15 | Linked SELECT | Diagnose linked fetch without leaking targets or link credentials. |
| T20 | Linked SELECT | Preserve logical and complex JDBC getters. |
| T33 | Linked SELECT | Decode complete linked Arrow IPC streams. |
| T36 | Linked SELECT | Page linked results using final-link continuation. |
| T46 | Linked SELECT | Bound linked workers, prefetch, waits, and memory. |
| T39 | SQL async | Complete public executeAsync/getExecutionResult SELECT lifecycle. |
| T28 | DDL | Return public DDL success without a ResultSet. |
| T31 | DDL | Reject missing schema for result-producing SQL while accepting no-result DDL. |
| T9 | Bindings | Bind supported prepared scalars using negotiated capability. |
| T22 | Bindings | Reject complex bindings with a coded error before request. |
| T38 | DML | Return exact affected-row counts, including zero. |
| T60 | DML | Preserve auto-commit, commit, rollback, and failure behavior. |
| T21 | Batches | Run native or legacy prepared batches with ordered counts. |
| T30 | Batches | Run Statement SQL batches sequentially with stop-on-error. |
| T14 | Discovery | Return catalog and schema SHOW results in either wire format. |
| T43 | Discovery | Preserve catalog and schema filtering and null-catalog recovery. |
| T48 | Discovery | Return table SHOW rows and typed-empty null-catalog syntax outcome. |
| T49 | Discovery | Return column SHOW rows and null-catalog recovery. |
| T13 | Relations | Return function SHOW rows and setting-dependent parity. |
| T37 | Relations | Return primary-key SHOW rows. |
| T54 | Relations | Return imported and cross-reference key rows. |
| T55 | Relations | Return typed-empty exported keys for valid tables without a request; preserve the null-table error. |
| T42 | Procedures | Return parameterized procedure metadata without unsupported headers. |
| T56 | Procedures | Preserve public typed-empty procedure failure behavior. |
| T24 | Async metadata | Submit and await typed SHOW handles with timeout and cancel. |
| T23 | Async metadata | Expose catalog typed async handle. |
| T40 | Async metadata | Expose schema typed async handle. |
| T50 | Async metadata | Expose table typed async handle. |
| T51 | Async metadata | Expose column typed async handle. |
| T41 | Async metadata | Expose function typed async handle. |
| T52 | Async metadata | Expose primary-key typed async handle. |
| T53 | Async metadata | Expose cross-reference typed async handle. |
| T58 | Inline-only | Keep download-off SELECT on SEA and inline after server fix. |
| T16 | Release | Document the completed feature and changelog. |
| T18 | Release | Collect full live parity, performance, and telemetry evidence. |
| T-val | Release | Run integrated and post-merge release gates before ramp. |
