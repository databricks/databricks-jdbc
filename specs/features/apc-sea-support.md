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
(API contract, Phase II, and metadata tabs). The server limits SQL text to 16 MiB
(16,777,216 UTF-8 bytes), rejects non-OSS-JDBC clients, and starts rollout opt-in
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
- Parity baseline for review and release ("warehouse" uses the same driver build).
  R1–R19 define behavior; this matrix records the disposition and caller impact of
  differences. AC gates provide evidence. For each non-functional exception, add the
  measured warehouse Thrift/SEA delta required by the shared paired-load gate.

  | Behavior | APC Thrift today | Warehouse SEA today | APC SEA | Disposition and caller impact |
  | --- | --- | --- | --- | --- |
  | Route/heartbeat | Default Thrift, heartbeat | Flag/override, `/status` | Dedicated flag, Arrow-first, no `/status` | APC status RPC is unavailable; SEA errors do not fall back (R1, R3, R7). |
  | SELECT/results | Inline/links | Inline/links, `row_limit` | APC pages/links, same rows/limit | Match rows; extra polling/pages may raise latency (R4, R10–R12; AC31). |
  | Download/codec | Thrift provider/result codec | SEA provider/requested codec | Hybrid; manifest codec | No external-only APC mode; `NONE` cannot force uncompressed output. Callers may receive inline/LZ4 data (R4, R17; AC8, AC101). |
  | Inline streaming switch | `EnableInlineStreaming=1` uses a streaming inline provider; `0` uses a lazy provider | Bounded inline Arrow or legacy inline JSON is selected by SEA settings, not this switch | APC uses the bounded inline provider at either value | Follow warehouse SEA: the switch cannot restore Thrift's lazy provider. Callers changing it may see different first-row latency and retained heap; measure both protocol deltas (R17; AC111). |
  | Streaming provider | `EnableStreamingChunkProvider` selects the Thrift provider | It selects warehouse SEA's linked provider when `UseBoundedSeaApi=0`; bounded SEA forces it on | APC uses the bounded linked/inline provider at either setting | APC continuation lacks reliable totals for the legacy provider; the switch cannot select it. Caller rows must match, while memory/latency may differ (R17; AC39). |
  | Fetch tuning/resources | `RowsFetchedPerBlock` tunes Thrift fetch; `cloudFetchThreadPoolSize` and HTTP settings tune links | `RowsFetchedPerBlock` does not tune SEA paging; linked workers/prefetch and SDK pool have separate scopes | Same SEA paging and bounded linked/inline resources | Thrift row-block tuning cannot transfer to SEA; callers use link/inline controls and may see different fetch latency or memory. Measure the exception (R17; AC30, AC31, AC46). |
  | Public SQL async | `executeAsync`/`getExecutionResult` use Thrift operation lifecycle | The same public API uses SEA submit/poll and linked results | SEA submit/poll, APC targeting, cancellation and close | Follow warehouse SEA; APC cannot use status heartbeats, so poll timing and network use can differ (R3, R10; AC55). |
  | DDL/DML/transactions | Counts and transaction SQL | Same public outcomes | Same public outcomes | Match counts, committed rows and errors; missing count is failure (R4, R19; AC33, AC48, AC102). |
  | Metadata/procedures | Native RPC; procedure errors empty | SHOW; procedure headers, empty-on-error | SHOW; no procedure headers, empty-on-error | APC server rejects procedure headers; default native header differs, but public shape remains (R5; AC65, AC75). |
  | Null-catalog schemas/columns | Native RPC | `42601` fan-out, partial rows | Same fan-out | Warehouse precedent; warnings and omitted catalogs reach callers (R5; AC83). |
  | Null-catalog tables/exported keys | Native RPC; valid-table exported keys empty | Tables empty on `42601`; valid-table exported keys empty | Same | Warehouse/DBR precedent may hide APC Thrift table rows; null exported-key table keeps coded error (R5; AC54, AC105). |
  | `getFunctions` setting | Native RPC or SHOW | SHOW at either value | SHOW at either value | Warehouse precedent; null-catalog rows/errors/latency may differ from APC Thrift (R5; AC82). |
  | Async metadata | JDBC waits | JDBC waits | V9+ handles overlap | Optional API; synchronous JDBC remains (R5; AC50). |
  | Bindings/batches/SQL size | Scalars/complex, ordered batches | Scalars; per-set counts, 16 MiB SQL limit | V8+ scalars, V10 aggregate batches, same limit | Complex binding fails early; V10 successful sets report `SUCCESS_NO_INFO`. In legacy batches `BatchInsertSize` bounds interpolated chunks only; native-scalar chunks use the parameter limit. Callers may shorten SQL/chunks (R6, R15; AC17, AC29, AC87, AC108). |
  | Logical getters | Thrift settings | Inline typed complex getters reject | Same SEA getters | Warehouse precedent; typed complex callers need links (R11; AC13). |
  | Saved close | Thrift ID; flag change can misroute | ID may encode SEA on Thrift | Protocol/target in new ID | Flag/target changes work with the original workspace endpoint/credentials; old IDs may need reopen (R9; AC25). |
  | Errors/retries/telemetry | Thrift transport/events | SEA mapping/SDK pool/events | SEA request-class scopes/events | Warehouse precedent may change error identity; no new write replay. Match events and resource bounds or record exceptions (R8, R17; AC30, AC51, AC85, AC89). |

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
input set. If execution fails, a `BatchUpdateException` marks every set
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

SEA's 16,777,216 UTF-8 byte SQL-text limit applies to single statements and
legacy generated chunks without silent split or fallback; an oversized request
propagates the warehouse SEA error. `BatchInsertSize` can reduce interpolated
legacy chunk size; native-scalar chunking follows the parameter limit.
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
Thrift handle for an APC SEA session. Given the original workspace endpoint (scheme, host, port)
and usable credentials,
new saved IDs let `Driver.closeConnection` close the original session after a flag or compute-target
change in the supplied URL or properties. The ID does not encode the endpoint or credentials;
endpoint and credential changes are outside this guarantee. Public statement IDs remain raw.
Internal identities retain the compute target and reject malformed IDs without logging identifiers.
Preserve decoding of legacy warehouse SEA and Thrift IDs;
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
The server sets a mask for every vector. Within each mask byte, bit 0 is the
first row (least-significant-bit first);
`1` means null. A null row still consumes its placeholder vector value. Rows
beyond a short mask are non-null; bits beyond the row count are ignored.
An unrecognized vector fails clearly. Test-plan fixtures cover emitted vectors,
short and long masks, and byte boundaries.
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
Unlike APC Thrift, which changes between streaming and lazy inline providers with
`EnableInlineStreaming`, APC SEA follows warehouse SEA's separate inline provider:
the switch does not select a provider or change APC SEA buffering. Callers may
observe different first-row latency and retained heap when migrating at `0`; AC111
measures and dispositions that difference.
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
A9: Yes, including JDBC partners allowed by `enableSqlExecForJdbc`. Deploy T35 before APC
assignment; partner enablement needs a separate rollout decision (R1, T17).
- Q10: How is repeated transfer avoided during `GetStatementResult` polling?
A10: At least three RUNNING polls of a large query transfer no result payload; terminal
response and fetch return all rows once (AC27).

## Acceptance Criteria

At first exposure, each public slice proves its completed call on scoped flag-ON APC staging
against APC Thrift and warehouse SEA where relevant. AC3/11/40/51/84/85 apply to each new
request class; AC30 applies to a new pool and AC10 to a new DEBUG path. For induced
failures, use a controlled staging fault proxy or service hook that preserves the real
request, then replaces only the specified response and records the mutation. If no such
hook is available, deterministic fake-service fixtures may close only that induced
failure branch; record the unavailable hook and retain live ordinary flag-on and paired
parity gates. Fixtures cannot close an ordinary live gate. Server-dependent gates stay
pending until deployment and live proof. Remaining AC IDs stay stable; retired duplicates
(AC41/47/103/104/110) are not reused.

For the shared paired-load gate (AC31/32/33/46/48/61/75/107), compare the same
driver build, staging region, warmed cluster, data shape, and one/32-caller workload
on APC SEA and APC Thrift. Median SEA/Thrift throughput must be at least 0.90,
latency at most 1.10, and peak heap at most 1.20. AC31/32 additionally bound
retained heap growth across two data sizes to max(8 MiB, 5% of added data bytes).
Keep raw measurements, fixture sizes, and error counts as release evidence.
The test plan owns rounds, sampling, and fixture matrices. For every proposed non-functional
exception in this spec, also measure warehouse SEA versus warehouse Thrift on the same driver build and
comparable workload, then record the APC and warehouse protocol deltas, server limitation or
warehouse precedent, caller impact, and disposition in the parity matrix. A measured exception
requires PECO/Perf Eng approval; an unmeasured or undispositioned difference leaves the gate open.

- AC1: GIVEN scoped flag-ON and flag-OFF APC staging WHEN `UseThriftClient` is unset, `0`, or `1` and `TreatMetadataCatalogNameAsPattern=1` is varied THEN live routing proves flag default, forced Thrift with flag ON, forced SEA with flag OFF, and explicit SEA precedence over catalog-pattern Thrift. Compare APC Thrift and warehouse SEA; fixtures cover Arrow-unavailable, legacy `cliservice`, lookup failure, and partner exclusion. Selected SEA errors never fall back.
- AC2: GIVEN flag-on APC staging WHEN a cluster-path connection opens, runs a primitive single-chunk SELECT, and closes THEN CreateSession carries that cluster ID without `warehouse_id`, rows/order/types match APC Thrift and warehouse SEA, and targeted TerminateSession succeeds after its server fix. Fixtures cover absent `arrow_schema` and unknown R16 wire formats without false success.
- AC3: GIVEN each new APC R2 operation WHEN it runs, including fetch/cancel/close after a flag change THEN R2 target/body placement and R3 no-status behavior hold, the original statement target is used, running statements close once, and the connection remains usable; warehouse serialization is unchanged.
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
- AC16: GIVEN each synchronous APC SHOW when introduced in emitted Arrow IPC or columnar format WHEN read on live staging THEN JDBC columns, values, nulls, and errors match the applicable baseline and R5. HIVE fixtures verify R13's least-significant-bit mask order, placeholder consumption, short-mask non-null rows, and ignored trailing bits; fixture-only formats remain pending live proof.
- AC17: GIVEN flag-on live V10 APC and paired APC Thrift/warehouse SEA routes WHEN a native prepared INSERT batch succeeds or fails through either batch API THEN ordered `parameter_sets` yield one `SUCCESS_NO_INFO` per successful input, never a copied aggregate; an execution failure yields a `BatchUpdateException` with `EXECUTE_FAILED` for every set and uncertain commit status. To induce a count-read failure after a real successful write, replace only its terminal count response through a controlled staging fault hook, then verify committed rows and that JDBC raises the distinct result error without failed-write counts, warns that rows may be committed, and clears the batch. Record the hook, request/response boundary, rows, and error identity. If response mutation is unavailable, a deterministic fixture closes only this post-write failure branch; live V10 success, execution failure, and paired load remain required.
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
- AC25: GIVEN new APC and warehouse SEA/Thrift IDs, the original workspace endpoint, and usable credentials WHEN public `getConnectionId()` supplies an ID to `Driver.closeConnection` after a flag or compute-target change THEN the ID works without a Thrift handle and closes only the original session. Legacy IDs still decode, malformed IDs fail without logging IDs, statement IDs stay raw, and the lifecycle/error identity matches both baselines. Prechange warehouse Thrift IDs may require reopen; endpoint or credential changes are outside this guarantee.
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
fails before success. Paired one/32-caller create/drop on isolated schemas passes the shared command load gate.
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
- AC45: GIVEN APC results WHEN `EnableSeaResponseCompression` changes THEN `0` sends `Accept-Encoding: identity`, `1` permits API compression, and neither alters presigned links; compare API bytes and latency with both baselines.
- AC46: GIVEN paired linked results at one and 32 callers WHEN `cloudFetchThreadPoolSize`, `LinkPrefetchWindow`, chunk wait, and slow-link settings vary independently THEN active downloads and buffered chunks stay within the per-result worker limit, fetched-ahead links within the window, and the aggregate limits scale by caller count. Rows, queue depth, timeouts, warnings, duration, and AC31 load bounds match the baselines; inline pages are unaffected.
- AC48: GIVEN live APC counted `INSERT`, `UPDATE`, `DELETE`, or `MERGE` WHEN public
update methods run THEN counts, including zero, match APC Thrift after T32. A missing
`num_affected_rows` column, row, or value produces a contract error, not successful zero.
Paired one/32-caller writes on disjoint keys pass the shared command load gate.
- AC49: GIVEN each live SELECT mode at its first exposure WHEN
`setMaxRows(0)`, `1`, and boundaries applicable to that mode run THEN positive limits send SEA
`row_limit`, zero is unlimited, and rows never exceed the limit. Compare rows and request
count with APC Thrift and warehouse SEA; fixtures cover unavailable boundaries.
- AC50: GIVEN each typed SHOW handle when introduced on live V9+ APC WHEN it submits a delayed operation THEN it returns while RUNNING and another call completes before await. Awaited rows/errors, timeout, cancel, and cleanup match synchronous APC and both baselines for that operation; older/versionless handles complete synchronously. Procedure SELECTs reject async before request. Cross-reference conversions are gated when their handles are introduced.
- AC51: GIVEN isolated 429/503 responses with and without `Retry-After` injected by a controlled staging fault hook for each newly exposed SEA API or presigned-link download class WHEN R17 retry settings apply THEN recorded attempts, delay, exception, SQLState, and vendor code match warehouse SEA without write replay. Cover exhausted CreateSession and verify Thrift HTTP settings affect links, not SDK API requests. If a class cannot be faulted on staging, a deterministic fixture closes only its injected retry/exhaustion branches; ordinary flag-on execution, paired load, and transport-scope behavior for that class still require live proof. Record the hook or fixture and affected class.
- AC52: GIVEN scoped flag-on staging and a paired APC Thrift or warehouse SEA route WHEN public JDBC runs no-result DDL and counted DML through both direct `ExecuteStatement` completion and polled `GetStatementResult` completion THEN live captures show successful DDL without a result schema and successful DML with the affected-row schema and value, including zero; public AC33/AC48 outcomes match the paired route and failed commands retain errors. Fixtures alone cannot close either terminal path.
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
- AC84: GIVEN each newly exposed live request class WHEN timeout, poll, and idle-expiry
settings vary THEN `MetadataOperationTimeout` bounds the overall SHOW wait and targets
cancel on expiry; `0` disables that deadline. `socketTimeout` bounds one SDK API or
linked request, `asyncexecpollinterval` changes delays after the first poll, and
`IdleHttpConnectionExpiry` evicts idle SDK connections only. A valid in-budget call
completes; timeout identity, timing, request counts, and usability match the applicable
APC Thrift and warehouse SEA baselines without protocol fallback.
- AC85: GIVEN each live execution, metadata, download, batch, and failure path WHEN
telemetry exports THEN R8's redacted event families, operation/format/counts, and error
identity match both baselines or document a measured exception.
- AC86: GIVEN live APC with `EnableSQLValidationForIsValid=1` WHEN
`isValid(timeout)` runs its `SELECT VERSION()` check THEN healthy/failed results, timeout,
and connection usability match APC Thrift and warehouse SEA.
- AC87: GIVEN SQL at 16,777,215/16,777,216/16,777,217 UTF-8 bytes WHEN an exposed
single statement or legacy INSERT chunk executes THEN APC SEA accepts up to the SEA limit
and propagates the warehouse SEA oversize error above it without fallback. Record bytes,
counts, committed rows, and APC Thrift differences; `BatchInsertSize` can reduce
interpolated legacy chunks, while native-scalar chunks follow the parameter limit.
- AC88: GIVEN two accessible catalogs and a metric view WHEN `enableMultipleCatalogSupport` and `EnableMetricViewMetadata` vary on all three live routes THEN catalog access, discovery, table types/rows, and APC session setting match R5; record any server-limited row difference and caller impact.
- AC89: GIVEN live invalid SQL, missing-object, and permission failures WHEN applicable execute
methods surface them THEN record SQLException class, SQLState, vendor code, and message category
across APC SEA/Thrift and warehouse SEA. APC SEA matches agreement between baselines, otherwise
measured warehouse SEA mapping with the caller impact; no failed command reports success.
- AC92: GIVEN live APC SELECT with a manifest-only initial response WHEN JDBC fetches the
first chunk THEN it sends a bodyless APC-targeted GetResultData, returns every row once,
and reports a failed fetch before false ResultSet success.
- AC101: GIVEN each APC inline or linked mode when introduced WHEN result-codec settings select `NONE` or `LZ4_FRAME` THEN APC decodes the manifest's actual codec, including unmarked data and LZ4 despite requested `NONE`, and returns baseline rows; invalid text retains R4's default. Record requested/actual codecs and the shared load-gate impact; a selectable APC codec requires a server change.
- AC102: GIVEN an isolated write table WHEN transaction and auto-commit settings vary on all three live routes THEN state, rows, no-ops, and errors match R19, including missing state-row and failed-command behavior; record cause, SQLState/vendor code, and usability.
- AC105: GIVEN live null-catalog `getTables` that receives `42601` WHEN each exposed
synchronous call or V9+ typed async handle is compared across APC SEA, APC Thrift, and
warehouse SEA THEN APC SEA and warehouse SEA return the same typed empty table result
without catalog fan-out; record any APC Thrift rows omitted and the caller impact.
- AC106: GIVEN a changed driver-owned APC SEA error WHEN it reaches JDBC THEN a test asserts its `DatabricksDriverErrorCode` name and unique number, and the PR records owner-verified telemetry taxonomy/dashboard updates or maintainer handoff and completes the telemetry-error template. Server errors retain server identity.
- AC107: GIVEN one and 32 concurrent callers on paired APC SEA and APC Thrift staging
WHEN each repeatedly opens, executes, cancels, and closes a statement/session, including
saved-ID close after a flag change, THEN no live session or statement remains; server
active counts and client handles/threads return to baseline, retained heap growth is at
most 8 MiB, and median SEA/Thrift cycle latency is at most 1.10. Record cleanup
delay or exceptions with caller impact and PECO/Perf Eng approval.
- AC108: GIVEN flag-on live APC legacy prepared INSERT batches with `EnableBatchedInserts=1` and paired APC Thrift/warehouse SEA WHEN `BatchInsertSize` and the 256-parameter limit vary THEN interpolated chunks change only with `BatchInsertSize`, while native-scalar chunks change only at the parameter limit; eligible sets combine into bounded request groups and ineligible sets remain individual. Record binding mode, negotiated version, request groups, committed rows, ordered counts, errors, and throughput against each baseline. With batching disabled, eligible sets no longer combine. No generated chunk exceeds the SEA SQL-text limit.
- AC109: GIVEN concurrent flag-on live APC multi-page inline SELECTs and paired APC Thrift/warehouse SEA WHEN `ThriftMaxBatchesInMemory` varies below two and above two THEN the effective per-result cap is at least two and no greater than the configured value above two, peak buffered batches and heap stay within that cap, all rows arrive once, and AC31 load bounds hold.
- AC111: GIVEN paired one- and 32-caller APC SEA/Thrift and warehouse SEA/Thrift inline SELECTs WHEN `EnableInlineStreaming` varies at `0` and `1` at each applicable SEA mode THEN rows and page counts are unchanged, each SEA route's provider selection is unchanged by this switch, and APC SEA stays within the same R17 buffered-batch cap. Measure APC Thrift's lazy/streaming effect; record first-row latency, peak and retained heap, protocol deltas, caller impact at `0`, and the shared AC31 load disposition. A non-functional exception requires PECO/Perf Eng approval.

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

`plan.json` assigns every task to one PR; each JDBC package branches from its single
predecessor and proves a public outcome on scoped flag-on staging before approval. This
linear ancestry lets the release package inherit every reviewed slice. The extra packages
beyond the 4–7 target keep distinct public outcomes and server gates in reviewable diffs
under about 1,500 changed lines: inline pages, links, SQL async, prepared execution,
commands, batch modes, procedures, typed metadata, inline-only results, and release evidence
have separate approval gates.
Connection work includes SDK retry, pool, and timeout evidence at CreateSession's first
exposure. The first metadata SHOW slice owns shared headers, result formats, decoding, and
completion behavior before relations and async handles use them. Procedure SELECTs precede
server-gated metadata SHOW so their approval remains independent. Each package
checks its newly exposed request classes against AC3/11/40/51/84/85, AC30 for a new pool,
and AC10 for a new DEBUG path. The final package alone owns docs/changelog and validates
the assembled stack. Server flag/partner and termination fixes gate their live checks;
T32 gates DDL/DML and dependent batches, while SC-254478 gates only inline-only and release.
Open live or performance gates block their own package and descendants, not earlier PRs.

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

Each task ID has one owner in `plan.json`; R/AC define its observable contract.
The groups below follow the stacked PR order.

- **Server prerequisites:** T17 default-off APC flag; T35 partner exclusion;
  T32 direct and polled DDL/DML terminal results.
- **Connection:** T1 APC target; T2 session open/terminate; T3 redacted route diagnostics;
  T4 explicit override/Arrow prerequisite; T6 flag/catalog preference/no fallback;
  T19 protocol and target in IDs; T29 saved-ID close; T44 legacy/raw ID compatibility;
  T26 SDK retry, pool, and socket scopes.
- **First SELECT:** T7 result polling without status; T8 Arrow/hybrid request;
  T10 primitive public rows; T12 direct-result/query timeout; T27 cancel/close;
  T45 R2 request targeting; T47 inline schema/batch; T57 operation telemetry;
  T61 manifest codec; T59 row limit; T63 SQL-backed isValid.
- **Inline SELECT:** T11 inline continuation; T25 manifest-only first fetch;
  T34 row offsets; T62 multiple Arrow batches.
- **Linked SELECT:** T15 redacted linked diagnostics; T20 logical getters;
  T33 linked IPC; T36 final-link paging; T46 link resource bounds.
- **SQL async:** T39 public SQL async.
- **Prepared SELECT:** T5 negotiated capability; T9 prepared scalars;
  T22 coded complex-binding rejection.
- **Procedures:** T42 parameterized header-free procedure queries;
  T56 typed-empty failure behavior.
- **Commands and transactions:** T28 no-result DDL; T31 required result schema;
  T38 counted DML; T60 transactions.
- **Batch modes:** T21 legacy prepared batches; T64 V10 native prepared batches;
  T30 Statement batches. Each mode has its own public PR and live gate.
- **Discovery foundation:** T14 catalog/schema SHOW; T43 catalog filtering/fan-out;
  T48 table SHOW; T49 column SHOW. This slice also owns shared metadata request,
  decoding, and synchronous completion support used by the following slices.
- **Relations:** T13 function SHOW; T37 primary-key SHOW; T54 imported/cross-reference keys;
  T55 exported-key typed empty and null-table error.
- **Typed async metadata:** T24 submit/await/cancel and T23 catalog handle first;
  T40 schema handle; T50 table handle; T51 column handle; T41 function handle;
  T52 primary-key handle; T53 cross-reference handle next.
- **Inline-only SELECT:** T58 download-off inline SELECT.
- **Release:** T16 docs/changelog; T18 assembled live evidence;
  T-val integrated and post-merge gate.
