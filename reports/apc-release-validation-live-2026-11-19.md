# APC SEA live staging matrix, AC1–AC29 (2026-11-18 / 2026-11-19)

Sanitized live evidence against the approved staging APC. Combines the 2026-11-18 dogfood/AC27 wire run with the 2026-11-19 follow-up and supplemental probes. No credential, raw session ID, statement ID, or cluster ID is recorded.

**Skip policy:** a live run may be marked **SKIP** only when blocked by an SC ticket filed under epic [SC-256186](https://databricks.atlassian.net/browse/SC-256186). Other gaps are **PASS** / **PARTIAL** / **FAIL** / **BLOCKED** (not skipped).

## Target

| Field | Value |
| --- | --- |
| Profile | `benchmarking-staging-aws-us-west-2` |
| Region | AWS us-west-2 staging |
| Runtime | `19.x-photon-scala2.13` |
| APC cluster | `RUNNING` (`UseThriftClient=0` forces SEA while SAFE flag is OFF) |
| Warehouse (AC11/AC20) | Existing `RUNNING` warehouse; `SELECT 1` only |
| Driver under test | Compiled APC-stack `jdbc-core` classes |

## AC1–AC29 matrix

| AC | Live outcome | Evidence / gate |
| --- | --- | --- |
| AC1 | **PASS** (flag-absent and flag-ON) | Flag-absent no-override → Thrift + `SELECT 1` (2026-11-18 / 2026-11-19). Flag-ON on dogfood (2026-11-19 evening): with `http.header.x-databricks-org-id=6051921418418893`, no-override routes `SEA_APC` / `apc_flag` + `SELECT 1` pass. Earlier afternoon FAIL was missing org-id on the shared dogfood host (feature-flags 403 → empty cache → false). See [dogfood flag-ON run](#dogfood-flag-on-run-2026-11-19). |
| AC2 | **PASS** | SEA APC open + `SELECT 1` (2026-11-18 dogfood). |
| AC3 | **PASS** | SEA execute/poll/fetch/close/timeout live (2026-11-18). `closeConnection(compound id)` terminated the session (2026-11-19). No SC skip. |
| AC4 | **PASS** | Ten live JDBC types matched Thrift; 100k SEA rows ordered + checksum (2026-11-18). Compressed/external-link shapes remain local. |
| AC5 | **SKIP** | [SC-256188](https://databricks.atlassian.net/browse/SC-256188) — Reyder ARROW_STREAM metadata / `is_native_metadata_result` (TO DO). Local/fake-service pass. |
| AC6 | **PASS** | Prepared scalar bind on SEA and Thrift (2026-11-18 / 2026-11-19). Below-V8 downgrade remains local. |
| AC7 | **BLOCKED** | Needs AIX/PowerPC with Arrow unavailable. No SC ticket; not available on this host. Local routing unit pass. |
| AC8 | **PARTIAL** + inline-only **SKIP** | Route/session live: `EnableQueryResultDownload=0` + `UseBoundedSeaApi=0/1` → SEA, cloud fetch disabled, `SELECT 1` (2026-11-18). Inline-only large paging **SKIP** → [SC-258787](https://databricks.atlassian.net/browse/SC-258787) (TO DO). |
| AC9 | **BLOCKED** | Needs server APC-unsupported / KP001 responses; not injectable from client. No SC ticket. Local open-outcome unit pass. |
| AC10 | **PARTIAL** | SEA open with `TelemetryLogLevel=INFO`; connection/session strings have no pwd/token (2026-11-19). Routing-event export sink not captured. Local telemetry unit pass. |
| AC11 | **PARTIAL** | Warehouse live `SEA_WAREHOUSE` + `SELECT 1` (2026-11-19). Request-body byte fixtures remain local. |
| AC12 | **PASS** | Direct-results-disabled SEA large query passed (2026-11-18). Request shapes remain local. |
| AC13 | **PASS** | Live 10-column type codes/value classes matched Thrift (2026-11-18). Full 21-column matrix local. |
| AC14 | **PASS** | Ordered 100k-row SEA result (2026-11-18). Multi-page cursor wire local. |
| AC15 | **PASS** | One-row, empty-result (schema retained), and `SET` result (2026-11-19). |
| AC16 | **SKIP** | Same gate as AC5: [SC-256188](https://databricks.atlassian.net/browse/SC-256188). Local columnar/metadata matrix pass. |
| AC17 | **PASS** | Thrift-created scratch table under writable UC schema; APC `EnableNativeBatching=1` 3-row parameterized INSERT batch + count check (2026-11-19 follow-up). Schema dropped after run. |
| AC18 | **PASS** | Live SEA `setQueryTimeout(1)` → `DatabricksTimeoutException` / `STATEMENT_EXECUTION_TIMEOUT` (2026-11-18). |
| AC19 | **PASS** (overrides and flag-ON) | Live `UseThriftClient=0/1` overrides (2026-11-18). Dogfood flag-ON (2026-11-19 evening, with org-id header): `UseThriftClient=1` → Thrift `explicit_thrift` + `SELECT 1` pass; `UseThriftClient=0` → `SEA_APC` `explicit_sea` + `SELECT 1` pass. Afternoon 403 was the same missing org-id issue as AC1. |
| AC20 | **PARTIAL** | Flag-OFF warehouse live `SEA_WAREHOUSE` + `SELECT 1` (2026-11-19). Flag-ON warehouse comparison not run on dogfood. |
| AC21 | **PASS** | Explicit `UseThriftClient=0` + `TreatMetadataCatalogNameAsPattern=1` selected SEA + `SELECT 1` (2026-11-19). Dogfood flag-ON pattern-only (`TreatMetadataCatalogNameAsPattern=1`) routes Thrift `catalog_pattern` + `SELECT 1` (pattern wins over flag); flag-default SEA half observed under AC1. |
| AC22 | **PASS** (INSERT) / **SKIP** (DDL vehicle) | INSERT statement batch success + stop-on-failure on Thrift-created scratch table (2026-11-19 follow-up). DDL/`SET` vehicle **SKIP** → [SC-258842](https://databricks.atlassian.net/browse/SC-258842): APC `CREATE TEMPORARY VIEW` → `APC result manifest has no schema` (Thrift control OK). |
| AC23 | **BLOCKED** | Needs CreateSession without `negotiated_protocol_version`; not injectable from client. No SC ticket. Local missing-version fake-service pass. |
| AC24 | **PASS** | Heartbeat on, 35s idle (3×10s), zero statement GET/`/status` (2026-11-19). |
| AC25 | **PASS** | Compound connection id, raw statement id, recovered cancel/close carry `backend_compute`, `closeConnection` terminates session (2026-11-19). |
| AC26 | **PARTIAL** | Warehouse live open covered under AC11. Format enum / fixture regression remains local (by design for wire fixtures). |
| AC27 | **PASS** | Four RUNNING polls, zero result-payload bytes; 100k ordered rows ([wire JSON](apc-release-validation-ac27-wire.json), 2026-11-18). |
| AC28 | **PASS** | Live SAFE config returns `enableSqlExecForJdbcApc` (default OFF; `override-testing` ON for dogfood `6051921418418893@aws@staging` and eng-nephos-e2). Connector-service feature-flags returns `true` for dogfood. Driver flag-ON with org-id header routes `SEA_APC` / `apc_flag`. Without the header on dogfood SPOG, feature-flags 403 and the driver treats the flag as false — operational note for shared hosts, not a SAFE config failure. T17 definition review approved source default OFF. |
| AC29 | **PASS** | ARRAY/STRUCT rejected with `UNSUPPORTED_APC_PARAMETER` / code 1045 and zero ExecuteStatement requests; scalar bind OK (2026-11-19). |

## SC-gated skips only

| AC | Ticket | Summary | Status |
| --- | --- | --- | --- |
| AC5, AC16 | [SC-256188](https://databricks.atlassian.net/browse/SC-256188) | `[APC][Reyder] Accept ARROW_STREAM metadata results and set is_native_metadata_result` | TO DO |
| AC8 inline-only paging | [SC-258787](https://databricks.atlassian.net/browse/SC-258787) | `[APC][Reyder] Honor CreateSession can_cloud_download=false` | TO DO |
| AC22 DDL vehicle | [SC-258842](https://databricks.atlassian.net/browse/SC-258842) | `[APC][Reyder] DDL statements fail on APC SEA with 'APC result manifest has no schema'` | TO DO |

## Finding: APC DDL without a result schema (SC-258842)

Skipped for AC22's DDL vehicle; tracked by [SC-258842](https://databricks.atlassian.net/browse/SC-258842) under [SC-256186](https://databricks.atlassian.net/browse/SC-256186).

| Route | Statement | Outcome |
| --- | --- | --- |
| APC SEA (`UseThriftClient=0`) | `CREATE OR REPLACE TEMPORARY VIEW … AS SELECT 1` | Fail: `APC result manifest has no schema` |
| Thrift (`UseThriftClient=1`) | Same | Pass |

## Finding: scratch-table permissions (resolved for INSERT ACs)

| Route | Statement | Outcome |
| --- | --- | --- |
| APC SEA | `CREATE TABLE default.…` | Fail (redacted BAD_REQUEST / no-schema) |
| Thrift | `CREATE TABLE default.…` | Fail: `INSUFFICIENT_PERMISSIONS` / SQLState `42501` |
| Thrift | `CREATE TABLE` under temporary UC schema with `CREATE_SCHEMA` on `main` | Pass; used to unblock AC17 / INSERT-AC22, then schema dropped |

## Counts

| Outcome | ACs |
| --- | --- |
| PASS (live) | AC1 (absent + flag-ON), AC2, AC3, AC4, AC6, AC12, AC13, AC14, AC15, AC17, AC18, AC19, AC21, AC22 (INSERT path), AC24, AC25, AC27, AC28, AC29 |
| PARTIAL | AC8 (route), AC10, AC11, AC20, AC26 |
| FAIL (observed) | _(none)_ |
| SKIP (SC) | AC5, AC16, AC8 inline-only, AC22 DDL vehicle (SC tickets still TO DO on 2026-11-19) |
| BLOCKED (not SC) | AC7; AC9; AC23 |

## Dogfood flag-ON run (2026-11-19)

Run with the compiled uber jar (`databricks-jdbc-3.4.5.jar`) and [`ApcFlagOnRoutes`](validation/ApcFlagOnRoutes.java) against dogfood (`dogfood` profile, workspace `6051921418418893`). Evening re-run sets `DATABRICKS_ORG_ID` → JDBC `http.header.x-databricks-org-id` (required on dogfood SPOG for connector-service feature-flags and SEA CreateSession). Cluster IDs are not recorded. eng-nephos-e2 was attempted as an alternate flag-ON host but cluster bootstrap fails (`CONTROL_PLANE_REQUEST_FAILURE` / go/ingress/lockdown).

| Mode | Connection option | Observed protocol | Route reason | `SELECT 1` |
| --- | --- | --- | --- | --- |
| flag-default | none (+ org-id header) | SEA_APC (`DatabricksSdkClient`) | `apc_flag` | pass |
| thrift-override | `UseThriftClient=1` | THRIFT | `explicit_thrift` | pass |
| sea-override | `UseThriftClient=0` | SEA_APC | `explicit_sea` | pass |
| sea-pattern | `TreatMetadataCatalogNameAsPattern=1` | THRIFT | `catalog_pattern` | pass |

Findings:
- Root cause of the afternoon FAIL: dogfood is a shared host; without `X-Databricks-Org-Id`, feature-flags returns 403 and the driver cache is empty (`getBoolean` → false). With the header, `enableSqlExecForJdbcApc=true` and all four modes match expected routing.
- Driver does not yet inject org-id from APC `/o/<orgId>/…` httpPath for feature-flags; validation uses the explicit `http.header.x-databricks-org-id` workaround.
