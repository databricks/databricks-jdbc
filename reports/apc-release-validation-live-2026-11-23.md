# APC SEA live staging matrix, AC1–AC29 (2026-11-23)

Sanitized live evidence for the cumulative JDBC stack through PR #1835. No credential, raw
session ID, statement ID, cluster ID, or workspace ID is recorded.

**Skip policy:** a live run may be marked **SKIP** only when blocked by an SC ticket under
[SC-256186](https://databricks.atlassian.net/browse/SC-256186). Other gaps are **PASS**,
**PARTIAL**, **FAIL**, or **BLOCKED**.

## Target

| Field | Value |
| --- | --- |
| Profile | `benchmarking-staging-aws-us-west-2` |
| Region | AWS us-west-2 staging |
| Runtime | `19.x-photon-scala2.13` |
| APC cluster | `RUNNING` |
| Warehouse | Existing accessible `RUNNING` warehouse; `SELECT 1` only |
| Driver under test | `854809f5caee6ff05d20213016dcc2fb6c3f4b8e` |

## AC1–AC29 matrix

| AC | Live outcome | Evidence / gate |
| --- | --- | --- |
| AC1 | **PASS** | Staging flag-absent no-override routed Thrift; dogfood flag-ON no-override routed SEA APC. Both passed `SELECT 1`. |
| AC2 | **PASS** | Explicit SEA opened an APC session and `SELECT 1` passed; Thrift control passed. |
| AC3 | **FAIL** | SEA execute/fetch, timeout, and heartbeat probes passed, but `Connection.getConnectionId()` threw `NullPointerException`; this prevents the required compound-ID close lifecycle. Thrift returned a valid connection ID. |
| AC4 | **PASS** | Ten JDBC values and classes matched Thrift exactly. |
| AC5 | **BLOCKED** | `getCatalogs`/metadata probe reached both SEA and Thrift but both returned SQLSTATE `42501` (insufficient privilege) before result-format validation. |
| AC6 | **PASS** | Prepared scalar BIGINT and STRING binds returned expected values on SEA and Thrift. |
| AC7 | **BLOCKED** | Arrow-unavailable routing requires an AIX/PowerPC host and cannot be injected from this client. |
| AC8 | **PARTIAL** | With cloud download disabled, `UseBoundedSeaApi=0/1` selected SEA with `cloud_fetch=false` and bounded mode false/true respectively; `SELECT 1` passed. Inline-only multi-page validation remains gated by SC-258787. |
| AC9 | **BLOCKED** | Server APC-unsupported/KP001 responses cannot be injected from this client. |
| AC10 | **FAIL** | INFO telemetry output did not contain the token, workspace, or cluster ID, and connection/context strings were clean; however, `DatabricksSession.toString()` contained the raw workspace ID. |
| AC11 | **PARTIAL** | Existing running warehouse selected `DatabricksSdkClient` and passed `SELECT 1`; serialized wire-fixture parity is not observable in this live probe. |
| AC12 | **PASS** | SEA and Thrift each returned 100,000 ordered rows with identical checksum when direct results were disabled. |
| AC13 | **PASS** | SEA and Thrift returned identical type codes and classes for BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, and TIMESTAMP. |
| AC14 | **PASS** | SEA and Thrift each returned 100,000 ordered rows with identical checksum, with direct results both enabled and disabled. |
| AC15 | **PASS** | One-row and empty-result schema passed; `SET spark.sql.ansi.enabled` returned a two-column row on both SEA and Thrift. |
| AC16 | **BLOCKED** | Both routes returned the same SQLSTATE `42501`; this principal cannot validate metadata result-format parity. |
| AC17 | **PASS** | `EnableNativeBatching=1` inserted three parameter sets, returned three count entries, and persisted three rows. Scratch schema cleanup passed. |
| AC18 | **PASS** | `setQueryTimeout(1)` raised `DatabricksTimeoutException` on both routes. SEA reported `STATEMENT_EXECUTION_TIMEOUT`; Thrift reported `OPERATION_TIMEOUT_ERROR`. |
| AC19 | **PASS** | On staging and flag-ON dogfood, `UseThriftClient=0` selected `DatabricksSdkClient` and `UseThriftClient=1` selected `DatabricksThriftServiceClient`; all probes passed `SELECT 1`. |
| AC20 | **PARTIAL** | Flag-OFF staging warehouse remained SEA and passed `SELECT 1`; the dogfood validation target was an APC cluster, so a flag-ON warehouse comparison was not run. |
| AC21 | **PASS** | On flag-ON dogfood, `TreatMetadataCatalogNameAsPattern=1` selected Thrift while default selected SEA. On staging, explicit `UseThriftClient=0` plus pattern selected SEA. All probes passed `SELECT 1`. |
| AC22 | **PASS** (statement batch) | The first statement committed, the intentionally invalid second statement failed, and the third did not execute. The SC-258842 DDL vehicle remains separately gated. |
| AC23 | **BLOCKED** | A CreateSession response without `negotiated_protocol_version` cannot be injected from this client. |
| AC24 | **PASS** | With heartbeat enabled at a 10-second interval, a completed APC result remained open for 35 seconds and emitted zero statement GET/status requests. |
| AC25 | **FAIL** | APC SEA `getConnectionId()` fails before producing the required round-trippable compound session ID, so `Driver.closeConnection` cannot exercise the required APC compute target. |
| AC26 | **PARTIAL** | Warehouse SEA open and `SELECT 1` passed; format serialization/deserialization remains covered outside this live probe. |
| AC27 | **PASS** | Sustained live query produced seven RUNNING polls and one terminal poll; every RUNNING response carried zero result-payload bytes. All 100,000 rows were ordered with checksum `4,999,950,000`. |
| AC28 | **PASS** | Live SAFE config is default OFF with a dogfood override ON. Driver evaluation selected Thrift by default on staging and SEA APC by default on dogfood. |
| AC29 | **FAIL** | Scalar binds passed, but an ARRAY supplied through `setObject` produced generic `DatabricksSQLFeatureNotSupportedException` / `UNSUPPORTED_OPERATION` / vendor code 0 instead of `UNSUPPORTED_APC_PARAMETER` / code 1045. No query executed. |

## SC-gated skips

| AC | Ticket | Summary | Status |
| --- | --- | --- | --- |
| AC5, AC16 | [SC-256188](https://databricks.atlassian.net/browse/SC-256188) | Reyder ARROW_STREAM metadata | Verify live |
| AC8 inline-only paging | [SC-258787](https://databricks.atlassian.net/browse/SC-258787) | Honor APC cloud-download setting | Verify live |
| AC22 DDL vehicle | [SC-258842](https://databricks.atlassian.net/browse/SC-258842) | APC DDL result manifest has no schema | Verify live |

## Finding: APC SEA connection ID construction

| Route | Operation | Outcome |
| --- | --- | --- |
| APC SEA | Open, then `SELECT 1` | Pass |
| APC SEA | `Connection.getConnectionId()` | Fail: `NullPointerException` in `SessionId.create` |
| APC Thrift | Same operations | Pass |

The SEA session ID is present, but `SessionId.create` treats every non-warehouse compute resource
as Thrift and dereferences `sessionHandle`. An APC SEA session has no Thrift session handle.

## Finding: APC session diagnostics expose workspace ID

| Surface | Token | Workspace ID | Cluster ID |
| --- | --- | --- | --- |
| INFO telemetry output | Not present | Not present | Not present |
| Connection/context `toString()` | Not present | Not present | Not present |
| Session `toString()` | Not present | **Present** | Not present |

This violates AC10's requirement that session stringification not expose the raw APC org/workspace ID.

## Finding: APC complex-parameter error contract

| Route | Operation | Outcome |
| --- | --- | --- |
| APC SEA | `PreparedStatement.setObject` with ARRAY | Rejected locally with `UNSUPPORTED_OPERATION`, vendor code 0 |
| APC SEA expected | Same | `UNSUPPORTED_APC_PARAMETER`, vendor code 1045, with no execute request |

The rejection is local and prevents execution, but it does not meet the APC-specific error contract.

## Counts

| Outcome | Count |
| --- | ---: |
| PASS | 16 |
| PARTIAL | 4 |
| FAIL | 4 |
| SKIP | 0 |
| BLOCKED | 5 |
| NOT RUN | 0 |

## Dogfood flag-ON run

| Mode | Option | Observed protocol | Route reason | `SELECT 1` |
| --- | --- | --- | --- | --- |
| flag-default | none | SEA_APC (`DatabricksSdkClient`) | Not exposed by current build | pass |
| thrift-override | `UseThriftClient=1` | THRIFT (`DatabricksThriftServiceClient`) | Not exposed by current build | pass |
| sea-override | `UseThriftClient=0` | SEA_APC (`DatabricksSdkClient`) | Not exposed by current build | pass |
| sea-pattern | `TreatMetadataCatalogNameAsPattern=1` | THRIFT (`DatabricksThriftServiceClient`) | Not exposed by current build | pass |

## Parity

| Probe | APC Thrift | APC SEA | Verdict |
| --- | --- | --- | --- |
| Routing and `SELECT 1` | Pass | Pass | Parity for query execution; SEA connection-ID construction fails separately |
| Query execution and paging | 100k rows pass, direct results on/off | 100k rows pass, direct results on/off | Ordered row count and checksum match |
| Types and values | Pass | Pass | Full parity for the ten-type live probe |
| Parameters and batches | Scalar pass | Scalar and native batch pass | Complex rejection occurs locally but returns the wrong error contract |
| Metadata | SQLSTATE `42501` | SQLSTATE `42501` | Same failure; insufficient privilege prevents format validation |
| Timeout and lifecycle | Timeout exception pass (`OPERATION_TIMEOUT_ERROR`) | Timeout exception pass (`STATEMENT_EXECUTION_TIMEOUT`) | Exception class parity; SQL state differs |
