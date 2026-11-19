# APC SEA live staging run, 2026-11-19

Sanitized evidence for ACs that the plan treats as runnable on a real APC without the gated SC fixes or a flag-ON assignment. Built from the APC branch `jdbc-core` classes against the approved staging profile. No credential, raw session ID, statement ID, or cluster ID is recorded.

## Target

| Field | Value |
| --- | --- |
| Captured UTC | 2026-11-19T00:42Z–00:45Z |
| Profile | `benchmarking-staging-aws-us-west-2` |
| Region | AWS us-west-2 staging |
| Runtime | `19.x-photon-scala2.13` |
| Cluster state | `RUNNING` |
| Client path | Explicit `UseThriftClient=0` (staging SAFE lookup remains flag OFF / `found=false`) |
| Driver under test | Compiled `jdbc-core` classes from the integrated APC stack |

## Results

| AC | Outcome | Evidence |
| --- | --- | --- |
| AC15 | **PASS** | One-row `SELECT` matched; empty-result `SELECT` kept its schema; `SET` returned a result with columns. |
| AC21 | **PARTIAL** | Explicit `UseThriftClient=0` with `TreatMetadataCatalogNameAsPattern=1` still selected `SEA_APC` and ran `SELECT 1`. Warning-log capture and the flag-ON half were not run (staging flag OFF). |
| AC22 | **FAIL** | Statement-batch path could not be completed. A single `CREATE OR REPLACE TEMPORARY VIEW ... AS SELECT 1` fails on APC with `APC result manifest has no schema`; the same statement succeeds over Thrift (`UseThriftClient=1`). `SET` batches are also unusable because `SET` returns a ResultSet. |
| AC24 | **PASS** | `EnableHeartbeat=1`, `HeartbeatIntervalSeconds=10`. After a large open ResultSet, 35s idle (three intervals) produced zero statement GET or `/status` requests. |
| AC25 | **PASS** | Connection id was APC compound form (`s|a|…`). ResultSet statement id was the raw server id. Recovered-statement cancel and close each sent a request carrying `backend_compute` in the body (finished-statement cancel/close were rejected by the server with HTTP 400; request shape still correct). Public `Driver.closeConnection(url, info, compoundId)` terminated the session; the next query on that connection failed. TerminateSession request body was not intercepted (client is internal to `Driver`). |
| AC29 | **PASS** | `setObject(..., Types.ARRAY)` and `setObject(..., Types.STRUCT)` each raised `UNSUPPORTED_APC_PARAMETER` (vendor code `1045`) with **zero** ExecuteStatement requests beforehand. A subsequent scalar `INTEGER` bind executed successfully. Negotiated protocol version was not independently asserted. |

## Not run in this pass

| AC | Reason |
| --- | --- |
| AC10 | Needs captured telemetry export; not instrumented in this runner. |
| AC11 / AC26 | Warehouse path / fake-service serialization; no warehouse in this run. |
| AC17 | Needs a writable scratch table for parameterized INSERT / native batch; not created. |
| AC1 flag-ON, AC19/AC20 flag comparison, AC28 live | Staging remains flag OFF; dogfood ON assignment still pending SAFE approval. |
| AC5, AC8 inline-only paging, AC3 TerminateSession server binding | Still gated on tracked SC deployments. |
| AC7 / AC9 / AC23 | Need AIX/Arrow-unavailable, server-forced KP001/unsupported errors, or a CreateSession missing `negotiated_protocol_version`. |

## Finding: APC DDL without a result schema

Control comparison for the AC22 failure:

| Route | Statement | Outcome |
| --- | --- | --- |
| APC SEA (`UseThriftClient=0`) | `CREATE OR REPLACE TEMPORARY VIEW jay_apc_ddl_control AS SELECT 1 AS v` | Fail: `APC result manifest has no schema` |
| Thrift (`UseThriftClient=1`) | Same statement | Pass |

This blocks using DDL (and `SET`) as the live AC22 batch vehicle. It is also relevant to the “manifest without result data” half of AC15: empty SELECT results still passed, but DDL responses without a schema fail on APC.

## Runner

Ad-hoc runner used for this pass (not committed as a packaged test): Java 11, classpath = `jdbc-core/target/classes` plus `mvn -pl jdbc-core dependency:build-classpath`, credentials from the Databricks CLI profile via env (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`). Token and identifiers were never written to this report.
