# APC SEA live staging run, 2026-11-19

Sanitized evidence for ACs run on a real APC against the approved staging profile. Built from the APC branch `jdbc-core` classes. No credential, raw session ID, statement ID, or cluster ID is recorded.

**Skip policy:** a live run may be marked skipped only when it is blocked by an SC ticket filed under epic [SC-256186](https://databricks.atlassian.net/browse/SC-256186). Soft skips (missing harness, warehouse not chosen, etc.) are not allowed.

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
| AC21 | **PARTIAL** | Explicit `UseThriftClient=0` with `TreatMetadataCatalogNameAsPattern=1` still selected `SEA_APC` and ran `SELECT 1`. Warning-log capture and the flag-ON half were not run (staging flag OFF; not an SC skip). |
| AC22 | **FAIL** | Statement-batch path could not be completed. A single `CREATE OR REPLACE TEMPORARY VIEW ... AS SELECT 1` fails on APC with `APC result manifest has no schema`; the same statement succeeds over Thrift (`UseThriftClient=1`). `SET` batches are also unusable because `SET` returns a ResultSet. |
| AC24 | **PASS** | `EnableHeartbeat=1`, `HeartbeatIntervalSeconds=10`. After a large open ResultSet, 35s idle (three intervals) produced zero statement GET or `/status` requests. |
| AC25 | **PASS** | Connection id was APC compound form (`s|a|…`). ResultSet statement id was the raw server id. Recovered-statement cancel and close each sent a request carrying `backend_compute` in the body (finished-statement cancel/close were rejected by the server with HTTP 400; request shape still correct). Public `Driver.closeConnection(url, info, compoundId)` terminated the session; the next query on that connection failed. |
| AC29 | **PASS** | `setObject(..., Types.ARRAY)` and `setObject(..., Types.STRUCT)` each raised `UNSUPPORTED_APC_PARAMETER` (vendor code `1045`) with **zero** ExecuteStatement requests beforehand. A subsequent scalar `INTEGER` bind executed successfully. Negotiated protocol version was not independently asserted. |

## Skipped (SC-gated only)

| AC | Skip ticket | Ticket summary | Status |
| --- | --- | --- | --- |
| AC5 (live metadata dogfood) | [SC-256188](https://databricks.atlassian.net/browse/SC-256188) | `[APC][Reyder] Accept ARROW_STREAM metadata results and set is_native_metadata_result` | TO DO |
| AC16 (live metadata / columnar metadata response shapes) | [SC-256188](https://databricks.atlassian.net/browse/SC-256188) | same as AC5 | TO DO |
| AC8 (server-dependent inline-only large paging with `can_cloud_download=false`) | [SC-258787](https://databricks.atlassian.net/browse/SC-258787) | `[APC][Reyder] Honor CreateSession can_cloud_download=false: APC execute still returns CloudFetch external links` | TO DO |

Unaffected AC8 route evidence (SEA selected, connection-context cloud fetch disabled, `SELECT 1`) already passed live on 2026-11-18 and is not skipped.

## Finding: APC DDL without a result schema

Control comparison for the AC22 failure:

| Route | Statement | Outcome |
| --- | --- | --- |
| APC SEA (`UseThriftClient=0`) | `CREATE OR REPLACE TEMPORARY VIEW jay_apc_ddl_control AS SELECT 1 AS v` | Fail: `APC result manifest has no schema` |
| Thrift (`UseThriftClient=1`) | Same statement | Pass |

This blocks using DDL (and `SET`) as the live AC22 batch vehicle. It is also relevant to the “manifest without result data” half of AC15: empty SELECT results still passed, but DDL responses without a schema fail on APC.

## Runner

Ad-hoc runner used for this pass (not committed as a packaged test): Java 11, classpath = `jdbc-core/target/classes` plus `mvn -pl jdbc-core dependency:build-classpath`, credentials from the Databricks CLI profile via env (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`). Token and identifiers were never written to this report.
