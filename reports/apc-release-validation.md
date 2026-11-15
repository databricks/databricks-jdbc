# APC SEA release validation, round 1

Validation target: JDBC commit `07a50400` on `jay-xiao/factory/apc-sea/apc-release-validation`, 2026-11-15 UTC. This is evidence only; no driver behavior changed in this package.

## Local results

- `mvn -q spotless:apply`: passed; no tracked files changed.
- `mvn test -pl jdbc-core -am`: passed, 4,371 tests, zero failures, zero errors, 88 skipped across 226 suites. The committed [Surefire evidence](apc-release-validation-surefire.json) records the AC-specific test cases and suite totals.
- `mvn -q clean install -DskipTests -Ddependency-check.skip=true`: passed. The dependency-check skip avoids the external NVD wait observed in the parent package; it does not skip compilation or packaging.
- The APC HTTP fake-service suites ran within the `jdbc-core` test command: `DatabricksSdkClientTest`, `ApcParameterFakeServiceTest`, `DatabricksMetadataHttpMatrixTest`, `DatabricksMetadataHttpFakeTest`, and `ApcRunningPollFakeServiceTest`. Each reported zero failures and zero errors.
- The implementation parent's [reviewer](</home/jay.xiao/factory-work/apc-sea-jdbc/runs/apc-sea-jdbc__code_review__apc-routing-and-release-docs.reviewer-bot__r6/36ce40775ce7/result.json>), [spec](</home/jay.xiao/factory-work/apc-sea-jdbc/runs/apc-sea-jdbc__code_review__apc-routing-and-release-docs.spec-compliance__r6/8b6c23d4e2ac/result.json>), and [safety](</home/jay.xiao/factory-work/apc-sea-jdbc/runs/apc-sea-jdbc__code_review__apc-routing-and-release-docs.safety-regression__r6/0ddace172fb2/result.json>) reviews all approved. T17's [SAFE definition review](</home/jay.xiao/factory-work/apc-sea-jdbc/runs/apc-sea-jdbc__code_review__apc-safe-sub-flag.spec-compliance__r1/fd85da60a465/result.json>) approved the exact flag name, parent batch destination, and default OFF value.

Each local-pass row below links to its test source. The Surefire evidence gives the executed case names and outcomes. Local pass does not imply dogfood completion.

| AC | Local evidence | Release gate |
| --- | --- | --- |
| AC1 | Pass: [routing](../src/test/java/com/databricks/jdbc/api/impl/ApcRoutingTest.java), [open outcomes](../src/test/java/com/databricks/jdbc/api/impl/ConnectionRoutingOpenTest.java) | Flag OFF/ON dogfood pending. |
| AC2 | Pass: [session wire](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | APC dogfood pending. |
| AC3 | Pass: [request wire](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java), [saved ID](../src/test/java/com/databricks/jdbc/dbclient/impl/common/SessionIdTest.java) | Dogfood pending; TerminateSession server binding needs deployment evidence. |
| AC4 | Pass: [Arrow chunks](../src/test/java/com/databricks/jdbc/api/impl/arrow/ApcArrowBatchChunkProviderTest.java), [execute wire](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | APC dogfood pending. |
| AC5 | Pass: [metadata HTTP matrix](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksMetadataHttpMatrixTest.java), [async metadata](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksMetadataAsyncTest.java), [HTTP fake](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksMetadataHttpFakeTest.java) | Metadata server fix deployment and dogfood pending. |
| AC6 | Pass: [capability tests](../src/test/java/com/databricks/jdbc/api/impl/ApcParameterCapabilitiesTest.java), [captured requests](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/ApcParameterFakeServiceTest.java) | APC dogfood pending. |
| AC7 | Pass: [routing](../src/test/java/com/databricks/jdbc/api/impl/ApcRoutingTest.java) | Platform dogfood pending. |
| AC8 | Pass: [session config](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java), [inline paging/error](../src/test/java/com/databricks/jdbc/api/impl/arrow/ApcArrowBatchChunkProviderTest.java) | Inline-only server fix deployment and dogfood pending. |
| AC9 | Pass: [open outcomes](../src/test/java/com/databricks/jdbc/api/impl/ConnectionRoutingOpenTest.java), [SDK errors](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | APC dogfood pending. |
| AC10 | Pass: [routing telemetry](../src/test/java/com/databricks/jdbc/api/impl/ConnectionRoutingOpenTest.java), [redacted diagnostics](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | APC dogfood pending. |
| AC11 | Pass: [warehouse request fixtures](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | Warehouse serialization locally verified. |
| AC12 | Pass: [sync/async wait timeout wire](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | Reyder dogfood pending. |
| AC13 | Pass: [21-column APC/Thrift type matrix](../src/test/java/com/databricks/jdbc/api/impl/ApcResultTypeMatrixTest.java), [schema builder](../src/test/java/com/databricks/jdbc/api/impl/arrow/ArrowBatchSchemaBuilderTest.java) | No loss in pinned local matrix; live parity pending. |
| AC14 | Pass: [inline/external paging](../src/test/java/com/databricks/jdbc/api/impl/arrow/ApcArrowBatchChunkProviderTest.java) | APC dogfood pending. |
| AC15 | Pass: [initial result variants](../src/test/java/com/databricks/jdbc/api/impl/arrow/ApcArrowBatchChunkProviderTest.java) | APC dogfood pending. |
| AC16 | Pass: [columnar reader](../src/test/java/com/databricks/jdbc/api/impl/ColumnarDataResultTest.java), [metadata response matrix](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksMetadataHttpMatrixTest.java) | Metadata dogfood pending. |
| AC17 | Pass: [native/fallback batches](../src/test/java/com/databricks/jdbc/api/impl/ApcParameterCapabilitiesTest.java), [captured batch wire](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/ApcParameterFakeServiceTest.java) | APC dogfood pending. |
| AC18 | Pass: [timeout HTTP fake](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/ApcRunningPollFakeServiceTest.java), [cancel wire](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | APC dogfood pending. |
| AC19 | Pass: [routing overrides](../src/test/java/com/databricks/jdbc/api/impl/ApcRoutingTest.java) | Override dogfood attempted; connection open failed. |
| AC20 | Pass: [warehouse routing](../src/test/java/com/databricks/jdbc/api/impl/ApcRoutingTest.java) | Flag comparison dogfood pending. |
| AC21 | Pass: [metadata-pattern precedence](../src/test/java/com/databricks/jdbc/api/impl/ApcRoutingTest.java) | APC dogfood pending. |
| AC22 | Pass: [sequential statement batch](../src/test/java/com/databricks/jdbc/api/impl/ApcParameterCapabilitiesTest.java) | APC dogfood pending. |
| AC23 | Pass: [missing-version wire](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/ApcParameterFakeServiceTest.java), [capability tests](../src/test/java/com/databricks/jdbc/api/impl/ApcParameterCapabilitiesTest.java) | APC dogfood pending. |
| AC24 | Pass: [heartbeat eligibility](../src/test/java/com/databricks/jdbc/api/impl/ResultSetHeartbeatEligibilityTest.java), [no `/status`](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | APC dogfood pending. |
| AC25 | Pass: [session ID](../src/test/java/com/databricks/jdbc/dbclient/impl/common/SessionIdTest.java), [statement ID](../src/test/java/com/databricks/jdbc/dbclient/impl/common/StatementIdTest.java), [targeted recovery](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | APC dogfood pending. |
| AC26 | Pass: [format dispatch](../src/test/java/com/databricks/jdbc/api/impl/ExecutionResultFactoryTest.java), [warehouse fixtures](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java) | Warehouse regression locally verified. |
| AC27 | Pass: [three status-only RUNNING polls](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/ApcRunningPollFakeServiceTest.java) | **Measured dogfood poll count and payload bytes pending.** |
| AC28 | Pass: [T17 SAFE definition review](</home/jay.xiao/factory-work/apc-sea-jdbc/runs/apc-sea-jdbc__code_review__apc-safe-sub-flag.spec-compliance__r1/fd85da60a465/result.json>) | Live SAFE lookup returned `found=false`; no assignment can be validated yet. |
| AC29 | Pass: [complex parameter rejection](../src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/ApcParameterFakeServiceTest.java), [error code](../src/test/java/com/databricks/jdbc/api/impl/ApcParameterCapabilitiesTest.java) | APC dogfood pending. |

## Dogfood gate

The [sanitized dogfood attempt record](apc-release-validation-dogfood.json) contains no credentials or cluster identifiers. The environment-provided HTTP path has APC form. A temporary Java 11 JDBC smoke runner used that path and the provided credential with explicit `UseThriftClient=0` and `UseThriftClient=1`, each issuing only `SELECT 1 AS validation_value`. Both attempts failed during connection open with `DatabricksSQLException`, SQL state `CONNECTION_ERROR`, vendor code `0`; no query ran and no protocol was observed. The target's cluster runtime and region were not available from an open session. This failure is not evidence that either driver route is correct or incorrect.

Smoke commands: `mvn -q -pl jdbc-core dependency:build-classpath -Dmdep.outputFile=/tmp/apc-release-validation-classpath` passed; `javac -cp "$classpath" /tmp/ApcDogfoodSmoke.java` initially produced a class for the default newer JDK, and `java -cp "$classpath:/tmp" ApcDogfoodSmoke` failed with `UnsupportedClassVersionError` (class version 69 versus Java 11's 55). `javac --release 11 -cp "$classpath" /tmp/ApcDogfoodSmoke.java` passed, then `timeout 150s java -cp "$classpath:/tmp" ApcDogfoodSmoke` completed and recorded the two connection-open failures above. Here `classpath` was `$(cat /tmp/apc-release-validation-classpath):jdbc-core/target/classes`; the runner and raw logs stayed under `/tmp`.

| Configuration | Region | Runtime | Observed protocol | Result |
| --- | --- | --- | --- | --- |
| APC flag OFF/absent, no override | Unobserved | Unobserved | Unobserved | Pending prepared test-region assignment. |
| APC flag ON, no override | Unobserved | Unobserved | Unobserved | Pending live flag creation and assignment. |
| `UseThriftClient=0` | Unobserved | Unobserved | Unobserved | Connection open failed (`CONNECTION_ERROR`, vendor `0`). |
| `UseThriftClient=1` | Unobserved | Unobserved | Unobserved | Connection open failed (`CONNECTION_ERROR`, vendor `0`). |

Read-only SAFE `flags.get_config` for `databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc` returned `found=false` on 2026-11-15. T17's local definition and reviews are approved, but it is not present in live SAFE configuration. Deployment of the tracked SC fixes was not evidenced in the test region; [SC-256188](https://databricks.atlassian.net/browse/SC-256188) and [SC-254478](https://databricks.atlassian.net/browse/SC-254478) were still `TO DO` when read. Client behavior and local tests retain the full contract assuming those fixes.

AC27 live measurement: RUNNING poll count **not measured**; RUNNING result-payload bytes **not measured**; terminal row count **not measured**. Release readiness is blocked until the required APC dogfood matrix, server-dependent checks, and this measured polling gate pass.
