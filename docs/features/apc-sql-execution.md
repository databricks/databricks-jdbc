# SQL Execution API for all-purpose clusters

The JDBC driver can run statements on an all-purpose cluster through the SQL Execution API
(SEA). This route is controlled by the `enableSqlExecForJdbcApc` client-config flag. It is
separate from the warehouse flag, `enableSqlExecForJdbc`, and defaults to Thrift when the APC
flag is off, absent, or unavailable. The flag is managed by Databricks; it is not a JDBC URL
property.

Use a cluster HTTP path containing concrete org and cluster IDs, such as
`/sql/protocolv1/o/<org-id>/<cluster-id>`. The legacy `cliservice` path always uses Thrift.
Warehouse paths continue to use warehouse routing and the warehouse flag.

For a cluster path, routing follows this order:

1. If Arrow is unavailable (including the default on AIX and PowerPC), use Thrift.
2. An explicit `UseThriftClient=1` selects Thrift; `UseThriftClient=0` selects SEA.
3. Without an explicit protocol setting, `TreatMetadataCatalogNameAsPattern=1` selects Thrift
   to preserve its native metadata behavior.
4. Otherwise, the APC flag selects SEA when on and Thrift when off or unavailable.

`UseThriftClient=0` takes precedence over `TreatMetadataCatalogNameAsPattern=1`; the driver
logs that the latter setting has no effect on SEA. `UseBoundedSeaApi` and
`EnableQueryResultDownload` do not select the APC protocol. With
`EnableQueryResultDownload=0`, SEA requests inline results and retains SEA routing.

APC SEA uses Arrow batches for query results. The driver targets every session and statement
request to the cluster. `setQueryTimeout` cancels a timed-out statement and raises the usual
timeout exception. Result-set heartbeats are unsupported for APC SEA, including when
`EnableHeartbeat=1`, because the server does not support status polling for this route.
Opening an APC SEA session fails directly if the server rejects CreateSession; the driver does
not retry on Thrift. Server failures retain the HTTP status and server error code. The driver
redacts server message text that may echo cluster IDs, SQL, or bound values; recognized
unsupported-cluster and unsupported-region errors keep a clear category in the message.
Connection-open diagnostics log compute type, protocol, routing reason, and outcome without
cluster, org, or session IDs. The driver does not export routing telemetry because the backend
`OssSqlDriverTelemetryLog` schema has no routing field. APC statement IDs
use the raw server statement ID in statement ID fields in logs and operation telemetry. The
internal compound statement ID, org ID, and cluster ID are not logged. Server error text that
may echo SQL or bound values is redacted before the SDK retries a request.
