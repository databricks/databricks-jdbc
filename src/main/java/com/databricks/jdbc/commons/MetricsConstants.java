package com.databricks.jdbc.commons;

import java.util.concurrent.TimeUnit;

public class MetricsConstants {
  public static final String METRICS_MAP_STRING = "metrics_map";
  public static final String METRICS_TYPE = "metrics_type";
  public static final String METRICS_URL =
      "https://aa87314c1e33d4c1f91a919f8cf9c4ba-387609431.us-west-2.elb.amazonaws.com:443/api/2.0/oss-sql-driver-telemetry/metrics";
  public static final String ERROR_URL =
      "https://aa87314c1e33d4c1f91a919f8cf9c4ba-387609431.us-west-2.elb.amazonaws.com:443/api/2.0/oss-sql-driver-telemetry/logs";
  public static final long INTERVAL_DURATION = TimeUnit.SECONDS.toMillis(10 * 60);
  public static final String WORKSPACE_ID = "workspace_id";
  public static final String SQL_QUERY_ID = "sql_query_id";
  public static final String TIMESTAMP = "timestamp";
  public static final String DRIVER_VERSION = "driver_version";
  public static final String CONNECTION_CONFIG = "connection_config";
  public static final String ERROR_CODE = "error_code";
}
