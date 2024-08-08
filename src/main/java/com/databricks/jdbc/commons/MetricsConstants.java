package com.databricks.jdbc.commons;

import java.util.concurrent.TimeUnit;

public class MetricsConstants {
  public static final String METRICS_MAP_STRING = "metrics_map";
  public static final String METRICS_TYPE = "metrics_type";
  public static final String TELEMETRY_BASE_URL =
      "https://aa87314c1e33d4c1f91a919f8cf9c4ba-387609431.us-west-2.elb.amazonaws.com:443";
  public static final String METRICS_URL =
      TELEMETRY_BASE_URL + "/api/2.0/oss-sql-driver-telemetry/metrics";
  public static final String ERROR_LOGGING_URL =
      TELEMETRY_BASE_URL + "/api/2.0/oss-sql-driver-telemetry/logs";
  public static final String USAGE_METRICS_URL =
      TELEMETRY_BASE_URL + "/api/2.0/oss-sql-driver-telemetry/usageMetrics";
  public static final long INTERVAL_DURATION = TimeUnit.SECONDS.toMillis(10 * 60);
  public static final String WORKSPACE_ID = "workspace_id";
  public static final String SQL_QUERY_ID = "sql_query_id";
  public static final String TIMESTAMP = "timestamp";
  public static final String DRIVER_VERSION = "driver_version";
  public static final String CONNECTION_CONFIG = "connection_config";
  public static final String ERROR_CODE = "error_code";
  public static final String JVM_NAME = "jvm_name";
  public static final String JVM_SPEC_VERSION = "jvm_spec_version";
  public static final String JVM_IMPL_VERSION = "jvm_impl_version";
  public static final String JVM_VENDOR = "jvm_vendor";
  public static final String OS_NAME = "os_name";
  public static final String OS_VERSION = "os_version";
  public static final String OS_ARCH = "os_arch";
  public static final String LOCALE_NAME = "locale_name";
  public static final String CHARSET_ENCODING = "charset_encoding";
}
