package com.databricks.jdbc.common;

import com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion;

public final class EnvironmentVariables {
  public static final int DEFAULT_STATEMENT_TIMEOUT_SECONDS = 0; // Infinite timeout
  public static final int DEFAULT_RESULT_ROW_LIMIT = 0; // no limit
  public static final int DEFAULT_STREAMING_BATCH_TIMEOUT_SECONDS = 300; // 5 minutes
  public static final int DEFAULT_ROW_LIMIT_PER_BLOCK =
      2000000; // Setting a limit for resource and cost efficiency
  public static final int DEFAULT_BYTE_LIMIT = 404857600;
  public static final boolean DEFAULT_ESCAPE_PROCESSING =
      false; // By default, we should not process the sql

  public static final TProtocolVersion JDBC_THRIFT_VERSION =
      TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9;

  public static final int DEFAULT_SLEEP_DELAY = 100; // 100 milliseconds
}
