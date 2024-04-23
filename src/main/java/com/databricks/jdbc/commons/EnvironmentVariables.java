package com.databricks.jdbc.commons;

import com.databricks.jdbc.client.impl.thrift.generated.TProtocolVersion;

public final class EnvironmentVariables {
  public static final int DEFAULT_STATEMENT_TIMEOUT_SECONDS = 300; // Timeout of 5 minutes
  public static final int DEFAULT_ROW_LIMIT =
      100000; // Setting a limit for resource and cost efficiency. Keeping it consistent across
  // drivers.
  public static final int DEFAULT_BYTE_LIMIT = 104857600; // Keeping it consistent across drivers.
  public static final boolean DEFAULT_ESCAPE_PROCESSING =
      false; // By default, we should not process the sql

  public static final TProtocolVersion JDBC_THRIFT_VERSION =
      TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9;
}
