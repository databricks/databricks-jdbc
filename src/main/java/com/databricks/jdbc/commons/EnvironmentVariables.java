package com.databricks.jdbc.commons;

public final class EnvironmentVariables {
  public static final int DEFAULT_STATEMENT_TIMEOUT_SECONDS = 300; // Timeout of 5 minutes
  public static final int DEFAULT_ROW_LIMIT = 0; // By default, we should fetch all rows
  public static final boolean DEFAULT_ESCAPE_PROCESSING =
      false; // By default, we should not process the sql
}
