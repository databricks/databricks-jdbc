package com.databricks.jdbc.client.impl.sdk;

public class PathConstants {
  public static String SESSION_PATH = "/api/2.0/sql/statements/sessions";
  public static String SESSION_PATH_WITH_ID = "/api/2.0/sql/statements/sessions/%s";
  public static String STATEMENT_PATH = "/api/2.0/sql/statements/";
  public static String STATEMENT_PATH_WITH_ID = "/api/2.0/sql/statements/%s";
  public static String RESULT_CHUNK_PATH = "/api/2.0/sql/statements/%s/result/chunks/%s";
}
