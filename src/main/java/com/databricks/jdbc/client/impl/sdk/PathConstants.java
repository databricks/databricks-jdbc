package com.databricks.jdbc.client.impl.sdk;

public class PathConstants {

  private static final String BASE_PATH = "/api/2.0/sql/statements/";
  public static final String SESSION_PATH = BASE_PATH + "sessions";
  public static final String SESSION_PATH_WITH_ID = BASE_PATH + "sessions/%s";
  public static final String STATEMENT_PATH = BASE_PATH;
  public static final String STATEMENT_PATH_WITH_ID = BASE_PATH + "%s";
  public static final String RESULT_CHUNK_PATH = SESSION_PATH_WITH_ID + "/result/chunks/%s";
}
