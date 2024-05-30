package com.databricks.jdbc.client.impl.sdk;

public class PathConstants {

  private static final String BASE_PATH = "/api/2.0/sql/";
  public static final String SESSION_PATH = BASE_PATH + "sessions/";
  public static final String STATEMENT_PATH = BASE_PATH + "statements/";
  public static final String DELETE_SESSION_PATH_WITH_ID = STATEMENT_PATH + "sessions/%s";
  public static final String STATEMENT_PATH_WITH_ID = STATEMENT_PATH + "%s";
  public static final String CANCEL_STATEMENT_PATH_WITH_ID = STATEMENT_PATH + "%s/cancel";
  public static final String RESULT_CHUNK_PATH = STATEMENT_PATH_WITH_ID + "/result/chunks/%s";
}
