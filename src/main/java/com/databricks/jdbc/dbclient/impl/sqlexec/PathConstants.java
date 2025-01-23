package com.databricks.jdbc.dbclient.impl.sqlexec;

public class PathConstants {

  private static final String BASE_PATH = "/api/2.0/sql/";
  public static final String SESSION_PATH = BASE_PATH + "sessions/";
  public static final String SESSION_PATH_WITH_ID = SESSION_PATH + "%s";
  public static final String STATEMENT_PATH = BASE_PATH + "statements/";
  public static final String STATEMENT_PATH_WITH_ID = STATEMENT_PATH + "%s";
  public static final String CANCEL_STATEMENT_PATH_WITH_ID = STATEMENT_PATH + "%s/cancel";
  public static final String RESULT_CHUNK_PATH = STATEMENT_PATH_WITH_ID + "/result/chunks/%s";
  public static final String TELEMETRY_PATH = "/telemetry-ext";
  public static final String TELEMETRY_PATH_UNAUTHENTICATED = "/telemetry-unauth";
  public static final String FS_BASE_PATH = "/api/2.0/fs";
  public static final String CREATE_UPLOAD_URL_PATH = FS_BASE_PATH + "/create-upload-url";
  public static final String CREATE_DOWNLOAD_URL_PATH = FS_BASE_PATH + "/create-download-url";
  public static final String CREATE_DELETE_URL_PATH = FS_BASE_PATH + "/create-delete-url";
  public static final String LIST_PATH = FS_BASE_PATH + "/list";
}
