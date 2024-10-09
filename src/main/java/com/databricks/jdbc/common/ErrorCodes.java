package com.databricks.jdbc.common;

public class ErrorCodes {
  public static final int CHUNK_DOWNLOAD_ERROR = 1001;
  public static final int EXECUTE_STATEMENT_FAILED = 1003;
  public static final int EXECUTE_STATEMENT_CANCELLED = 2001;
  public static final int EXECUTE_STATEMENT_CLOSED = 2001;
  public static final int RESULT_SET_ERROR = 1004;
  public static final int COMMUNICATION_FAILURE = 1005;
  public static final int MAX_FIELD_SIZE_EXCEEDED = 1006;
  public static final int CURSOR_NAME_NOT_FOUND = 1007;
  public static final int MORE_RESULTS_UNSUPPORTED = 1008;
  public static final int UNSUPPORTED_FETCH_FORWARD = 1009;
  public static final int BATCH_OPERATION_UNSUPPORTED = 1010;
  public static final int EXECUTE_METHOD_UNSUPPORTED = 1011;
  public static final int POOLABLE_METHOD_UNSUPPORTED = 1012;
  public static final int STATEMENT_EXECUTION_TIMEOUT = 1014;
  public static final int STATEMENT_CLOSED = 1015;
  public static final int VOLUME_OPERATION_PARSING_ERROR = 1015;
}
