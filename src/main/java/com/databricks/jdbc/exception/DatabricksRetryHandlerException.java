package com.databricks.jdbc.exception;

import java.io.IOException;

public class DatabricksRetryHandlerException extends IOException {
  private final int errCode;

  public DatabricksRetryHandlerException(String message, int errCode) {
    super(message);
    this.errCode = errCode;
  }

  public int getErrCode() {
    return errCode;
  }
}
