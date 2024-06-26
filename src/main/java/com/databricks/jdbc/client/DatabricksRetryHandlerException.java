package com.databricks.jdbc.client;

import java.io.IOException;

public class DatabricksRetryHandlerException extends IOException {
  private int errCode = 0;

  public DatabricksRetryHandlerException(String message, int errCode) {
    super(message);
    this.errCode = errCode;
  }

  public int getErrCode() {
    return errCode;
  }
}
