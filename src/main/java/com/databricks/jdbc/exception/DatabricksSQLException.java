package com.databricks.jdbc.exception;

import java.sql.SQLException;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {

  public DatabricksSQLException(String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
  }

  public DatabricksSQLException(String reason) {
    // TODO: Add proper error code
    super(reason, null, 0);
  }

  public DatabricksSQLException(String reason, int vendorCode) {
    super(reason, null, vendorCode);
  }

  public DatabricksSQLException(String reason, Throwable cause) {
    super(reason, cause);
  }

  public DatabricksSQLException(String reason, Throwable cause, String sqlState, int errorCode) {
    super(reason, sqlState, errorCode, cause);
  }
}
