package com.databricks.jdbc.exception;

import java.sql.SQLException;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {

  public DatabricksSQLException(String reason) {
    // TODO: Add vendor-specific (databricks) exception code
    super(reason, null, 0);
  }

  public DatabricksSQLException(String reason, String sqlState) {
    super(reason, sqlState);
  }

  public DatabricksSQLException(String reason, int vendorCode) {
    super(reason, null, vendorCode);
  }

  public DatabricksSQLException(String reason, Throwable cause) {
    super(reason, cause);
  }

  public DatabricksSQLException(String reason, Throwable cause, int errorCode) {
    super(reason, null, errorCode, cause);
  }
}
