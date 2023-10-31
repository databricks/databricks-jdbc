package com.databricks.jdbc.core;

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
}
