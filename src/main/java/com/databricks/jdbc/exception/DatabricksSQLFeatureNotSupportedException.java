package com.databricks.jdbc.exception;

import java.sql.SQLFeatureNotSupportedException;

public class DatabricksSQLFeatureNotSupportedException extends SQLFeatureNotSupportedException {

  public DatabricksSQLFeatureNotSupportedException(String reason) {
    // TODO: Add vendor-specific (databricks) exception code
    super(reason, null, 0);
  }
}
