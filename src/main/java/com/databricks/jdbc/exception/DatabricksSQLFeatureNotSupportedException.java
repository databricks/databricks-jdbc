package com.databricks.jdbc.exception;

import java.sql.SQLFeatureNotSupportedException;

public class DatabricksSQLFeatureNotSupportedException extends SQLFeatureNotSupportedException {

  public DatabricksSQLFeatureNotSupportedException(String reason) {
    // TODO: Add proper error code
    super(reason);
  }
}
