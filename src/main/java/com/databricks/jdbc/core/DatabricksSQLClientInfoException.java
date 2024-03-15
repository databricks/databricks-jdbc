package com.databricks.jdbc.core;

import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.util.Map;

public class DatabricksSQLClientInfoException extends SQLClientInfoException {
  public DatabricksSQLClientInfoException(
      String message, Map<String, ClientInfoStatus> failedProperties) {
    super(message, failedProperties);
  }
}
