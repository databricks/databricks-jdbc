package com.databricks.jdbc.exception;

import java.sql.BatchUpdateException;

public class DatabricksBatchUpdateException extends BatchUpdateException {
  public DatabricksBatchUpdateException(String reason, int[] updateCounts, Throwable cause) {
    super(reason, updateCounts, cause);
  }

  public DatabricksBatchUpdateException(
      String reason, String SQLState, int vendorCode, int[] updateCounts, Throwable cause) {
    super(reason, SQLState, vendorCode, updateCounts, cause);
  }
}
