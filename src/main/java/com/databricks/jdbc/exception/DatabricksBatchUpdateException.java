package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

import java.sql.BatchUpdateException;

public class DatabricksBatchUpdateException extends BatchUpdateException {
  public DatabricksBatchUpdateException(String reason,DatabricksDriverErrorCode internalErrorCode, int[] updateCounts, Throwable cause) {
    super(reason, updateCounts, cause);
  }

  public DatabricksBatchUpdateException(
      String reason, String SQLState, int vendorCode, int[] updateCounts, Throwable cause) {
    super(reason, SQLState, vendorCode, updateCounts, cause);
  }
}
