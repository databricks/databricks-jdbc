package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.BatchUpdateException;

public class DatabricksBatchUpdateException extends BatchUpdateException {
  public DatabricksBatchUpdateException(
      String reason,
      DatabricksDriverErrorCode internalErrorCode,
      int[] updateCounts,
      IDatabricksConnectionContext connectionContext) {
    super(reason, internalErrorCode.toString(), updateCounts);
  }

  public DatabricksBatchUpdateException(
      String reason,
      String SQLState,
      int vendorCode,
      int[] updateCounts,
      Throwable cause,
      IDatabricksConnectionContext connectionContext) {
    super(reason, SQLState, vendorCode, updateCounts, cause);
  }
}
