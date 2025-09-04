package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.BatchUpdateException;
import java.util.Arrays;

public class DatabricksBatchUpdateException extends BatchUpdateException {

  private long[] longUpdateCounts;
  private int[] updateCounts;

  public DatabricksBatchUpdateException(
      String reason, DatabricksDriverErrorCode internalErrorCode, long[] longUpdateCounts) {
    super(reason, internalErrorCode.toString(), null);
    this.longUpdateCounts =
        (longUpdateCounts == null)
            ? null
            : Arrays.copyOf(longUpdateCounts, longUpdateCounts.length);
    this.updateCounts = (longUpdateCounts == null) ? null : copyUpdateCount(longUpdateCounts);
    exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(), internalErrorCode.toString(), reason);
  }

  public DatabricksBatchUpdateException(
      String reason, String SQLState, int vendorCode, long[] longUpdateCounts, Throwable cause) {
    super(reason, SQLState, vendorCode, longUpdateCounts, cause);
    this.longUpdateCounts =
        (longUpdateCounts == null)
            ? null
            : Arrays.copyOf(longUpdateCounts, longUpdateCounts.length);
    this.updateCounts = (longUpdateCounts == null) ? null : copyUpdateCount(longUpdateCounts);
    exportFailureLog(DatabricksThreadContextHolder.getConnectionContext(), SQLState, reason);
  }

  public long[] getLargeUpdateCounts() {
    return (longUpdateCounts == null)
        ? null
        : Arrays.copyOf(longUpdateCounts, longUpdateCounts.length);
  }

  public int[] getUpdateCounts() {
    return (updateCounts == null) ? null : Arrays.copyOf(updateCounts, updateCounts.length);
  }

  private int[] copyUpdateCount(long[] uc) {
    int[] copy = new int[uc.length];
    for (int i = 0; i < uc.length; i++) {
      copy[i] = (int) uc[i];
    }
    return copy;
  }
}
