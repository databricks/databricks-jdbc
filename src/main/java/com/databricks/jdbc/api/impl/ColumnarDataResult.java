package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnarData;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Reads one inline SEA columnar result through the shared columnar view. */
public class ColumnarDataResult implements IExecutionResult {
  private ColumnarRowView rowView;
  private final int rowCount;
  private long currentRow = -1;
  private boolean closed;

  public ColumnarDataResult(ResultManifest manifest, ResultData data)
      throws DatabricksSQLException {
    ColumnarData columnarData = data == null ? null : data.getColumnarData();
    if (columnarData == null) {
      throw invalid("Missing columnar_data in SEA result");
    }
    rowView = new ColumnarRowView(columnarData);
    if (manifest != null
        && manifest.getSchema() != null
        && manifest.getSchema().getColumnCount() != null
        && manifest.getSchema().getColumnCount() != rowView.getColumnCount()) {
      throw invalid("Columnar result does not match manifest schema");
    }
    rowCount = rowView.getRowCount();
  }

  private static DatabricksSQLException invalid(String message) {
    return new DatabricksSQLException(
        message,
        DatabricksDriverErrorCode.INVALID_STATE.name(),
        DatabricksDriverErrorCode.INVALID_STATE);
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    if (closed) {
      throw new DatabricksSQLException(
          "Result is already closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
    if (currentRow < 0 || currentRow >= rowCount) {
      throw invalid("Cursor is outside the columnar result");
    }
    return rowView.getValue((int) currentRow, columnIndex);
  }

  @Override
  public long getCurrentRow() {
    return currentRow;
  }

  @Override
  public boolean next() {
    if (hasNext()) {
      currentRow++;
      return true;
    }
    if (!closed) {
      currentRow = rowCount;
    }
    return false;
  }

  @Override
  public boolean hasNext() {
    return !closed && currentRow + 1 < rowCount;
  }

  @Override
  public void close() {
    closed = true;
    rowView = null;
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public long getChunkCount() {
    return 1;
  }
}
