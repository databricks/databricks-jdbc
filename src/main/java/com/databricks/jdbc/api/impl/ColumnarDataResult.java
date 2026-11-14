package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnVector;
import com.databricks.jdbc.model.core.ColumnarData;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.Collections;
import java.util.List;

/** Reads one inline SEA columnar chunk without copying its values into rows. */
public class ColumnarDataResult implements IExecutionResult {
  private final List<ColumnVector> columns;
  private final int rowCount;
  private long currentRow = -1;
  private boolean closed;

  public ColumnarDataResult(ResultManifest manifest, ResultData data)
      throws DatabricksSQLException {
    ColumnarData columnarData = data == null ? null : data.getColumnarData();
    if (columnarData == null) {
      throw invalid("Missing columnar_data in SEA result");
    }
    columns =
        columnarData.getColumns() == null ? Collections.emptyList() : columnarData.getColumns();
    if (manifest != null
        && manifest.getSchema() != null
        && manifest.getSchema().getColumnCount() != null
        && manifest.getSchema().getColumnCount() != columns.size()) {
      throw invalid("Columnar result does not match manifest schema");
    }
    rowCount = columns.isEmpty() ? 0 : sizeOf(columns.get(0));
    for (ColumnVector column : columns) {
      if (sizeOf(column) != rowCount) {
        throw invalid("Columnar result has inconsistent vector lengths");
      }
    }
  }

  private static int sizeOf(ColumnVector column) throws DatabricksSQLException {
    if (column == null || column.getValues() == null) {
      throw invalid("Unsupported columnar values arm");
    }
    return column.getValues().size();
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
    if (columnIndex < 0 || columnIndex >= columns.size()) {
      throw invalid("Column index out of bounds " + columnIndex);
    }
    ColumnVector column = columns.get(columnIndex);
    byte[] mask = column.getNullMask();
    int byteIndex = (int) currentRow / 8;
    if (mask != null
        && byteIndex < mask.length
        && ((mask[byteIndex] & 0xff) & (1 << ((int) currentRow % 8))) != 0) {
      return null;
    }
    return column.getValue((int) currentRow);
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
    return false;
  }

  @Override
  public boolean hasNext() {
    return !closed && currentRow + 1 < rowCount;
  }

  @Override
  public void close() {
    closed = true;
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
