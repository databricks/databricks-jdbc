package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class InlineJsonResult implements IExecutionResult {

  private long currentRow;
  private List<List<Object>> data;
  // Exact java.time values for DATE/TIMESTAMP cells, parallel to data; null when there are none.
  private List<List<Object>> temporalData;
  private JsonChunkProvider chunkProvider;
  private boolean isClosed;

  public InlineJsonResult(
      ResultManifest resultManifest,
      ResultData resultData,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {

    this.chunkProvider = new JsonChunkProvider(resultManifest, resultData, statementId, session);
    // Fetching data all at once as the data is at most 26Mb in total (SEA)
    this.data = chunkProvider.getAllData();

    this.currentRow = -1;
    this.isClosed = false;
  }

  public InlineJsonResult(Object[][] rows) {
    this(
        Arrays.stream(rows)
            .map(row -> Arrays.stream(row).collect(Collectors.toList()))
            .collect(Collectors.toList()));
  }

  public InlineJsonResult(List<List<Object>> rows) {
    this.data = rows.stream().map(ArrayList::new).collect(Collectors.toList());
    this.currentRow = -1;
    this.isClosed = false;
  }

  /**
   * Creates an in-memory result whose DATE/TIMESTAMP cells also carry exact {@code java.time}
   * values, returned by {@link #getTemporalObject}.
   *
   * @param rows the values returned by {@link #getObject}
   * @param temporalRows exact values with the same shape as {@code rows}, or {@link Supplier}s of
   *     them, which are evaluated when the cell is read; null entries fall back to {@code rows}
   */
  InlineJsonResult(List<List<Object>> rows, List<List<Object>> temporalRows) {
    this(rows);
    if (temporalRows != null && temporalRows.size() == rows.size()) {
      this.temporalData = temporalRows.stream().map(ArrayList::new).collect(Collectors.toList());
    }
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    if (isClosed()) {
      throw new DatabricksSQLException(
          "Result is already closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
    if (currentRow == -1) {
      throw new DatabricksSQLException(
          "Cursor is before first row", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (columnIndex < data.get((int) currentRow).size()) {
      return data.get((int) currentRow).get(columnIndex);
    }
    throw new DatabricksSQLException(
        "Column index out of bounds " + columnIndex, DatabricksDriverErrorCode.INVALID_STATE);
  }

  /** {@inheritDoc} */
  @Override
  public Object getTemporalObject(int columnIndex) throws DatabricksSQLException {
    Object object = getObject(columnIndex);
    if (object != null && temporalData != null) {
      List<Object> row = temporalData.get((int) currentRow);
      Object exact = columnIndex < row.size() ? row.get(columnIndex) : null;
      if (exact instanceof Supplier) {
        exact = ((Supplier<?>) exact).get();
      }
      if (exact != null) {
        return exact;
      }
    }
    return object;
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
    return !isClosed() && currentRow < data.size() - 1;
  }

  @Override
  public void close() {
    this.isClosed = true;
    this.data = null;
    this.temporalData = null;
    if (chunkProvider != null) {
      chunkProvider.close();
    }
  }

  @Override
  public long getRowCount() {
    return data.size();
  }

  @Override
  public long getChunkCount() {
    return chunkProvider.getChunkCount();
  }

  private boolean isClosed() {
    return isClosed;
  }
}
