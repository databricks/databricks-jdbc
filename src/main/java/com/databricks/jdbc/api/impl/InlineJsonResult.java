package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import java.util.*;
import java.util.stream.Collectors;

public class InlineJsonResult implements IExecutionResult {

  private long currentRow;
  private List<List<Object>> data;

  private boolean isClosed;

  public InlineJsonResult(ResultManifest resultManifest, ResultData resultData) {
    this(getDataList(resultData.getDataArray()));
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

  private static List<List<Object>> getDataList(Collection<Collection<String>> dataArray) {
    if (dataArray == null) {
      return new ArrayList<>();
    }
    List<List<Object>> dataList = new ArrayList<>();
    for (Collection<String> innerCollection : dataArray) {
      if (innerCollection == null) {
        dataList.add(Collections.emptyList());
      } else {
        dataList.add(new ArrayList<>(innerCollection));
      }
    }
    return dataList;
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    if (isClosed()) {
      throw new DatabricksSQLException("Result is already closed");
    }
    if (currentRow == -1) {
      throw new DatabricksSQLException("Cursor is before first row");
    }
    if (columnIndex < data.get((int) currentRow).size()) {
      return data.get((int) currentRow).get(columnIndex);
    }
    throw new DatabricksSQLException("Column index out of bounds " + columnIndex);
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
  }

  @Override
  public long getRowCount() {
    return data.size();
  }

  @Override
  public long getChunkCount() {
    return 0;
  }

  private boolean isClosed() {
    return isClosed;
  }
}
