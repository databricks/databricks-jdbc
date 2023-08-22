package com.databricks.jdbc.core;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class InlineJsonResult implements IExecutionResult {

  private int currentRow;
  private final List<List<String>> data;
  private final ResultManifest resultManifest;
  private final ResultData resultData;

  InlineJsonResult(ResultManifest resultManifest, ResultData resultData) {
    this.resultManifest = resultManifest;
    this.resultData = resultData;
    this.data = getDataList(resultData.getDataArray());
    this.currentRow = -1;
  }
  InlineJsonResult(Object[][] rows) {
    this.resultData = null;
    this.resultManifest = null;
    this.data = Arrays.stream(rows).map(a -> Arrays.stream(a).map(o -> o == null ? null : o.toString()).collect(Collectors.toList())).collect(Collectors.toList());
    this.currentRow = -1;
  }

  InlineJsonResult(List<List<Object>> rows) {
    this.resultData = null;
    this.resultManifest = null;
    this.data = rows.stream().map(a -> a.stream().map(Object::toString).collect(Collectors.toList())).collect(Collectors.toList());
    this.currentRow = -1;
  }

  private static List<List<String>> getDataList(Collection<Collection<String>> dataArray) {
    if (dataArray == null) {
      return new ArrayList<>();
    }
    return dataArray.stream().map(c -> c.stream().collect(toImmutableList())).collect(toImmutableList());
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (columnIndex < data.get(currentRow).size()) {
      return data.get(currentRow).get(columnIndex);
    }
    throw new DatabricksSQLException("Column index out of bounds " + columnIndex);
  }

  @Override
  public synchronized int getCurrentRow() {
    return currentRow;
  }

  @Override
  public synchronized boolean next() {
    // TODO: handle pagination
    if (currentRow < data.size() -1) {
      currentRow++;
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean hasNext() {
    return currentRow < data.size() - 1;
  }
}
