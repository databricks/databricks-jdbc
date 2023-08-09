package com.databricks.jdbc.core;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class InlineJsonResult implements IExecutionResult {

  private int currentRow;
  private final List<List<String>> data;
  private final ResultManifest resultManifest;
  private final ResultData resultData;

  InlineJsonResult(ResultManifest resultManifest, ResultData resultData) {
    this.resultManifest = resultManifest;
    this.resultData = resultData;
    this.data = getDataList(resultData.getDataArray());
    this.currentRow = 0;
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
}
