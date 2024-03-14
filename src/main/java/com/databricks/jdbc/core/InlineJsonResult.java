package com.databricks.jdbc.core;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class InlineJsonResult implements IExecutionResult {

  private long currentRow;
  private List<List<String>> data;

  private boolean isClosed;

  InlineJsonResult(ResultManifest resultManifest, ResultData resultData) {
    this.data = getDataList(resultData.getDataArray());
    this.currentRow = -1;
    this.isClosed = false;
  }

  InlineJsonResult(TGetResultSetMetadataResp resultManifest, TRowSet resultData) {
    int rows = ((TDoubleColumn) resultData.getColumns().get(0).getFieldValue()).getValues().size();
    int columns = resultData.getColumns().size()-1;
    data = new ArrayList<>();
    for(int i=0;i<rows;i++) {
    List<String>currentRowValues = new ArrayList<>();
    for(int j=0;j<columns;j++){
        TColumn tColumn = resultData.getColumns().get(j);
        if (tColumn.isSetDoubleVal()) {
          currentRowValues.add(tColumn.getDoubleVal().getValues().get(i).toString());
        }
        else{
          currentRowValues.add(tColumn.getStringVal().getValues().get(i));
        }
      }
      data.add(currentRowValues);
    }
    System.out.println("here is data\n"+data.toString());
    this.currentRow = -1;
    this.isClosed = false;
  }

  InlineJsonResult(Object[][] rows) {
    this.data =
        Arrays.stream(rows)
            .map(
                a ->
                    Arrays.stream(a)
                        .map(o -> o == null ? null : o.toString())
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());
    this.currentRow = -1;
    this.isClosed = false;
  }

  InlineJsonResult(List<List<Object>> rows) {
    this.data =
        rows.stream()
            .map(
                a ->
                    a.stream()
                        .map(o -> o == null ? null : o.toString())
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());
    this.currentRow = -1;
    this.isClosed = false;
  }

  private static List<List<String>> getDataList(Collection<Collection<String>> dataArray) {
    if (dataArray == null) {
      return new ArrayList<>();
    }
    return dataArray.stream()
        .map(c -> c.stream().collect(toImmutableList()))
        .collect(toImmutableList());
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (isClosed()) {
      throw new DatabricksSQLException("Method called on closed result");
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
  public synchronized long getCurrentRow() {
    return currentRow;
  }

  @Override
  public synchronized boolean next() {
    if (hasNext()) {
      currentRow++;
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean hasNext() {
    return !this.isClosed() && currentRow < data.size() - 1;
  }

  @Override
  public void close() {
    this.isClosed = true;
    this.data = null;
  }

  private boolean isClosed() {
    return isClosed;
  }
}
