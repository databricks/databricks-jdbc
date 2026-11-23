package com.databricks.jdbc.model.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/** Inline metadata represented as physical column vectors. */
public class ColumnarData {
  @JsonProperty("columns")
  private List<ColumnVector> columns;

  public List<ColumnVector> getColumns() {
    return columns;
  }

  public ColumnarData setColumns(List<ColumnVector> columns) {
    this.columns = columns;
    return this;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ColumnarData && Objects.equals(columns, ((ColumnarData) other).columns);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(columns);
  }
}
