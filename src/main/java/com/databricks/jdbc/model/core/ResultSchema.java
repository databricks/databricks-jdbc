package com.databricks.jdbc.model.core;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Objects;

/**
 * Result schema POJO
 *
 * <p>TODO: Replace this class with the corresponding SDK implementation once it becomes available
 */
public class ResultSchema {
  @JsonProperty("column_count")
  private Long columnCount;

  @JsonProperty("columns")
  private Collection<ColumnInfo> columns;

  public ResultSchema setColumnCount(Long columnCount) {
    this.columnCount = columnCount;
    return this;
  }

  public Long getColumnCount() {
    return this.columnCount;
  }

  public ResultSchema setColumns(Collection<ColumnInfo> columns) {
    this.columns = columns;
    return this;
  }

  public Collection<ColumnInfo> getColumns() {
    return this.columns;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      ResultSchema that = (ResultSchema) o;
      return Objects.equals(this.columnCount, that.columnCount)
          && Objects.equals(this.columns, that.columns);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[] {this.columnCount, this.columns});
  }

  public String toString() {
    return (new ToStringer(ResultSchema.class))
        .add("columnCount", this.columnCount)
        .add("columns", this.columns)
        .toString();
  }
}
