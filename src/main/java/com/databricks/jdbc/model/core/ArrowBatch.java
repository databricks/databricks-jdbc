package com.databricks.jdbc.model.core;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Objects;

/** One native Arrow record-batch message and its row count. */
public class ArrowBatch {
  @JsonProperty("batch")
  private byte[] batch;

  @JsonProperty("row_count")
  private Long rowCount;

  public byte[] getBatch() {
    return batch;
  }

  public ArrowBatch setBatch(byte[] batch) {
    this.batch = batch;
    return this;
  }

  public Long getRowCount() {
    return rowCount;
  }

  public ArrowBatch setRowCount(Long rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrowBatch that = (ArrowBatch) o;
    return Arrays.equals(batch, that.batch) && Objects.equals(rowCount, that.rowCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(batch), rowCount);
  }

  @Override
  public String toString() {
    return new ToStringer(ArrowBatch.class)
        .add("batchByteCount", batch == null ? null : batch.length)
        .add("rowCount", rowCount)
        .toString();
  }
}
