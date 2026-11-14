package com.databricks.jdbc.model.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Objects;

/** An Arrow record batch without an IPC schema header. */
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
  public boolean equals(Object other) {
    if (!(other instanceof ArrowBatch)) return false;
    ArrowBatch that = (ArrowBatch) other;
    return Arrays.equals(batch, that.batch) && Objects.equals(rowCount, that.rowCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(batch), rowCount);
  }
}
