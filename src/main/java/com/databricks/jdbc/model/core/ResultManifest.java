package com.databricks.jdbc.model.core;

import com.databricks.jdbc.common.CompressionType;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.ResultSchema;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Objects;

/**
 * Result manifest POJO
 *
 * <p>TODO: Replace this class with the corresponding SDK implementation once it becomes available
 */
public class ResultManifest {
  @JsonProperty("chunks")
  private Collection<BaseChunkInfo> chunks;

  @JsonProperty("format")
  private Format format;

  @JsonProperty("schema")
  private ResultSchema schema;

  @JsonProperty("total_byte_count")
  private Long totalByteCount;

  @JsonProperty("total_chunk_count")
  private Long totalChunkCount;

  @JsonProperty("total_row_count")
  private Long totalRowCount;

  @JsonProperty("truncated")
  private Boolean truncated;

  @JsonProperty("compression_codec")
  private CompressionType compressionType;

  @JsonProperty("is_volume_operation")
  private Boolean isVolumeOperation;

  public ResultManifest() {}

  public ResultManifest setChunks(Collection<BaseChunkInfo> chunks) {
    this.chunks = chunks;
    return this;
  }

  public CompressionType getCompressionType() {
    return this.compressionType;
  }

  public ResultManifest setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  public Collection<BaseChunkInfo> getChunks() {
    return this.chunks;
  }

  public ResultManifest setFormat(Format format) {
    this.format = format;
    return this;
  }

  public Format getFormat() {
    return this.format;
  }

  public ResultManifest setSchema(ResultSchema schema) {
    this.schema = schema;
    return this;
  }

  public ResultSchema getSchema() {
    return this.schema;
  }

  public ResultManifest setTotalByteCount(Long totalByteCount) {
    this.totalByteCount = totalByteCount;
    return this;
  }

  public Long getTotalByteCount() {
    return this.totalByteCount;
  }

  public ResultManifest setTotalChunkCount(Long totalChunkCount) {
    this.totalChunkCount = totalChunkCount;
    return this;
  }

  public Long getTotalChunkCount() {
    return this.totalChunkCount;
  }

  public ResultManifest setTotalRowCount(Long totalRowCount) {
    this.totalRowCount = totalRowCount;
    return this;
  }

  public Long getTotalRowCount() {
    return this.totalRowCount;
  }

  public ResultManifest setTruncated(Boolean truncated) {
    this.truncated = truncated;
    return this;
  }

  public Boolean getTruncated() {
    return this.truncated;
  }

  public ResultManifest setIsVolumeOperation(Boolean isVolumeOperation) {
    this.isVolumeOperation = isVolumeOperation;
    return this;
  }

  public Boolean getIsVolumeOperation() {
    return this.isVolumeOperation;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      ResultManifest that = (ResultManifest) o;
      return Objects.equals(this.chunks, that.chunks)
          && Objects.equals(this.format, that.format)
          && Objects.equals(this.schema, that.schema)
          && Objects.equals(this.totalByteCount, that.totalByteCount)
          && Objects.equals(this.totalChunkCount, that.totalChunkCount)
          && Objects.equals(this.totalRowCount, that.totalRowCount)
          && Objects.equals(this.truncated, that.truncated)
          && Objects.equals(this.isVolumeOperation, that.isVolumeOperation);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(
        this.chunks,
        this.format,
        this.schema,
        this.totalByteCount,
        this.totalChunkCount,
        this.totalRowCount,
        this.truncated,
        this.isVolumeOperation);
  }

  public String toString() {
    return (new ToStringer(ResultManifest.class))
        .add("chunks", this.chunks)
        .add("format", this.format)
        .add("schema", this.schema)
        .add("totalByteCount", this.totalByteCount)
        .add("totalChunkCount", this.totalChunkCount)
        .add("totalRowCount", this.totalRowCount)
        .add("truncated", this.truncated)
        .add("isVolumeOperation", this.isVolumeOperation)
        .toString();
  }
}
