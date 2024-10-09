package com.databricks.jdbc.model.core;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

/**
 * External link POJO
 *
 * <p>TODO: Replace this class with the corresponding SDK implementation once it becomes available
 */
public class ExternalLink {
  @JsonProperty("byte_count")
  private Long byteCount;

  @JsonProperty("chunk_index")
  private Long chunkIndex;

  @JsonProperty("expiration")
  private String expiration;

  @JsonProperty("external_link")
  private String externalLink;

  @JsonProperty("http_headers")
  private Map<String, String> httpHeaders;

  @JsonProperty("next_chunk_index")
  private Long nextChunkIndex;

  @JsonProperty("next_chunk_internal_link")
  private String nextChunkInternalLink;

  @JsonProperty("row_count")
  private Long rowCount;

  @JsonProperty("row_offset")
  private Long rowOffset;

  public ExternalLink setByteCount(Long byteCount) {
    this.byteCount = byteCount;
    return this;
  }

  public Long getByteCount() {
    return byteCount;
  }

  public ExternalLink setChunkIndex(Long chunkIndex) {
    this.chunkIndex = chunkIndex;
    return this;
  }

  public Long getChunkIndex() {
    return chunkIndex;
  }

  public ExternalLink setExpiration(String expiration) {
    this.expiration = expiration;
    return this;
  }

  public String getExpiration() {
    return expiration;
  }

  public ExternalLink setExternalLink(String externalLink) {
    this.externalLink = externalLink;
    return this;
  }

  public ExternalLink setHttpHeaders(Map<String, String> httpHeaders) {
    this.httpHeaders = httpHeaders;
    return this;
  }

  public Map<String, String> getHttpHeaders() {
    return this.httpHeaders;
  }

  public String getExternalLink() {
    return externalLink;
  }

  public ExternalLink setNextChunkIndex(Long nextChunkIndex) {
    this.nextChunkIndex = nextChunkIndex;
    return this;
  }

  public Long getNextChunkIndex() {
    return nextChunkIndex;
  }

  public ExternalLink setNextChunkInternalLink(String nextChunkInternalLink) {
    this.nextChunkInternalLink = nextChunkInternalLink;
    return this;
  }

  public String getNextChunkInternalLink() {
    return nextChunkInternalLink;
  }

  public ExternalLink setRowCount(Long rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  public Long getRowCount() {
    return rowCount;
  }

  public ExternalLink setRowOffset(Long rowOffset) {
    this.rowOffset = rowOffset;
    return this;
  }

  public Long getRowOffset() {
    return rowOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExternalLink that = (ExternalLink) o;
    return Objects.equals(byteCount, that.byteCount)
        && Objects.equals(chunkIndex, that.chunkIndex)
        && Objects.equals(expiration, that.expiration)
        && Objects.equals(externalLink, that.externalLink)
        && Objects.equals(nextChunkIndex, that.nextChunkIndex)
        && Objects.equals(nextChunkInternalLink, that.nextChunkInternalLink)
        && Objects.equals(rowCount, that.rowCount)
        && Objects.equals(rowOffset, that.rowOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        byteCount,
        chunkIndex,
        expiration,
        externalLink,
        nextChunkIndex,
        nextChunkInternalLink,
        rowCount,
        rowOffset);
  }

  @Override
  public String toString() {
    return new ToStringer(ExternalLink.class)
        .add("byteCount", byteCount)
        .add("chunkIndex", chunkIndex)
        .add("expiration", expiration)
        .add("externalLink", externalLink)
        .add("nextChunkIndex", nextChunkIndex)
        .add("nextChunkInternalLink", nextChunkInternalLink)
        .add("rowCount", rowCount)
        .add("rowOffset", rowOffset)
        .toString();
  }
}
