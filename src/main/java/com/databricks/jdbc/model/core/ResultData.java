package com.databricks.jdbc.model.core;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Objects;

/* TODO: This file is picked from databricks-sdk-java. Remove this once arclight changes are included in API-SPEC */
public class ResultData {

  @JsonProperty("byte_count")
  private Long byteCount;

  @JsonProperty("chunk_index")
  private Long chunkIndex;

  @JsonProperty("data_array")
  private Collection<Collection<String>> dataArray;

  /** */
  @JsonProperty("external_links")
  private Collection<ExternalLink> externalLinks;

  @JsonProperty("next_chunk_index")
  private Long nextChunkIndex;

  @JsonProperty("next_chunk_internal_link")
  private String nextChunkInternalLink;

  @JsonProperty("row_count")
  private Long rowCount;

  @JsonProperty("row_offset")
  private Long rowOffset;

  public ResultData setByteCount(Long byteCount) {
    this.byteCount = byteCount;
    return this;
  }

  public Long getByteCount() {
    return byteCount;
  }

  public ResultData setChunkIndex(Long chunkIndex) {
    this.chunkIndex = chunkIndex;
    return this;
  }

  public Long getChunkIndex() {
    return chunkIndex;
  }

  public ResultData setDataArray(Collection<Collection<String>> dataArray) {
    this.dataArray = dataArray;
    return this;
  }

  public Collection<Collection<String>> getDataArray() {
    return dataArray;
  }

  public ResultData setExternalLinks(Collection<ExternalLink> externalLinks) {
    this.externalLinks = externalLinks;
    return this;
  }

  public Collection<ExternalLink> getExternalLinks() {
    return externalLinks;
  }

  public ResultData setNextChunkIndex(Long nextChunkIndex) {
    this.nextChunkIndex = nextChunkIndex;
    return this;
  }

  public Long getNextChunkIndex() {
    return nextChunkIndex;
  }

  public ResultData setNextChunkInternalLink(String nextChunkInternalLink) {
    this.nextChunkInternalLink = nextChunkInternalLink;
    return this;
  }

  public String getNextChunkInternalLink() {
    return nextChunkInternalLink;
  }

  public ResultData setRowCount(Long rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  public Long getRowCount() {
    return rowCount;
  }

  public ResultData setRowOffset(Long rowOffset) {
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
    ResultData that = (ResultData) o;
    return Objects.equals(byteCount, that.byteCount)
        && Objects.equals(chunkIndex, that.chunkIndex)
        && Objects.equals(dataArray, that.dataArray)
        && Objects.equals(externalLinks, that.externalLinks)
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
        dataArray,
        externalLinks,
        nextChunkIndex,
        nextChunkInternalLink,
        rowCount,
        rowOffset);
  }

  @Override
  public String toString() {
    return new ToStringer(ResultData.class)
        .add("byteCount", byteCount)
        .add("chunkIndex", chunkIndex)
        .add("dataArray", dataArray)
        .add("externalLinks", externalLinks)
        .add("nextChunkIndex", nextChunkIndex)
        .add("nextChunkInternalLink", nextChunkInternalLink)
        .add("rowCount", rowCount)
        .add("rowOffset", rowOffset)
        .toString();
  }
}
