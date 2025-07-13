package com.databricks.jdbc.model.telemetry.latency;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ChunkDetails {

  @JsonProperty("initial_chunk_latency_millis")
  private Long initialChunkLatencyMillis;

  @JsonProperty("slowest_chunk_latency_millis")
  private Long slowestChunkLatencyMillis;

  @JsonProperty("total_chunks_present")
  private Long totalChunksPresent;

  @JsonProperty("total_chunks_iterated")
  private Long totalChunksIterated;

  @JsonProperty("sum_chunks_download_time_millis")
  private Long sumChunksDownloadTimeMillis;

  public ChunkDetails() {
    this.totalChunksIterated = null;
    this.sumChunksDownloadTimeMillis = 0L;
    this.totalChunksPresent = null;
  }

  public ChunkDetails setInitialChunkLatencyMillis(Long initialChunkLatencyMillis) {
    this.initialChunkLatencyMillis = initialChunkLatencyMillis;
    return this;
  }

  public ChunkDetails setSlowestChunkLatencyMillis(Long slowestChunkLatencyMillis) {
    this.slowestChunkLatencyMillis = slowestChunkLatencyMillis;
    return this;
  }

  public ChunkDetails setTotalChunksPresent(Long totalChunksPresent) {
    this.totalChunksPresent = totalChunksPresent;
    return this;
  }

  public ChunkDetails setTotalChunksIterated(Long totalChunksIterated) {
    this.totalChunksIterated = totalChunksIterated;
    return this;
  }

  public ChunkDetails setSumChunksDownloadTimeMillis(Long sumChunksDownloadTimeMillis) {
    this.sumChunksDownloadTimeMillis = sumChunksDownloadTimeMillis;
    return this;
  }

  public Long getInitialChunkLatencyMillis() {
    return initialChunkLatencyMillis;
  }

  public Long getSlowestChunkLatencyMillis() {
    return slowestChunkLatencyMillis;
  }

  public Long getTotalChunksPresent() {
    return totalChunksPresent;
  }

  public Long getTotalChunksIterated() {
    return totalChunksIterated;
  }

  public Long getSumChunksDownloadTimeMillis() {
    return sumChunksDownloadTimeMillis;
  }

  @Override
  public String toString() {
    return new ToStringer(ChunkDetails.class)
        .add("initialChunkLatencyMillis", initialChunkLatencyMillis)
        .add("slowestChunkLatencyMillis", slowestChunkLatencyMillis)
        .add("totalChunksPresent", totalChunksPresent)
        .add("totalChunksIterated", totalChunksIterated)
        .add("sumChunksDownloadTimeMillis", sumChunksDownloadTimeMillis)
        .toString();
  }
}
