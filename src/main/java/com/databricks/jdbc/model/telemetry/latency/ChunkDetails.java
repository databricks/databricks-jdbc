package com.databricks.jdbc.model.telemetry.latency;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ChunkDetails {

  @JsonProperty("initial_chunk_latency_millis")
  private Long initialChunkLatencyMillis;

  @JsonProperty("slowest_chunk_latency_millis")
  private Long slowestChunkLatencyMillis;

  @JsonProperty("total_chunks_present")
  private Integer totalChunksPresent;

  @JsonProperty("total_chunks_iterated")
  private Integer totalChunksIterated;

  @JsonProperty("sum_chunks_download_time_millis")
  private Long sumChunksDownloadTimeMillis;

  public ChunkDetails setInitialChunkLatencyMillis(Long initialChunkLatencyMillis) {
    this.initialChunkLatencyMillis = initialChunkLatencyMillis;
    return this;
  }

  public ChunkDetails setSlowestChunkLatencyMillis(Long slowestChunkLatencyMillis) {
    this.slowestChunkLatencyMillis = slowestChunkLatencyMillis;
    return this;
  }

  public ChunkDetails setTotalChunksPresent(Integer totalChunksPresent) {
    this.totalChunksPresent = totalChunksPresent;
    return this;
  }

  public ChunkDetails setTotalChunksIterated(Integer totalChunksIterated) {
    this.totalChunksIterated = totalChunksIterated;
    return this;
  }

  public ChunkDetails setSumChunksDownloadTimeMillis(Long sumChunksDownloadTimeMillis) {
    this.sumChunksDownloadTimeMillis = sumChunksDownloadTimeMillis;
    return this;
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
