package com.databricks.jdbc.telemetry.latency;

import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;

/**
 * Wrapper class for statement latency details that contains ChunkDetails. Provides additional
 * context and metadata for statement-level latency tracking.
 */
public class StatementLatencyDetails {

  private final ChunkDetails chunkDetails;
  private final String statementId;
  private final long startTimeNanos;
  private final String sessionId;

  public StatementLatencyDetails(ChunkDetails chunkDetails, String statementId, String sessionId) {
    this.chunkDetails = chunkDetails;
    this.statementId = statementId;
    this.sessionId = sessionId;
    this.startTimeNanos = System.nanoTime();
  }

  public ChunkDetails getChunkDetails() {
    return chunkDetails;
  }

  public String getStatementId() {
    return statementId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public long getStartTimeNanos() {
    return startTimeNanos;
  }

  public long getElapsedTimeMillis() {
    return (System.nanoTime() - startTimeNanos) / 1_000_000;
  }

  /**
   * Sets the total number of chunks after object creation. This allows for delayed initialization
   * when the total count is not known upfront.
   *
   * @param totalChunks the total number of chunks
   */
  public void setTotalChunks(long totalChunks) {
    chunkDetails.setTotalChunksPresent(totalChunks);
  }

  /**
   * Gets the total number of chunks.
   *
   * @return the total number of chunks
   */
  public long getTotalChunks() {
    Long totalChunks = chunkDetails.getTotalChunksPresent();
    return totalChunks != null ? totalChunks : 0L;
  }

  @Override
  public String toString() {
    return String.format(
        "StatementLatencyDetails{statementId='%s', sessionId='%s', elapsedTime=%dms, totalChunks=%d}",
        statementId, sessionId, getElapsedTimeMillis(), getTotalChunks());
  }
}
