package com.databricks.jdbc.telemetry.latency;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handler for tracking chunk-related latency metrics for Databricks JDBC driver. This class manages
 * per-statement chunk details and provides logic for data collection.
 */
public class ChunkLatencyHandler {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ChunkLatencyHandler.class);

  // Singleton instance for global access
  private static final ChunkLatencyHandler INSTANCE = new ChunkLatencyHandler();

  // Per-statement latency tracking using ChunkDetails
  private final ConcurrentHashMap<String, ChunkDetails> statementTrackers =
      new ConcurrentHashMap<>();

  private ChunkLatencyHandler() {
    // Private constructor for singleton
  }

  public static ChunkLatencyHandler getInstance() {
    return INSTANCE;
  }

  /**
   * Initialize tracking for a statement with the total number of chunks.
   *
   * @param statementId the statement ID
   * @param totalChunks the total number of chunks in the result set
   */
  public void initializeStatement(StatementId statementId, long totalChunks) {
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, skipping initialization");
      return;
    }
    statementTrackers.put(statementId.toString(), new ChunkDetails(totalChunks));
    LOGGER.trace(
        "Initialized chunk tracking for statement {} with {} total chunks",
        statementId.toString(),
        totalChunks);
  }

  /**
   * Records the latency for downloading a chunk and updates metrics.
   *
   * @param statementId the statement ID
   * @param chunkIndex the index of the chunk being downloaded
   * @param latencyMillis the time taken to download the chunk in milliseconds
   */
  public void recordChunkDownloadLatency(String statementId, long chunkIndex, long latencyMillis) {
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, skipping chunk latency recording");
      return;
    }

    ChunkDetails chunkDetails =
        statementTrackers.computeIfAbsent(statementId, k -> new ChunkDetails(0));

    // Record initial chunk latency (first chunk downloaded)
    if (chunkIndex == 0) {
      chunkDetails.setInitialChunkLatencyMillis(latencyMillis);
    }

    // Update slowest chunk latency
    Long currentSlowest = chunkDetails.getSlowestChunkLatencyMillis();
    if (currentSlowest == null || latencyMillis > currentSlowest) {
      chunkDetails.setSlowestChunkLatencyMillis(latencyMillis);
    }

    // Add to sum of all chunk download times
    Long currentSum = chunkDetails.getSumChunksDownloadTimeMillis();
    if (currentSum == null) {
      currentSum = 0L;
    }
    chunkDetails.setSumChunksDownloadTimeMillis(currentSum + latencyMillis);

    LOGGER.trace(
        "Recorded chunk {} latency: {}ms for statement {}", chunkIndex, latencyMillis, statementId);
  }

  /**
   * Records when a chunk is iterated/consumed by the result set.
   *
   * @param statementId the statement ID
   * @param chunkIndex the index of the chunk being iterated
   */
  public void recordChunkIteration(String statementId, long chunkIndex) {
    if (statementId == null) {
      return;
    }

    ChunkDetails chunkDetails = statementTrackers.get(statementId);
    if (chunkDetails != null) {
      Long currentIterated = chunkDetails.getTotalChunksIterated();
      if (currentIterated == null) {
        currentIterated = 0L;
      }
      chunkDetails.setTotalChunksIterated(currentIterated + 1);
      LOGGER.trace("Recorded chunk {} iteration for statement {}", chunkIndex, statementId);
    }
  }

  /**
   * Gets the collected chunk details for a statement without removing the tracker.
   *
   * @param statementId the statement ID
   * @return the ChunkDetails object or null if no tracker found
   */
  public ChunkDetails getChunkDetails(String statementId) {
    if (statementId == null) {
      return null;
    }

    return statementTrackers.get(statementId);
  }

  /**
   * Gets the collected chunk details and removes the tracker from memory (cleanup).
   *
   * @param statementId the statement ID
   * @return the ChunkDetails object or null if no tracker found
   */
  public ChunkDetails getChunkDetailsAndCleanup(String statementId) {
    if (statementId == null) {
      return null;
    }

    ChunkDetails chunkDetails = statementTrackers.remove(statementId);
    if (chunkDetails == null) {
      LOGGER.trace("No chunk latency telemetry found for statement {}", statementId);
      return null;
    }
    return chunkDetails;
  }

  /**
   * Clears tracking data for a statement (useful for cleanup).
   *
   * @param statementId the statement ID to clear tracking for
   */
  public void clearStatement(StatementId statementId) {
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, skipping cleanup");
      return;
    }

    String statementIdStr = statementId.toString();
    statementTrackers.remove(statementIdStr);
    LOGGER.trace("Cleared tracking for statement {}", statementIdStr);
  }

  /**
   * Gets all pending chunk details and clears the trackers. This method is called when the
   * connection/client is being closed.
   *
   * @return a map of statement ID to ChunkDetails for all pending statements
   */
  public Map<String, ChunkDetails> getAllPendingChunkDetails() {
    if (statementTrackers.isEmpty()) {
      return Collections.emptyMap();
    }

    LOGGER.trace(
        "Retrieved {} pending chunk details for telemetry export", statementTrackers.size());

    Map<String, ChunkDetails> pendingDetails = new ConcurrentHashMap<>(statementTrackers);
    statementTrackers.clear();
    return pendingDetails;
  }
}
