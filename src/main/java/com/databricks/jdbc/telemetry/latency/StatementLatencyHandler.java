package com.databricks.jdbc.telemetry.latency;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handler for tracking statement-level latency metrics for Databricks JDBC driver. This class
 * manages per-statement latency details and provides logic for data collection.
 */
public class StatementLatencyHandler {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(StatementLatencyHandler.class);

  // Singleton instance for global access
  private static final StatementLatencyHandler INSTANCE = new StatementLatencyHandler();

  // Per-statement latency tracking using StatementLatencyDetails
  private final ConcurrentHashMap<String, StatementLatencyDetails> statementTrackers =
      new ConcurrentHashMap<>();

  private StatementLatencyHandler() {
    // Private constructor for singleton
  }

  public static StatementLatencyHandler getInstance() {
    return INSTANCE;
  }

  /**
   * Initialize tracking for a statement with the total number of chunks.
   *
   * @param statementId the statement ID
   * @param totalChunks the total number of chunks in the result set
   * @param sessionId the session ID
   */
  public void initializeStatement(StatementId statementId, long totalChunks, String sessionId) {
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, skipping initialization");
      return;
    }

    String statementIdStr = statementId.toString();
    if (statementTrackers.containsKey(statementIdStr)) {
      LOGGER.trace(
          "Statement {} already exists in trackers, updating total chunks to {}",
          statementIdStr,
          totalChunks);
      StatementLatencyDetails existingDetails = statementTrackers.get(statementIdStr);
      existingDetails.setTotalChunks(totalChunks);
      return;
    }

    ChunkDetails chunkDetails = new ChunkDetails(totalChunks);
    StatementLatencyDetails latencyDetails =
        new StatementLatencyDetails(chunkDetails, statementIdStr, sessionId);
    statementTrackers.put(statementIdStr, latencyDetails);
    LOGGER.trace(
        "Initialized statement latency tracking for statement {} with {} total chunks",
        statementIdStr,
        totalChunks);
  }

  /**
   * Initialize tracking for a statement without knowing the total number of chunks upfront. The
   * total chunks can be set later using setTotalChunks on the StatementLatencyDetails.
   *
   * @param statementId the statement ID
   * @param sessionId the session ID
   */
  public void initializeStatement(StatementId statementId, String sessionId) {
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, skipping initialization");
      return;
    }

    String statementIdStr = statementId.toString();
    if (statementTrackers.containsKey(statementIdStr)) {
      LOGGER.trace(
          "Statement {} already exists in trackers, skipping initialization", statementIdStr);
      return;
    }

    ChunkDetails chunkDetails = new ChunkDetails(0); // Initialize with 0, can be updated later
    StatementLatencyDetails latencyDetails =
        new StatementLatencyDetails(chunkDetails, statementIdStr, sessionId);
    statementTrackers.put(statementIdStr, latencyDetails);
    LOGGER.trace(
        "Initialized statement latency tracking for statement {} (created before operation status check)",
        statementIdStr);
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

    StatementLatencyDetails latencyDetails =
        statementTrackers.computeIfAbsent(
            statementId,
            k -> {
              ChunkDetails chunkDetails = new ChunkDetails(0);
              return new StatementLatencyDetails(chunkDetails, statementId, null);
            });

    ChunkDetails chunkDetails = latencyDetails.getChunkDetails();

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
   * Updates the total number of chunks for an existing statement tracker.
   *
   * @param statementId the statement ID
   * @param totalChunks the total number of chunks to set
   */
  public void updateTotalChunks(String statementId, long totalChunks) {
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, skipping total chunks update");
      return;
    }

    StatementLatencyDetails latencyDetails = statementTrackers.get(statementId);
    if (latencyDetails != null) {
      latencyDetails.setTotalChunks(totalChunks);
      LOGGER.trace("Updated total chunks to {} for statement {}", totalChunks, statementId);
    } else {
      LOGGER.trace("No tracker found for statement {}, cannot update total chunks", statementId);
    }
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

    StatementLatencyDetails latencyDetails =
        statementTrackers.computeIfAbsent(
            statementId,
            k -> {
              LOGGER.trace(
                  "No latency details found for statement {}, creating new tracker", statementId);
              ChunkDetails chunkDetails = new ChunkDetails(0);
              return new StatementLatencyDetails(chunkDetails, statementId, null);
            });

    ChunkDetails chunkDetails = latencyDetails.getChunkDetails();
    Long currentIterated = chunkDetails.getTotalChunksIterated();
    if (currentIterated == null) {
      currentIterated = 0L;
    }
    chunkDetails.setTotalChunksIterated(currentIterated + 1);
    LOGGER.trace("Recorded chunk {} iteration for statement {}", chunkIndex, statementId);
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

    StatementLatencyDetails latencyDetails = statementTrackers.get(statementId);
    return latencyDetails != null ? latencyDetails.getChunkDetails() : null;
  }

  /**
   * Gets the collected statement latency details without removing the tracker.
   *
   * @param statementId the statement ID
   * @return the StatementLatencyDetails object or null if no tracker found
   */
  public StatementLatencyDetails getStatementLatencyDetails(String statementId) {
    if (statementId == null) {
      return null;
    }

    return statementTrackers.get(statementId);
  }

  /**
   * Gets the collected statement latency details and removes the tracker from memory (cleanup).
   *
   * @param statementId the statement ID
   * @return the StatementLatencyDetails object or null if no tracker found
   */
  public StatementLatencyDetails getStatementLatencyDetailsAndCleanup(String statementId) {
    if (statementId == null) {
      return null;
    }

    StatementLatencyDetails latencyDetails = statementTrackers.remove(statementId);
    if (latencyDetails == null) {
      LOGGER.trace("No statement latency telemetry found for statement {}", statementId);
      return null;
    }
    return latencyDetails;
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
   * Gets all pending statement latency details and clears the trackers. This method is called when
   * the connection/client is being closed.
   *
   * @return a map of statement ID to StatementLatencyDetails for all pending statements
   */
  public Map<String, StatementLatencyDetails> getAllPendingStatementLatencyDetails() {
    if (statementTrackers.isEmpty()) {
      return Collections.emptyMap();
    }

    LOGGER.trace(
        "Retrieved {} pending statement latency details for telemetry export",
        statementTrackers.size());

    Map<String, StatementLatencyDetails> pendingDetails =
        new ConcurrentHashMap<>(statementTrackers);
    statementTrackers.clear();
    return pendingDetails;
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

    Map<String, ChunkDetails> pendingDetails = new ConcurrentHashMap<>();
    for (Map.Entry<String, StatementLatencyDetails> entry : statementTrackers.entrySet()) {
      pendingDetails.put(entry.getKey(), entry.getValue().getChunkDetails());
    }
    statementTrackers.clear();
    return pendingDetails;
  }
}
