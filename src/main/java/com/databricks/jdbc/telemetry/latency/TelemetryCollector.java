package com.databricks.jdbc.telemetry.latency;

import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.StatementTelemetryDetails;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Context handler for tracking telemetry details for Databricks JDBC driver. This class manages
 * per-statement telemetry details and provides logic for data collection.
 */
public class TelemetryCollector {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TelemetryCollector.class);

  // Singleton instance for global access
  private static final TelemetryCollector INSTANCE = new TelemetryCollector();

  // Per-statement latency tracking using StatementLatencyDetails
  private final ConcurrentHashMap<String, StatementTelemetryDetails> statementTrackers =
      new ConcurrentHashMap<>();

  private TelemetryCollector() {
    // Private constructor for singleton
  }

  public static TelemetryCollector getInstance() {
    return INSTANCE;
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
      LOGGER.trace("Statement ID is null, skipping latency recording");
      return;
    }
    statementTrackers
        .computeIfAbsent(statementId, k -> new StatementTelemetryDetails(statementId))
        .recordChunkDownloadLatency(chunkIndex, latencyMillis);
  }

  public void recordOperationLatency(long latencyMillis, String methodName) {
    String statementId = DatabricksThreadContextHolder.getStatementId();
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, skipping latency recording");
      return;
    }
    statementTrackers
        .computeIfAbsent(statementId, k -> new StatementTelemetryDetails(statementId))
        .recordOperationLatency(
            latencyMillis, TelemetryHelper.mapMethodToOperationType(methodName));
  }

  /**
   * Records when a result set is iterated/consumed.
   *
   * @param statementId the statement ID
   * @param totalChunks the total chunks present (if any)
   * @param hasNext if there are any more results left to be iterated
   */
  public void recordResultSetIteration(String statementId, Long totalChunks, boolean hasNext) {
    if (statementId == null) return;

    StatementTelemetryDetails details =
        statementTrackers.computeIfAbsent(
            statementId, k -> new StatementTelemetryDetails(statementId));

    if (totalChunks != null && totalChunks > 0) {
      details.recordChunkIteration(totalChunks);
    }
    details.recordResultSetIteration(totalChunks, hasNext);
  }

  /**
   * Gets the telemetry details for a statement.
   *
   * @param statementId the statement ID
   */
  public StatementTelemetryDetails getTelemetryDetails(String statementId) {
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, returning null telemetry details");
      return null;
    }
    return statementTrackers.get(statementId);
  }

  /**
   * Exports the telemetry details for a statement and clears the tracker for the statement.
   *
   * @param statementId the statement ID
   */
  public void exportTelemetryDetailsAndClear(String statementId) {
    StatementTelemetryDetails statementTelemetryDetails = statementTrackers.remove(statementId);
    TelemetryHelper.exportTelemetryLog(statementTelemetryDetails);
  }

  /**
   * Exports all pending telemetry details and clears the trackers. This method is called when the
   * connection/client is being closed.
   */
  public void exportAllPendingTelemetryDetails() {
    LOGGER.trace(" {} pending telemetry details for telemetry export", statementTrackers.size());
    statementTrackers.forEach(
        (statementId, statementTelemetryDetails) -> {
          TelemetryHelper.exportTelemetryLog(statementTelemetryDetails);
        });
    statementTrackers.clear();
  }

  public void recordGetOperationStatus(String statementId, long latencyMillis) {
    statementTrackers
        .computeIfAbsent(statementId, k -> new StatementTelemetryDetails(statementId))
        .recordGetOperationStatusLatency(latencyMillis);
  }

  /**
   * Records when a chunk is iterated/consumed by the result set.
   *
   * @param statementId the statement ID
   */
  @VisibleForTesting
  void recordChunkIteration(String statementId, Long totalChunks) {
    if (statementId == null) {
      return;
    }
    statementTrackers
        .computeIfAbsent(statementId, k -> new StatementTelemetryDetails(statementId))
        .recordChunkIteration(totalChunks);
  }
}
