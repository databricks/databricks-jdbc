package com.databricks.jdbc.telemetry.latency;

import static com.databricks.jdbc.telemetry.TelemetryHelper.getStatementIdString;

import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TSparkRowSetType;
import com.databricks.jdbc.model.telemetry.StatementTelemetryDetails;
import com.databricks.jdbc.model.telemetry.enums.ExecutionResultFormat;
import com.databricks.jdbc.model.telemetry.latency.OperationType;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.service.sql.Format;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Context handler for tracking telemetry details for Databricks JDBC driver. This class manages
 * per-statement telemetry details and provides logic for data collection.
 */
public class TelemetryCollector {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TelemetryCollector.class);

  // Per-statement latency tracking using StatementLatencyDetails
  private final ConcurrentHashMap<String, StatementTelemetryDetails> statementTrackers =
      new ConcurrentHashMap<>();

  /**
   * Package-private constructor - instances should only be created via TelemetryCollectorManager
   */
  TelemetryCollector() {
    // Constructor for per-connection instances
  }

  /**
   * Records the latency for downloading a chunk and updates metrics.
   *
   * @param statementId the statement ID string
   * @param chunkIndex the index of the chunk being downloaded
   * @param latencyMillis the time taken to download the chunk in milliseconds
   */
  public void recordChunkDownloadLatency(String statementId, long chunkIndex, long latencyMillis) {
    if (statementId == null) {
      LOGGER.trace("Statement ID is null, skipping chunk latency recording");
      return;
    }
    statementTrackers
        .computeIfAbsent(statementId, k -> new StatementTelemetryDetails(statementId))
        .recordChunkDownloadLatency(chunkIndex, latencyMillis);
  }

  public void recordTotalChunks(StatementId statementId, long totalChunks) {
    String statementIdString = getStatementIdString(statementId);
    if (statementIdString == null) {
      LOGGER.trace("Statement ID is null, skipping total chunk telemetry recording");
      return;
    }
    statementTrackers
        .computeIfAbsent(statementIdString, k -> new StatementTelemetryDetails(statementIdString))
        .getChunkDetails()
        .setTotalChunksPresent(totalChunks);
  }

  public void recordOperationLatency(long latencyMillis, String methodName) {
    // It is possible that statement ID is not present in case of openSession. In which case, we
    // send telemetry latency log without the statement ID
    String statementId = DatabricksThreadContextHolder.getStatementId();
    OperationType operationType = TelemetryHelper.mapMethodToOperationType(methodName);
    if (isTelemetryCollected(statementId) && isCloseOperation(operationType)) {
      // This is terminal state, we will have to export all data corresponding to the statementID
      statementTrackers.get(statementId).recordOperationLatency(latencyMillis, operationType);
      exportTelemetryDetailsAndClear(statementId);
      return;
    }
    TelemetryHelper.exportTelemetryLog(
        new StatementTelemetryDetails(statementId)
            .recordOperationLatency(latencyMillis, operationType),
        TelemetryLogLevel.INFO);
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
    statementTrackers
        .computeIfAbsent(statementId, k -> new StatementTelemetryDetails(statementId))
        .recordResultSetIteration(totalChunks, hasNext);
  }

  /**
   * Gets the telemetry details for a statement if present Otherwise creates a new one and persist
   *
   * @param statementId the statement ID
   */
  public StatementTelemetryDetails getOrCreateTelemetryDetails(String statementId) {
    if (statementId == null) {
      return null;
    }
    return statementTrackers.computeIfAbsent(
        statementId, k -> new StatementTelemetryDetails(statementId));
  }

  /**
   * Exports all pending telemetry details and clears the trackers. This method is called when the
   * connection/client is being closed.
   */
  public void exportAllPendingTelemetryDetails() {
    LOGGER.trace(" {} pending telemetry details for telemetry export", statementTrackers.size());
    statementTrackers.forEach(
        (statementId, statementTelemetryDetails) -> {
          // Info log level is used to export the latency telemetry
          TelemetryHelper.exportTelemetryLog(statementTelemetryDetails, TelemetryLogLevel.INFO);
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

  @VisibleForTesting
  boolean isCloseOperation(OperationType operationType) {
    return (operationType == OperationType.CLOSE_STATEMENT
        || operationType == OperationType.CANCEL_STATEMENT
        || operationType == OperationType.DELETE_SESSION);
  }

  boolean isTelemetryCollected(String statementId) {
    return statementId != null && statementTrackers.containsKey(statementId);
  }

  /**
   * Exports the telemetry details for a statement and clears the tracker for the statement.
   *
   * @param statementId the statement ID
   */
  private void exportTelemetryDetailsAndClear(String statementId) {
    StatementTelemetryDetails statementTelemetryDetails = statementTrackers.remove(statementId);
    // Info log level is used to export the latency telemetry
    TelemetryHelper.exportTelemetryLog(statementTelemetryDetails, TelemetryLogLevel.INFO);
  }

  public void setResultFormat(
      IDatabricksStatementInternal statement, TSparkRowSetType executionResultFormat) {
    if (statement == null || statement.getStatementId() == null) {
      return;
    }
    setExecutionFormat(
        statement.getStatementId().toSQLExecStatementId(), getResultFormat(executionResultFormat));
  }

  public void setResultFormat(StatementId statementId, Format executionResultFormat) {
    if (statementId == null) {
      return;
    }
    setExecutionFormat(statementId.toSQLExecStatementId(), getResultFormat(executionResultFormat));
  }

  private void setExecutionFormat(String statementId, ExecutionResultFormat executionResultFormat) {
    statementTrackers
        .computeIfAbsent(statementId, k -> new StatementTelemetryDetails(statementId))
        .setExecutionResultFormat(executionResultFormat);
  }

  private ExecutionResultFormat getResultFormat(TSparkRowSetType resultFormat) {
    switch (resultFormat) {
      case ARROW_BASED_SET:
        return ExecutionResultFormat.INLINE_ARROW;
      case URL_BASED_SET:
        return ExecutionResultFormat.EXTERNAL_LINKS;
      case COLUMN_BASED_SET:
        return ExecutionResultFormat.COLUMNAR_INLINE;
      default:
        return ExecutionResultFormat.FORMAT_UNSPECIFIED;
    }
  }

  private ExecutionResultFormat getResultFormat(Format resultFormat) {
    switch (resultFormat) {
      case ARROW_STREAM:
        return ExecutionResultFormat.EXTERNAL_LINKS;
      case JSON_ARRAY:
        return ExecutionResultFormat.INLINE_JSON;
      default:
        return ExecutionResultFormat.FORMAT_UNSPECIFIED;
    }
  }
}
