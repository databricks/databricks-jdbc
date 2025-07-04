package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import com.databricks.jdbc.telemetry.latency.ChunkLatencyHandler;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class TelemetryClient implements ITelemetryClient {

  private final IDatabricksConnectionContext context;
  private final DatabricksConfig databricksConfig;
  private final int eventsBatchSize;
  private final boolean isAuthEnabled;
  private final ExecutorService executorService;
  private List<TelemetryFrontendLog> eventsBatch;

  public TelemetryClient(
      IDatabricksConnectionContext connectionContext,
      ExecutorService executorService,
      DatabricksConfig config) {
    this.eventsBatch = new LinkedList<>();
    this.eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.isAuthEnabled = true;
    this.context = connectionContext;
    this.databricksConfig = config;
    this.executorService = executorService;
  }

  public TelemetryClient(
      IDatabricksConnectionContext connectionContext, ExecutorService executorService) {
    this.eventsBatch = new LinkedList<>();
    this.eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.isAuthEnabled = false;
    this.context = connectionContext;
    this.databricksConfig = null;
    this.executorService = executorService;
  }

  @Override
  public void exportEvent(TelemetryFrontendLog event) {
    synchronized (this) {
      eventsBatch.add(event);
    }

    if (eventsBatch.size() == eventsBatchSize) {
      flush();
    }
  }

  @Override
  public void close() {
    // Export any pending chunk latency telemetry before flushing
    ChunkLatencyHandler.getInstance()
        .getAllPendingChunkDetails()
        .forEach(
            (statementId, chunkDetails) -> {
              TelemetryHelper.exportChunkLatencyTelemetry(chunkDetails, statementId);
            });
    flush();
  }

  @Override
  public void closeStatement(String statementId) {
    ChunkDetails chunkDetails =
        ChunkLatencyHandler.getInstance().getChunkDetailsAndCleanup(statementId);
    if (chunkDetails != null) {
      TelemetryHelper.exportChunkLatencyTelemetry(chunkDetails, statementId);
    }
    flush();
  }

  private void flush() {
    synchronized (this) {
      List<TelemetryFrontendLog> logsToBeFlushed = eventsBatch;
      executorService.submit(
          new TelemetryPushTask(logsToBeFlushed, isAuthEnabled, context, databricksConfig));
      eventsBatch = new LinkedList<>();
    }
  }

  int getCurrentSize() {
    return eventsBatch.size();
  }
}
