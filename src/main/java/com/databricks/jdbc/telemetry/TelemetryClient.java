package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
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

  /** Returns an athenticated Telemetry Client */
  public TelemetryClient(
      IDatabricksConnectionContext connectionContext,
      ExecutorService executorService,
      DatabricksConfig config) {
    this(connectionContext, true, config, executorService);
  }

  /** Returns an unathenticated Telemetry Client */
  public TelemetryClient(
      IDatabricksConnectionContext connectionContext,
      boolean isAuthenticated,
      ExecutorService executorService) {
    this(connectionContext, false, null, executorService);
  }

  private TelemetryClient(
      IDatabricksConnectionContext connectionContext,
      boolean isAuthenticated,
      DatabricksConfig databricksConfig,
      ExecutorService executorService) {
    this.eventsBatch = new LinkedList<>();
    this.eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.isAuthEnabled = isAuthenticated;
    this.context = connectionContext;
    this.databricksConfig = databricksConfig;
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
