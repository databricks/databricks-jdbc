package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TelemetryClient implements ITelemetryClient {

  private final IDatabricksConnectionContext context;
  private final DatabricksConfig databricksConfig;
  private final int eventsBatchSize;
  private final boolean isAuthEnabled;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService;
  private List<TelemetryFrontendLog> eventsBatch;
  private volatile long lastFlushedTime;
  private ScheduledFuture<?> flushTask;
  private final int flushIntervalMillis;

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
    this.scheduledExecutorService =
        java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
    this.flushIntervalMillis = context.getTelemetryFlushIntervalInMilliseconds();
    this.lastFlushedTime = System.currentTimeMillis();
    schedulePeriodicFlush();
  }

  public TelemetryClient(
      IDatabricksConnectionContext connectionContext, ExecutorService executorService) {
    this.eventsBatch = new LinkedList<>();
    this.eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.isAuthEnabled = false;
    this.context = connectionContext;
    this.databricksConfig = null;
    this.executorService = executorService;
    this.scheduledExecutorService =
        java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
    this.flushIntervalMillis = context.getTelemetryFlushIntervalInMilliseconds();
    this.lastFlushedTime = System.currentTimeMillis();
    schedulePeriodicFlush();
  }

  private void schedulePeriodicFlush() {
    if (flushTask != null) {
      flushTask.cancel(false);
    }
    flushTask =
        scheduledExecutorService.scheduleAtFixedRate(
            this::periodicFlush, flushIntervalMillis, flushIntervalMillis, TimeUnit.MILLISECONDS);
  }

  private void periodicFlush() {
    long now = System.currentTimeMillis();
    if (now - lastFlushedTime >= flushIntervalMillis) {
      flush();
    }
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
    // Export any pending latency telemetry before flushing
    TelemetryCollector.getInstance().exportAllPendingTelemetryDetails();
    flush();
    if (flushTask != null) {
      flushTask.cancel(false);
    }
    scheduledExecutorService.shutdown();
  }

  private void flush() {
    synchronized (this) {
      if (!eventsBatch.isEmpty()) {
        List<TelemetryFrontendLog> logsToBeFlushed = eventsBatch;
        executorService.submit(
            new TelemetryPushTask(logsToBeFlushed, isAuthEnabled, context, databricksConfig));
        eventsBatch = new LinkedList<>();
      }
      lastFlushedTime = System.currentTimeMillis();
    }
  }

  int getCurrentSize() {
    synchronized (this) {
      return eventsBatch.size();
    }
  }
}
