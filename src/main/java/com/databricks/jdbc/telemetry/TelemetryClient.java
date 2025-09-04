package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TelemetryClient implements ITelemetryClient {

  private final IDatabricksConnectionContext context;
  private final DatabricksConfig databricksConfig;
  private final int eventsBatchSize;
  private final ExecutorService executorService;
  private final ITelemetryPushClient telemetryPushClient;
  private final ScheduledExecutorService scheduledExecutorService;
  private List<TelemetryFrontendLog> eventsBatch;
  private volatile long lastFlushedTime;
  private ScheduledFuture<?> flushTask;
  private final int flushIntervalMillis;

  private static ThreadFactory createSchedulerThreadFactory() {
    return new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "Telemetry-Scheduler-" + threadNumber.getAndIncrement());
        thread.setDaemon(true);
        return thread;
      }
    };
  }

  public TelemetryClient(
      IDatabricksConnectionContext connectionContext,
      ExecutorService executorService,
      DatabricksConfig config) {
    this.eventsBatch = new LinkedList<>();
    this.eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.context = connectionContext;
    this.databricksConfig = config;
    this.executorService = executorService;
    this.scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(createSchedulerThreadFactory());
    this.flushIntervalMillis = context.getTelemetryFlushIntervalInMilliseconds();
    this.lastFlushedTime = System.currentTimeMillis();
    this.telemetryPushClient =
        TelemetryClientFactory.getTelemetryPushClient(
            true /* isAuthEnabled */, context, databricksConfig);
    schedulePeriodicFlush();
  }

  public TelemetryClient(
      IDatabricksConnectionContext connectionContext, ExecutorService executorService) {
    this.eventsBatch = new LinkedList<>();
    this.eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.context = connectionContext;
    this.databricksConfig = null;
    this.executorService = executorService;
    this.scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(createSchedulerThreadFactory());
    this.flushIntervalMillis = context.getTelemetryFlushIntervalInMilliseconds();
    this.lastFlushedTime = System.currentTimeMillis();
    this.telemetryPushClient =
        TelemetryClientFactory.getTelemetryPushClient(
            false /* isAuthEnabled */, context, null /* databricksConfig */);
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
      flush(true);
    }
  }

  @Override
  public void exportEvent(TelemetryFrontendLog event) {
    synchronized (this) {
      eventsBatch.add(event);
    }

    if (isBatchFull()) {
      flush(false);
    }
  }

  @Override
  public void close() {
    // Export any pending latency telemetry before flushing
    TelemetryCollector.getInstance().exportAllPendingTelemetryDetails();
    flush(true);
    if (flushTask != null) {
      flushTask.cancel(false);
    }
    scheduledExecutorService.shutdown();
  }

  /**
   * @param forceFlush - Flushes the eventsBatch for all size variations if forceFlush, otherwise
   *     only flushes if eventsBatch size has breached
   */
  private void flush(boolean forceFlush) {
    synchronized (this) {
      if (!forceFlush ? isBatchFull() : !eventsBatch.isEmpty()) {
        List<TelemetryFrontendLog> logsToBeFlushed = eventsBatch;
        executorService.submit(new TelemetryPushTask(logsToBeFlushed, telemetryPushClient));
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

  private boolean isBatchFull() {
    return eventsBatch.size() >= eventsBatchSize;
  }
}
