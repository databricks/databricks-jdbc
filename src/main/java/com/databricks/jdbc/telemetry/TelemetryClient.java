package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.jdbc.telemetry.latency.TelemetryCollectorManager;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.CompletableFuture;

public class TelemetryClient implements ITelemetryClient {
  private static final int MINIMUM_TELEMETRY_FLUSH_MILLISECONDS = 1000;
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TelemetryClient.class);
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

  public TelemetryClient(
      IDatabricksConnectionContext connectionContext,
      ExecutorService executorService,
      ScheduledExecutorService scheduledExecutorService,
      DatabricksConfig config) {
    this.eventsBatch = new LinkedList<>();
    this.eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.context = connectionContext;
    this.databricksConfig = config;
    this.executorService = executorService;
    /*
     * The scheduledExecutorService is shared across all telemetry clients and only schedules
     * periodic flush checks. The actual flush work (network I/O) is submitted asynchronously
     * to the executorService (10-thread pool), so a slow flush on one statement does not block
     * flushes for other statements as long as worker threads are available in the pool.
     */
    this.scheduledExecutorService = scheduledExecutorService;
    this.flushIntervalMillis =
        Math.max(
            context.getTelemetryFlushIntervalInMilliseconds(),
            MINIMUM_TELEMETRY_FLUSH_MILLISECONDS); // To avoid illegalArgument exception in any case
    this.lastFlushedTime = System.currentTimeMillis();
    this.telemetryPushClient =
        TelemetryClientFactory.getTelemetryPushClient(
            true /* isAuthEnabled */, context, databricksConfig);
    schedulePeriodicFlush();
  }

  public TelemetryClient(
      IDatabricksConnectionContext connectionContext,
      ExecutorService executorService,
      ScheduledExecutorService scheduledExecutorService) {
    this.eventsBatch = new LinkedList<>();
    eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.context = connectionContext;
    this.databricksConfig = null;
    this.executorService = executorService;
    this.scheduledExecutorService = scheduledExecutorService;
    this.flushIntervalMillis =
        Math.max(
            context.getTelemetryFlushIntervalInMilliseconds(),
            MINIMUM_TELEMETRY_FLUSH_MILLISECONDS); // To avoid illegalArgument exception in any
    // case;
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
    // Export any pending latency telemetry before flushing for this connection
    TelemetryCollector collector =
        TelemetryCollectorManager.getInstance().getOrCreateCollector(context);
    collector.exportAllPendingTelemetryDetails();

    try {
      // Synchronously flush the remaining events and wait for the task to complete
      flush(true).get();
    } catch (Exception e) {
      // Log the exception but do not re-throw, as the goal is to shut down gracefully
      // even if the final flush fails.
      // This makes the close() operation robust.
      // The `get()` method will block until the task is complete (or fails), making the close()
      // method synchronous.
      LOGGER.trace(
          "Caught error while performing final synchronous flush for telemetry. Error: {}", e);
    }

    // Cancel the scheduled periodic flush task
    if (flushTask != null) {
      flushTask.cancel(false);
    }

    // Note: Both executorService and scheduledExecutorService are shared resources
    // managed by TelemetryClientFactory and should not be shut down here.
  }

  /**
   * Submits a flush task to the executor service. Non-blocking: uses a shared thread pool (10
   * threads) so slow flushes don't block other statements.
   *
   * @param forceFlush - Flushes the eventsBatch for all size variations if forceFlush, otherwise
   *     only flushes if eventsBatch size has breached
   * @return a Future representing the pending completion of the task.
   */
  private Future<?> flush(boolean forceFlush) {
    synchronized (this) {
      if (!forceFlush ? isBatchFull() : !eventsBatch.isEmpty()) {
        List<TelemetryFrontendLog> logsToBeFlushed = eventsBatch;
        try {
          // Submit the task to the executor service and return the Future.
          Future<?> future =
              executorService.submit(new TelemetryPushTask(logsToBeFlushed, telemetryPushClient));
          eventsBatch = new LinkedList<>();
          lastFlushedTime = System.currentTimeMillis();
          return future;
        } catch (RejectedExecutionException e) {
          // This happens if the executor service has been shut down before the flush.
          // We log and return a completed future gracefully.
          LOGGER.trace(
              "Executor service is not accepting new tasks. Discarding telemetry events. Error: {}",
              e.getMessage());
          eventsBatch.clear();
          lastFlushedTime = System.currentTimeMillis();
          return CompletableFuture.completedFuture(null);
        }
      }
    }
    lastFlushedTime = System.currentTimeMillis();
    // Return a completed future if there is nothing to flush.
    return CompletableFuture.completedFuture(null);
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
