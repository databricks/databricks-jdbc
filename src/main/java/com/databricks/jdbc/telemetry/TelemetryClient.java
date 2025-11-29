package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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
    eventsBatchSize = connectionContext.getTelemetryBatchSize();
    this.context = connectionContext;
    this.databricksConfig = null;
    this.executorService = executorService;
    this.scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(createSchedulerThreadFactory());
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
    // Export any pending latency telemetry before flushing
    TelemetryCollector.getInstance().exportAllPendingTelemetryDetails();

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

    // Shut down the scheduler.
    // The executorService is assumed to be a shared resource and is not shut down here.
    scheduledExecutorService.shutdown();
    try {
      if (!scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduledExecutorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      LOGGER.trace("Interrupted while waiting for flush to finish. Error: {}", ie);
      Thread.currentThread().interrupt();
      scheduledExecutorService.shutdownNow();
    }
  }

  /**
   * Submits a flush task to the executor service.
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
