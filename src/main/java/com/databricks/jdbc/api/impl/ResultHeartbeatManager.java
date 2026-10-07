package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Manages periodic heartbeat tasks to keep server-side result state alive while the client consumes
 * results slowly. One instance per connection, shared across all statements.
 *
 * <p>Each active result set can register a heartbeat task that periodically calls
 * GetStatementStatus (SEA) or GetOperationStatus (Thrift) to signal the server that the client is
 * still consuming results. This prevents premature operation expiry and warehouse auto-stop.
 *
 * <p>Heartbeats are stopped when:
 *
 * <ul>
 *   <li>All results are consumed (ResultSet.next() returns false)
 *   <li>ResultSet.close() is called
 *   <li>Statement.close() is called (safety net)
 *   <li>Connection.close() calls shutdown()
 *   <li>The heartbeat task itself detects a terminal state or max consecutive failures
 * </ul>
 */
class ResultHeartbeatManager {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ResultHeartbeatManager.class);

  private final ScheduledExecutorService scheduler;
  private final Map<StatementId, ScheduledFuture<?>> activeHeartbeats = new ConcurrentHashMap<>();
  private final Map<StatementId, java.util.concurrent.atomic.AtomicBoolean> stoppedFlags =
      new ConcurrentHashMap<>();

  /** Sentinel returned by getStoppedFlag when the flag has been removed (already stopped). */
  private static final java.util.concurrent.atomic.AtomicBoolean ALREADY_STOPPED =
      new java.util.concurrent.atomic.AtomicBoolean(true);

  private final int intervalSeconds;
  private volatile boolean isShutdown = false;

  // Small pool to prevent one blocked heartbeat RPC from starving others on the same connection.
  // A connection with multiple active statements needs concurrent heartbeat ticks.
  private static final int HEARTBEAT_THREAD_POOL_SIZE = 2;

  private static final java.util.concurrent.atomic.AtomicLong MANAGER_COUNTER =
      new java.util.concurrent.atomic.AtomicLong(0);

  ResultHeartbeatManager(int intervalSeconds, String connectionUuid) {
    this.intervalSeconds = intervalSeconds;
    long managerId = MANAGER_COUNTER.incrementAndGet();
    String threadPrefix =
        connectionUuid != null
            ? "databricks-jdbc-heartbeat-" + connectionUuid + "-" + managerId
            : "databricks-jdbc-heartbeat-" + managerId;
    this.scheduler =
        Executors.newScheduledThreadPool(
            HEARTBEAT_THREAD_POOL_SIZE,
            r -> {
              Thread t = new Thread(r, threadPrefix);
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Starts a periodic heartbeat for the given statement. The task runs every {@code
   * intervalSeconds} after an initial delay equal to the interval.
   *
   * @param statementId the statement to keep alive
   * @param heartbeatTask the task that sends the heartbeat RPC. Must handle its own exceptions.
   */
  void startHeartbeat(StatementId statementId, Runnable heartbeatTask) {
    if (isShutdown || statementId == null) {
      return;
    }

    // Stop any existing heartbeat for this statement (e.g., re-execution)
    stopHeartbeat(statementId);

    // Create a fresh stopped flag for the new heartbeat
    resetStoppedFlag(statementId);

    LOGGER.debug(
        "Starting heartbeat for statement {} with interval {}s",
        statementId.toSQLExecStatementId(),
        intervalSeconds);

    ScheduledFuture<?> future =
        scheduler.scheduleWithFixedDelay(
            heartbeatTask, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    activeHeartbeats.put(statementId, future);
  }

  /**
   * Stops the heartbeat for the given statement. Idempotent — safe to call multiple times or for
   * statements that have no active heartbeat.
   */
  void stopHeartbeat(StatementId statementId) {
    if (statementId == null) {
      return;
    }

    // Set the stopped flag BEFORE canceling — prevents in-flight RPC from calling a closed client
    java.util.concurrent.atomic.AtomicBoolean flag = stoppedFlags.remove(statementId);
    if (flag != null) {
      flag.set(true);
    }

    ScheduledFuture<?> future = activeHeartbeats.remove(statementId);
    if (future != null) {
      future.cancel(false); // don't interrupt if currently running
      LOGGER.debug("Stopped heartbeat for statement {}", statementId.toSQLExecStatementId());
    }
  }

  /**
   * Returns the stopped flag for the given statement, or a sentinel ALREADY_STOPPED if the flag has
   * been removed (i.e., stopHeartbeat was already called). Uses get() instead of computeIfAbsent()
   * to prevent re-creating a false flag after stopHeartbeat() removed it.
   */
  java.util.concurrent.atomic.AtomicBoolean getStoppedFlag(StatementId statementId) {
    java.util.concurrent.atomic.AtomicBoolean flag = stoppedFlags.get(statementId);
    return flag != null ? flag : ALREADY_STOPPED;
  }

  /**
   * Creates or resets the stopped flag for a statement. Called only from startHeartbeat() when
   * setting up a new heartbeat — not from the heartbeat task itself.
   */
  private void resetStoppedFlag(StatementId statementId) {
    stoppedFlags.put(statementId, new java.util.concurrent.atomic.AtomicBoolean(false));
  }

  /** Stops all heartbeats and shuts down the scheduler. Called on Connection.close(). */
  void shutdown() {
    isShutdown = true;

    // Set all stopped flags FIRST — prevents in-flight RPCs from calling closed clients
    for (java.util.concurrent.atomic.AtomicBoolean flag : stoppedFlags.values()) {
      flag.set(true);
    }
    stoppedFlags.clear();

    for (Map.Entry<StatementId, ScheduledFuture<?>> entry : activeHeartbeats.entrySet()) {
      entry.getValue().cancel(false);
      LOGGER.debug(
          "Stopped heartbeat for statement {} during shutdown",
          entry.getKey().toSQLExecStatementId());
    }
    activeHeartbeats.clear();

    // Wait for in-flight RPCs to complete. 10s gives reasonable headroom for HTTP
    // timeouts to fire first (~300s connection timeout won't be an issue here since
    // stopped flags are already set, so RPCs will short-circuit on next check).
    // If tasks don't finish in time, shutdownNow() sends interrupts.
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        LOGGER.debug("Heartbeat tasks did not terminate in 10s, forcing shutdown");
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }

    LOGGER.debug("Heartbeat manager shut down");
  }

  /** Returns the number of active heartbeats. Visible for testing. */
  int getActiveHeartbeatCount() {
    return activeHeartbeats.size();
  }

  boolean isShutdown() {
    return isShutdown;
  }

  int getIntervalSeconds() {
    return intervalSeconds;
  }
}
