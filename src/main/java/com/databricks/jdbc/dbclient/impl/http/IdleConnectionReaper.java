package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.conn.HttpClientConnectionManager;

/**
 * Closes expired and idle pooled connections for every {@link DatabricksHttpClient} from one daemon
 * thread, instead of one {@code IdleConnectionEvictor} thread per client. Each registration keeps
 * its own connection manager and idle limit, on the same schedule as {@code IdleConnectionEvictor}.
 * The thread starts with the first registration and stops when the last one is closed.
 */
final class IdleConnectionReaper {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(IdleConnectionReaper.class);
  @VisibleForTesting static final String THREAD_NAME = "databricks-jdbc-connection-evictor";
  // IdleConnectionEvictor's check interval when there is no idle limit.
  private static final long DEFAULT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);

  private static ScheduledThreadPoolExecutor executor; // guarded by IdleConnectionReaper.class
  private static int registrations; // guarded by IdleConnectionReaper.class

  private IdleConnectionReaper() {}

  /** Starts evicting connections of {@code manager} idle longer than {@code maxIdle}. */
  static synchronized Registration register(
      HttpClientConnectionManager manager, long maxIdle, TimeUnit unit) {
    if (executor == null) {
      executor =
          new ScheduledThreadPoolExecutor(
              1,
              runnable -> {
                Thread thread = new Thread(runnable, THREAD_NAME);
                thread.setDaemon(true);
                return thread;
              });
      executor.setRemoveOnCancelPolicy(true);
    }
    long maxIdleMs = unit.toMillis(maxIdle);
    long intervalMs = maxIdleMs > 0 ? maxIdleMs : DEFAULT_INTERVAL_MS;
    ScheduledFuture<?> future =
        executor.scheduleWithFixedDelay(
            () -> evict(manager, maxIdleMs), intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    registrations++;
    return new Registration(future);
  }

  private static void evict(HttpClientConnectionManager manager, long maxIdleMs) {
    try {
      manager.closeExpiredConnections();
      if (maxIdleMs > 0) {
        manager.closeIdleConnections(maxIdleMs, TimeUnit.MILLISECONDS);
      }
    } catch (RuntimeException e) {
      // Keep the shared schedule alive; a failing pool must not stop eviction for the others.
      LOGGER.debug("Idle connection eviction failed: {}", e.getMessage());
    }
  }

  private static synchronized void release() {
    if (--registrations == 0) {
      executor.shutdown();
      executor = null;
    }
  }

  @VisibleForTesting
  static synchronized boolean isRunning() {
    return executor != null;
  }

  @VisibleForTesting
  static synchronized int registrationCount() {
    return registrations;
  }

  /** An active eviction schedule for one connection manager. */
  static final class Registration implements AutoCloseable {
    private final ScheduledFuture<?> future;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Registration(ScheduledFuture<?> future) {
      this.future = future;
    }

    boolean isActive() {
      return !closed.get();
    }

    /** Stops eviction for this manager. Idempotent. */
    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        future.cancel(false);
        release();
      }
    }
  }
}
