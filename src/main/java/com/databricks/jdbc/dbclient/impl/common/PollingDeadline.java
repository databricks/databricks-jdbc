package com.databricks.jdbc.dbclient.impl.common;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Request-local cancellation for a status poll, including its retry waits. */
final class PollingDeadline implements AutoCloseable {
  private static final ThreadLocal<PollingDeadline> CURRENT = new ThreadLocal<>();
  private static final ScheduledThreadPoolExecutor TIMER = createTimer();

  private final Thread owner = Thread.currentThread();
  private final boolean previouslyInterrupted = owner.isInterrupted();
  private final ScheduledFuture<?> timer;
  private Runnable abortRequest;
  private boolean expired;
  private boolean closed;

  private static ScheduledThreadPoolExecutor createTimer() {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            task -> {
              Thread thread = new Thread(task, "databricks-jdbc-poll-deadline");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  PollingDeadline(long remainingMillis) {
    CURRENT.set(this);
    timer = TIMER.schedule(this::expire, remainingMillis, TimeUnit.MILLISECONDS);
  }

  static boolean registerAbort(Runnable abort) {
    PollingDeadline deadline = CURRENT.get();
    if (deadline != null) {
      synchronized (deadline) {
        if (deadline.expired) {
          abort.run();
          return false;
        } else {
          deadline.abortRequest = abort;
        }
      }
    }
    return true;
  }

  static void clearAbort() {
    PollingDeadline deadline = CURRENT.get();
    if (deadline != null) {
      synchronized (deadline) {
        deadline.abortRequest = null;
      }
    }
  }

  private synchronized void expire() {
    if (!closed) {
      expired = true;
      try {
        if (abortRequest != null) {
          abortRequest.run();
        }
      } finally {
        owner.interrupt();
      }
    }
  }

  synchronized boolean isExpired() {
    return expired;
  }

  @Override
  public synchronized void close() {
    closed = true;
    timer.cancel(false);
    abortRequest = null;
    CURRENT.remove();
    if (expired && !previouslyInterrupted) {
      Thread.interrupted();
    }
  }
}
