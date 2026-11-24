package com.databricks.jdbc.dbclient.impl.common;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Request-local cancellation for a status poll, including its retry waits. */
public final class PollingDeadline implements AutoCloseable {
  private static final ThreadLocal<PollingDeadline> CURRENT = new ThreadLocal<>();
  private static final ScheduledThreadPoolExecutor TIMER = createTimer();

  private final Thread owner = Thread.currentThread();
  private final boolean previouslyInterrupted = owner.isInterrupted();
  private final PollingDeadline previous = CURRENT.get();
  private final CountDownLatch interruptSent = new CountDownLatch(1);
  private final ScheduledFuture<?> timer;
  private AbortRegistration abortRequest;
  private boolean externallyInterrupted;
  private boolean expired;
  private boolean closed;

  private static ScheduledThreadPoolExecutor createTimer() {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            task -> {
              Thread thread = new Thread(task, "databricks-jdbc-poll-deadline");
              thread.setDaemon(true);
              thread.setContextClassLoader(null);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    executor.setKeepAliveTime(1, TimeUnit.SECONDS);
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  PollingDeadline(long remainingMillis) {
    this(remainingMillis, TIMER);
  }

  PollingDeadline(long remainingMillis, ScheduledThreadPoolExecutor executor) {
    timer = executor.schedule(this::expire, remainingMillis, TimeUnit.MILLISECONDS);
    CURRENT.set(this);
  }

  public static boolean isCurrentExpired() {
    PollingDeadline deadline = CURRENT.get();
    return deadline != null && deadline.isExpired();
  }

  static boolean registerAbort(Object resource, Runnable abort) {
    PollingDeadline deadline = CURRENT.get();
    if (deadline != null) {
      synchronized (deadline) {
        if (!deadline.expired) {
          deadline.abortRequest = new AbortRegistration(resource, abort);
          return true;
        }
      }
      abort.run();
      return false;
    }
    return true;
  }

  static void clearAbort(Object resource) {
    PollingDeadline deadline = CURRENT.get();
    AbortRegistration registration = null;
    if (deadline != null) {
      synchronized (deadline) {
        if (deadline.abortRequest != null && deadline.abortRequest.resource == resource) {
          registration = deadline.abortRequest;
          deadline.abortRequest = null;
        }
      }
    }
    if (registration != null) {
      registration.disarm();
    }
  }

  private void expire() {
    AbortRegistration registration;
    synchronized (this) {
      if (closed) {
        return;
      }
      expired = true;
      registration = abortRequest;
      externallyInterrupted = owner.isInterrupted();
    }
    try {
      owner.interrupt();
    } finally {
      interruptSent.countDown();
    }
    if (registration != null) {
      registration.abort();
    }
  }

  synchronized boolean isExpired() {
    return expired;
  }

  @Override
  public void close() {
    AbortRegistration registration;
    boolean wasExpired;
    synchronized (this) {
      closed = true;
      timer.cancel(false);
      registration = abortRequest;
      abortRequest = null;
      wasExpired = expired;
    }
    if (registration != null) {
      registration.disarm();
    }
    if (previous == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(previous);
    }
    if (wasExpired) {
      boolean waiting = true;
      while (waiting) {
        try {
          interruptSent.await();
          waiting = false;
        } catch (InterruptedException ignored) {
        }
      }
      Thread.interrupted();
    }
    if (previouslyInterrupted || externallyInterrupted) {
      owner.interrupt();
    }
  }

  private static final class AbortRegistration {
    private final Object resource;
    private final Runnable action;
    private boolean active = true;

    private AbortRegistration(Object resource, Runnable action) {
      this.resource = resource;
      this.action = action;
    }

    private synchronized void abort() {
      if (active) {
        action.run();
      }
    }

    private synchronized void disarm() {
      active = false;
    }
  }
}
