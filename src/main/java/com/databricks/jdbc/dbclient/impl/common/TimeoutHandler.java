package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Utility class to handle statement execution timeouts. */
public class TimeoutHandler {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TimeoutHandler.class);
  private static final long STATUS_GRACE_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final ThreadPoolExecutor CANCELLATIONS =
      new ThreadPoolExecutor(
          0,
          Integer.MAX_VALUE,
          30,
          TimeUnit.SECONDS,
          new SynchronousQueue<>(),
          task -> {
            Thread thread = new Thread(task, "databricks-jdbc-timeout-cancel");
            thread.setDaemon(true);
            thread.setContextClassLoader(null);
            return thread;
          });

  private final long startTimeMillis;
  private final int timeoutSeconds;
  private final String operationDescription;
  private final IDatabricksConnectionContext connectionContext;
  private final AtomicBoolean cancellationDispatched = new AtomicBoolean();
  private DatabricksTimeoutException timeoutFailure;
  private final Runnable onTimeoutAction; // do something on timeout
  private final DatabricksDriverErrorCode
      internalErrorCode; // internal error code to be used for timeout error (if any)

  /**
   * Creates a new timeout handler with the provided parameters.
   *
   * @param timeoutSeconds Timeout in seconds, 0 means no timeout
   * @param operationDescription Description of the operation for logging
   * @param onTimeoutAction Runnable to call when a timeout occurs
   * @param internalErrorCode Internal driver error code to annotate timeout exceptions
   */
  public TimeoutHandler(
      int timeoutSeconds,
      String operationDescription,
      Runnable onTimeoutAction,
      DatabricksDriverErrorCode internalErrorCode) {
    this(
        timeoutSeconds,
        operationDescription,
        onTimeoutAction,
        internalErrorCode,
        DatabricksThreadContextHolder.getConnectionContext());
  }

  public TimeoutHandler(
      int timeoutSeconds,
      String operationDescription,
      Runnable onTimeoutAction,
      DatabricksDriverErrorCode internalErrorCode,
      IDatabricksConnectionContext connectionContext) {
    this.startTimeMillis = System.currentTimeMillis();
    this.timeoutSeconds = timeoutSeconds;
    this.operationDescription = operationDescription;
    this.onTimeoutAction = onTimeoutAction;
    this.internalErrorCode = internalErrorCode;
    this.connectionContext = connectionContext;
  }

  /**
   * Checks if the operation has timed out. If a timeout occurs, it attempts to execute the timeout
   * action and throws a DatabricksTimeoutException.
   *
   * @throws DatabricksTimeoutException if the operation timed out
   */
  public void checkTimeout() throws DatabricksTimeoutException {
    if (timeoutSeconds <= 0) {
      return; // Timeout not enabled
    }

    if (remainingMillis() <= 0) {
      throw timeoutException(null);
    }
  }

  @FunctionalInterface
  public interface Poll<T, E extends Exception> {
    T execute() throws E;
  }

  public <T, E extends Exception> T executeWithTimeout(Poll<T, E> poll)
      throws E, DatabricksTimeoutException {
    return executeWithTimeout(poll, STATUS_GRACE_MILLIS);
  }

  private <T, E extends Exception> T executeWithTimeout(Poll<T, E> poll, long graceMillis)
      throws E, DatabricksTimeoutException {
    checkTimeout();
    if (timeoutSeconds <= 0) {
      return poll.execute();
    }
    T result = null;
    Throwable timeoutCause = null;
    boolean expired;
    try (PollingDeadline deadline =
        new PollingDeadline(Math.max(0, remainingMillis() + graceMillis))) {
      try {
        result = poll.execute();
      } catch (Exception failure) {
        if (!deadline.isExpired()) {
          @SuppressWarnings("unchecked")
          E originalFailure = (E) failure;
          throw originalFailure;
        }
        timeoutCause = failure;
      }
      expired = deadline.isExpired() || remainingMillis() + graceMillis <= 0;
    }
    if (expired) {
      throw timeoutException(
          timeoutCause != null
              ? timeoutCause
              : new TimeoutException("Status poll exceeded its deadline"));
    }
    return result;
  }

  public void sleep(long millis) throws InterruptedException, DatabricksTimeoutException {
    executeWithTimeout(
        () -> {
          TimeUnit.MILLISECONDS.sleep(millis);
          return null;
        },
        0);
  }

  private long remainingMillis() {
    return TimeUnit.SECONDS.toMillis(timeoutSeconds)
        - (System.currentTimeMillis() - startTimeMillis);
  }

  private DatabricksTimeoutException timeoutException(Throwable cause) {
    DatabricksTimeoutException failure;
    synchronized (this) {
      if (timeoutFailure == null) {
        String message =
            String.format(
                "Statement execution timed-out after %d seconds. Operation: %s",
                timeoutSeconds, operationDescription);
        LOGGER.error(message);
        timeoutFailure = new DatabricksTimeoutException(message, cause, internalErrorCode);
      }
      failure = timeoutFailure;
    }
    if (onTimeoutAction != null && cancellationDispatched.compareAndSet(false, true)) {
      dispatchCancellation();
    }
    return failure;
  }

  private void dispatchCancellation() {
    String statementId = DatabricksThreadContextHolder.getStatementId();
    String sessionId = DatabricksThreadContextHolder.getSessionId();
    StatementType statementType = DatabricksThreadContextHolder.getStatementType();
    Runnable cancel =
        () -> {
          IDatabricksConnectionContext previousContext =
              DatabricksThreadContextHolder.getConnectionContext();
          String previousStatement = DatabricksThreadContextHolder.getStatementId();
          String previousSession = DatabricksThreadContextHolder.getSessionId();
          StatementType previousType = DatabricksThreadContextHolder.getStatementType();
          Integer previousRetry = DatabricksThreadContextHolder.getRetryCount();
          try {
            DatabricksThreadContextHolder.setConnectionContext(connectionContext);
            DatabricksThreadContextHolder.setStatementId(statementId);
            DatabricksThreadContextHolder.setSessionId(sessionId);
            DatabricksThreadContextHolder.setStatementType(statementType);
            try (PollingDeadline deadline = new PollingDeadline(TimeUnit.SECONDS.toMillis(30))) {
              onTimeoutAction.run();
              if (deadline.isExpired()) {
                LOGGER.warn("Timeout action exceeded 30 seconds for {}", operationDescription);
              }
            }
          } catch (Exception failure) {
            LOGGER.warn(
                "Failed to execute timeout action for {}: {}",
                operationDescription,
                failure.toString());
          } finally {
            DatabricksThreadContextHolder.clearAllContext();
            DatabricksThreadContextHolder.setSessionId(previousSession);
            DatabricksThreadContextHolder.setConnectionContext(previousContext);
            DatabricksThreadContextHolder.setStatementId(previousStatement);
            DatabricksThreadContextHolder.setStatementType(previousType);
            DatabricksThreadContextHolder.setRetryCount(previousRetry);
          }
        };
    boolean interrupted = Thread.interrupted();
    try {
      if (DriverUtil.isRunningAgainstFake()) {
        cancel.run();
      } else {
        awaitCancellation(cancel, CANCELLATIONS);
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  void awaitCancellation(Runnable cancel, Executor executor) {
    boolean interrupted = Thread.interrupted();
    FutureTask<Void> task = new FutureTask<>(cancel, null);
    try {
      try {
        executor.execute(task);
      } catch (Throwable unavailable) {
        LOGGER.warn(
            "Running timeout action inline for {}: {}",
            operationDescription,
            unavailable.toString());
        task.run();
      }
      long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (true) {
        try {
          task.get(Math.max(0, waitUntil - System.nanoTime()), TimeUnit.NANOSECONDS);
          return;
        } catch (InterruptedException externalInterrupt) {
          interrupted = true;
        } catch (TimeoutException stillRunning) {
          return;
        } catch (ExecutionException failure) {
          LOGGER.warn(
              "Failed to execute timeout action for {}: {}",
              operationDescription,
              failure.getCause().toString());
          return;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Factory method to create a timeout handler for a databricks client with a statement ID. This
   * works with any client that implements {@link IDatabricksClient}.
   *
   * @param timeoutSeconds Timeout in seconds
   * @param statementId The statement ID
   * @param client The Databricks client
   * @param internalErrorCode Internal driver error code to annotate timeout exceptions
   * @return A new TimeoutHandler instance
   */
  public static TimeoutHandler forStatement(
      int timeoutSeconds,
      StatementId statementId,
      IDatabricksClient client,
      DatabricksDriverErrorCode internalErrorCode) {
    return forStatement(
        timeoutSeconds,
        statementId,
        client,
        internalErrorCode,
        DatabricksThreadContextHolder.getConnectionContext());
  }

  public static TimeoutHandler forStatement(
      int timeoutSeconds,
      StatementId statementId,
      IDatabricksClient client,
      DatabricksDriverErrorCode internalErrorCode,
      IDatabricksConnectionContext connectionContext) {

    return new TimeoutHandler(
        timeoutSeconds,
        "Statement ID: " + StatementId.loggableStatementId(statementId),
        () -> {
          try {
            client.cancelStatement(statementId);
          } catch (Exception e) {
            LOGGER.warn(
                "Cancel statement on timeout failed for {}: {}",
                StatementId.loggableStatementId(statementId),
                e.getMessage());
          }
        },
        internalErrorCode,
        connectionContext);
  }
}
