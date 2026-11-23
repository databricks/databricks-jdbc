package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Utility class to handle statement execution timeouts. */
public class TimeoutHandler {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TimeoutHandler.class);
  private static final ThreadPoolExecutor CANCELLATIONS =
      new ThreadPoolExecutor(
          0,
          64,
          30,
          TimeUnit.SECONDS,
          new SynchronousQueue<>(),
          task -> {
            Thread thread = new Thread(task, "databricks-jdbc-timeout-cancel");
            thread.setDaemon(true);
            return thread;
          });

  private final long startTimeMillis;
  private final int timeoutSeconds;
  private final String operationDescription;
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
    this.startTimeMillis = System.currentTimeMillis();
    this.timeoutSeconds = timeoutSeconds;
    this.operationDescription = operationDescription;
    this.onTimeoutAction = onTimeoutAction;
    this.internalErrorCode = internalErrorCode;
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
    checkTimeout();
    if (timeoutSeconds <= 0) {
      return poll.execute();
    }
    try (PollingDeadline deadline = new PollingDeadline(Math.max(0, remainingMillis()))) {
      T result;
      try {
        result = poll.execute();
      } catch (Exception failure) {
        if (deadline.isExpired() || remainingMillis() <= 0) {
          throw timeoutException(failure);
        }
        @SuppressWarnings("unchecked")
        E originalFailure = (E) failure;
        throw originalFailure;
      }
      if (deadline.isExpired() || remainingMillis() <= 0) {
        throw timeoutException(new TimeoutException("Status poll exceeded its deadline"));
      }
      return result;
    }
  }

  public void sleep(long millis) throws InterruptedException, DatabricksTimeoutException {
    executeWithTimeout(
        () -> {
          TimeUnit.MILLISECONDS.sleep(millis);
          return null;
        });
  }

  private long remainingMillis() {
    return TimeUnit.SECONDS.toMillis(timeoutSeconds)
        - (System.currentTimeMillis() - startTimeMillis);
  }

  private DatabricksTimeoutException timeoutException(Throwable cause) {
    if (onTimeoutAction != null) {
      try {
        CANCELLATIONS.execute(
            () -> {
              try (PollingDeadline ignored = new PollingDeadline(TimeUnit.SECONDS.toMillis(30))) {
                onTimeoutAction.run();
              } catch (Exception failure) {
                LOGGER.warn("Failed to execute timeout action");
              }
            });
      } catch (RejectedExecutionException failure) {
        LOGGER.warn("Timeout cancellation workers are busy");
      }
    }
    String message =
        String.format(
            "Statement execution timed-out after %d seconds. Operation: %s",
            timeoutSeconds, operationDescription);
    LOGGER.error(message);
    return new DatabricksTimeoutException(message, cause, internalErrorCode);
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

    return new TimeoutHandler(
        timeoutSeconds,
        "Statement ID: " + StatementId.loggableStatementId(statementId),
        () -> {
          try {
            client.cancelStatement(statementId);
          } catch (Exception e) {
            LOGGER.warn("Cancel statement on timeout failed: " + e.getMessage());
          }
        },
        internalErrorCode);
  }
}
