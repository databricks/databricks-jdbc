package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.concurrent.TimeUnit;

/** Utility class to handle statement execution timeouts. */
public class TimeoutHandler {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TimeoutHandler.class);

  private final long startTimeMillis;
  private final int timeoutSeconds;
  private final String operationDescription;
  private final Runnable onTimeoutAction; // do something on timeout
  private final DatabricksDriverErrorCode
      internalErrorCode; // internal error code to be used for timeout error (if any)
  private final boolean includeVendorCode;
  private final boolean preciseDeadline;

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
    this(timeoutSeconds, operationDescription, onTimeoutAction, internalErrorCode, false);
  }

  public TimeoutHandler(
      int timeoutSeconds,
      String operationDescription,
      Runnable onTimeoutAction,
      DatabricksDriverErrorCode internalErrorCode,
      boolean includeVendorCode) {
    this(
        timeoutSeconds,
        operationDescription,
        onTimeoutAction,
        internalErrorCode,
        includeVendorCode,
        System.currentTimeMillis(),
        false);
  }

  private TimeoutHandler(
      int timeoutSeconds,
      String operationDescription,
      Runnable onTimeoutAction,
      DatabricksDriverErrorCode internalErrorCode,
      boolean includeVendorCode,
      long startTimeMillis,
      boolean preciseDeadline) {
    this.startTimeMillis = startTimeMillis;
    this.timeoutSeconds = timeoutSeconds;
    this.operationDescription = operationDescription;
    this.onTimeoutAction = onTimeoutAction;
    this.internalErrorCode = internalErrorCode;
    this.includeVendorCode = includeVendorCode;
    this.preciseDeadline = preciseDeadline;
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

    long currentTimeMillis = System.currentTimeMillis();
    long elapsedMillis = currentTimeMillis - startTimeMillis;
    if (preciseDeadline
        ? elapsedMillis >= TimeUnit.SECONDS.toMillis(timeoutSeconds)
        : TimeUnit.MILLISECONDS.toSeconds(elapsedMillis) > timeoutSeconds) {
      // Attempt to execute the timeout action
      try {
        if (onTimeoutAction != null) {
          onTimeoutAction.run();
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to execute timeout action: " + e.getMessage());
      }

      String timeoutErrorMessage =
          String.format(
              "Statement execution timed-out after %d seconds. Operation: %s",
              timeoutSeconds, operationDescription);
      LOGGER.error(timeoutErrorMessage);
      throw new DatabricksTimeoutException(
          timeoutErrorMessage, /*cause*/ null, internalErrorCode, includeVendorCode);
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
    return forStatement(timeoutSeconds, statementId, client, internalErrorCode, false);
  }

  /** Creates a timeout handler that omits sensitive statement details from diagnostics. */
  public static TimeoutHandler forStatement(
      int timeoutSeconds,
      StatementId statementId,
      IDatabricksClient client,
      DatabricksDriverErrorCode internalErrorCode,
      boolean redactDetails) {
    return forStatement(
        timeoutSeconds,
        statementId,
        client,
        internalErrorCode,
        redactDetails,
        System.currentTimeMillis());
  }

  /** Starts the deadline at the initial ExecuteStatement request, before its response arrives. */
  public static TimeoutHandler forStatement(
      int timeoutSeconds,
      StatementId statementId,
      IDatabricksClient client,
      DatabricksDriverErrorCode internalErrorCode,
      boolean redactDetails,
      long startTimeMillis) {

    return new TimeoutHandler(
        timeoutSeconds,
        "Statement ID: "
            + (redactDetails ? "[REDACTED]" : StatementId.loggableStatementId(statementId)),
        () -> {
          try {
            client.cancelStatement(statementId);
          } catch (Exception e) {
            LOGGER.warn(
                redactDetails
                    ? "Cancel statement on timeout failed"
                    : "Cancel statement on timeout failed: " + e.getMessage());
          }
        },
        internalErrorCode,
        false,
        startTimeMillis,
        statementId.computeResource() instanceof AllPurposeCluster);
  }
}
