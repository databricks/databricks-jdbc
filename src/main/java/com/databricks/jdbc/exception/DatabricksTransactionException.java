package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

/**
 * Exception thrown for transaction-specific errors in the Databricks JDBC driver.
 *
 * <p>This exception is used when server-side transaction errors occur that don't have a
 * corresponding standard SQLException subclass. Examples include:
 *
 * <ul>
 *   <li>AUTOCOMMIT_SET_DURING_ACTIVE_TRANSACTION
 *   <li>AUTOCOMMIT_SET_FALSE_ALREADY_DISABLED
 *   <li>MULTI_STATEMENT_TRANSACTION_NO_ACTIVE_TRANSACTION
 * </ul>
 *
 * <p>For errors that have standard SQLException subclasses (e.g., transaction rollback required),
 * those standard exceptions should be used instead.
 */
public class DatabricksTransactionException extends SQLException {

  /**
   * Constructs a DatabricksTransactionException with the specified reason, SQL state, vendor code,
   * and cause.
   *
   * @param reason a description of the exception
   * @param sqlState the SQL state code
   * @param vendorCode the vendor-specific error code
   * @param cause the underlying cause of the exception
   */
  public DatabricksTransactionException(
      String reason, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
    logTelemetryEvent(sqlState, reason);
  }

  /**
   * Constructs a DatabricksTransactionException with the specified reason, driver error code, and
   * cause.
   *
   * <p>This constructor intelligently extracts vendor codes and SQL state from the cause exception:
   *
   * <ul>
   *   <li>If the cause is a SQLException with a non-zero error code, that vendor code is preserved
   *   <li>Otherwise, attempts to extract vendor code from the exception chain using
   *       DatabricksVendorCode
   *   <li>SQL state is preserved from the SQLException if available
   * </ul>
   *
   * @param reason a description of the exception
   * @param cause the underlying cause of the exception
   * @param internalError the driver error code (used for telemetry and as SQL state fallback)
   */
  public DatabricksTransactionException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError) {
    this(reason, getSqlStateFromCause(cause, internalError), getVendorCodeFromCause(cause), cause);
  }

  /**
   * Gets SQL state from the cause exception if available, otherwise uses the error code name.
   *
   * @param cause the underlying cause of the exception
   * @param internalError the driver error code
   * @return the SQL state string
   */
  private static String getSqlStateFromCause(
      Throwable cause, DatabricksDriverErrorCode internalError) {
    if (cause instanceof SQLException) {
      String sqlState = ((SQLException) cause).getSQLState();
      if (sqlState != null && !sqlState.isEmpty()) {
        return sqlState;
      }
    }
    return internalError.name();
  }

  /**
   * Gets vendor code from the cause exception. Respects existing vendor codes from SQLExceptions,
   * otherwise attempts to extract from the exception chain.
   *
   * @param cause the underlying cause of the exception
   * @return the vendor code (0 if none available)
   */
  private static int getVendorCodeFromCause(Throwable cause) {
    // First, check if the cause is a SQLException with an existing vendor code
    if (cause instanceof SQLException) {
      int vendorCode = ((SQLException) cause).getErrorCode();
      if (vendorCode != 0) {
        return vendorCode;
      }
    }
    // Otherwise, try to extract vendor code from the exception chain
    return DatabricksVendorCode.getVendorCode(cause);
  }

  private void logTelemetryEvent(String sqlState, String reason) {
    exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        sqlState,
        reason,
        TelemetryLogLevel.ERROR);
  }
}
