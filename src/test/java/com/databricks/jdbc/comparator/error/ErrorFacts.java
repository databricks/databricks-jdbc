package com.databricks.jdbc.comparator.error;

import java.sql.SQLException;

/**
 * The comparable fields read from a thrown {@link Throwable}, so an error can be compared
 * field-by-field the way a ResultSet's columns are.
 *
 * <p>Every field is populated defensively — a null message or a non-SQL Throwable produces a
 * well-formed {@code ErrorFacts} rather than an NPE. All fields come from real accessors; we make
 * no assumptions about the message's internal format.
 */
public final class ErrorFacts {

  public final String exceptionClass;
  public final String sqlState;
  public final int vendorCode;
  public final String message;

  private ErrorFacts(String exceptionClass, String sqlState, int vendorCode, String message) {
    this.exceptionClass = exceptionClass;
    this.sqlState = sqlState;
    this.vendorCode = vendorCode;
    this.message = message;
  }

  /** Reads comparable fields from a thrown Throwable. Never throws, never returns null. */
  public static ErrorFacts from(Throwable t) {
    String exceptionClass = t.getClass().getName();
    String message = t.getMessage();
    String sqlState = null;
    int vendorCode = 0;
    if (t instanceof SQLException) {
      SQLException sqlException = (SQLException) t;
      sqlState = sqlException.getSQLState();
      vendorCode = sqlException.getErrorCode();
    }
    return new ErrorFacts(exceptionClass, sqlState, vendorCode, message);
  }

  /** Short class name for concise diff strings (e.g. "DatabricksValidationException"). */
  public String simpleClassName() {
    int lastDot = exceptionClass.lastIndexOf('.');
    return lastDot >= 0 ? exceptionClass.substring(lastDot + 1) : exceptionClass;
  }
}
