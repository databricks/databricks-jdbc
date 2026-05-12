package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.COMMUNICATION_LINK_FAILURE_SQLSTATE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.SERIALIZATION_FAILURE_SQLSTATE;

/**
 * Reclassifies SQL states for known transient or mis-categorized server errors so callers can
 * programmatically identify retryable failures.
 *
 * <p>Patterns handled:
 *
 * <ul>
 *   <li>Unity Catalog unavailability ({@code UC_CLIENT_EXCEPTION}) → {@code 08S01} (communication
 *       link failure, retryable).
 *   <li>Connection-acquisition / parquet read deadlines ({@code PARQUET_FAILED_READ_FOOTER}, {@code
 *       DEADLINE_EXCEEDED: acquiring connection}) → {@code 08S01}.
 *   <li>Server-side {@code java.util.ConcurrentModificationException} mis-mapped to {@code 42000}
 *       (syntax/access violation) → {@code 40001} (serialization failure, retryable). Only applied
 *       when the original SQL state is {@code 42000} so unrelated {@code 42xxx} states are
 *       preserved.
 * </ul>
 *
 * <p>Patterns are anchored on stable server-emitted tokens (Spark error classes, fully-qualified
 * Java exception names) rather than English prose, so server message rewording does not silently
 * regress the classifier.
 */
public final class SqlStateClassifier {

  private static final String SYNTAX_OR_ACCESS_VIOLATION_SQLSTATE = "42000";

  private SqlStateClassifier() {}

  /**
   * Returns a remapped SQL state if {@code errorMessage} matches a known transient pattern, or
   * {@code originalSqlState} otherwise. Pure function — callers should log the remap separately
   * with their own context (statement ID, response).
   */
  public static String classifyTransientSqlState(String errorMessage, String originalSqlState) {
    if (errorMessage == null) {
      return originalSqlState;
    }
    // Spark error classes are the outer cause; check before nested-Java-exception patterns like
    // ConcurrentModificationException, which may appear inside a UC/Parquet wrapping.
    if (errorMessage.contains("UC_CLIENT_EXCEPTION")
        || errorMessage.contains("PARQUET_FAILED_READ_FOOTER")
        || errorMessage.contains("DEADLINE_EXCEEDED: acquiring connection")) {
      return COMMUNICATION_LINK_FAILURE_SQLSTATE;
    }
    if (SYNTAX_OR_ACCESS_VIOLATION_SQLSTATE.equals(originalSqlState)
        && errorMessage.contains("java.util.ConcurrentModificationException")) {
      return SERIALIZATION_FAILURE_SQLSTATE;
    }
    return originalSqlState;
  }
}
