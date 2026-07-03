package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Negative async-execution cases for the Databricks extension API ({@link IDatabricksStatement}):
 * {@code getExecutionResult()} before any async execution, {@code executeAsync} on invalid SQL, and
 * {@code getExecutionResult()} after the async statement was closed. Compares how the two endpoints
 * surface each failure via {@link ErrorDiffs}.
 *
 * <p>Obtains the extension via {@code stmt.unwrap(IDatabricksStatement.class)}. Read-only; safe on
 * the shared connections. The statement is closed in a {@code finally}.
 */
public class NegativeAsyncProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  private static final String RESULT_BEFORE_EXEC =
      "getExecutionResult() before any async execution";
  private static final String ASYNC_INVALID_SQL = "executeAsync() on invalid SQL";
  private static final String RESULT_AFTER_CLOSE =
      "getExecutionResult() after the statement is closed";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(RESULT_BEFORE_EXEC, RESULT_BEFORE_EXEC),
        new TestCase(ASYNC_INVALID_SQL, ASYNC_INVALID_SQL),
        new TestCase(RESULT_AFTER_CLOSE, RESULT_AFTER_CLOSE));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String id = testCase.getIdentifier();
    CapturedOutcome left = Captures.capture(() -> runCase(conn1, id));
    CapturedOutcome right = Captures.capture(() -> runCase(conn2, id));
    ComparisonResult result = new ComparisonResult(label, id, testCase.getArgs());
    ErrorDiffs.foldInto(result, left, right, "result ", "");
    return result;
  }

  private Object runCase(Connection conn, String id) throws Exception {
    Statement stmt = conn.createStatement();
    IDatabricksStatement async = stmt.unwrap(IDatabricksStatement.class);
    try {
      switch (id) {
        case RESULT_BEFORE_EXEC:
          return async.getExecutionResult(); // no async execution has been submitted
        case ASYNC_INVALID_SQL:
          {
            // executeAsync only SUBMITS; the syntax error surfaces on result retrieval. Poll
            // getExecutionResult and drain it so the async failure is actually captured (not the
            // successful submit).
            async.executeAsync("SELET 1 FROM " + TABLE); // syntax error
            for (int i = 0; i < 60; i++) {
              ResultSet rs = async.getExecutionResult(); // throws if the async query failed
              if (rs.next()) {
                return "returned a row"; // unexpected — no error
              }
              // No row yet; if the query is still running, wait and re-poll.
              Thread.sleep(500);
            }
            return "no error after polling";
          }
        case RESULT_AFTER_CLOSE:
          {
            async.executeAsync("SELECT id FROM " + TABLE + " LIMIT 1");
            stmt.close();
            return async.getExecutionResult(); // statement closed
          }
        default:
          throw new IllegalArgumentException("Unknown case: " + id);
      }
    } finally {
      try {
        stmt.close();
      } catch (Exception ignored) {
        // best-effort; some cases close it deliberately above
      }
    }
  }
}
