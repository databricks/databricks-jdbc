package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Negative UC Volume cases — GET a missing file, PUT outside the allowed local paths, PUT a missing
 * local file, REMOVE a missing file, and a volume op after the statement is closed. Compares how
 * the two endpoints surface each failure via {@link ErrorDiffs}.
 *
 * <p>Runs under the {@code VOLUME_OPERATIONS} connection config (which sets {@code
 * VolumeOperationAllowedLocalPaths=/tmp}), matching the positive VOLUME_OPERATIONS suite. Uses the
 * shared connections — these ops fail without mutating connection/session state.
 */
public class NegativeVolumeProvider implements SuiteProvider {

  private static final String VOLUME_PATH =
      "/Volumes/comparator_tests/oss_jdbc_tests/comparator_volume";

  private static final String GET_MISSING = "GET missing volume file";
  private static final String PUT_DISALLOWED = "PUT from a disallowed local path";
  private static final String PUT_MISSING_LOCAL = "PUT a missing local file";
  private static final String REMOVE_MISSING = "REMOVE a missing file";
  private static final String OP_AFTER_CLOSE = "Volume op after statement close";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(GET_MISSING, GET_MISSING),
        new TestCase(PUT_DISALLOWED, PUT_DISALLOWED),
        new TestCase(PUT_MISSING_LOCAL, PUT_MISSING_LOCAL),
        new TestCase(REMOVE_MISSING, REMOVE_MISSING),
        new TestCase(OP_AFTER_CLOSE, OP_AFTER_CLOSE));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String id = testCase.getIdentifier();
    CapturedOutcome left = Captures.capture(() -> runOp(conn1, id));
    CapturedOutcome right = Captures.capture(() -> runOp(conn2, id));
    ComparisonResult result = new ComparisonResult(label, id, testCase.getArgs());
    ErrorDiffs.foldInto(result, left, right, "result ", "");
    return result;
  }

  private Object runOp(Connection conn, String id) throws Exception {
    switch (id) {
      case GET_MISSING:
        return exec(conn, "GET '" + VOLUME_PATH + "/__no_such_file__.txt' TO '/tmp/neg_get.txt'");
      case PUT_DISALLOWED:
        // /etc is outside VolumeOperationAllowedLocalPaths=/tmp -> driver-side rejection.
        return exec(conn, "PUT '/etc/hostname' INTO '" + VOLUME_PATH + "/neg_put.txt' OVERWRITE");
      case PUT_MISSING_LOCAL:
        return exec(
            conn,
            "PUT '/tmp/__no_such_local_file__.txt' INTO '"
                + VOLUME_PATH
                + "/neg_put2.txt' OVERWRITE");
      case REMOVE_MISSING:
        return exec(conn, "REMOVE '" + VOLUME_PATH + "/__no_such_file__.txt'");
      case OP_AFTER_CLOSE:
        {
          Statement stmt = conn.createStatement();
          stmt.close();
          // Using a closed statement — expect a "statement closed" style error.
          return stmt.execute("REMOVE '" + VOLUME_PATH + "/whatever.txt'");
        }
      default:
        throw new IllegalArgumentException("Unknown case: " + id);
    }
  }

  private Object exec(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      return stmt.execute(sql);
    }
  }
}
