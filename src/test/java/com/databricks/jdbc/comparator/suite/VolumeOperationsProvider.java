package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Compares UC Volume PUT/GET/REMOVE operations between Thrift and SEA.
 *
 * <p>Creates a temp local file, PUTs it to the volume on both connections (separate paths), GETs it
 * back, verifies content, then REMOVEs. Compares return values and exceptions.
 */
public class VolumeOperationsProvider implements SuiteProvider {

  private static final String VOLUME_PATH =
      "/Volumes/comparator_tests/oss_jdbc_tests/comparator_volume";

  @Override
  public List<TestCase> getTestCases() {
    return Collections.singletonList(new TestCase("VOLUME_FLOW", "Volume PUT → GET → REMOVE"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    List<String> differences = new ArrayList<>();

    // Create temp file with known content
    Path tempFile = Files.createTempFile("comparator_volume_test_", ".txt");
    Files.writeString(tempFile, "comparator test data 12345");

    String remotePath1 = VOLUME_PATH + "/test_thrift.txt";
    String remotePath2 = VOLUME_PATH + "/test_sea.txt";
    String localPath = tempFile.toAbsolutePath().toString();

    // Create temp paths for GET (file must not exist before GET writes to it)
    Path getPath1 = Files.createTempFile("comparator_get_thrift_", ".txt");
    Path getPath2 = Files.createTempFile("comparator_get_sea_", ".txt");
    Files.delete(getPath1);
    Files.delete(getPath2);

    try {
      // 1. PUT
      compareSql(
          conn1,
          conn2,
          "PUT '" + localPath + "' INTO '" + remotePath1 + "' OVERWRITE",
          "PUT '" + localPath + "' INTO '" + remotePath2 + "' OVERWRITE",
          differences,
          "PUT");

      // 2. GET
      compareSql(
          conn1,
          conn2,
          "GET '" + remotePath1 + "' TO '" + getPath1.toAbsolutePath() + "'",
          "GET '" + remotePath2 + "' TO '" + getPath2.toAbsolutePath() + "'",
          differences,
          "GET");

      // Verify content
      if (getPath1.toFile().exists() && getPath2.toFile().exists()) {
        String content1 = Files.readString(getPath1);
        String content2 = Files.readString(getPath2);
        if (!content1.equals(content2)) {
          differences.add("GET content mismatch: '" + content1 + "' vs '" + content2 + "'");
        }
        String original = Files.readString(tempFile);
        if (!content1.equals(original)) {
          differences.add("Thrift GET content differs from original");
        }
        if (!content2.equals(original)) {
          differences.add("SEA GET content differs from original");
        }
      }

      // 3. REMOVE
      compareSql(
          conn1,
          conn2,
          "REMOVE '" + remotePath1 + "'",
          "REMOVE '" + remotePath2 + "'",
          differences,
          "REMOVE");

    } finally {
      Files.deleteIfExists(tempFile);
      Files.deleteIfExists(getPath1);
      Files.deleteIfExists(getPath2);
    }

    ComparisonResult result = new ComparisonResult(label, "VOLUME_FLOW", testCase.getArgs());
    result.dataDifferences = differences;
    return result;
  }

  private void compareSql(
      Connection conn1,
      Connection conn2,
      String sql1,
      String sql2,
      List<String> differences,
      String operation) {
    CapturedOutcome left = Captures.capture(() -> executeSql(conn1, sql1));
    CapturedOutcome right = Captures.capture(() -> executeSql(conn2, sql2));

    if (left.threw() || right.threw()) {
      // Honor the ERROR_COMPARISON_MODE gate: deep comparison when enabled, legacy class-only
      // when off (the rollout kill switch), matching the ResultSetComparator path.
      for (String d : ErrorDiffs.compare(left, right, "result ")) {
        differences.add(operation + ": " + d);
      }
    }
  }

  private boolean executeSql(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      return stmt.execute(sql);
    }
  }
}
