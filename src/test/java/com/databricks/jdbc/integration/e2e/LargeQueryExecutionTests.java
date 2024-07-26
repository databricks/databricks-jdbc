package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class LargeQueryExecutionTests {
  @Test
  void testQueryYieldingLargeNarrowResultSet() throws SQLException {
    String largeQuerySQL = "SELECT * FROM range(37500000)"; // ~300MB of data
    ResultSet rs = executeQuery(largeQuerySQL);
    int rows = 0;
    while (rs != null && rs.next()) {
      rows++;
    }
    assertEquals(37500000, rows, "Expected 37500000 rows, got " + rows);
  }

  @Test
  void testQueryYieldingLargeWideResultSet() throws SQLException {
    int resultSize = 300 * 1000 * 100; // 30 MB
    int width = 8192; // B
    int rows = resultSize / width;
    int cols = width / 36;

    // Generate the UUID columns
    String uuids =
        IntStream.rangeClosed(0, cols)
            .mapToObj(i -> "uuid() uuid" + i)
            .collect(Collectors.joining(", "));

    // Create the SQL query
    String query = String.format("SELECT id, %s FROM RANGE(%d)", uuids, rows);
    ResultSet rs = executeQuery(query);
    int rowCount = 0;
    while (rs != null && rs.next()) {
      assertEquals(rs.getInt("id"), rowCount, "Expected id to be equal to row number");
      assertEquals(rs.getString("uuid0").length(), 36, "Expected UUID length of 36");
      rowCount++;
    }
    assertEquals(rows, rowCount, "Expected " + rows + " rows, got " + rowCount);
  }
}
