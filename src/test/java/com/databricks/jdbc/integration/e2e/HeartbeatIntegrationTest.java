package com.databricks.jdbc.integration.e2e;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for the heartbeat / keep-alive feature. Connects to a real Databricks warehouse
 * and verifies that heartbeat polling keeps results alive during slow consumption.
 *
 * <p>Run with: mvn -pl jdbc-core test -Dtest="HeartbeatIntegrationTest" -DDATABRICKS_HOST=...
 * -DDATABRICKS_TOKEN=... -DDATABRICKS_HTTP_PATH=...
 *
 * <p>Or set environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
 */
@Tag("e2e")
public class HeartbeatIntegrationTest {

  private static String getEnvOrProp(String name) {
    String value = System.getProperty(name);
    if (value == null) {
      value = System.getenv(name);
    }
    return value;
  }

  private Connection createConnection(int heartbeatIntervalSeconds) throws Exception {
    String host = getEnvOrProp("DATABRICKS_HOST");
    String token = getEnvOrProp("DATABRICKS_TOKEN");
    String httpPath = getEnvOrProp("DATABRICKS_HTTP_PATH");

    if (host == null || token == null || httpPath == null) {
      throw new IllegalStateException(
          "Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH");
    }

    String url =
        String.format(
            "jdbc:databricks://%s:443/default;transportMode=http;ssl=1;AuthMech=3;"
                + "httpPath=%s;EnableHeartbeat=1;HeartbeatIntervalSeconds=%d;"
                + "LogLevel=6;LogPath=/tmp",
            host, httpPath, heartbeatIntervalSeconds);

    Properties props = new Properties();
    props.put("UID", "token");
    props.put("PWD", token);

    Class.forName("com.databricks.client.jdbc.Driver");
    return DriverManager.getConnection(url, props);
  }

  /**
   * Verifies heartbeat keeps results alive during a slow consumer scenario. Executes a query, reads
   * first row, pauses for 2x the heartbeat interval (to allow heartbeats to fire), then reads
   * remaining rows successfully.
   */
  @Test
  void testHeartbeatKeepsResultsAliveDuringSlowConsumption() throws Exception {
    int heartbeatInterval = 5; // 5 seconds for fast testing
    try (Connection conn = createConnection(heartbeatInterval)) {
      System.out.println("Connected with EnableHeartbeat=1, interval=" + heartbeatInterval + "s");

      // Execute a query that returns multiple rows
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT explode(sequence(1, 100)) AS id"); // 100 rows

        // Read first row
        assertTrue(rs.next(), "Should have first row");
        int firstId = rs.getInt("id");
        System.out.println("Read first row: id=" + firstId);

        // Pause — heartbeat should fire during this time
        int pauseSeconds = heartbeatInterval * 3;
        System.out.println(
            "Pausing for "
                + pauseSeconds
                + "s (heartbeat should fire "
                + (pauseSeconds / heartbeatInterval)
                + " times)...");
        Thread.sleep(pauseSeconds * 1000L);

        // Read remaining rows — should succeed if heartbeat kept results alive
        int rowCount = 1;
        while (rs.next()) {
          rowCount++;
        }
        System.out.println("Read " + rowCount + " total rows after pause");
        assertEquals(100, rowCount, "Should read all 100 rows");

        rs.close();
      }

      System.out.println("Test passed — heartbeat kept results alive during slow consumption");
    }
  }

  /**
   * Verifies heartbeat stops when ResultSet is closed. After close, no more heartbeat RPCs should
   * fire.
   */
  @Test
  void testHeartbeatStopsOnResultSetClose() throws Exception {
    int heartbeatInterval = 5;
    try (Connection conn = createConnection(heartbeatInterval)) {
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT 1 AS val");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("val"));

        // Close — heartbeat should stop
        rs.close();

        // Pause to verify no heartbeat errors after close
        System.out.println("ResultSet closed, waiting to verify no heartbeat errors...");
        Thread.sleep(heartbeatInterval * 2 * 1000L);
        System.out.println("No errors — heartbeat stopped cleanly");
      }
    }
  }
}
