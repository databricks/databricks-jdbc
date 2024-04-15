package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LargeQueriesBenchmarkingTest {
  private Connection connection;

  private String SCHEMA_NAME = "main";
  private String TABLE_NAME = "tpcds_sf100_delta.catalog_sales";

  private static final String RESULTS_TABLE =
      "main.jdbc_large_queries_benchmarking_schema.benchmarking_results";
  private int ATTEMPTS = 10;

  private int ROWS = 1000000;

  long totalTimeForOSSDriver = 0;
  long totalTimeForDatabricksDriver = 0;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getBenchmarkingJDBCConnection();
  }

  @AfterEach
  void tearDown() throws SQLException {
    insertBenchmarkingDataIntoBenchfood();
    connection.close();
  }

  @Test
  void testLargeQueries() throws SQLException {
    // Currently connection is held by OSS driver
    long startTime = System.currentTimeMillis();
    measureLargeQueriesPerformance();
    long endTime = System.currentTimeMillis();
    System.out.println(
        "Time taken to execute large queries by OSS Driver: "
            + (endTime - startTime)
            + "ms "
            + "for "
            + ROWS
            + " rows and "
            + ATTEMPTS
            + " attempts");
    totalTimeForOSSDriver = endTime - startTime;

    connection.close();

    DriverManager.deregisterDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    connection = getBenchmarkingJDBCConnection();

    startTime = System.currentTimeMillis();
    measureLargeQueriesPerformance();
    endTime = System.currentTimeMillis();
    System.out.println(
        "Time taken to execute large queries by Databricks Driver: "
            + (endTime - startTime)
            + "ms "
            + "for "
            + ROWS
            + " rows and "
            + ATTEMPTS
            + " attempts");
    totalTimeForDatabricksDriver = endTime - startTime;

    connection.close();
  }

  void measureLargeQueriesPerformance() {
    Random random = new Random();
    for (int i = 0; i < ATTEMPTS; i++) {
      System.out.println("Attempt: " + i);
      int offset =
          i * 1000000 + random.nextInt(1000); // Randomization to avoid possible query caching
      try (Statement statement = connection.createStatement()) {
        statement.executeQuery(
            "SELECT * FROM "
                + SCHEMA_NAME
                + "."
                + TABLE_NAME
                + " LIMIT "
                + ROWS
                + " OFFSET "
                + offset);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private void insertBenchmarkingDataIntoBenchfood() throws SQLException {
    connection = getBenchfoodJDBCConnection();

    String sql =
        "INSERT INTO "
            + RESULTS_TABLE
            + "(DateTime, OSS_AVG, DATABRICKS_AVG, OSS_TOTAL, DATABRICKS_TOTAL) VALUES (?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setTimestamp(1, java.sql.Timestamp.valueOf(LocalDateTime.now()));
      stmt.setDouble(2, ((totalTimeForOSSDriver * 1.0) / ATTEMPTS));
      stmt.setDouble(3, ((totalTimeForDatabricksDriver * 1.0) / ATTEMPTS));
      stmt.setLong(4, totalTimeForOSSDriver);
      stmt.setLong(5, totalTimeForDatabricksDriver);
      stmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
