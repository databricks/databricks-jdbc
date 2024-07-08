package com.databricks.jdbc.integration.benchmarking;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class LargeQueriesBenchmarkingTest {
  private Connection connection;
  private static final String SCHEMA_NAME = "main";
  private static final String TABLE_NAME = "tpcds_sf100_delta.catalog_sales";

  private static String RESULTS_TABLE =
          "main.jdbc_large_queries_benchmarking_schema.benchmarking_results";
  private static final int ATTEMPTS = 10;

  private static final int ROWS = 1000000;

  long timesForOSSDriver[] = new long[ATTEMPTS];
  long timesForDatabricksDriver[] = new long[ATTEMPTS];

  long totalTimeForOSSDriver = 0;
  long totalTimeForDatabricksDriver = 0;

  @BeforeEach
  void setUp() throws SQLException {
    // No setup needed here since we will handle connections in the test method
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  static Stream<String> modeProvider() {
    return Stream.of("SEA", "THRIFT");
  }

  @ParameterizedTest
  @MethodSource("modeProvider")
  void testLargeQueries(String mode) throws SQLException {
    runTestsForMode(mode);
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    insertBenchmarkingDataIntoBenchfood();
  }

  private void runTestsForMode(String mode) throws SQLException {
    switch (mode) {
      case "SEA":
        connection = getBenchmarkingJDBCConnection();
        RESULTS_TABLE = "main.jdbc_large_queries_benchmarking_schema.benchmarking_results";
        break;
      case "THRIFT":
        connection = getBenchmarkingJDBCConnectionForThrift();
                DriverManager.getConnection(
                        getBenchmarkingJDBCUrlForThrift(),
                        "token",
                        getDatabricksBenchmarkingToken());
        RESULTS_TABLE = "main.jdbc_thrift_large_queries_benchmarking_schema.benchmarking_results";
        break;
      default:
        throw new IllegalArgumentException("Invalid testing mode");
    }

    runTestsForDriver(1); // Test for OSS driver
    switchDriver(mode);
    runTestsForDriver(2); // Test for Databricks driver

    connection.close();
  }

  private void runTestsForDriver(int recording) throws SQLException {
    long startTime = System.currentTimeMillis();
    measureLargeQueriesPerformance(recording);
    long endTime = System.currentTimeMillis();
    System.out.println(
            "Time taken to execute large queries by " + (recording == 1 ? "OSS" : "Databricks") + " Driver: "
                    + (endTime - startTime)
                    + "ms for "
                    + ROWS
                    + " rows and "
                    + ATTEMPTS
                    + " attempts");

    System.out.println(
            "Driver used : "
                    + connection.getMetaData().getDriverVersion()
                    + " "
                    + connection.getMetaData().getDriverName());
  }

  private void switchDriver(String mode) throws SQLException {
    connection.close();
    Enumeration<Driver> drivers = DriverManager.getDrivers();

    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      if (driver.getClass().getName().contains("DatabricksDriver")) {
        DriverManager.deregisterDriver(driver);
      }
    }

    switch (mode) {
      case "SEA":
        connection =
                DriverManager.getConnection(
                        getBenchmarkingJDBCUrl(), "token", getDatabricksBenchmarkingToken());
        break;
      case "THRIFT":
        connection =
                DriverManager.getConnection(
                        getBenchmarkingJDBCUrlForThrift(),
                        "token",
                        getDatabricksBenchmarkingToken());
        break;
      default:
        throw new IllegalArgumentException("Invalid testing mode");
    }
  }

  void measureLargeQueriesPerformance(int recording) {
    Random random = new Random();
    for (int i = 0; i < ATTEMPTS; i++) {
      System.out.println("Attempt: " + i);
      int offset =
              i * 1000000 + random.nextInt(1000000); // Randomization to avoid possible query caching
      try (Statement statement = connection.createStatement()) {
        long startTime = System.currentTimeMillis();
        ResultSet rs =
                statement.executeQuery(
                        "SELECT * FROM "
                                + SCHEMA_NAME
                                + "."
                                + TABLE_NAME
                                + " LIMIT "
                                + ROWS
                                + " OFFSET "
                                + offset);
        int cnt = 0;
        while (rs.next()) {
          cnt++;
        }
        System.out.println("Number of rows fetched: " + cnt);
        long endTime = System.currentTimeMillis();
        if (recording == 1) {
          timesForOSSDriver[i] = endTime - startTime;
        } else {
          timesForDatabricksDriver[i] = endTime - startTime;
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private void insertBenchmarkingDataIntoBenchfood() throws SQLException {
    Arrays.sort(timesForOSSDriver);
    Arrays.sort(timesForDatabricksDriver);

    // Removing min and max times for better average calculation
    for (int i = 1; i < ATTEMPTS - 1; i++) {
      totalTimeForOSSDriver += timesForOSSDriver[i];
      totalTimeForDatabricksDriver += timesForDatabricksDriver[i];
    }

    connection = getBenchfoodJDBCConnection();

    String sql =
            "INSERT INTO "
                    + RESULTS_TABLE
                    + "(DateTime, OSS_AVG, DATABRICKS_AVG, OSS_TOTAL, DATABRICKS_TOTAL) VALUES (?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setTimestamp(1, java.sql.Timestamp.valueOf(LocalDateTime.now()));
      stmt.setDouble(2, ((totalTimeForOSSDriver * 1.0) / (ATTEMPTS - 2)));
      stmt.setDouble(3, ((totalTimeForDatabricksDriver * 1.0) / (ATTEMPTS - 2)));
      stmt.setLong(4, totalTimeForOSSDriver);
      stmt.setLong(5, totalTimeForDatabricksDriver);
      stmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
