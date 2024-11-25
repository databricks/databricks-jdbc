package com.databricks.jdbc.integration.benchmarking;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.databricks.jdbc.integration.IntegrationTestUtil.getBenchmarkingJDBCConnection;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
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

  private Driver simbaDriver;

  long timesForOSSDriver[] = new long[ATTEMPTS];
  long timesForDatabricksDriver[] = new long[ATTEMPTS];

  long totalTimeForOSSDriver = 0;
  long totalTimeForDatabricksDriver = 0;

  @BeforeEach
  void setUp() throws SQLException {
    // No setup needed here since we will handle connections in the test method
    loadSimbaDriver();
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  static Stream<String> modeProvider() {
    return Stream.of("SEA", "THRIFT", "THRIFT_ALL_PURPOSE_CLUSTER");
  }

  @ParameterizedTest
  @MethodSource("modeProvider")
  void testLargeQueries(String mode) throws SQLException {
    runTestsForMode(mode);

    DriverManager.registerDriver(new com.databricks.client.jdbc.Driver());
    insertBenchmarkingDataIntoBenchfood();
  }

  private void warmupCompute() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT 1");
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void runTestsForMode(String mode) throws SQLException {
    switch (mode) {
      case "SEA":
        connection = getBenchmarkingJDBCConnection();
        RESULTS_TABLE = "main.jdbc_large_queries_benchmarking_schema.benchmarking_results";
        break;
      case "THRIFT":
        connection = getBenchmarkingJDBCConnectionForThrift();
        RESULTS_TABLE = "main.jdbc_thrift_large_queries_benchmarking_schema.benchmarking_results";
        break;
      case "THRIFT_ALL_PURPOSE_CLUSTER":
        connection = getBenchmarkingJDBCConnectionForThriftAllPurposeCluster();
        RESULTS_TABLE =
            "main.jdbc_thrift_large_queries_benchmarking_schema.benchmarking_results_all_purpose";
        break;
      default:
        throw new IllegalArgumentException("Invalid testing mode");
    }

    // Warmup compute to ensure benchmarking results are not skewed towards the first driver
    warmupCompute();

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
        "Time taken to execute large queries by "
            + (recording == 1 ? "OSS" : "Databricks")
            + " Driver: "
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

    switch (mode) {
      case "SEA":
      case "THRIFT":
        connection =
            getConnectionForSimbaDriver(
                getBenchmarkingJDBCUrl(), "token", getDatabricksBenchmarkingToken());
        break;
      case "THRIFT_ALL_PURPOSE_CLUSTER":
        connection =
            getConnectionForSimbaDriver(
                getBenchmarkingJDBCUrlForThriftAllPurposeCluster(),
                "token",
                getThriftDatabricksToken());
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
                "SELECT * FROM " + TABLE_NAME + " LIMIT " + ROWS + " OFFSET " + offset);
        int cnt = 0;
        while (rs.next()) {
          cnt++;
        }
        System.out.println("Number of rows fetched: " + cnt);
        long endTime = System.currentTimeMillis();
        System.out.println(
            "Time taken to execute large queries by "
                + (recording == 1 ? "OSS" : "Databricks")
                + " Driver: "
                + (endTime - startTime)
                + "ms");
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
      stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
      stmt.setDouble(2, ((totalTimeForOSSDriver * 1.0) / (ATTEMPTS - 2)));
      stmt.setDouble(3, ((totalTimeForDatabricksDriver * 1.0) / (ATTEMPTS - 2)));
      stmt.setLong(4, totalTimeForOSSDriver);
      stmt.setLong(5, totalTimeForDatabricksDriver);
      stmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private void loadSimbaDriver() {
    try {
      File file = new File("src/test/resources/DatabricksJDBC42.jar");
      URL url = file.toURI().toURL();

      URLClassLoader urlClassLoader =
          new CustomClassLoader(new URL[] {url}, this.getClass().getClassLoader());

      Class<?> driverClass =
          Class.forName("com.databricks.client.jdbc.Driver", true, urlClassLoader);
      simbaDriver = (Driver) driverClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Connection getConnectionForSimbaDriver(String url, String user, String password)
      throws SQLException {
    Properties props = new Properties();
    props.put("user", user);
    props.put("password", password);
    return simbaDriver.connect(url, props);
  }
}
