package com.databricks.jdbc.integration.benchmarking;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class MetadataBenchmarkingTests {

  private Connection connection;
  private static final int NUM_SCHEMAS = 10;
  private static final int NUM_TABLES = 10;
  private static final int NUM_COLUMNS = 10;

  private static final int NUM_SECTIONS = 7;

  private static final int ATTEMPTS = 30;

  private static final String BASE_SCHEMA_NAME = "jdbc_new_metadata_benchmark_schema";

  private static String RESULTS_TABLE =
      "main.jdbc_new_metadata_benchmark_schema.benchmarking_results";

  private static final String BASE_TABLE_NAME = "table";

  long totalTimesForSection[][] = new long[2][NUM_SECTIONS];
  double avgTimesForSection[][] = new double[2][NUM_SECTIONS];

  private Driver simbaDriver;

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
    return Stream.of("SEA", "THRIFT");
  }

  @ParameterizedTest
  @MethodSource("modeProvider")
  void benchmarkDrivers(String mode) throws SQLException {
    setUpMode(mode);
    runTestsForDriver(0); // Test for OSS driver
    switchDriver(mode);
    runTestsForDriver(1); // Test for Databricks driver
    DriverManager.registerDriver(new com.databricks.client.jdbc.Driver());
    insertResultsIntoTable();
  }

  private void setUpMode(String mode) throws SQLException {
    switch (mode) {
      case "SEA":
        // Currently using dogfood since we don't have new metadata support in test warehouse
        connection = getValidJDBCConnection();
        RESULTS_TABLE = "main.jdbc_new_metadata_benchmark_schema.benchmarking_results";
        break;
      case "THRIFT":
        connection =
            DriverManager.getConnection(
                getJDBCUrl(Map.of("usethriftclient", "1")), "token", getDatabricksToken());
        RESULTS_TABLE = "main.jdbc_metadata_benchmarking_thrift.benchmarking_results";
        break;
      default:
        throw new IllegalArgumentException("Invalid testing mode");
    }
    setUpSchemas();
    setUpTables();
  }

  private void runTestsForDriver(int recording) throws SQLException {
    long startTime = System.currentTimeMillis();
    measureMetadataPerformance(recording);
    long endTime = System.currentTimeMillis();
    System.out.println(
        "Time taken by "
            + (recording == 0 ? "OSS JDBC" : "Databricks JDBC")
            + ": "
            + (endTime - startTime)
            + "ms");
  }

  private void switchDriver(String mode) throws SQLException {
    switch (mode) {
      case "SEA":
      case "THRIFT":
        connection = getConnectionForSimbaDriver(getJDBCUrl(), "token", getDatabricksToken());
        break;
      default:
        throw new IllegalArgumentException("Invalid testing mode");
    }
  }

  private void setUpSchemas() throws SQLException {
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      connection
          .createStatement()
          .execute(
              "CREATE SCHEMA IF NOT EXISTS " + getDatabricksCatalog() + "." + BASE_SCHEMA_NAME + i);
      System.out.println("Created schema " + i);
    }
  }

  private void setUpTables() throws SQLException {
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      for (int j = 0; j < NUM_TABLES; j++) {
        connection
            .createStatement()
            .execute(
                "CREATE TABLE IF NOT EXISTS "
                    + getDatabricksCatalog()
                    + "."
                    + BASE_SCHEMA_NAME
                    + i
                    + "."
                    + BASE_TABLE_NAME
                    + j
                    + " "
                    + getColumnString());
      }
      System.out.println("Created tables for schema " + i);
    }
  }

  private String getColumnString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(id INT");
    for (int i = 0; i < NUM_COLUMNS; i++) {
      sb.append(", col" + i + " STRING");
    }
    sb.append(")");
    return sb.toString();
  }

  private void tearDownSchemas() {
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      executeSQL(
          "DROP SCHEMA IF EXISTS "
              + getDatabricksCatalog()
              + "."
              + BASE_SCHEMA_NAME
              + i
              + " CASCADE");
    }
  }

  void measureMetadataPerformance(int recording) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    System.out.println("STARTED MEASURING METADATA PERFORMANCE...");

    for (int section = 0; section < NUM_SECTIONS; section++) {
      long startTime = System.currentTimeMillis();
      try {
        switch (section) {
          case 0:
            System.out.println("START OF SECTION 1");
            for (int i = 0; i < ATTEMPTS; i++) {
              metaData.getSchemas(getDatabricksCatalog(), "%");
            }
            break;
          case 1:
            System.out.println("START OF SECTION 2");
            for (int i = 0; i < ATTEMPTS; i++) {
              metaData.getSchemas(getDatabricksCatalog(), BASE_SCHEMA_NAME + "%");
            }
            break;
          case 2:
            System.out.println("START OF SECTION 3");
            for (int i = 0; i < ATTEMPTS; i++) {
              metaData.getTables(getDatabricksCatalog(), "%", "%", null);
            }
            break;
          case 3:
            System.out.println("START OF SECTION 4");
            for (int i = 0; i < ATTEMPTS; i++) {
              metaData.getTables(getDatabricksCatalog(), BASE_SCHEMA_NAME + "0", "%", null);
            }
            break;
          case 4:
            System.out.println("START OF SECTION 5");
            for (int i = 0; i < ATTEMPTS; i++) {
              metaData.getColumns(getDatabricksCatalog(), "%", "%", "%");
            }
            break;
          case 5:
            System.out.println("START OF SECTION 6");
            for (int i = 0; i < ATTEMPTS; i++) {
              metaData.getColumns(getDatabricksCatalog(), BASE_SCHEMA_NAME + "0", "%", "%");
            }
            break;
          case 6:
            System.out.println("START OF SECTION 7");
            for (int i = 0; i < ATTEMPTS; i++) {
              metaData.getColumns(
                  getDatabricksCatalog(), BASE_SCHEMA_NAME + "0", BASE_TABLE_NAME + "0", null);
            }
            break;
          default:
            throw new IllegalArgumentException("Invalid section number");
        }
        long endTime = System.currentTimeMillis();
        totalTimesForSection[recording][section] = endTime - startTime;
        avgTimesForSection[recording][section] =
            totalTimesForSection[recording][section] / ATTEMPTS;
        System.out.println("END OF SECTION " + (section + 1));
      } catch (SQLException e) {
        e.printStackTrace();
        totalTimesForSection[recording][section] = 0;
        avgTimesForSection[recording][section] = 0;
        System.out.println("ERROR IN SECTION " + (section + 1));
      }
    }
  }

  private void insertResultsIntoTable() throws SQLException {
    connection = getBenchfoodJDBCConnection();
    // SQL statement with placeholders
    String sql =
        "INSERT INTO "
            + RESULTS_TABLE
            + "(DateTime, "
            + "s1_avg_oss, s1_tot_oss, s1_avg_db, s1_tot_db, "
            + "s2_avg_oss, s2_tot_oss, s2_avg_db, s2_tot_db, "
            + "s3_avg_oss, s3_tot_oss, s3_avg_db, s3_tot_db, "
            + "s4_avg_oss, s4_tot_oss, s4_avg_db, s4_tot_db, "
            + "s5_avg_oss, s5_tot_oss, s5_avg_db, s5_tot_db, "
            + "s6_avg_oss, s6_tot_oss, s6_avg_db, s6_tot_db, "
            + "s7_avg_oss, s7_tot_oss, s7_avg_db, s7_tot_db) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      // Set the TIMESTAMP for the current date and time
      stmt.setTimestamp(1, java.sql.Timestamp.valueOf(LocalDateTime.now()));

      // Loop to set the values for each section
      int parameterIndex = 2; // Start after the TIMESTAMP
      for (int i = 0; i < NUM_SECTIONS; i++) {
        stmt.setDouble(parameterIndex++, avgTimesForSection[0][i]); // sX_avg_oss
        stmt.setLong(parameterIndex++, totalTimesForSection[0][i]); // sX_tot_oss
        stmt.setDouble(parameterIndex++, avgTimesForSection[1][i]); // sX_avg_db
        stmt.setLong(parameterIndex++, totalTimesForSection[1][i]); // sX_tot_db
      }

      // Execute the insert operation
      stmt.executeUpdate();
      System.out.println("Data successfully logged");
    } catch (SQLException e) {
      e.printStackTrace();
      System.out.println("Error logging data");
    }
  }

  private String getDatabricksCatalog() {
    return "jdbcbenchmarkingcatalog";
  }

  private void loadSimbaDriver() {
    try {
      File file = new File("src/test/resources/DatabricksJDBC42.jar");
      URL url = file.toURI().toURL();

      URLClassLoader urlClassLoader =
          new CustomClassLoader(new URL[] {url}, this.getClass().getClassLoader());

      Class<?> driverClass =
          Class.forName("com.databricks.client.jdbc.Driver", true, urlClassLoader);
      simbaDriver = (java.sql.Driver) driverClass.getDeclaredConstructor().newInstance();
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
