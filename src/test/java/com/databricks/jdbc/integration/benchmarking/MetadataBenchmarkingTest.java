package com.databricks.jdbc.integration.benchmarking;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Enumeration;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetadataBenchmarkingTest {

  private Connection connection;
  private static final int NUM_SCHEMAS = 10;
  private static final int NUM_TABLES = 10;
  private static final int NUM_COLUMNS = 10;

  private static final int NUM_SECTIONS = 8;

  private static final int ATTEMPTS = 10;

  private static final String BASE_SCHEMA_NAME = "jdbc_new_metadata_benchmark_schema";

  private static final String RESULTS_TABLE =
      "main.jdbc_new_metadata_benchmark_schema.benchmarking_results";

  private static final String BASE_TABLE_NAME = "table";

  long totalTimesForSection[][] = new long[2][NUM_SECTIONS];
  double avgTimesForSection[][] = new double[2][NUM_SECTIONS];

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection(Map.of("useLegacyMetadata", "0"));
    setUpSchemas();
    setUpTables();
  }

  @AfterEach
  void tearDown() throws SQLException {
    connection = getValidJDBCConnection();
//    insertResultsIntoTable();
    tearDownSchemas();
    connection.close();
  }

  private void setUpSchemas() {
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      executeSQL(
          "CREATE SCHEMA IF NOT EXISTS " + getDatabricksCatalog() + "." + BASE_SCHEMA_NAME + i);
      System.out.println("Created schema " + i);
    }
  }

  private void setUpTables() {
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      for (int j = 0; j < NUM_TABLES; j++) {
        executeSQL(
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
    System.out.println("START OF SECTION 1");
    // SECTION 1: Get all schemas
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < ATTEMPTS; i++) {
      ResultSet r = metaData.getSchemas(getDatabricksCatalog(), "*");
      printResultSet(r);
    }
    long endTime = System.currentTimeMillis();
    totalTimesForSection[recording][0] = endTime - startTime;
    avgTimesForSection[recording][0] = totalTimesForSection[recording][0] / ATTEMPTS;
    System.out.println("END OF SECTION 1");

    System.out.println("START OF SECTION 2");
    // SECTION 2: Get all schemas matching a name
    startTime = System.currentTimeMillis();
    for (int i = 0; i < ATTEMPTS; i++) {
      ResultSet r = metaData.getSchemas(getDatabricksCatalog(), BASE_SCHEMA_NAME + "*");
      printResultSet(r);
    }
    endTime = System.currentTimeMillis();
    totalTimesForSection[recording][1] = endTime - startTime;
    avgTimesForSection[recording][1] = totalTimesForSection[recording][1] / ATTEMPTS;
    System.out.println("END OF SECTION 2");

    System.out.println("START OF SECTION 3");
    // SECTION 3: Get all tables for all schema
    startTime = System.currentTimeMillis();
    for (int i = 0; i < ATTEMPTS; i++) {
      ResultSet r = metaData.getTables(getDatabricksCatalog(), "*", "*", null);
      printResultSet(r);
    }
    endTime = System.currentTimeMillis();
    totalTimesForSection[recording][2] = endTime - startTime;
    avgTimesForSection[recording][2] = totalTimesForSection[recording][2] / ATTEMPTS;
    System.out.println("END OF SECTION 3");

    System.out.println("START OF SECTION 4");
    // SECTION 4: Get all tables for a schema
    startTime = System.currentTimeMillis();
    for (int i = 0; i < ATTEMPTS; i++) {
      ResultSet r = metaData.getTables(getDatabricksCatalog(), BASE_SCHEMA_NAME + "*", "*", null);
      printResultSet(r);
    }
    endTime = System.currentTimeMillis();
    totalTimesForSection[recording][3] = endTime - startTime;
    avgTimesForSection[recording][3] = totalTimesForSection[recording][3] / ATTEMPTS;
    System.out.println("END OF SECTION 4");

    System.out.println("START OF SECTION 5");
    // SECTION 5: Get all columns for all tables
    startTime = System.currentTimeMillis();
    for (int i = 0; i < ATTEMPTS; i++) {
      metaData.getColumns(getDatabricksCatalog(), "%", "%", "%");
    }
    endTime = System.currentTimeMillis();
    totalTimesForSection[recording][4] = endTime - startTime;
    avgTimesForSection[recording][4] = totalTimesForSection[recording][4] / ATTEMPTS;
    System.out.println("END OF SECTION 5");

    System.out.println("START OF SECTION 6");
    // SECTION 6: Get all columns for all tables in a schema
    startTime = System.currentTimeMillis();
    for (int i = 0; i < ATTEMPTS; i++) {
      ResultSet r = metaData.getColumns(getDatabricksCatalog(), BASE_SCHEMA_NAME + "*", "*", "*");
      printResultSet(r);
    }
    endTime = System.currentTimeMillis();
    totalTimesForSection[recording][5] = endTime - startTime;
    avgTimesForSection[recording][5] = totalTimesForSection[recording][5] / ATTEMPTS;
    System.out.println("END OF SECTION 6");

    System.out.println("START OF SECTION 7");
    // SECTION 7: Get all columns for a table
    startTime = System.currentTimeMillis();
    for (int i = 0; i < ATTEMPTS; i++) {
      ResultSet r = metaData.getColumns(
          getDatabricksCatalog(), BASE_SCHEMA_NAME + "*", BASE_TABLE_NAME + "*", null);
      printResultSet(r);
    }
    endTime = System.currentTimeMillis();
    totalTimesForSection[recording][6] = endTime - startTime;
    avgTimesForSection[recording][6] = totalTimesForSection[recording][6] / ATTEMPTS;
    System.out.println("END OF SECTION 7");


    System.out.println("START OF SECTION 8");
    // SECTION 7: Get all functions in a given catalog
    startTime = System.currentTimeMillis();
    for (int i = 0; i < ATTEMPTS; i++) {
      ResultSet r = metaData.getFunctions(getDatabricksCatalog(), null, null);
      printResultSet(r);
    }
    endTime = System.currentTimeMillis();
    totalTimesForSection[recording][6] = endTime - startTime;
    avgTimesForSection[recording][6] = totalTimesForSection[recording][6] / ATTEMPTS;
    System.out.println("END OF SECTION 8");


  }

  @Test
  void benchmarkDrivers() throws SQLException {
    // Currently connection is held by OSS driver
    long startTime = System.currentTimeMillis();
    measureMetadataPerformance(0);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken by OSS JDBC: " + (endTime - startTime) + "ms");

    // Switching to Databricks JDBC
    connection.close();

    Enumeration<Driver> drivers = DriverManager.getDrivers();

    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      if (driver.getClass().getName().contains("DatabricksDriver")) {
        DriverManager.deregisterDriver(driver);
      }
    }

    connection = DriverManager.getConnection(getJDBCUrl(), "token", getDatabricksToken());

    // Currently connection is held by Databricks driver
    startTime = System.currentTimeMillis();
    measureMetadataPerformance(1);
    endTime = System.currentTimeMillis();
    System.out.println("Time taken by Databricks JDBC: " + (endTime - startTime) + "ms");

    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());

    System.out.println("STATS");
    System.out.println("Section Descriptions");
    System.out.println("1. Get all schemas");
    System.out.println("2. Get all schemas matching a name");
    System.out.println("3. Get all tables for all schema");
    System.out.println("4. Get all tables for a schema");
    System.out.println("5. Get all columns for all tables for all schema");
    System.out.println("6. Get all columns for all tables in a schema");
    System.out.println("7. Get all columns for a table");
    System.out.println("8. Get all functions for a catalog");
    for (int i = 0; i < NUM_SECTIONS; i++) {
      System.out.println(
          "Average time taken by OSS JDBC for section "
              + (i + 1)
              + ": "
              + avgTimesForSection[0][i]
              + "ms");
      System.out.println(
          "Average time taken by Databricks JDBC for section "
              + (i + 1)
              + ": "
              + avgTimesForSection[1][i]
              + "ms");
      System.out.println(
          "Total time taken by OSS JDBC for section "
              + (i + 1)
              + ": "
              + totalTimesForSection[0][i]
              + "ms");
      System.out.println(
          "Total time taken by Databricks JDBC for section "
              + (i + 1)
              + ": "
              + totalTimesForSection[1][i]
              + "ms");
    }
    connection.close();
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
            + "s7_avg_oss, s7_tot_oss, s7_avg_db, s7_tot_db, "
            + "s8_avg_oss, s8_tot_oss, s8_avg_db, s8_tot_db) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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

  public void printResultSet(ResultSet resultSet) throws SQLException {
    System.out.println("\n\nPrinting resultSet...........\n");
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int columnsNumber = rsmd.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1; i <= columnsNumber; i++) {
        try {
          Object columnValue = resultSet.getObject(i);
          System.out.print(columnValue + "\t\t");
        } catch (Exception e) {
          System.out.print(
                  "NULL\t\t"); // It is possible for certain columns to be non-existent (edge case)
        }
      }
      System.out.println();
    }
  }
}
