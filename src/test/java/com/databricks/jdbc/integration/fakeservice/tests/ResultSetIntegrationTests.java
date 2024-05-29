package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.integration.fakeservice.BaseFakeServiceIntegrationTests;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

/** Integration tests for ResultSet operations. */
public class ResultSetIntegrationTests extends BaseFakeServiceIntegrationTests {

  @Test
  void testRetrievalOfBasicDataTypes() throws SQLException {
    String tableName = "basic_data_types_table";
    setupDatabaseTable(tableName);
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(insertSQL);

    String query = "SELECT id, col1 FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(query);

    while (resultSet.next()) {
      assertEquals(1, resultSet.getInt("id"), "ID should be of type Integer and value 1");
      assertEquals(
          "value1", resultSet.getString("col1"), "col1 should be of type String and value value1");
    }
    deleteTable(tableName);
  }

  @Test
  void testRetrievalOfComplexDataTypes() throws SQLException {
    String tableName = "complex_data_types_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY, "
            + "datetime_col TIMESTAMP, "
            + "decimal_col DECIMAL(10, 2), "
            + "date_col DATE"
            + ");";
    setupDatabaseTable(tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, datetime_col, decimal_col, date_col) VALUES "
            + "(1, '2021-01-01 00:00:00', 123.45, '2021-01-01')";
    executeSQL(insertSQL);

    String query =
        "SELECT datetime_col, decimal_col, date_col FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(query);

    while (resultSet.next()) {
      assertInstanceOf(
          Timestamp.class,
          resultSet.getTimestamp("datetime_col"),
          "datetime_col should be of type Timestamp");
      assertInstanceOf(
          BigDecimal.class,
          resultSet.getBigDecimal("decimal_col"),
          "decimal_col should be of type BigDecimal");
      assertInstanceOf(
          Date.class, resultSet.getDate("date_col"), "date_col should be of type Date");
    }
    deleteTable(tableName);
  }

  @Test
  void testHandlingNullValues() throws SQLException {
    String tableName = "null_values_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY, "
            + "nullable_col VARCHAR(255)"
            + ");";
    setupDatabaseTable(tableName, createTableSQL);

    String insertSQL = "INSERT INTO " + getFullyQualifiedTableName(tableName) + " (id) VALUES (1)";
    executeSQL(insertSQL);

    String query = "SELECT nullable_col FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(query);

    while (resultSet.next()) {
      String field = resultSet.getString("nullable_col");
      assertNull(field, "Field should be null when not set");
    }
    deleteTable(tableName);
  }

  @Test
  void testNavigationInsideResultSet() throws SQLException {
    String tableName = "navigation_table";
    int numRows = 10; // Number of rows to insert and navigate through

    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY"
            + ");";
    setupDatabaseTable(tableName, createTableSQL);

    for (int i = 1; i <= numRows; i++) {
      String insertSQL =
          "INSERT INTO " + getFullyQualifiedTableName(tableName) + " (id) VALUES (" + i + ")";
      executeSQL(insertSQL);
    }

    String query =
        "SELECT id FROM " + getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName;
    ResultSet resultSet = executeQuery(query);

    int count = 0;
    try {
      while (resultSet.next()) {
        count++;
      }
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) {
          e.printStackTrace(); // Log or handle the exception as needed
        }
      }
    }

    assertEquals(
        numRows,
        count,
        "Should have navigated through " + numRows + " rows, but navigated through " + count);
    deleteTable(tableName);
  }
}
