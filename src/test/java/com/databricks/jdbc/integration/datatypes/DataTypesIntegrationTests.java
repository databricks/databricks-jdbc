package com.databricks.jdbc.integration.datatypes;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DataTypesIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testDataTypesHandling() throws SQLException {
    // Create table with various data types
    String tableName = "data_types_test";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY, "
            + "test_varchar VARCHAR(255), "
            + "test_boolean BOOLEAN, "
            + "test_int INT, "
            + "test_float FLOAT, "
            + "test_double DOUBLE, "
            + "test_decimal DECIMAL(10, 2), "
            + "test_byte TINYINT, "
            + "test_date DATE, "
            + "test_timestamp TIMESTAMP"
            + ");";
    setupDatabaseTable(tableName, createTableSQL);

    // Insert data into table
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + "(id, test_varchar, test_boolean, test_int, test_float, test_double, test_decimal, test_byte, test_date, test_timestamp) VALUES "
            + "(1, 'Test', TRUE, 42, 3.14, 2.718, 12345.67, 127, '2021-01-01', '2021-01-01 12:34:56.789')";
    executeSQL(insertSQL);

    // Fetch and verify the inserted data
    String query = "SELECT * FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {

      assertTrue(resultSet.next(), "Data row should exist");

      assertEquals("Test", resultSet.getString("test_varchar"), "VARCHAR data type mismatch");
      assertTrue(resultSet.getBoolean("test_boolean"), "BOOLEAN data type mismatch");
      assertEquals(42, resultSet.getInt("test_int"), "INT data type mismatch");
      assertEquals(3.14, resultSet.getFloat("test_float"), 0.001, "FLOAT data type mismatch");
      assertEquals(
          Date.valueOf("2021-01-01"), resultSet.getDate("test_date"), "DATE data type mismatch");

      Timestamp timestampFromDb = resultSet.getTimestamp("test_timestamp");

      ZoneId dbZoneId = ZoneId.of("UTC");
      ZoneId targetZoneId = ZoneId.of("Asia/Kolkata");

      ZonedDateTime zonedDateTime = timestampFromDb.toInstant().atZone(dbZoneId);
      ZonedDateTime targetZonedDateTime = zonedDateTime.withZoneSameInstant(targetZoneId);
      LocalDateTime localDateTime = targetZonedDateTime.toLocalDateTime();
      LocalDateTime expectedLocalDateTime =
          LocalDateTime.of(2021, Month.JANUARY, 1, 18, 4, 56, 789000000);
      assertEquals(
          expectedLocalDateTime, localDateTime, "Timestamp mismatch after timezone adjustment.");
    }

    deleteTable(tableName);
  }
}
