package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.sql.*;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Integration tests for string edge cases and nested complex types. */
public class DataTypesIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection connection;
  private Connection inlineConnection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
    Properties properties = new Properties();
    properties.setProperty("enableArrow", "0");
    inlineConnection = getValidJDBCConnection(properties);
  }

  @AfterEach
  void cleanUp() throws SQLException {
    closeConnection(connection);
    closeConnection(inlineConnection);
  }

  @Test
  void testStringEdgeCases() throws SQLException {
    String tableName = "string_edge_cases_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, test_string VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createTableSQL);
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, test_string) VALUES "
            + "(1, '   leading and trailing spaces   '),"
            + "(2, 'こんにちは'),"
            + "(3, 'special chars: !@#$%^&*()'),"
            + "(4, 'string with \"double quotes\" inside'),"
            + "(5, NULL)";
    executeSQL(connection, insertSQL);

    String query =
        "SELECT id, test_string FROM " + getFullyQualifiedTableName(tableName) + " ORDER BY id";
    ResultSet resultSet = executeQuery(connection, query);
    ResultSet inlineResultSet = executeQuery(inlineConnection, query);

    validateStringResults(resultSet);
    validateStringResults(inlineResultSet);

    deleteTable(connection, tableName);
  }

  @ParameterizedTest
  @MethodSource("nullHandlingProvider")
  void testNullHandling(String query, int expectedType) throws SQLException {
    ResultSet resultSet = executeQuery(connection, query);
    ResultSet inlineResultSet = executeQuery(inlineConnection, query);
    assertTrue(resultSet.next());
    assertTrue(inlineResultSet.next());
    assertNull(resultSet.getObject(1));
    assertNull(inlineResultSet.getObject(1));
    assertEquals(expectedType, resultSet.getMetaData().getColumnType(1));
    assertEquals(expectedType, inlineResultSet.getMetaData().getColumnType(1));
    resultSet.close();
    inlineResultSet.close();
  }

  private static Stream<Arguments> nullHandlingProvider() {
    return Stream.of(
        Arguments.of("SELECT NULL", Types.VARCHAR),
        Arguments.of("SELECT CAST(NULL AS DOUBLE)", Types.DOUBLE),
        Arguments.of("SELECT NULL UNION (SELECT 1) order by 1", Types.INTEGER));
  }

  private void validateStringResults(ResultSet resultSet) throws SQLException {
    int rowCount = 0;
    while (resultSet.next()) {
      rowCount++;
      int id = resultSet.getInt("id");
      String value = resultSet.getString("test_string");
      switch (id) {
        case 1:
          assertEquals("   leading and trailing spaces   ", value);
          break;
        case 2:
          assertEquals("こんにちは", value);
          break;
        case 3:
          assertEquals("special chars: !@#$%^&*()", value);
          break;
        case 4:
          assertEquals("string with \"double quotes\" inside", value);
          break;
        case 5:
          assertNull(value);
          break;
        default:
          fail("Unexpected row id: " + id);
      }
    }
    assertEquals(5, rowCount);
  }

  @Test
  void testVariantTypes() throws SQLException {
    String tableName = "variant_types_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, variant_col VARIANT)";
    setupDatabaseTable(connection, tableName, createTableSQL);

    // Insert rows with JSON data via PARSE_JSON:
    // - A simple JSON object
    // - A nested JSON object with an array and boolean value
    // - A null variant
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, variant_col) VALUES "
            + "(1, PARSE_JSON('{\"key\": \"value\", \"number\": 123}')), "
            + "(2, PARSE_JSON('{\"nested\": {\"a\": \"b\", \"c\": [1, 2, 3]}, \"flag\": true}')), "
            + "(3, NULL)";
    executeSQL(connection, insertSQL);

    String query =
        "SELECT id, variant_col FROM " + getFullyQualifiedTableName(tableName) + " ORDER BY id";
    ResultSet rs = executeQuery(connection, query);
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals(Types.OTHER, rsmd.getColumnType(2));
    assertEquals("VARIANT", rsmd.getColumnTypeName(2));
    int rowCount = 0;
    while (rs.next()) {
      rowCount++;
      int id = rs.getInt("id");
      Object variant = rs.getObject("variant_col");
      switch (id) {
        case 1:
          String variantStr1 = variant.toString();
          assertTrue(variantStr1.contains("\"key\":\"value\""));
          assertTrue(variantStr1.contains("\"number\":123"));
          break;
        case 2:
          String variantStr2 = variant.toString();
          assertTrue(variantStr2.contains("\"nested\""));
          assertTrue(variantStr2.contains("\"a\":\"b\""));
          assertTrue(variantStr2.contains("\"c\":[1,2,3]"));
          assertTrue(variantStr2.contains("\"flag\":true"));
          break;
        case 3:
          assertNull(variant);
          break;
        default:
          fail("Unexpected row id in variant test: " + id);
      }
    }
    assertEquals(3, rowCount);
    deleteTable(connection, tableName);
  }

  @Test
  void testTimestampWithTimezoneConversion() throws SQLException {
    String tableName = "timestamp_test_timezone";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, ts TIMESTAMP)";
    setupDatabaseTable(connection, tableName, createTableSQL);

    /*
     * Use from_utc_timestamp to simulate converting a UTC timestamp into a specific timezone.
     * converting '2021-06-15 12:34:56.789' from UTC to America/Los_Angeles.
     * expected local time should be:
     *    2021-06-15 12:34:56.789 UTC  --> 2021-06-15 05:34:56.789 PDT
     */
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, ts) VALUES "
            + "(1, from_utc_timestamp('2021-06-15 12:34:56.789', 'America/Los_Angeles'))";
    executeSQL(connection, insertSQL);

    // Query and validate the timezone conversion result.
    String query = "SELECT id, ts FROM " + getFullyQualifiedTableName(tableName) + " ORDER BY id";
    ResultSet rs = executeQuery(connection, query);
    assertTrue(rs.next());
    int id = rs.getInt("id");
    Timestamp ts = rs.getTimestamp("ts");
    assertEquals(1, id);
    Timestamp expected = Timestamp.valueOf("2021-06-15 05:34:56.789"); // Expected PDT value.
    assertEquals(expected, ts);
    deleteTable(connection, tableName);
  }

  @Test
  void testTimestamp() throws SQLException {
    String tableName = "timestamp_test_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, ts TIMESTAMP)";
    setupDatabaseTable(connection, tableName, createTableSQL);
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, ts) VALUES "
            + "(1, '2021-01-01 10:00:00'), "
            + "(2, '2021-06-15 12:34:56.789'), "
            + "(3, NULL)";
    executeSQL(connection, insertSQL);

    String query = "SELECT id, ts FROM " + getFullyQualifiedTableName(tableName) + " ORDER BY id";
    ResultSet resultSet = executeQuery(connection, query);
    ResultSet inlineResultSet = executeQuery(inlineConnection, query);
    validateTimestampResults(resultSet);
    validateTimestampResults(inlineResultSet);

    deleteTable(connection, tableName);
  }

  private void validateTimestampResults(ResultSet resultSet) throws SQLException {
    int rowCount = 0;
    while (resultSet.next()) {
      rowCount++;
      int id = resultSet.getInt("id");
      Timestamp ts = resultSet.getTimestamp("ts");
      switch (id) {
        case 1:
          Timestamp expected1 = Timestamp.valueOf("2021-01-01 10:00:00");
          assertEquals(expected1, ts);
          break;
        case 2:
          Timestamp expected2 = Timestamp.valueOf("2021-06-15 12:34:56.789");
          assertEquals(expected2, ts);
          break;
        case 3:
          assertNull(ts);
          break;
        default:
          fail("Unexpected row id in timestamp test: " + id);
      }
    }
    assertEquals(3, rowCount);
  }

  private void closeConnection(Connection connection) throws SQLException {
    if (connection != null) {
      if (((DatabricksConnection) connection).getConnectionContext().getClientType()
              == DatabricksClientType.THRIFT
          && getFakeServiceMode() == FakeServiceExtension.FakeServiceMode.REPLAY) {
        // Hacky fix for THRIFT + REPLAY mode
      } else {
        connection.close();
      }
    }
  }
}
