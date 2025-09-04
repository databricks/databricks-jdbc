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

  private static final String LEADING_TRAILING_SPACES = "   leading and trailing spaces   ";
  private static final String UNICODE_TEXT = "こんにちは";
  private static final String SPECIAL_CHARS = "special chars: !@#$%^&*()";
  private static final String DOUBLE_QUOTES = "string with \"double quotes\" inside";

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
    String query =
        "SELECT * FROM (VALUES "
            + "(1, '"
            + LEADING_TRAILING_SPACES
            + "'),"
            + "(2, '"
            + UNICODE_TEXT
            + "'),"
            + "(3, '"
            + SPECIAL_CHARS
            + "'),"
            + "(4, '"
            + DOUBLE_QUOTES
            + "'),"
            + "(5, NULL)"
            + ") AS string_edge_cases(id, test_string) "
            + "ORDER BY id";
    ResultSet resultSet = executeQuery(connection, query);
    ResultSet inlineResultSet = executeQuery(inlineConnection, query);

    validateStringResults(resultSet);
    validateStringResults(inlineResultSet);

    resultSet.close();
    inlineResultSet.close();
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
          assertEquals(LEADING_TRAILING_SPACES, value);
          break;
        case 2:
          assertEquals(UNICODE_TEXT, value);
          break;
        case 3:
          assertEquals(SPECIAL_CHARS, value);
          break;
        case 4:
          assertEquals(DOUBLE_QUOTES, value);
          break;
        case 5:
          assertEquals(null, value);
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
    rs.close();
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
    rs.close();
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
    resultSet.close();
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

  @Test
  void testIntervalTypes() throws SQLException {
    String tableName = "intervals_demo";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (iv_year INTERVAL YEAR,"
            + " iv_month INTERVAL MONTH,"
            + " iv_year_to_mon INTERVAL YEAR TO MONTH,"
            + " iv_day INTERVAL DAY,"
            + " iv_day_to_hour INTERVAL DAY TO HOUR,"
            + " iv_day_to_min INTERVAL DAY TO MINUTE,"
            + " iv_day_to_sec INTERVAL DAY TO SECOND,"
            + " iv_hour INTERVAL HOUR,"
            + " iv_hour_to_min INTERVAL HOUR TO MINUTE,"
            + " iv_hour_to_sec INTERVAL HOUR TO SECOND,"
            + " iv_min_to_sec INTERVAL MINUTE TO SECOND,"
            + " iv_second INTERVAL SECOND)";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " VALUES ("
            + " INTERVAL '100' YEAR,"
            + " INTERVAL '1200' MONTH,"
            + " INTERVAL '100-0' YEAR TO MONTH,"
            + " INTERVAL '3' DAY,"
            + " INTERVAL '3 4' DAY TO HOUR,"
            + " INTERVAL '3 04:30' DAY TO MINUTE,"
            + " INTERVAL '3 04:30:20.123' DAY TO SECOND,"
            + " INTERVAL '27' HOUR,"
            + " INTERVAL '27:15' HOUR TO MINUTE,"
            + " INTERVAL '27:15:20.456' HOUR TO SECOND,"
            + " INTERVAL '90:30.5' MINUTE TO SECOND,"
            + " INTERVAL '45.789' SECOND)";
    executeSQL(connection, insertSQL);

    String query = "SELECT * FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(connection, query);

    validateIntervalResults(resultSet);

    deleteTable(connection, tableName);
    resultSet.close();
  }

  private void validateIntervalResults(ResultSet resultSet) throws SQLException {
    assertTrue(resultSet.next());

    // Year-Month intervals
    assertEquals("100-0", resultSet.getString("iv_year"));
    assertEquals("100-0", resultSet.getString("iv_month"));
    assertEquals("100-0", resultSet.getString("iv_year_to_mon"));

    // Day-Time intervals
    assertEquals("3 00:00:00.000000000", resultSet.getString("iv_day"));
    assertEquals("3 04:00:00.000000000", resultSet.getString("iv_day_to_hour"));
    assertEquals("3 04:30:00.000000000", resultSet.getString("iv_day_to_min"));
    assertEquals("3 04:30:20.123000000", resultSet.getString("iv_day_to_sec"));

    assertEquals("1 03:00:00.000000000", resultSet.getString("iv_hour"));
    assertEquals("1 03:15:00.000000000", resultSet.getString("iv_hour_to_min"));
    assertEquals("1 03:15:20.456000000", resultSet.getString("iv_hour_to_sec"));

    assertEquals("0 01:30:30.500000000", resultSet.getString("iv_min_to_sec"));
    assertEquals("0 00:00:45.789000000", resultSet.getString("iv_second"));

    assertFalse(resultSet.next());
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
