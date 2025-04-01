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
    createTable(connection, tableName);
    insertStringTestData(connection, tableName);

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

  private void createTable(Connection connection, String tableName) throws SQLException {
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, test_string VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createTableSQL);
  }

  private void insertStringTestData(Connection connection, String tableName) throws SQLException {
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
  }

  private void validateStringResults(ResultSet resultSet) throws SQLException {
    while (resultSet.next()) {
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
