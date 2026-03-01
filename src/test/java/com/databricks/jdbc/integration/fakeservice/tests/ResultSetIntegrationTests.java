package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.math.BigDecimal;
import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for ResultSet operations. */
public class ResultSetIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      if (((DatabricksConnection) connection).getConnectionContext().getClientType()
              == DatabricksClientType.THRIFT
          && getFakeServiceMode() == FakeServiceExtension.FakeServiceMode.REPLAY) {
        // Hacky fix
        // Wiremock has error in stub matching for close operation in THRIFT + REPLAY mode
      } else {
        connection.close();
      }
    }
  }

  @Test
  void testRetrievalOfBasicDataTypes() throws SQLException {
    String tableName = "basic_data_types_table";
    setupDatabaseTable(connection, tableName);
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(connection, insertSQL);

    String query = "SELECT id, col1 FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(connection, query);

    while (resultSet.next()) {
      assertEquals(1, resultSet.getInt("id"), "ID should be of type Integer and value 1");
      assertEquals(
          "value1", resultSet.getString("col1"), "col1 should be of type String and value value1");
    }
    deleteTable(connection, tableName);
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
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, datetime_col, decimal_col, date_col) VALUES "
            + "(1, '2021-01-01 00:00:00', 123.45, '2021-01-01')";
    executeSQL(connection, insertSQL);

    String query =
        "SELECT datetime_col, decimal_col, date_col FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(connection, query);
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
    deleteTable(connection, tableName);
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
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL = "INSERT INTO " + getFullyQualifiedTableName(tableName) + " (id) VALUES (1)";
    executeSQL(connection, insertSQL);

    String query = "SELECT nullable_col FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(connection, query);

    while (resultSet.next()) {
      String field = resultSet.getString("nullable_col");
      assertNull(field, "Field should be null when not set");
    }
    deleteTable(connection, tableName);
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
    setupDatabaseTable(connection, tableName, createTableSQL);

    for (int i = 1; i <= numRows; i++) {
      String insertSQL =
          "INSERT INTO " + getFullyQualifiedTableName(tableName) + " (id) VALUES (" + i + ")";
      executeSQL(connection, insertSQL);
    }

    String query =
        "SELECT id FROM " + getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName;
    ResultSet resultSet = executeQuery(connection, query);

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
    deleteTable(connection, tableName);
  }

  // --- ResultSet property and navigation tests ---

  @Test
  void testFindColumn() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 42 AS my_col, 'hello' AS another_col");

    assertTrue(rs.next());
    assertEquals(1, rs.findColumn("my_col"), "findColumn should return 1 for first column");
    assertEquals(2, rs.findColumn("another_col"), "findColumn should return 2 for second column");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetHoldability() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS val");

    assertTrue(rs.next());
    int holdability = rs.getHoldability();
    assertTrue(
        holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
            || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT,
        "Holdability should be a valid JDBC constant");

    rs.close();
    stmt.close();
  }

  @Test
  void testResultSet_GetWarningsAndClearWarnings() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS val");

    assertTrue(rs.next());
    // getWarnings should not throw (may return null if no warnings)
    SQLWarning warning = rs.getWarnings();
    // clearWarnings should not throw
    rs.clearWarnings();
    assertNull(rs.getWarnings(), "After clearWarnings, getWarnings should return null");

    rs.close();
    stmt.close();
  }

  // --- ResultSetMetaData additional tests ---

  @Test
  void testResultSetMetaData_GetTableName() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 42 AS id, 'test' AS name");

    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // getTableName returns null in this driver (server does not return table name metadata)
    String tblName = rsmd.getTableName(1);
    assertNull(tblName, "getTableName should return null (not populated by server)");

    rs.close();
    stmt.close();
  }

  @Test
  void testResultSetMetaData_GetSchemaAndCatalogName() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 42 AS id, 'test' AS name");

    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // getSchemaName returns null (server does not return schema name metadata)
    String schemaName = rsmd.getSchemaName(1);
    assertNull(schemaName, "getSchemaName should return null (not populated by server)");

    // getCatalogName returns empty string
    String catalogName = rsmd.getCatalogName(1);
    assertNotNull(catalogName, "getCatalogName should return non-null");

    rs.close();
    stmt.close();
  }

  // --- ResultSet getter method tests ---

  @Test
  void testGetBoolean() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT true AS bool_val");

    assertTrue(rs.next());
    assertTrue(rs.getBoolean("bool_val"), "getBoolean should return true");
    assertTrue(rs.getBoolean(1), "getBoolean by index should return true");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetByte() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT CAST(42 AS TINYINT) AS byte_val");

    assertTrue(rs.next());
    assertEquals((byte) 42, rs.getByte("byte_val"), "getByte should return 42");
    assertEquals((byte) 42, rs.getByte(1), "getByte by index should return 42");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetShort() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT CAST(1234 AS SMALLINT) AS short_val");

    assertTrue(rs.next());
    assertEquals((short) 1234, rs.getShort("short_val"), "getShort should return 1234");
    assertEquals((short) 1234, rs.getShort(1), "getShort by index should return 1234");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetLong() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT CAST(9876543210 AS BIGINT) AS long_val");

    assertTrue(rs.next());
    assertEquals(9876543210L, rs.getLong("long_val"), "getLong should return 9876543210");
    assertEquals(9876543210L, rs.getLong(1), "getLong by index should return 9876543210");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetFloat() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT CAST(3.14 AS FLOAT) AS float_val");

    assertTrue(rs.next());
    assertEquals(3.14f, rs.getFloat("float_val"), 0.01f, "getFloat should return ~3.14");
    assertEquals(3.14f, rs.getFloat(1), 0.01f, "getFloat by index should return ~3.14");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetDouble() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT CAST(2.718281828 AS DOUBLE) AS dbl_val");

    assertTrue(rs.next());
    assertEquals(2.718281828, rs.getDouble("dbl_val"), 0.0001, "getDouble should return ~2.718");
    assertEquals(2.718281828, rs.getDouble(1), 0.0001, "getDouble by index should return ~2.718");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetBigDecimal() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT CAST(12345.6789 AS DECIMAL(10, 4)) AS dec_val");

    assertTrue(rs.next());
    assertEquals(
        new BigDecimal("12345.6789"), rs.getBigDecimal("dec_val"), "getBigDecimal should match");
    assertEquals(
        new BigDecimal("12345.6789"), rs.getBigDecimal(1), "getBigDecimal by index should match");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetDate() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT CAST('2024-06-15' AS DATE) AS date_val");

    assertTrue(rs.next());
    Date expected = Date.valueOf("2024-06-15");
    assertEquals(expected, rs.getDate("date_val"), "getDate should match");
    assertEquals(expected, rs.getDate(1), "getDate by index should match");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetTimestamp() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT CAST('2024-06-15 10:30:00' AS TIMESTAMP) AS ts_val");

    assertTrue(rs.next());
    Timestamp retrieved = rs.getTimestamp("ts_val");
    assertNotNull(retrieved, "getTimestamp should return non-null");
    Timestamp retrievedByIndex = rs.getTimestamp(1);
    assertNotNull(retrievedByIndex, "getTimestamp by index should return non-null");

    rs.close();
    stmt.close();
  }

  @Test
  void testGetObject_WithTypeConversion() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 42 AS int_val, 'hello' AS str_val");

    assertTrue(rs.next());
    Object intObj = rs.getObject("int_val");
    assertNotNull(intObj, "getObject for INT should return non-null");

    Object strObj = rs.getObject("str_val");
    assertNotNull(strObj, "getObject for STRING should return non-null");
    assertEquals("hello", strObj.toString(), "getObject for STRING should return 'hello'");

    rs.close();
    stmt.close();
  }

  // --- ResultSet navigation and position tests ---

  @Test
  void testForwardNavigation_MultipleRows() throws SQLException {
    String tableName = "nav_forward_table";
    setupDatabaseTable(connection, tableName);
    String fqn = getFullyQualifiedTableName(tableName);
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a', 'b')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'c', 'd')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (3, 'e', 'f')");

    ResultSet rs = executeQuery(connection, "SELECT id FROM " + fqn + " ORDER BY id");
    assertNotNull(rs);

    int count = 0;
    while (rs.next()) {
      count++;
      assertEquals(count, rs.getInt("id"), "Row " + count + " should have id=" + count);
    }
    assertEquals(3, count, "Should navigate through all 3 rows");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testIsBeforeFirst_BeforeNavigation() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);
    assertTrue(rs.isBeforeFirst(), "Cursor should be before first row initially");

    rs.next();
    assertFalse(rs.isBeforeFirst(), "Cursor should not be before first after next()");
    rs.close();
  }

  @Test
  void testIsFirst_OnFirstRow() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);
    assertFalse(rs.isFirst(), "isFirst() should be false before calling next()");

    assertTrue(rs.next(), "Should have first row");
    assertTrue(rs.isFirst(), "isFirst() should be true on first row");
    rs.close();
  }

  @Test
  void testNextReturnsFalse_AfterExhausted() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);

    assertTrue(rs.next(), "Should have first row");
    assertEquals(1, rs.getInt("num"));
    assertFalse(rs.next(), "Should have no more rows after last");
    rs.close();
  }

  @Test
  void testGetRow_TracksPosition() throws SQLException {
    String tableName = "nav_getrow_table";
    setupDatabaseTable(connection, tableName);
    String fqn = getFullyQualifiedTableName(tableName);
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a', 'b')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'c', 'd')");

    ResultSet rs = executeQuery(connection, "SELECT id FROM " + fqn + " ORDER BY id");
    assertNotNull(rs);

    assertEquals(0, rs.getRow(), "getRow() should return 0 before first row");

    assertTrue(rs.next());
    assertEquals(1, rs.getRow(), "getRow() should return 1 on first row");

    assertTrue(rs.next());
    assertEquals(2, rs.getRow(), "getRow() should return 2 on second row");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testResultSetType_ForwardOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.TYPE_FORWARD_ONLY, rs.getType(), "ResultSet type should be TYPE_FORWARD_ONLY");
    rs.close();
  }

  @Test
  void testResultSetConcurrency_ReadOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.CONCUR_READ_ONLY,
        rs.getConcurrency(),
        "ResultSet concurrency should be CONCUR_READ_ONLY");
    rs.close();
  }

  @Test
  void testFetchSize_DefaultsToZero() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(0, rs.getFetchSize(), "getFetchSize() should return 0 (unsupported)");
    rs.close();
  }

  @Test
  void testSetFetchSize_AcceptedWithWarning() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertDoesNotThrow(() -> rs.setFetchSize(100), "setFetchSize should not throw");
    rs.close();
  }

  @Test
  void testFetchDirection_ForwardOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.FETCH_FORWARD, rs.getFetchDirection(), "Fetch direction should be FETCH_FORWARD");
    rs.close();
  }
}
