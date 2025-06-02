package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Integration tests for PreparedStatement operations. */
public class PreparedStatementIntegrationTests extends AbstractFakeServiceIntegrationTests {

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

  private void insertTestData(String tableName) {
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(connection, insertSQL);
  }

  @Test
  void testPreparedStatementExecution() throws SQLException {
    String tableName = "prepared_statement_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(tableName);

    String selectSQL = "SELECT * FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(selectSQL)) {
      statement.setInt(1, 1);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "Should return at least one result");
        assertEquals("value1", resultSet.getString("col1"), "Column 'col1' should match");
        assertEquals("value2", resultSet.getString("col2"), "Column 'col2' should match");
      }
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testParameterBindingInPreparedStatement() throws SQLException {
    String tableName = "parameter_binding_test_table";
    setupDatabaseTable(connection, tableName);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(insertSQL)) {
      statement.setInt(1, 2);
      statement.setString(2, "value1");
      statement.setString(3, "value2");
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be inserted");
    }

    verifyInsertedData(tableName, 2, "value1", "value2");
    deleteTable(connection, tableName);
  }

  @Test
  void testPreparedStatementComplexQueryExecution() throws SQLException {
    String tableName = "prepared_statement_complex_query_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(tableName);

    String updateSQL =
        "UPDATE " + getFullyQualifiedTableName(tableName) + " SET col1 = ? WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(updateSQL)) {
      statement.setString(1, "Updated value");
      statement.setInt(2, 1);
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be updated");
    }

    verifyInsertedData(tableName, 1, "Updated value", "value2");
    deleteTable(connection, tableName);
  }

  @Test
  void testHandlingNullValuesWithPreparedStatement() throws SQLException {
    String tableName = "prepared_statement_null_handling_test_table";
    setupDatabaseTable(connection, tableName);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(insertSQL)) {
      statement.setInt(1, 6);
      statement.setNull(2, java.sql.Types.VARCHAR);
      statement.setString(3, "value1");
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be inserted with a null col1");
    }

    verifyInsertedData(tableName, 6, null, "value1");
    deleteTable(connection, tableName);
  }

  @Test
  public void testGetMetaData_NoResultSet() throws Exception {
    String tableName = "prepared_statement_all_data_types_test_table";

    String dropSQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);
    executeSQL(connection, dropSQL);

    String createSQL =
        "CREATE TABLE "
            + getFullyQualifiedTableName(tableName)
            + " (id INT,\n"
            + "    name STRING,\n"
            + "    age INT,\n"
            + "    salary DECIMAL(10, 2),\n"
            + "    birth_date DATE,\n"
            + "    registration_timestamp TIMESTAMP,\n"
            + "    registration_timestamp_ntz TIMESTAMP_NTZ,\n"
            + "    is_active BOOLEAN,\n"
            + "    data BINARY,\n"
            + "    scores ARRAY<INT>,\n"
            + "    properties MAP<STRING, STRING>,\n"
            + "    address STRUCT<street: STRING, city: STRING, zip: INT>,\n"
            + "    variant_col variant)";
    setupDatabaseTable(connection, tableName, createSQL);

    String selectSQL = "SELECT * FROM " + getFullyQualifiedTableName(tableName);

    PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);

    MetadataResultSetBuilder metadataResultSetBuilder = new MetadataResultSetBuilder(null);

    // MetaData without executing the query
    ResultSetMetaData resultSetMetaData = preparedStatement.getMetaData();

    // Metadata after executing the query
    preparedStatement.executeQuery();
    ResultSetMetaData expectedMetaData = preparedStatement.getMetaData();

    assertEquals(expectedMetaData.getColumnCount(), resultSetMetaData.getColumnCount());
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(expectedMetaData.getColumnName(i), resultSetMetaData.getColumnName(i));

      if (expectedMetaData.getColumnName(i).equals("variant_col")) {
        // Skip type comparison for variant_col as it differs between SEA and Thrift
        // SEA treats VARIANT as TypeName  - VARIANT and Type as Types.OTHER (1111)
        // Thrift treats VARIANT as TypeName - STRING and Type as Types.VARCHAR (12)
      } else {
        assertEquals(expectedMetaData.getColumnType(i), resultSetMetaData.getColumnType(i));
        // Striping base type name as SEA returns ARRAY<INT> and Thrift returns ARRAY
        assertEquals(
            metadataResultSetBuilder.stripBaseTypeName(expectedMetaData.getColumnTypeName(i)),
            resultSetMetaData.getColumnTypeName(i));
      }
      assertEquals(expectedMetaData.getPrecision(i), resultSetMetaData.getPrecision(i));
      assertEquals(expectedMetaData.getScale(i), resultSetMetaData.getScale(i));
    }
    deleteTable(connection, tableName);
  }

  static Stream<byte[]> sampleByteDataProvider() {
    return Stream.of(new byte[] {1, 2, 3}, new byte[] {});
  }

  @ParameterizedTest
  @MethodSource("sampleByteDataProvider")
  void testPreparedStatementSetBytesValidArray(byte[] sampleData) throws SQLException {
    Properties p = new Properties();
    p.setProperty("supportManyParameters", "1");
    connection = getValidJDBCConnection(p);
    String tableName = "prepared_statement_test_bytes_table";
    String createSql =
        "CREATE TABLE " + getFullyQualifiedTableName(tableName) + " (col1 INT, col2 BINARY)";
    setupDatabaseTable(connection, tableName, createSql);

    String insertSql = "INSERT INTO " + getFullyQualifiedTableName(tableName) + " VALUES (?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
      statement.setInt(1, 1);
      statement.setBytes(2, sampleData);
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be inserted");
    }
    String selectSql =
        "SELECT col1, col2 FROM " + getFullyQualifiedTableName(tableName) + " WHERE col1 = ?";
    try (PreparedStatement statement = connection.prepareStatement(selectSql)) {
      statement.setInt(1, 1);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "Should return at least one result");
        assertArrayEquals(sampleData, resultSet.getBytes("col2"), "Column 'col2' should match");
      }
    }
    deleteTable(connection, tableName);
  }

  static Stream<Object[]> setObjectTestProvider() {
    return Stream.of(
        new Object[] {1, 1, Types.INTEGER, "INT", 0, 1},
        new Object[] {
          2, 1.236, Types.DOUBLE, "DOUBLE", 2, 1.236 // rounding does not happen for double
        },
        new Object[] {
          3, new BigDecimal("123.45"), Types.DECIMAL, "DECIMAL(10, 2)", 2, new BigDecimal("123.45")
        },
        new Object[] {
          4,
          new BigDecimal("123.456"),
          Types.DECIMAL,
          "DECIMAL(10, 2)",
          2,
          new BigDecimal("123.46") // Rounding case
        },
        new Object[] {
          5,
          new BigDecimal("0.00"),
          Types.DECIMAL,
          "DECIMAL(10, 4)",
          4,
          new BigDecimal("0.0000") // edge case (precision < scale)
        },
        new Object[] {6, "test", Types.VARCHAR, "VARCHAR(255)", 0, "test"},
        new Object[] {7, null, Types.VARCHAR, "VARCHAR(255)", 0, null},
        new Object[] {8, true, Types.BOOLEAN, "BOOLEAN", 0, true});
  }

  @ParameterizedTest
  @MethodSource("setObjectTestProvider")
  void testSetObject(
      int id, Object value, int sqlType, String sqlTypeName, int scaleOrLength, Object expected)
      throws SQLException {
    String tableName = "prepared_statement_set_object_test_table";
    String createSql =
        "CREATE TABLE "
            + getFullyQualifiedTableName(tableName)
            + " (id INT, col1 "
            + sqlTypeName
            + ")";
    setupDatabaseTable(connection, tableName, createSql);

    String insertSql = "INSERT INTO " + getFullyQualifiedTableName(tableName) + " VALUES (?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
      statement.setInt(1, id);
      statement.setObject(2, value, sqlType, scaleOrLength);
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be inserted");
    }

    String selectSql =
        "SELECT col1 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(selectSql)) {
      statement.setInt(1, id);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "Should return at least one result");
        assertEquals(
            expected, resultSet.getObject("col1"), "Column 'col1' should match expected value");
      }
    }

    deleteTable(connection, tableName);
  }

  private void verifyInsertedData(
      String tableName, int id, String col1Expected, String col2Expected) throws SQLException {
    String selectSQL =
        "SELECT col1, col2 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(selectSQL)) {
      statement.setInt(1, id);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "Should return at least one result");
        assertEquals(
            col1Expected,
            resultSet.getString("col1"),
            "Column 'col1' should match expected value.");
        assertEquals(
            col2Expected,
            resultSet.getString("col2"),
            "Column 'col2' should match expected value.");
      }
    }
  }
}
