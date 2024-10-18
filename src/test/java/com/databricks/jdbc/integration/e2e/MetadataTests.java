package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetadataTests {

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
  void testDatabaseMetadataRetrieval() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    // Basic database information
    assertFalse(
        metaData.getDatabaseProductName().isEmpty(), "Database product name should not be empty");
    assertFalse(
        metaData.getDatabaseProductVersion().isEmpty(),
        "Database product version should not be empty");
    assertFalse(metaData.getDriverName().isEmpty(), "Driver name should not be empty");
    assertFalse(metaData.getUserName().isEmpty(), "Username should not be empty");

    // Capabilities of the database
    assertTrue(
        metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY),
        "Database should support TYPE_FORWARD_ONLY ResultSet");

    // Limits imposed by the database (0 refers to infinite)
    assertTrue(metaData.getMaxConnections() >= 0, "Max connections should be greater than 0");
    assertTrue(
        metaData.getMaxTableNameLength() >= 0, "Max table name length should be greater than 0");
    assertTrue(
        metaData.getMaxColumnsInTable() >= 0, "Max columns in table should be greater than 0");
  }

  @Test
  void testResultSetMetadataRetrieval() throws SQLException {
    String tableName = "resultset_metadata_test_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY, "
            + "name VARCHAR(255), "
            + "age INT"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, name, age) VALUES (1, 'Madhav', 24)";
    executeSQL(connection, insertSQL);

    String query = "SELECT id, name, age FROM " + getFullyQualifiedTableName(tableName);

    ResultSet resultSet = executeQuery(connection, query);
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    // Check the number of columns
    int expectedColumnCount = 3;
    assertEquals(
        expectedColumnCount, resultSetMetaData.getColumnCount(), "Expected column count mismatch");

    // Check metadata for each column
    assertEquals("id", resultSetMetaData.getColumnName(1), "First column should be id");
    assertEquals(
        java.sql.Types.INTEGER,
        resultSetMetaData.getColumnType(1),
        "id column should be of type INTEGER");

    assertEquals("name", resultSetMetaData.getColumnName(2), "Second column should be name");
    assertEquals(
        java.sql.Types.VARCHAR,
        resultSetMetaData.getColumnType(2),
        "name column should be of type VARCHAR");

    assertEquals("age", resultSetMetaData.getColumnName(3), "Third column should be age");
    assertEquals(
        java.sql.Types.INTEGER,
        resultSetMetaData.getColumnType(3),
        "age column should be of type INTEGER");

    // Additional checks for column properties
    for (int i = 1; i <= expectedColumnCount; i++) {
      assertEquals(
          ResultSetMetaData.columnNullable,
          resultSetMetaData.isNullable(i),
          "Column " + i + " should be nullable");
    }
    String SQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);
    executeSQL(connection, SQL);
  }

  @Test
  void testCatalogInformation() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    // Test getCatalogs
    try (ResultSet catalogs = metaData.getCatalogs()) {
      assertTrue(catalogs.next(), "There should be at least one catalog");
      do {
        String catalogName = catalogs.getString("TABLE_CAT");
        assertNotNull(catalogName, "Catalog name should not be null");
      } while (catalogs.next());
    }
  }

  @Test
  void testSchemaInformation() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet schemas = metaData.getSchemas("main", ".*")) {
      assertTrue(schemas.next(), "There should be at least one schema");
      do {
        String schemaName = schemas.getString("TABLE_SCHEM");
        assertNotNull(schemaName, "Schema name should not be null");
      } while (schemas.next());
    }
  }

  @Test
  void testTableInformation() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String catalog = "main";
    String schemaPattern = "jdbc_test_schema";
    String tableName = "catalog_and_schema_test_table";
    setupDatabaseTable(connection, tableName);
    try (ResultSet tables = metaData.getTables(catalog, schemaPattern, "%", null)) {
      assertTrue(
          tables.next(), "There should be at least one table in the specified catalog and schema");
      do {
        String fetchedTableName = tables.getString("TABLE_NAME");
        assertNotNull(fetchedTableName, "Table name should not be null");
      } while (tables.next());
    }
    deleteTable(connection, tableName);
  }

  @Test
  void testTableInformationExactMatch() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String catalog = "main";
    String schemaPattern = "jdbc_test_schema";
    String tableName = "catalog_and_schema_test_table";
    setupDatabaseTable(connection, tableName);
    try (ResultSet tables = metaData.getTables(catalog, schemaPattern, tableName, null)) {
      assertTrue(
          tables.next(), "There should be at least one table in the specified catalog and schema");
      do {
        String fetchedTableName = tables.getString("TABLE_NAME");
        assertEquals(
            tableName, fetchedTableName, "Table name should match the specified table name");
      } while (tables.next());
    }
    deleteTable(connection, tableName);
  }
}
