package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.SESSION_PATH;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.STATEMENT_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.github.tomakehurst.wiremock.client.CountMatchingStrategy;
import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for metadata retrieval. */
public class MetadataIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    try {
      connection = getValidJDBCConnection();
    } catch (SQLException e) {
      connection = null;
    }
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

    if (isSqlExecSdkClient()) {
      // Create session request is sent
      getDatabricksApiExtension().verify(1, postRequestedFor(urlEqualTo(SESSION_PATH)));
    }
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
    assert resultSet != null;
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    // Check the number of columns
    int expectedColumnCount = 3;
    assertEquals(
        expectedColumnCount, resultSetMetaData.getColumnCount(), "Expected column count mismatch");

    // Check metadata for each column
    assertEquals("id", resultSetMetaData.getColumnName(1), "First column should be id");
    assertEquals(
        Types.INTEGER, resultSetMetaData.getColumnType(1), "id column should be of type INTEGER");

    assertEquals("name", resultSetMetaData.getColumnName(2), "Second column should be name");
    assertEquals(
        Types.VARCHAR, resultSetMetaData.getColumnType(2), "name column should be of type VARCHAR");

    assertEquals("age", resultSetMetaData.getColumnName(3), "Third column should be age");
    assertEquals(
        Types.INTEGER, resultSetMetaData.getColumnType(3), "age column should be of type INTEGER");

    // Additional checks for column properties
    for (int i = 1; i <= expectedColumnCount; i++) {
      assertEquals(
          ResultSetMetaData.columnNullable,
          resultSetMetaData.isNullable(i),
          "Column " + i + " should be nullable");
    }
    String SQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);
    executeSQL(connection, SQL);

    if (isSqlExecSdkClient()) {
      // At least 5 statement requests are sent: drop, create, insert, select, drop
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 5),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testCatalogAndSchemaInformation() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    // Test getCatalogs
    try (ResultSet catalogs = metaData.getCatalogs()) {
      assertTrue(catalogs.next(), "There should be at least one catalog");
      do {
        String catalogName = catalogs.getString("TABLE_CAT");
        assertNotNull(catalogName, "Catalog name should not be null");
      } while (catalogs.next());
    }

    // Test getSchemas
    try (ResultSet schemas = metaData.getSchemas("main", "jdbc%")) {
      assertTrue(schemas.next(), "There should be at least one schema");
      do {
        String schemaName = schemas.getString("TABLE_SCHEM");
        assertNotNull(schemaName, "Schema name should not be null");
      } while (schemas.next());
    }

    // Verify tables retrieval with specific catalog and schema
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

    // Test to get particular table
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

    if (isSqlExecSdkClient()) {
      // At least 7 statement requests are sent:
      // show catalogs, show schemas, drop table, create table, show tables, show particular table,
      // drop
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 7),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  // --- ResultSetMetaData property tests ---

  @Test
  void testResultSetMetaData_ColumnLabelAndTypeName() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS my_number, 'hello' AS my_text");
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals(2, rsmd.getColumnCount());

    // Column labels should match the aliases
    assertEquals("my_number", rsmd.getColumnLabel(1), "Column label should match alias");
    assertEquals("my_text", rsmd.getColumnLabel(2), "Column label should match alias");

    // Column type names should be non-empty
    assertNotNull(rsmd.getColumnTypeName(1), "Column type name should not be null");
    assertFalse(rsmd.getColumnTypeName(1).isEmpty(), "Column type name should not be empty");
    assertNotNull(rsmd.getColumnTypeName(2), "Column type name should not be null");

    rs.close();
  }

  @Test
  void testResultSetMetaData_PrecisionAndScale() throws SQLException {
    String tableName = "metadata_precision_scale_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT, "
            + "price DECIMAL(10, 2), "
            + "quantity BIGINT"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, price, quantity) VALUES (1, 99.95, 1000)";
    executeSQL(connection, insertSQL);

    ResultSet rs =
        executeQuery(
            connection, "SELECT id, price, quantity FROM " + getFullyQualifiedTableName(tableName));
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // DECIMAL(10, 2) should have precision=10, scale=2
    assertEquals(10, rsmd.getPrecision(2), "DECIMAL precision should be 10");
    assertEquals(2, rsmd.getScale(2), "DECIMAL scale should be 2");

    // INT and BIGINT should have scale=0
    assertEquals(0, rsmd.getScale(1), "INT scale should be 0");
    assertEquals(0, rsmd.getScale(3), "BIGINT scale should be 0");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testResultSetMetaData_ColumnDisplaySize() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num, 'hello' AS str");
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // Display size should be > 0 for all columns
    assertTrue(rsmd.getColumnDisplaySize(1) > 0, "Display size for INT should be > 0");
    assertTrue(rsmd.getColumnDisplaySize(2) > 0, "Display size for STRING should be > 0");

    rs.close();
  }

  @Test
  void testResultSetMetaData_ColumnClassName() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num, 'hello' AS str, true AS flag");
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // Each column should have a non-null, non-empty class name
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      String className = rsmd.getColumnClassName(i);
      assertNotNull(className, "Column " + i + " class name should not be null");
      assertFalse(className.isEmpty(), "Column " + i + " class name should not be empty");
    }

    rs.close();
  }

  @Test
  void testResultSetMetaData_BooleanProperties() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num, 'hello' AS str");
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      // isAutoIncrement should return false (Databricks doesn't support auto-increment)
      assertFalse(rsmd.isAutoIncrement(i), "Column " + i + " should not be auto-increment");

      // isSearchable should return true for standard query results
      assertTrue(rsmd.isSearchable(i), "Column " + i + " should be searchable");

      // isCurrency should return false
      assertFalse(rsmd.isCurrency(i), "Column " + i + " should not be currency");

      // isReadOnly should return true (ResultSet is read-only)
      assertTrue(rsmd.isReadOnly(i), "Column " + i + " should be read-only");
    }

    rs.close();
  }

  @Test
  void testResultSetMetaData_SignedProperty() throws SQLException {
    String tableName = "metadata_signed_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "int_col INT, "
            + "str_col STRING, "
            + "bool_col BOOLEAN"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (int_col, str_col, bool_col) VALUES (42, 'text', true)";
    executeSQL(connection, insertSQL);

    ResultSet rs =
        executeQuery(
            connection,
            "SELECT int_col, str_col, bool_col FROM " + getFullyQualifiedTableName(tableName));
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // INT should be signed
    assertTrue(rsmd.isSigned(1), "INT column should be signed");
    // STRING should not be signed
    assertFalse(rsmd.isSigned(2), "STRING column should not be signed");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testResultSetMetaData_MultipleDataTypes() throws SQLException {
    String tableName = "metadata_multitypes_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "tinyint_col TINYINT, "
            + "smallint_col SMALLINT, "
            + "int_col INT, "
            + "bigint_col BIGINT, "
            + "float_col FLOAT, "
            + "double_col DOUBLE, "
            + "string_col STRING, "
            + "bool_col BOOLEAN, "
            + "date_col DATE, "
            + "ts_col TIMESTAMP"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    ResultSet rs =
        executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals(10, rsmd.getColumnCount(), "Should have 10 columns");

    // Verify each column has a valid SQL type
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      int sqlType = rsmd.getColumnType(i);
      String typeName = rsmd.getColumnTypeName(i);
      assertNotNull(typeName, "Column " + i + " type name should not be null");
      assertTrue(sqlType != 0, "Column " + i + " should have a non-zero SQL type");
    }

    // Verify specific type mappings
    assertEquals("tinyint_col", rsmd.getColumnName(1));
    assertEquals("smallint_col", rsmd.getColumnName(2));
    assertEquals("int_col", rsmd.getColumnName(3));
    assertEquals("bigint_col", rsmd.getColumnName(4));
    assertEquals("float_col", rsmd.getColumnName(5));
    assertEquals("double_col", rsmd.getColumnName(6));
    assertEquals("string_col", rsmd.getColumnName(7));
    assertEquals("bool_col", rsmd.getColumnName(8));
    assertEquals("date_col", rsmd.getColumnName(9));
    assertEquals("ts_col", rsmd.getColumnName(10));

    rs.close();
    deleteTable(connection, tableName);
  }

  // --- DatabaseMetaData.getTypeInfo() test ---

  @Test
  void testGetTypeInfo_ReturnsTypeCatalog() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet typeInfo = metaData.getTypeInfo();
    assertNotNull(typeInfo, "getTypeInfo() should return non-null ResultSet");

    int typeCount = 0;
    while (typeInfo.next()) {
      typeCount++;
      String typeName = typeInfo.getString("TYPE_NAME");
      int dataType = typeInfo.getInt("DATA_TYPE");
      assertNotNull(typeName, "TYPE_NAME should not be null");
      assertFalse(typeName.isEmpty(), "TYPE_NAME should not be empty");
    }
    assertTrue(typeCount > 0, "Should have at least one type in the type catalog");

    typeInfo.close();
  }

  // --- ParameterMetaData tests ---

  @Test
  void testParameterMetaData_GetParameterCount() throws SQLException {
    PreparedStatement pstmt = connection.prepareStatement("SELECT ? AS p1, ? AS p2, ? AS p3");
    ParameterMetaData pmd = pstmt.getParameterMetaData();
    assertNotNull(pmd, "ParameterMetaData should not be null");
    assertEquals(3, pmd.getParameterCount(), "Should detect 3 parameter markers");

    pstmt.close();
  }

  @Test
  void testParameterMetaData_NoParameters() throws SQLException {
    PreparedStatement pstmt = connection.prepareStatement("SELECT 1 AS num");
    ParameterMetaData pmd = pstmt.getParameterMetaData();
    assertNotNull(pmd, "ParameterMetaData should not be null");
    assertEquals(0, pmd.getParameterCount(), "Should detect 0 parameter markers");

    pstmt.close();
  }

  @Test
  void testGetProceduresAndProcedureColumns() throws SQLException {
    assumeTrue(isSqlExecSdkClient(), "This test only runs for SQL Execution API");

    DatabaseMetaData metaData = connection.getMetaData();
    String catalog = "main";
    String schema = getDatabricksSchema();
    String procName = "jdbc_test_compute_area";

    // Create a test procedure with IN and OUT parameters
    executeSQL(
        connection,
        "CREATE OR REPLACE PROCEDURE "
            + catalog
            + "."
            + schema
            + "."
            + procName
            + "(x DOUBLE, y DOUBLE, OUT area DOUBLE)\n"
            + "LANGUAGE SQL\n"
            + "SQL SECURITY INVOKER\n"
            + "COMMENT 'Test procedure for JDBC integration tests'\n"
            + "AS BEGIN\n"
            + "  SET area = x * y;\n"
            + "END");

    try {
      // Test getProcedures - find our procedure by exact name
      try (ResultSet procedures = metaData.getProcedures(catalog, schema, procName)) {
        assertTrue(procedures.next(), "Should find the created procedure");
        assertEquals(catalog, procedures.getString("PROCEDURE_CAT"));
        assertEquals(schema, procedures.getString("PROCEDURE_SCHEM"));
        assertEquals(procName, procedures.getString("PROCEDURE_NAME"));
        assertEquals("Test procedure for JDBC integration tests", procedures.getString("REMARKS"));
        assertEquals(1, procedures.getShort("PROCEDURE_TYPE"), "Should be SQL_PT_PROCEDURE");
        assertEquals(procName, procedures.getString("SPECIFIC_NAME"));
        assertFalse(procedures.next(), "Should be exactly one match");
      }

      // Test getProcedures - pattern matching
      try (ResultSet procedures = metaData.getProcedures(catalog, schema, "jdbc_test_%")) {
        assertTrue(procedures.next(), "Pattern should match our procedure");
      }

      // Test getProcedureColumns - all parameters
      try (ResultSet columns = metaData.getProcedureColumns(catalog, schema, procName, "%")) {
        // Parameter x (IN, DOUBLE)
        assertTrue(columns.next(), "Should have parameter x");
        assertEquals(procName, columns.getString("PROCEDURE_NAME"));
        assertEquals("x", columns.getString("COLUMN_NAME"));
        assertEquals(1, columns.getShort("COLUMN_TYPE"), "x should be SQL_PARAM_INPUT");
        assertEquals(8, columns.getInt("DATA_TYPE"), "DOUBLE maps to SQL type code 8");
        assertEquals("DOUBLE", columns.getString("TYPE_NAME"));
        assertEquals(0, columns.getInt("ORDINAL_POSITION"));

        // Parameter y (IN, DOUBLE)
        assertTrue(columns.next(), "Should have parameter y");
        assertEquals("y", columns.getString("COLUMN_NAME"));
        assertEquals(1, columns.getShort("COLUMN_TYPE"), "y should be SQL_PARAM_INPUT");

        // Parameter area (OUT, DOUBLE)
        assertTrue(columns.next(), "Should have parameter area");
        assertEquals("area", columns.getString("COLUMN_NAME"));
        assertEquals(4, columns.getShort("COLUMN_TYPE"), "area should be SQL_PARAM_OUTPUT");

        assertFalse(columns.next(), "Should have exactly 3 parameters");
      }

      // Test getProcedureColumns - filter by column name
      try (ResultSet columns = metaData.getProcedureColumns(catalog, schema, procName, "area")) {
        assertTrue(columns.next(), "Should find the 'area' parameter");
        assertEquals("area", columns.getString("COLUMN_NAME"));
        assertEquals(4, columns.getShort("COLUMN_TYPE"), "area should be SQL_PARAM_OUTPUT");
        assertFalse(columns.next(), "Should be exactly one match");
      }

    } finally {
      executeSQL(connection, "DROP PROCEDURE IF EXISTS " + catalog + "." + schema + "." + procName);
    }
  }

  @Test
  void testMetadataOperationsWithHyphenatedIdentifiers() throws SQLException {
    assumeTrue(isSqlExecSdkClient(), "This test only runs for SQL Execution API");
    Connection testConnection = connection;
    if (testConnection == null) {
      testConnection = getValidJDBCConnection();
    }
    DatabaseMetaData metaData = testConnection.getMetaData();

    String existingCatalog = "main";
    String schemaWithHyphens = "test-schema-hyphen";
    String tableWithHyphens = "test-table-hyphen";

    try {
      executeSQL(
          testConnection,
          "CREATE SCHEMA IF NOT EXISTS `" + existingCatalog + "`.`" + schemaWithHyphens + "`");

      executeSQL(
          testConnection,
          "CREATE TABLE IF NOT EXISTS `"
              + existingCatalog
              + "`.`"
              + schemaWithHyphens
              + "`.`"
              + tableWithHyphens
              + "` (id INT, name STRING)");

      try (ResultSet tables = metaData.getTables(existingCatalog, schemaWithHyphens, "%", null)) {
        assertTrue(tables.next(), "Should retrieve tables from schema with hyphens");
        String tableName = tables.getString("TABLE_NAME");
        assertNotNull(tableName, "Table name should not be null");
      }

      try (ResultSet columns =
          metaData.getColumns(existingCatalog, schemaWithHyphens, tableWithHyphens, null)) {
        assertTrue(columns.next(), "Should retrieve columns from table with hyphens");
        String columnName = columns.getString("COLUMN_NAME");
        assertNotNull(columnName, "Column name should not be null");
      }

    } finally {
      executeSQL(
          testConnection,
          "DROP SCHEMA IF EXISTS `" + existingCatalog + "`.`" + schemaWithHyphens + "` CASCADE");
    }
  }

  // --- DatabaseMetaData capability and info tests ---

  @Test
  void testDatabaseMetaData_GetColumns() throws SQLException {
    String tableName = "meta_columns_test_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT, name VARCHAR(255), amount DECIMAL(10, 2))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet columns =
        metaData.getColumns(getDatabricksCatalog(), getDatabricksSchema(), tableName, null)) {
      int columnCount = 0;
      while (columns.next()) {
        columnCount++;
        assertNotNull(columns.getString("COLUMN_NAME"), "COLUMN_NAME should not be null");
        assertNotNull(columns.getString("TYPE_NAME"), "TYPE_NAME should not be null");
      }
      assertEquals(3, columnCount, "Should have 3 columns");
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testDatabaseMetaData_GetPrimaryKeys() throws SQLException {
    String tableName = "meta_pk_test_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, name VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet pks =
        metaData.getPrimaryKeys(getDatabricksCatalog(), getDatabricksSchema(), tableName)) {
      // getPrimaryKeys may or may not return results depending on the backend
      // We just verify the call doesn't throw
      assertNotNull(pks, "getPrimaryKeys should return non-null ResultSet");
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testDatabaseMetaData_GetTableTypes() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet tableTypes = metaData.getTableTypes()) {
      assertNotNull(tableTypes, "getTableTypes() should return non-null");
      assertTrue(tableTypes.next(), "Should have at least one table type");

      boolean hasTable = false;
      do {
        String type = tableTypes.getString("TABLE_TYPE");
        if ("TABLE".equals(type)) hasTable = true;
      } while (tableTypes.next());
      assertTrue(hasTable, "TABLE type should be in the table types list");
    }
  }

  @Test
  void testDatabaseMetaData_SupportsTransactions() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    // Should not throw - just verify it returns a boolean
    metaData.supportsTransactions();
  }

  @Test
  void testDatabaseMetaData_SupportsResultSetType() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    assertTrue(
        metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY),
        "Should support TYPE_FORWARD_ONLY");
  }

  @Test
  void testDatabaseMetaData_GetURL() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String url = metaData.getURL();
    assertNotNull(url, "getURL() should return non-null");
    assertTrue(url.startsWith("jdbc:databricks://"), "URL should start with jdbc:databricks://");
  }

  @Test
  void testDatabaseMetaData_GetDriverVersion() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String version = metaData.getDriverVersion();
    assertNotNull(version, "getDriverVersion() should return non-null");
    assertFalse(version.isEmpty(), "getDriverVersion() should return non-empty string");
  }

  // --- Mega-assertion test for all boolean property getters (Snowflake pattern) ---

  @Test
  void testDatabaseMetaData_AllBooleanProperties() throws SQLException {
    DatabaseMetaData md = connection.getMetaData();

    // --- Methods expected to return true ---
    assertTrue(md.allProceduresAreCallable(), "allProceduresAreCallable");
    assertTrue(md.allTablesAreSelectable(), "allTablesAreSelectable");
    assertTrue(md.nullsAreSortedLow(), "nullsAreSortedLow");
    assertTrue(md.storesMixedCaseIdentifiers(), "storesMixedCaseIdentifiers");
    assertTrue(md.supportsMixedCaseQuotedIdentifiers(), "supportsMixedCaseQuotedIdentifiers");
    assertTrue(md.supportsColumnAliasing(), "supportsColumnAliasing");
    assertTrue(md.nullPlusNonNullIsNull(), "nullPlusNonNullIsNull");
    assertTrue(md.supportsConvert(), "supportsConvert() no-arg");
    assertTrue(md.supportsTableCorrelationNames(), "supportsTableCorrelationNames");
    assertTrue(md.supportsExpressionsInOrderBy(), "supportsExpressionsInOrderBy");
    assertTrue(md.supportsGroupBy(), "supportsGroupBy");
    assertTrue(md.supportsGroupByBeyondSelect(), "supportsGroupByBeyondSelect");
    assertTrue(md.supportsLikeEscapeClause(), "supportsLikeEscapeClause");
    assertTrue(md.supportsMultipleTransactions(), "supportsMultipleTransactions");
    assertTrue(md.supportsMinimumSQLGrammar(), "supportsMinimumSQLGrammar");
    assertTrue(md.supportsCoreSQLGrammar(), "supportsCoreSQLGrammar");
    assertTrue(md.supportsANSI92EntryLevelSQL(), "supportsANSI92EntryLevelSQL");
    assertTrue(md.supportsFullOuterJoins(), "supportsFullOuterJoins");
    assertTrue(md.supportsSchemasInDataManipulation(), "supportsSchemasInDataManipulation");
    assertTrue(md.supportsSchemasInTableDefinitions(), "supportsSchemasInTableDefinitions");
    assertTrue(md.supportsSchemasInIndexDefinitions(), "supportsSchemasInIndexDefinitions");
    assertTrue(md.supportsSchemasInPrivilegeDefinitions(), "supportsSchemasInPrivilegeDefinitions");
    assertTrue(md.supportsCatalogsInDataManipulation(), "supportsCatalogsInDataManipulation");
    assertTrue(md.supportsCatalogsInProcedureCalls(), "supportsCatalogsInProcedureCalls");
    assertTrue(md.supportsCatalogsInTableDefinitions(), "supportsCatalogsInTableDefinitions");
    assertTrue(md.supportsCatalogsInIndexDefinitions(), "supportsCatalogsInIndexDefinitions");
    assertTrue(
        md.supportsCatalogsInPrivilegeDefinitions(), "supportsCatalogsInPrivilegeDefinitions");
    assertTrue(md.supportsStoredProcedures(), "supportsStoredProcedures");
    assertTrue(md.supportsSubqueriesInComparisons(), "supportsSubqueriesInComparisons");
    assertTrue(md.supportsSubqueriesInExists(), "supportsSubqueriesInExists");
    assertTrue(md.supportsSubqueriesInIns(), "supportsSubqueriesInIns");
    assertTrue(md.supportsSubqueriesInQuantifieds(), "supportsSubqueriesInQuantifieds");
    assertTrue(md.supportsCorrelatedSubqueries(), "supportsCorrelatedSubqueries");
    assertTrue(md.supportsUnion(), "supportsUnion");
    assertTrue(md.supportsUnionAll(), "supportsUnionAll");
    assertTrue(md.supportsOpenStatementsAcrossCommit(), "supportsOpenStatementsAcrossCommit");
    assertTrue(md.supportsOpenStatementsAcrossRollback(), "supportsOpenStatementsAcrossRollback");
    assertTrue(md.isCatalogAtStart(), "isCatalogAtStart");
    assertTrue(md.autoCommitFailureClosesAllResultSets(), "autoCommitFailureClosesAllResultSets");

    // --- Methods expected to return false ---
    assertFalse(md.isReadOnly(), "isReadOnly");
    assertFalse(md.nullsAreSortedHigh(), "nullsAreSortedHigh");
    assertFalse(md.nullsAreSortedAtStart(), "nullsAreSortedAtStart");
    assertFalse(md.nullsAreSortedAtEnd(), "nullsAreSortedAtEnd");
    assertFalse(md.usesLocalFiles(), "usesLocalFiles");
    assertFalse(md.usesLocalFilePerTable(), "usesLocalFilePerTable");
    assertFalse(md.supportsMixedCaseIdentifiers(), "supportsMixedCaseIdentifiers");
    assertFalse(md.storesUpperCaseIdentifiers(), "storesUpperCaseIdentifiers");
    assertFalse(md.storesLowerCaseIdentifiers(), "storesLowerCaseIdentifiers");
    assertFalse(md.storesUpperCaseQuotedIdentifiers(), "storesUpperCaseQuotedIdentifiers");
    assertFalse(md.storesLowerCaseQuotedIdentifiers(), "storesLowerCaseQuotedIdentifiers");
    assertFalse(md.storesMixedCaseQuotedIdentifiers(), "storesMixedCaseQuotedIdentifiers");
    assertFalse(md.supportsAlterTableWithAddColumn(), "supportsAlterTableWithAddColumn");
    assertFalse(md.supportsAlterTableWithDropColumn(), "supportsAlterTableWithDropColumn");
    assertFalse(
        md.supportsDifferentTableCorrelationNames(), "supportsDifferentTableCorrelationNames");
    assertFalse(md.supportsOrderByUnrelated(), "supportsOrderByUnrelated");
    assertFalse(md.supportsGroupByUnrelated(), "supportsGroupByUnrelated");
    assertFalse(md.supportsMultipleResultSets(), "supportsMultipleResultSets");
    assertFalse(md.supportsNonNullableColumns(), "supportsNonNullableColumns");
    assertFalse(md.supportsExtendedSQLGrammar(), "supportsExtendedSQLGrammar");
    assertFalse(md.supportsANSI92IntermediateSQL(), "supportsANSI92IntermediateSQL");
    assertFalse(md.supportsANSI92FullSQL(), "supportsANSI92FullSQL");
    assertFalse(md.supportsIntegrityEnhancementFacility(), "supportsIntegrityEnhancementFacility");
    assertFalse(md.supportsOuterJoins(), "supportsOuterJoins");
    assertFalse(md.supportsLimitedOuterJoins(), "supportsLimitedOuterJoins");
    assertFalse(md.supportsPositionedDelete(), "supportsPositionedDelete");
    assertFalse(md.supportsPositionedUpdate(), "supportsPositionedUpdate");
    assertFalse(md.supportsSelectForUpdate(), "supportsSelectForUpdate");
    assertFalse(md.supportsOpenCursorsAcrossCommit(), "supportsOpenCursorsAcrossCommit");
    assertFalse(md.supportsOpenCursorsAcrossRollback(), "supportsOpenCursorsAcrossRollback");
    assertFalse(md.doesMaxRowSizeIncludeBlobs(), "doesMaxRowSizeIncludeBlobs");
    assertFalse(md.supportsSavepoints(), "supportsSavepoints");
    assertFalse(md.supportsNamedParameters(), "supportsNamedParameters");
    assertFalse(md.supportsMultipleOpenResults(), "supportsMultipleOpenResults");
    assertFalse(md.supportsGetGeneratedKeys(), "supportsGetGeneratedKeys");
    assertFalse(md.supportsStatementPooling(), "supportsStatementPooling");
    assertFalse(md.locatorsUpdateCopy(), "locatorsUpdateCopy");
    assertFalse(
        md.supportsStoredFunctionsUsingCallSyntax(), "supportsStoredFunctionsUsingCallSyntax");
    assertFalse(md.supportsBatchUpdates(), "supportsBatchUpdates");
    assertFalse(md.generatedKeyAlwaysReturned(), "generatedKeyAlwaysReturned");

    // --- Parameterized boolean methods ---
    assertTrue(
        md.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY),
        "supportsResultSetType(TYPE_FORWARD_ONLY)");
    assertFalse(
        md.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE),
        "supportsResultSetType(TYPE_SCROLL_INSENSITIVE)");
    assertFalse(
        md.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE),
        "supportsResultSetType(TYPE_SCROLL_SENSITIVE)");

    assertTrue(
        md.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY),
        "supportsResultSetConcurrency(FORWARD_ONLY, READ_ONLY)");
    assertFalse(
        md.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE),
        "supportsResultSetConcurrency(FORWARD_ONLY, UPDATABLE)");

    assertTrue(
        md.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT),
        "supportsResultSetHoldability(CLOSE_CURSORS_AT_COMMIT)");
    assertFalse(
        md.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT),
        "supportsResultSetHoldability(HOLD_CURSORS_OVER_COMMIT)");

    assertTrue(
        md.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ),
        "supportsTransactionIsolationLevel(REPEATABLE_READ)");
    assertFalse(
        md.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE),
        "supportsTransactionIsolationLevel(SERIALIZABLE)");

    // Visibility/detectability methods (all return false regardless of type parameter)
    assertFalse(md.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY), "ownUpdatesAreVisible");
    assertFalse(md.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY), "ownDeletesAreVisible");
    assertFalse(md.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY), "ownInsertsAreVisible");
    assertFalse(md.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY), "othersUpdatesAreVisible");
    assertFalse(md.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY), "othersDeletesAreVisible");
    assertFalse(md.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY), "othersInsertsAreVisible");
    assertFalse(md.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY), "updatesAreDetected");
    assertFalse(md.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY), "deletesAreDetected");
    assertFalse(md.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY), "insertsAreDetected");
  }

  // --- Mega-assertion test for all string/int property getters ---

  @Test
  void testDatabaseMetaData_AllStringAndIntProperties() throws SQLException {
    DatabaseMetaData md = connection.getMetaData();

    // --- String property getters ---
    assertNotNull(md.getDatabaseProductName(), "getDatabaseProductName");
    assertNotNull(md.getDatabaseProductVersion(), "getDatabaseProductVersion");
    assertNotNull(md.getDriverName(), "getDriverName");
    assertNotNull(md.getDriverVersion(), "getDriverVersion");
    assertNotNull(md.getURL(), "getURL");
    assertNotNull(md.getUserName(), "getUserName");
    assertNotNull(md.getIdentifierQuoteString(), "getIdentifierQuoteString");
    assertNotNull(md.getSQLKeywords(), "getSQLKeywords");
    assertNotNull(md.getNumericFunctions(), "getNumericFunctions");
    assertNotNull(md.getStringFunctions(), "getStringFunctions");
    assertNotNull(md.getSystemFunctions(), "getSystemFunctions");
    assertNotNull(md.getTimeDateFunctions(), "getTimeDateFunctions");
    assertNotNull(md.getSearchStringEscape(), "getSearchStringEscape");
    assertNotNull(md.getExtraNameCharacters(), "getExtraNameCharacters");
    assertNotNull(md.getSchemaTerm(), "getSchemaTerm");
    assertNotNull(md.getProcedureTerm(), "getProcedureTerm");
    assertNotNull(md.getCatalogTerm(), "getCatalogTerm");
    assertNotNull(md.getCatalogSeparator(), "getCatalogSeparator");

    // Verify some specific expected values
    assertEquals(".", md.getCatalogSeparator(), "catalogSeparator should be dot");
    assertEquals("`", md.getIdentifierQuoteString(), "identifierQuoteString should be backtick");
    assertEquals("schema", md.getSchemaTerm(), "schemaTerm should be 'schema'");
    assertEquals("catalog", md.getCatalogTerm(), "catalogTerm should be 'catalog'");

    // --- Int property getters (max limits) ---
    // Most return 0 (meaning no limit)
    assertTrue(md.getMaxBinaryLiteralLength() >= 0, "getMaxBinaryLiteralLength");
    assertTrue(md.getMaxCharLiteralLength() >= 0, "getMaxCharLiteralLength");
    assertTrue(md.getMaxColumnNameLength() >= 0, "getMaxColumnNameLength");
    assertTrue(md.getMaxColumnsInGroupBy() >= 0, "getMaxColumnsInGroupBy");
    assertTrue(md.getMaxColumnsInIndex() >= 0, "getMaxColumnsInIndex");
    assertTrue(md.getMaxColumnsInOrderBy() >= 0, "getMaxColumnsInOrderBy");
    assertTrue(md.getMaxColumnsInSelect() >= 0, "getMaxColumnsInSelect");
    assertTrue(md.getMaxColumnsInTable() >= 0, "getMaxColumnsInTable");
    assertTrue(md.getMaxConnections() >= 0, "getMaxConnections");
    assertTrue(md.getMaxCursorNameLength() >= 0, "getMaxCursorNameLength");
    assertTrue(md.getMaxIndexLength() >= 0, "getMaxIndexLength");
    assertTrue(md.getMaxSchemaNameLength() >= 0, "getMaxSchemaNameLength");
    assertTrue(md.getMaxProcedureNameLength() >= 0, "getMaxProcedureNameLength");
    assertTrue(md.getMaxCatalogNameLength() >= 0, "getMaxCatalogNameLength");
    assertTrue(md.getMaxRowSize() >= 0, "getMaxRowSize");
    assertTrue(md.getMaxStatementLength() >= 0, "getMaxStatementLength");
    assertTrue(md.getMaxStatements() >= 0, "getMaxStatements");
    assertTrue(md.getMaxTableNameLength() >= 0, "getMaxTableNameLength");
    assertTrue(md.getMaxTablesInSelect() >= 0, "getMaxTablesInSelect");
    assertTrue(md.getMaxUserNameLength() >= 0, "getMaxUserNameLength");
    assertTrue(md.getMaxLogicalLobSize() >= 0, "getMaxLogicalLobSize");

    // Transaction and version ints
    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        md.getDefaultTransactionIsolation(),
        "defaultTransactionIsolation");
    assertTrue(md.getDriverMajorVersion() >= 0, "getDriverMajorVersion");
    assertTrue(md.getDriverMinorVersion() >= 0, "getDriverMinorVersion");
    assertTrue(md.getDatabaseMajorVersion() >= 0, "getDatabaseMajorVersion");
    assertTrue(md.getDatabaseMinorVersion() >= 0, "getDatabaseMinorVersion");
    assertTrue(md.getJDBCMajorVersion() >= 0, "getJDBCMajorVersion");
    assertTrue(md.getJDBCMinorVersion() >= 0, "getJDBCMinorVersion");
    assertEquals(
        DatabaseMetaData.sqlStateSQL,
        md.getSQLStateType(),
        "getSQLStateType should be sqlStateSQL");
    assertEquals(
        ResultSet.CLOSE_CURSORS_AT_COMMIT, md.getResultSetHoldability(), "getResultSetHoldability");
  }

  // --- ResultSet-returning metadata methods ---

  @Test
  void testDatabaseMetaData_GetTypeInfo() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet typeInfo = metaData.getTypeInfo()) {
      assertNotNull(typeInfo, "getTypeInfo() should return non-null");
      assertTrue(typeInfo.next(), "Should have at least one type info row");
      do {
        String typeName = typeInfo.getString("TYPE_NAME");
        assertNotNull(typeName, "TYPE_NAME should not be null");
        int dataType = typeInfo.getInt("DATA_TYPE");
        // DATA_TYPE should be a valid java.sql.Types constant (can be any int)
        assertFalse(typeInfo.wasNull(), "DATA_TYPE should not be null");
      } while (typeInfo.next());
    }
  }

  @Test
  void testDatabaseMetaData_GetSchemas_WithCatalog() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet schemas = metaData.getSchemas(getDatabricksCatalog(), "%")) {
      assertNotNull(schemas, "getSchemas() should return non-null");
      assertTrue(schemas.next(), "Should have at least one schema");
      do {
        String schemaName = schemas.getString("TABLE_SCHEM");
        assertNotNull(schemaName, "TABLE_SCHEM should not be null");
      } while (schemas.next());
    }
  }

  @Test
  void testDatabaseMetaData_GetProcedures() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet procedures =
        metaData.getProcedures(getDatabricksCatalog(), getDatabricksSchema(), "%")) {
      assertNotNull(procedures, "getProcedures() should return non-null ResultSet");
      // Databricks may return empty but should not throw
    }
  }

  @Test
  void testDatabaseMetaData_GetProcedureColumns() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet procCols =
        metaData.getProcedureColumns(getDatabricksCatalog(), getDatabricksSchema(), "%", "%")) {
      assertNotNull(procCols, "getProcedureColumns() should return non-null ResultSet");
    }
  }

  @Test
  void testDatabaseMetaData_GetFunctions() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet functions =
        metaData.getFunctions(getDatabricksCatalog(), getDatabricksSchema(), "%")) {
      assertNotNull(functions, "getFunctions() should return non-null ResultSet");
      // May or may not have functions in test schema, just verify no exception
    }
  }

  @Test
  void testDatabaseMetaData_GetFunctionColumns() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet funcCols =
        metaData.getFunctionColumns(getDatabricksCatalog(), getDatabricksSchema(), "%", "%")) {
      assertNotNull(funcCols, "getFunctionColumns() should return non-null ResultSet");
    }
  }

  @Test
  void testDatabaseMetaData_GetImportedKeys() throws SQLException {
    String tableName = "meta_imported_keys_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT, name VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet importedKeys =
        metaData.getImportedKeys(getDatabricksCatalog(), getDatabricksSchema(), tableName)) {
      assertNotNull(importedKeys, "getImportedKeys() should return non-null ResultSet");
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testDatabaseMetaData_GetExportedKeys() throws SQLException {
    String tableName = "meta_exported_keys_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT, name VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet exportedKeys =
        metaData.getExportedKeys(getDatabricksCatalog(), getDatabricksSchema(), tableName)) {
      assertNotNull(exportedKeys, "getExportedKeys() should return non-null ResultSet");
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testDatabaseMetaData_GetCrossReference() throws SQLException {
    String parentTable = "meta_cross_ref_parent";
    String foreignTable = "meta_cross_ref_child";
    String parentCreateSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(parentTable)
            + " (id INT, name VARCHAR(255))";
    String foreignCreateSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(foreignTable)
            + " (id INT, parent_id INT)";
    setupDatabaseTable(connection, parentTable, parentCreateSQL);
    setupDatabaseTable(connection, foreignTable, foreignCreateSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet crossRef =
        metaData.getCrossReference(
            getDatabricksCatalog(),
            getDatabricksSchema(),
            parentTable,
            getDatabricksCatalog(),
            getDatabricksSchema(),
            foreignTable)) {
      assertNotNull(crossRef, "getCrossReference() should return non-null ResultSet");
    }

    deleteTable(connection, parentTable);
    deleteTable(connection, foreignTable);
  }

  @Test
  void testDatabaseMetaData_GetIndexInfo() throws SQLException {
    String tableName = "meta_index_info_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT, name VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet indexInfo =
        metaData.getIndexInfo(
            getDatabricksCatalog(), getDatabricksSchema(), tableName, false, true)) {
      assertNotNull(indexInfo, "getIndexInfo() should return non-null ResultSet");
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testDatabaseMetaData_GetColumnPrivileges() throws SQLException {
    String tableName = "meta_col_priv_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT, name VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet colPrivs =
        metaData.getColumnPrivileges(
            getDatabricksCatalog(), getDatabricksSchema(), tableName, "%")) {
      assertNotNull(colPrivs, "getColumnPrivileges() should return non-null ResultSet");
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testDatabaseMetaData_GetTablePrivileges() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet tablePrivs =
        metaData.getTablePrivileges(getDatabricksCatalog(), getDatabricksSchema(), "%")) {
      assertNotNull(tablePrivs, "getTablePrivileges() should return non-null ResultSet");
    }
  }

  @Test
  void testDatabaseMetaData_GetVersionColumns() throws SQLException {
    String tableName = "meta_version_cols_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT, name VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet versionCols =
        metaData.getVersionColumns(getDatabricksCatalog(), getDatabricksSchema(), tableName)) {
      assertNotNull(versionCols, "getVersionColumns() should return non-null ResultSet");
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testDatabaseMetaData_GetBestRowIdentifier() throws SQLException {
    String tableName = "meta_best_row_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, name VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet bestRow =
        metaData.getBestRowIdentifier(
            getDatabricksCatalog(),
            getDatabricksSchema(),
            tableName,
            DatabaseMetaData.bestRowSession,
            true)) {
      assertNotNull(bestRow, "getBestRowIdentifier() should return non-null ResultSet");
    }

    deleteTable(connection, tableName);
  }

  @Test
  void testDatabaseMetaData_GetUDTs() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet udts =
        metaData.getUDTs(getDatabricksCatalog(), getDatabricksSchema(), "%", null)) {
      assertNotNull(udts, "getUDTs() should return non-null ResultSet");
    }
  }

  @Test
  void testDatabaseMetaData_GetSuperTypes() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet superTypes =
        metaData.getSuperTypes(getDatabricksCatalog(), getDatabricksSchema(), "%")) {
      assertNotNull(superTypes, "getSuperTypes() should return non-null ResultSet");
    }
  }

  @Test
  void testDatabaseMetaData_GetSuperTables() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet superTables =
        metaData.getSuperTables(getDatabricksCatalog(), getDatabricksSchema(), "%")) {
      assertNotNull(superTables, "getSuperTables() should return non-null ResultSet");
    }
  }

  @Test
  void testDatabaseMetaData_GetAttributes() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet attrs =
        metaData.getAttributes(getDatabricksCatalog(), getDatabricksSchema(), "%", "%")) {
      assertNotNull(attrs, "getAttributes() should return non-null ResultSet");
    }
  }

  @Test
  void testDatabaseMetaData_GetClientInfoProperties() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet clientInfo = metaData.getClientInfoProperties()) {
      assertNotNull(clientInfo, "getClientInfoProperties() should return non-null ResultSet");
    }
  }

  @Test
  void testDatabaseMetaData_GetPseudoColumns() throws SQLException {
    String tableName = "meta_pseudo_cols_table";
    String createSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT, name VARCHAR(255))";
    setupDatabaseTable(connection, tableName, createSQL);

    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet pseudoCols =
        metaData.getPseudoColumns(getDatabricksCatalog(), getDatabricksSchema(), tableName, "%")) {
      assertNotNull(pseudoCols, "getPseudoColumns() should return non-null ResultSet");
    }

    deleteTable(connection, tableName);
  }

  // --- Other DatabaseMetaData methods ---

  @Test
  void testDatabaseMetaData_GetConnection() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    Connection conn = metaData.getConnection();
    assertNotNull(conn, "getConnection() should return non-null");
    assertFalse(conn.isClosed(), "Connection from getConnection() should not be closed");
  }

  @Test
  void testDatabaseMetaData_GetRowIdLifetime() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    RowIdLifetime lifetime = metaData.getRowIdLifetime();
    assertEquals(
        RowIdLifetime.ROWID_UNSUPPORTED, lifetime, "getRowIdLifetime should be ROWID_UNSUPPORTED");
  }

  @Test
  void testDatabaseMetaData_WrapperMethods() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    assertTrue(
        metaData.isWrapperFor(DatabaseMetaData.class),
        "Should be wrapper for DatabaseMetaData.class");
    assertNotNull(
        metaData.unwrap(DatabaseMetaData.class), "unwrap(DatabaseMetaData.class) should succeed");
  }
}
