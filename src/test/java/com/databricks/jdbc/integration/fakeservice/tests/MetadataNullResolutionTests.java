package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.*;
import org.junit.jupiter.api.*;

/**
 * Integration tests for null catalog/schema resolution in key-based SEA metadata operations.
 *
 * <p>These tests verify that null catalog resolves to current_catalog, null schema resolves to
 * current_schema (when catalog is null or matches current_catalog), and null table returns empty
 * results — matching Thrift server behavior.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetadataNullResolutionTests extends AbstractFakeServiceIntegrationTests {

  private static final String TEST_SCHEMA_NAME = "sea_null_resolve_test";
  private static final String PARENT_TABLE = "null_resolve_parent";
  private static final String CHILD_TABLE = "null_resolve_child";

  private Connection connection;
  private String testCatalog;

  @BeforeEach
  void setUp() throws SQLException {
    try {
      connection = getValidJDBCConnection();
      testCatalog = getDatabricksCatalog();
    } catch (SQLException e) {
      connection = null;
    }
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Test
  @Order(0)
  void testSetup_createSchemaAndTables() throws SQLException {
    assertNotNull(connection, "Connection should be established");
    Statement stmt = connection.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s", testCatalog, TEST_SCHEMA_NAME));
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s.%s "
                + "(id INT NOT NULL, name STRING, "
                + "CONSTRAINT pk_null_resolve_parent PRIMARY KEY(id))",
            testCatalog, TEST_SCHEMA_NAME, PARENT_TABLE));
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s.%s "
                + "(id INT NOT NULL, parent_id INT NOT NULL, "
                + "CONSTRAINT pk_null_resolve_child PRIMARY KEY(id), "
                + "CONSTRAINT fk_null_resolve_child FOREIGN KEY(parent_id) "
                + "REFERENCES %s.%s.%s(id))",
            testCatalog,
            TEST_SCHEMA_NAME,
            CHILD_TABLE,
            testCatalog,
            TEST_SCHEMA_NAME,
            PARENT_TABLE));
    stmt.execute(String.format("USE CATALOG %s", testCatalog));
    stmt.execute(String.format("USE SCHEMA %s", TEST_SCHEMA_NAME));
    stmt.close();
  }

  // ==================== getPrimaryKeys ====================

  @Test
  @Order(1)
  void testGetPrimaryKeys_nullCatalogNullSchema() throws SQLException {
    assertNotNull(connection);
    Statement stmt = connection.createStatement();
    stmt.execute(String.format("USE CATALOG %s", testCatalog));
    stmt.execute(String.format("USE SCHEMA %s", TEST_SCHEMA_NAME));
    stmt.close();

    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs = md.getPrimaryKeys(null, null, PARENT_TABLE)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should find primary key when catalog and schema are null");
      assertEquals(testCatalog, rs.getString("TABLE_CAT"));
      assertEquals(TEST_SCHEMA_NAME, rs.getString("TABLE_SCHEM"));
      assertEquals(PARENT_TABLE, rs.getString("TABLE_NAME"));
      assertEquals("id", rs.getString("COLUMN_NAME"));
      assertFalse(rs.next(), "Should have exactly one primary key column");
    }
  }

  @Test
  @Order(2)
  void testGetPrimaryKeys_nullCatalogExplicitSchema() throws SQLException {
    assertNotNull(connection);
    Statement stmt = connection.createStatement();
    stmt.execute(String.format("USE CATALOG %s", testCatalog));
    stmt.close();

    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs = md.getPrimaryKeys(null, TEST_SCHEMA_NAME, PARENT_TABLE)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should find primary key when catalog is null but schema is provided");
      assertEquals(testCatalog, rs.getString("TABLE_CAT"));
      assertEquals(TEST_SCHEMA_NAME, rs.getString("TABLE_SCHEM"));
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(3)
  void testGetPrimaryKeys_fullySpecified() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs = md.getPrimaryKeys(testCatalog, TEST_SCHEMA_NAME, PARENT_TABLE)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should find primary key when fully specified");
      assertEquals("id", rs.getString("COLUMN_NAME"));
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(4)
  void testGetPrimaryKeys_nullTableThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () -> md.getPrimaryKeys(testCatalog, TEST_SCHEMA_NAME, null),
        "Should throw when table is null");
  }

  @Test
  @Order(5)
  void testGetPrimaryKeys_explicitCatalogNullSchemaThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () -> md.getPrimaryKeys(testCatalog, null, PARENT_TABLE),
        "Should throw when schema is null and catalog is explicitly provided");
  }

  @Test
  @Order(6)
  void testGetPrimaryKeys_emptyTableThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () -> md.getPrimaryKeys(testCatalog, TEST_SCHEMA_NAME, ""),
        "Should throw when table is empty string");
  }

  // ==================== getImportedKeys ====================

  @Test
  @Order(10)
  void testGetImportedKeys_nullCatalogNullSchema() throws SQLException {
    assertNotNull(connection);
    Statement stmt = connection.createStatement();
    stmt.execute(String.format("USE CATALOG %s", testCatalog));
    stmt.execute(String.format("USE SCHEMA %s", TEST_SCHEMA_NAME));
    stmt.close();

    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs = md.getImportedKeys(null, null, CHILD_TABLE)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should find imported key when catalog and schema are null");
      assertEquals(testCatalog, rs.getString("PKTABLE_CAT"));
      assertEquals(TEST_SCHEMA_NAME, rs.getString("PKTABLE_SCHEM"));
      assertEquals(PARENT_TABLE, rs.getString("PKTABLE_NAME"));
      assertEquals("id", rs.getString("PKCOLUMN_NAME"));
      assertEquals(testCatalog, rs.getString("FKTABLE_CAT"));
      assertEquals(TEST_SCHEMA_NAME, rs.getString("FKTABLE_SCHEM"));
      assertEquals(CHILD_TABLE, rs.getString("FKTABLE_NAME"));
      assertEquals("parent_id", rs.getString("FKCOLUMN_NAME"));
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(11)
  void testGetImportedKeys_fullySpecified() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs = md.getImportedKeys(testCatalog, TEST_SCHEMA_NAME, CHILD_TABLE)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should find imported key when fully specified");
      assertEquals(PARENT_TABLE, rs.getString("PKTABLE_NAME"));
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(12)
  void testGetImportedKeys_nullTableThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () -> md.getImportedKeys(testCatalog, TEST_SCHEMA_NAME, null),
        "Should throw when table is null");
  }

  @Test
  @Order(13)
  void testGetExportedKeys_nullTableThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () -> md.getExportedKeys(testCatalog, TEST_SCHEMA_NAME, null),
        "Should throw when table is null");
  }

  // ==================== getCrossReference ====================

  @Test
  @Order(20)
  void testGetCrossReference_nullCatalogsNullSchemas() throws SQLException {
    assertNotNull(connection);
    Statement stmt = connection.createStatement();
    stmt.execute(String.format("USE CATALOG %s", testCatalog));
    stmt.execute(String.format("USE SCHEMA %s", TEST_SCHEMA_NAME));
    stmt.close();

    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs = md.getCrossReference(null, null, PARENT_TABLE, null, null, CHILD_TABLE)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should find cross reference when catalogs and schemas are null");
      assertEquals(PARENT_TABLE, rs.getString("PKTABLE_NAME"));
      assertEquals("id", rs.getString("PKCOLUMN_NAME"));
      assertEquals(CHILD_TABLE, rs.getString("FKTABLE_NAME"));
      assertEquals("parent_id", rs.getString("FKCOLUMN_NAME"));
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(21)
  void testGetCrossReference_nullCatalogsExplicitSchemas() throws SQLException {
    assertNotNull(connection);
    Statement stmt = connection.createStatement();
    stmt.execute(String.format("USE CATALOG %s", testCatalog));
    stmt.close();

    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs =
        md.getCrossReference(
            null, TEST_SCHEMA_NAME, PARENT_TABLE, null, TEST_SCHEMA_NAME, CHILD_TABLE)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should find cross reference with null catalogs and explicit schemas");
      assertEquals(PARENT_TABLE, rs.getString("PKTABLE_NAME"));
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(22)
  void testGetCrossReference_fullySpecified() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs =
        md.getCrossReference(
            testCatalog,
            TEST_SCHEMA_NAME,
            PARENT_TABLE,
            testCatalog,
            TEST_SCHEMA_NAME,
            CHILD_TABLE)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should find cross reference when fully specified");
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(23)
  void testGetCrossReference_nullForeignTableReturnsEmpty() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs =
        md.getCrossReference(
            testCatalog, TEST_SCHEMA_NAME, PARENT_TABLE, testCatalog, TEST_SCHEMA_NAME, null)) {
      assertNotNull(rs);
      assertFalse(rs.next(), "Should return empty when foreign table is null");
    }
  }

  @Test
  @Order(24)
  void testGetCrossReference_nullParentTableThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () ->
            md.getCrossReference(
                testCatalog, TEST_SCHEMA_NAME, null, testCatalog, TEST_SCHEMA_NAME, CHILD_TABLE),
        "Should throw when parent table is null");
  }

  @Test
  @Order(25)
  void testGetCrossReference_emptyForeignTableThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () ->
            md.getCrossReference(
                testCatalog, TEST_SCHEMA_NAME, PARENT_TABLE, testCatalog, TEST_SCHEMA_NAME, ""),
        "Should throw when foreign table is empty string");
  }

  @Test
  @Order(26)
  void testGetCrossReference_emptyParentTableThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () ->
            md.getCrossReference(
                testCatalog, TEST_SCHEMA_NAME, "", testCatalog, TEST_SCHEMA_NAME, CHILD_TABLE),
        "Should throw when parent table is empty string");
  }

  @Test
  @Order(27)
  void testGetCrossReference_allEmptyThrows() throws SQLException {
    assertNotNull(connection);
    DatabaseMetaData md = connection.getMetaData();
    assertThrows(
        SQLException.class,
        () -> md.getCrossReference("", "", "", "", "", ""),
        "Should throw when all parameters are empty strings");
  }

  // ==================== Cleanup ====================

  @Test
  @Order(99)
  void testCleanup_dropSchemaAndTables() throws SQLException {
    assertNotNull(connection, "Connection should be established");
    Statement stmt = connection.createStatement();
    stmt.execute(
        String.format("DROP TABLE IF EXISTS %s.%s.%s", testCatalog, TEST_SCHEMA_NAME, CHILD_TABLE));
    stmt.execute(
        String.format(
            "DROP TABLE IF EXISTS %s.%s.%s", testCatalog, TEST_SCHEMA_NAME, PARENT_TABLE));
    stmt.execute(String.format("DROP SCHEMA IF EXISTS %s.%s", testCatalog, TEST_SCHEMA_NAME));
    stmt.close();
  }
}
