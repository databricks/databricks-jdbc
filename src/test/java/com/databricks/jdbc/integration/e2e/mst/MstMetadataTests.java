package com.databricks.jdbc.integration.e2e.mst;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for DatabaseMetaData RPCs and PreparedStatement.getMetaData() inside active MST
 * transactions.
 *
 * <p>SEA backend: metadata RPCs issue SHOW/DESCRIBE SQL which is blocked by MSTCheckRule → xfail.
 * Thrift backend: metadata RPCs use Thrift RPC which bypasses MST → returns stale data.
 *
 * <p>Uses JDBC API mode (setAutoCommit) for transaction control.
 */
public class MstMetadataTests extends AbstractMstTestBase {

  static Stream<Arguments> backends() {
    return Stream.of(Arguments.of(0, "SEA"), Arguments.of(1, "Thrift"));
  }

  private void init(int useThrift) throws SQLException {
    initBackend(useThrift);
    // Insert a row and start a transaction for metadata testing
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " VALUES (1, 'metadata_test')");
    }
    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " VALUES (2, 'in_txn')");
    }
  }

  @AfterEach
  void tearDown() {
    cleanup();
  }

  @Override
  protected void startTransaction(Connection conn) throws SQLException {
    conn.setAutoCommit(false);
  }

  @Override
  protected void commitTransaction(Connection conn) throws SQLException {
    conn.commit();
  }

  @Override
  protected void rollbackTransaction(Connection conn) throws SQLException {
    conn.rollback();
  }

  // ========================== METADATA RPC TESTS ==========================

  @ParameterizedTest(name = "D.1 getColumns [{1}]")
  @MethodSource("backends")
  void testGetColumnsInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getColumns(catalog, schema, TEST_TABLE, null),
          "SEA: getColumns should throw in MST (issues SHOW COLUMNS)");
    } else {
      ResultSet rs = dbmd.getColumns(catalog, schema, TEST_TABLE, null);
      assertTrue(rs.next(), "Thrift: getColumns should return results (stale)");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.2 getTables [{1}]")
  @MethodSource("backends")
  void testGetTablesInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getTables(catalog, schema, TEST_TABLE, null),
          "SEA: getTables should throw in MST");
    } else {
      ResultSet rs = dbmd.getTables(catalog, schema, TEST_TABLE, null);
      assertTrue(rs.next(), "Thrift: getTables should return results (stale)");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.3 getSchemas [{1}]")
  @MethodSource("backends")
  void testGetSchemasInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getSchemas(catalog, schema),
          "SEA: getSchemas should throw in MST");
    } else {
      ResultSet rs = dbmd.getSchemas(catalog, schema);
      assertTrue(rs.next(), "Thrift: getSchemas should return results (stale)");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.4 getCatalogs [{1}]")
  @MethodSource("backends")
  void testGetCatalogsInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class, () -> dbmd.getCatalogs(), "SEA: getCatalogs should throw in MST");
    } else {
      ResultSet rs = dbmd.getCatalogs();
      assertTrue(rs.next(), "Thrift: getCatalogs should return results (stale)");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.5 getPrimaryKeys [{1}]")
  @MethodSource("backends")
  void testGetPrimaryKeysInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getPrimaryKeys(catalog, schema, TEST_TABLE),
          "SEA: getPrimaryKeys should throw in MST");
    } else {
      ResultSet rs = dbmd.getPrimaryKeys(catalog, schema, TEST_TABLE);
      assertNotNull(rs, "Thrift: getPrimaryKeys should return ResultSet (stale)");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.6 getCrossReference [{1}]")
  @MethodSource("backends")
  void testGetCrossReferenceInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getCrossReference(catalog, schema, TEST_TABLE, null, null, null),
          "SEA: getCrossReference should throw in MST");
    } else {
      ResultSet rs = dbmd.getCrossReference(catalog, schema, TEST_TABLE, null, null, null);
      assertNotNull(rs, "Thrift: getCrossReference should return ResultSet");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.7 getFunctions [{1}]")
  @MethodSource("backends")
  void testGetFunctionsInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getFunctions(catalog, null, "%"),
          "SEA: getFunctions should throw in MST");
    } else {
      ResultSet rs = dbmd.getFunctions(catalog, null, "%");
      assertNotNull(rs, "Thrift: getFunctions should return ResultSet (stale)");
      rs.close();
    }
    connection.rollback();
  }

  // ========================== PREPARED STATEMENT METADATA ==========================

  @ParameterizedTest(name = "D.8 getMetaDataBeforeExecute [{1}]")
  @MethodSource("backends")
  void testPreparedStatementGetMetaDataBeforeExecute(int useThrift, String backend)
      throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    // getMetaData() before execute issues DESCRIBE QUERY — blocked on both backends
    try (PreparedStatement ps =
        connection.prepareStatement("SELECT * FROM " + fqTable + " WHERE id = ?")) {
      assertThrows(
          SQLException.class,
          ps::getMetaData,
          "getMetaData() before execute should throw in MST (issues DESCRIBE QUERY)");
    }
    connection.rollback();
  }

  // ========================== STALENESS TESTS (Thrift only) ==========================

  @ParameterizedTest(name = "D.9 columnsStaleAfterConcurrentAddColumn [{1}]")
  @MethodSource("backends")
  void testGetColumnsStaleAfterConcurrentAddColumn(int useThrift, String backend)
      throws SQLException {
    init(useThrift);
    Assumptions.assumeTrue(isThrift(), "Staleness test only applicable to Thrift backend");

    DatabaseMetaData dbmd = connection.getMetaData();

    // Baseline: get column names before concurrent DDL
    Set<String> columnsBefore = new HashSet<>();
    try (ResultSet rs = dbmd.getColumns(catalog, schema, TEST_TABLE, null)) {
      while (rs.next()) {
        columnsBefore.add(rs.getString("COLUMN_NAME").toLowerCase());
      }
    }

    // Concurrent connection adds a column
    try (Connection extConn = createConnection();
        Statement stmt = extConn.createStatement()) {
      stmt.execute("ALTER TABLE " + getFullyQualifiedTableName() + " ADD COLUMN new_col STRING");
    }

    // Re-read columns in same transaction — should NOT see new column (stale)
    Set<String> columnsAfter = new HashSet<>();
    try (ResultSet rs = dbmd.getColumns(catalog, schema, TEST_TABLE, null)) {
      while (rs.next()) {
        columnsAfter.add(rs.getString("COLUMN_NAME").toLowerCase());
      }
    }

    assertFalse(
        columnsAfter.contains("new_col"),
        "Thrift getColumns() should return stale data — new column should NOT be visible");
    assertEquals(columnsBefore, columnsAfter, "Column set should be identical (stale)");

    connection.rollback();
  }

  @ParameterizedTest(name = "D.10 tablesStaleAfterConcurrentCreate [{1}]")
  @MethodSource("backends")
  void testGetTablesStaleAfterConcurrentCreateTable(int useThrift, String backend)
      throws SQLException {
    init(useThrift);
    Assumptions.assumeTrue(isThrift(), "Staleness test only applicable to Thrift backend");

    DatabaseMetaData dbmd = connection.getMetaData();
    String newTable = "mst_staleness_test_" + System.currentTimeMillis();
    String fqNewTable = catalog + "." + schema + "." + newTable;

    // Baseline: check table doesn't exist
    try (ResultSet rs = dbmd.getTables(catalog, schema, newTable, null)) {
      assertFalse(rs.next(), "New table should not exist yet");
    }

    // Concurrent connection creates the table
    try (Connection extConn = createConnection();
        Statement stmt = extConn.createStatement()) {
      stmt.execute(
          "CREATE TABLE "
              + fqNewTable
              + " (id INT) USING DELTA"
              + " TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");
    }

    // Re-read in same transaction — should NOT see new table (stale)
    try (ResultSet rs = dbmd.getTables(catalog, schema, newTable, null)) {
      assertFalse(
          rs.next(),
          "Thrift getTables() should return stale data — new table should NOT be visible");
    }

    connection.rollback();

    // Cleanup the created table
    try (Connection cleanupConn = createConnection();
        Statement stmt = cleanupConn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + fqNewTable);
    }
  }
}
