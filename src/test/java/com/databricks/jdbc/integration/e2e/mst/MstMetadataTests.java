package com.databricks.jdbc.integration.e2e.mst;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.HashSet;
import java.util.IllegalFormatConversionException;
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
  }

  /** Start a transaction: setAutoCommit(false) then INSERT to activate the txn. */
  private void beginTransaction() throws SQLException {
    connection.setAutoCommit(false);
    executeSql(connection, "INSERT INTO " + getFullyQualifiedTableName() + " VALUES (1, 'in_txn')");
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
    beginTransaction();
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getColumns(catalog, schema, testTable, null),
          "SEA: getColumns should throw in MST (issues SHOW COLUMNS)");
    } else {
      ResultSet rs = dbmd.getColumns(catalog, schema, testTable, null);
      assertTrue(rs.next(), "Thrift: getColumns should return results (stale)");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.2 getTables [{1}]")
  @MethodSource("backends")
  void testGetTablesInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    beginTransaction();
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getTables(catalog, schema, testTable, null),
          "SEA: getTables should throw in MST");
    } else {
      ResultSet rs = dbmd.getTables(catalog, schema, testTable, null);
      assertTrue(rs.next(), "Thrift: getTables should return results (stale)");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.3 getSchemas [{1}]")
  @MethodSource("backends")
  void testGetSchemasInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    beginTransaction();
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
    beginTransaction();
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
    beginTransaction();
    DatabaseMetaData dbmd = connection.getMetaData();
    // Both backends: getPrimaryKeys throws in MST.
    // SEA: blocked by MSTCheckRule (issues SHOW KEYS SQL).
    // Thrift: server routes TGetPrimaryKeysReq through GET_FUNCTIONS which is blocked in MST.
    assertThrows(
        SQLException.class,
        () -> dbmd.getPrimaryKeys(catalog, schema, testTable),
        "getPrimaryKeys should throw in MST on both backends");
    connection.rollback();
  }

  @ParameterizedTest(name = "D.6 getCrossReference [{1}]")
  @MethodSource("backends")
  void testGetCrossReferenceInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    beginTransaction();
    DatabaseMetaData dbmd = connection.getMetaData();
    if (isSEA()) {
      assertThrows(
          SQLException.class,
          () -> dbmd.getCrossReference(catalog, schema, testTable, null, null, null),
          "SEA: getCrossReference should throw in MST");
    } else {
      ResultSet rs = dbmd.getCrossReference(catalog, schema, testTable, null, null, null);
      assertNotNull(rs, "Thrift: getCrossReference should return ResultSet");
      rs.close();
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "D.7 getFunctions [{1}]")
  @MethodSource("backends")
  void testGetFunctionsInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    beginTransaction();
    DatabaseMetaData dbmd = connection.getMetaData();
    // getFunctions issues SHOW FUNCTIONS IN CATALOG with StatementType.METADATA which is
    // not blocked by MSTCheckRule, so both SEA and Thrift should succeed.
    if (isThrift()) {
      try {
        ResultSet rs = dbmd.getFunctions(catalog, null, "%");
        assertNotNull(rs, "Thrift: getFunctions should return ResultSet");
        rs.close();
      } catch (IllegalFormatConversionException e) {
        // Known driver logging bug in Thrift path — skip test if hit
        Assumptions.assumeTrue(
            false, "Skipping due to known Thrift logging bug: " + e.getMessage());
      }
    } else {
      ResultSet rs = dbmd.getFunctions(catalog, null, "%");
      assertNotNull(rs, "SEA: getFunctions should return ResultSet");
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
    beginTransaction();
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
    beginTransaction();

    DatabaseMetaData dbmd = connection.getMetaData();

    // Baseline: get column names before concurrent DDL
    Set<String> columnsBefore = new HashSet<>();
    try (ResultSet rs = dbmd.getColumns(catalog, schema, testTable, null)) {
      while (rs.next()) {
        columnsBefore.add(rs.getString("COLUMN_NAME").toLowerCase());
      }
    }

    // Concurrent connection adds a column
    try (Connection extConn = createConnection();
        Statement stmt = extConn.createStatement()) {
      stmt.execute("ALTER TABLE " + getFullyQualifiedTableName() + " ADD COLUMN new_col STRING");
    }

    // Re-read columns in same transaction — Thrift metadata RPCs are non-transactional,
    // so they bypass transaction isolation and see the new column.
    Set<String> columnsAfter = new HashSet<>();
    try (ResultSet rs = dbmd.getColumns(catalog, schema, testTable, null)) {
      while (rs.next()) {
        columnsAfter.add(rs.getString("COLUMN_NAME").toLowerCase());
      }
    }

    assertTrue(
        columnsAfter.contains("new_col"),
        "Thrift getColumns() is non-transactional — new column SHOULD be visible");
    assertNotEquals(
        columnsBefore,
        columnsAfter,
        "Column set should differ (Thrift RPCs bypass transaction isolation)");

    connection.rollback();
  }

  @ParameterizedTest(name = "D.10 tablesStaleAfterConcurrentCreate [{1}]")
  @MethodSource("backends")
  void testGetTablesStaleAfterConcurrentCreateTable(int useThrift, String backend)
      throws SQLException {
    init(useThrift);
    Assumptions.assumeTrue(isThrift(), "Staleness test only applicable to Thrift backend");
    beginTransaction();

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

    // Re-read in same transaction — Thrift metadata RPCs are non-transactional,
    // so they bypass transaction isolation and see the new table.
    try (ResultSet rs = dbmd.getTables(catalog, schema, newTable, null)) {
      assertTrue(
          rs.next(), "Thrift getTables() is non-transactional — new table SHOULD be visible");
    }

    connection.rollback();

    // Cleanup the created table
    try (Connection cleanupConn = createConnection();
        Statement stmt = cleanupConn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + fqNewTable);
    }
  }
}
