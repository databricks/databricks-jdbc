package com.databricks.jdbc.integration.e2e.mst;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for execute method variants (executeUpdate, executeLargeUpdate, executeBatch,
 * PreparedStatement.executeBatch) inside active MST transactions.
 *
 * <p>SEA: row counts may be stale/incorrect (LC-13424) — TBD after E2E validation. Thrift: row
 * counts should be correct.
 *
 * <p>Uses JDBC API mode for transaction control.
 */
public class MstExecuteVariantTests extends AbstractMstTestBase {

  static Stream<Arguments> backends() {
    return Stream.of(Arguments.of(0, "SEA"), Arguments.of(1, "Thrift"));
  }

  private void init(int useThrift) throws SQLException {
    initBackend(useThrift);
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

  // ========================== EXECUTE VARIANT TESTS ==========================

  @ParameterizedTest(name = "F.1 executeUpdate [{1}]")
  @MethodSource("backends")
  void testExecuteUpdateRowCount(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      int rowCount = stmt.executeUpdate("INSERT INTO " + fqTable + " VALUES (1, 'exec_update')");
      assertTrue(rowCount >= -1, "executeUpdate should return a row count");
    }
    if (isSEA()) {
      // SEA: executeUpdate silently poisons the transaction (LC-13424) — commit fails
      assertThrows(
          SQLException.class,
          () -> connection.commit(),
          "SEA: commit should fail after executeUpdate in MST (LC-13424)");
    } else {
      connection.commit();
      assertEquals(
          1, getRowCountFromSeparateConnection(fqTable), "Data should persist after commit");
    }
  }

  @ParameterizedTest(name = "F.2 executeLargeUpdate [{1}]")
  @MethodSource("backends")
  void testExecuteLargeUpdateRowCount(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      long rowCount =
          stmt.executeLargeUpdate("INSERT INTO " + fqTable + " VALUES (1, 'large_update')");
      assertTrue(rowCount >= -1, "executeLargeUpdate should return a row count");
    }
    if (isSEA()) {
      // SEA: executeLargeUpdate silently poisons the transaction (LC-13424)
      assertThrows(
          SQLException.class,
          () -> connection.commit(),
          "SEA: commit should fail after executeLargeUpdate in MST (LC-13424)");
    } else {
      connection.commit();
      assertEquals(1, getRowCountFromSeparateConnection(fqTable));
    }
  }

  @ParameterizedTest(name = "F.3 executeBatch [{1}]")
  @MethodSource("backends")
  void testExecuteBatchRowCounts(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    if (isSEA()) {
      // SEA: executeBatch aborts the transaction (LC-13424)
      assertThrows(
          SQLException.class,
          () -> {
            try (Statement stmt = connection.createStatement()) {
              stmt.addBatch("INSERT INTO " + fqTable + " VALUES (1, 'batch1')");
              stmt.addBatch("INSERT INTO " + fqTable + " VALUES (2, 'batch2')");
              stmt.addBatch("INSERT INTO " + fqTable + " VALUES (3, 'batch3')");
              stmt.executeBatch();
            }
          },
          "SEA: executeBatch should throw in MST (LC-13424)");
    } else {
      try (Statement stmt = connection.createStatement()) {
        stmt.addBatch("INSERT INTO " + fqTable + " VALUES (1, 'batch1')");
        stmt.addBatch("INSERT INTO " + fqTable + " VALUES (2, 'batch2')");
        stmt.addBatch("INSERT INTO " + fqTable + " VALUES (3, 'batch3')");
        int[] counts = stmt.executeBatch();

        assertNotNull(counts, "executeBatch should return row count array");
        assertEquals(3, counts.length, "Should have 3 row counts for 3 batch statements");
        for (int count : counts) {
          assertTrue(count >= -1, "Thrift: each batch row count should be valid, got: " + count);
        }
      }
      connection.commit();
      assertEquals(3, getRowCountFromSeparateConnection(fqTable));
    }
  }

  @ParameterizedTest(name = "F.4 PreparedStatement.executeBatch [{1}]")
  @MethodSource("backends")
  void testPreparedStatementExecuteBatchRowCounts(int useThrift, String backend)
      throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    if (isSEA()) {
      // SEA: PreparedStatement.executeBatch aborts the transaction (LC-13424)
      assertThrows(
          SQLException.class,
          () -> {
            try (PreparedStatement ps =
                connection.prepareStatement("INSERT INTO " + fqTable + " VALUES (?, ?)")) {
              ps.setInt(1, 1);
              ps.setString(2, "ps_batch1");
              ps.addBatch();

              ps.setInt(1, 2);
              ps.setString(2, "ps_batch2");
              ps.addBatch();

              ps.setInt(1, 3);
              ps.setString(2, "ps_batch3");
              ps.addBatch();

              ps.executeBatch();
            }
          },
          "SEA: PreparedStatement.executeBatch should throw in MST (LC-13424)");
    } else {
      try (PreparedStatement ps =
          connection.prepareStatement("INSERT INTO " + fqTable + " VALUES (?, ?)")) {
        ps.setInt(1, 1);
        ps.setString(2, "ps_batch1");
        ps.addBatch();

        ps.setInt(1, 2);
        ps.setString(2, "ps_batch2");
        ps.addBatch();

        ps.setInt(1, 3);
        ps.setString(2, "ps_batch3");
        ps.addBatch();

        int[] counts = ps.executeBatch();

        assertNotNull(counts, "PreparedStatement.executeBatch should return row count array");
        assertEquals(3, counts.length);
        for (int count : counts) {
          assertTrue(count >= -1, "Thrift: each batch row count should be valid");
        }
      }
      connection.commit();
      assertEquals(3, getRowCountFromSeparateConnection(fqTable));
    }
  }
}
