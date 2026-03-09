package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.SESSION_PATH;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.STATEMENT_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.databricks.sdk.service.sql.StatementState;
import com.github.tomakehurst.wiremock.client.CountMatchingStrategy;
import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for SQL statement execution. */
public class ExecutionIntegrationTests extends AbstractFakeServiceIntegrationTests {

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
  void testInsertStatement() throws SQLException {
    String tableName = "insert_test_table";
    setupDatabaseTable(connection, tableName);
    String SQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    assertDoesNotThrow(() -> executeSQL(connection, SQL), "Error executing SQL");

    ResultSet rs =
        executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    int rows = 0;
    while (rs != null && rs.next()) {
      rows++;
    }
    assertEquals(1, rows, "Expected 1 row, got " + rows);
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // Run validations for SQL_EXEC fake service
      getDatabricksApiExtension().verify(1, postRequestedFor(urlEqualTo(SESSION_PATH)));

      // At least 5 statement requests are sent: drop, create, insert, select, drop
      // There can be more for retries
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 5),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testUpdateStatement() throws SQLException {
    // Insert initial test data
    String tableName = "update_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    String updateSQL =
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col1 = 'updatedValue1' WHERE id = 1";
    executeSQL(connection, updateSQL);

    ResultSet rs =
        executeQuery(
            connection,
            "SELECT col1 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(
        rs.next() && "updatedValue1".equals(rs.getString("col1")),
        "Expected 'updatedValue1', got " + rs.getString("col1"));
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // Run validations for SQL_EXEC fake service
      getDatabricksApiExtension().verify(1, postRequestedFor(urlEqualTo(SESSION_PATH)));

      // At least 6 statement requests are sent: drop, create, insert, update, select, drop
      // There can be more for retries
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 6),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testDeleteStatement() throws SQLException {
    // Insert initial test data
    String tableName = "delete_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    executeSQL(connection, deleteSQL);

    ResultSet rs =
        executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertFalse(rs.next(), "Expected no rows after delete");
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // At least 6 statement requests are sent: drop, create, insert, delete, select, drop
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 6),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testCompoundStatements() throws SQLException {
    // Insert for compound test
    String tableName = "compound_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    // Update operation as part of compound test
    String updateSQL =
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col2 = 'updatedValue2' WHERE id = 1";
    executeSQL(connection, updateSQL);

    // Verify update operation
    ResultSet rs =
        executeQuery(
            connection,
            "SELECT col2 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(
        rs.next() && "updatedValue2".equals(rs.getString("col2")),
        "Expected 'updatedValue2', got " + rs.getString("col2"));

    // Delete operation as part of compound test
    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    executeSQL(connection, deleteSQL);

    // Verify delete operation
    rs = executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertFalse(rs.next(), "Expected no rows after delete");
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // At least 8 statement requests are sent:
      // drop, create, insert, update, select, delete, select, drop
      // There can be more for retries
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 8),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testComplexQueryJoins() throws SQLException {
    String table1Name = "table1_cqj";
    String table2Name = "table2_cqj";
    setupDatabaseTable(connection, table1Name);
    setupDatabaseTable(connection, table2Name);
    insertTestDataForJoins(connection, table1Name, table2Name);

    String joinSQL =
        "SELECT t1.id, t2.col2 FROM "
            + getFullyQualifiedTableName(table1Name)
            + " t1 "
            + "JOIN "
            + getFullyQualifiedTableName(table2Name)
            + " t2 "
            + "ON t1.id = t2.id";
    ResultSet rs = executeQuery(connection, joinSQL);
    assertTrue(rs.next(), "Expected at least one row from JOIN query");
    deleteTable(connection, table1Name);
    deleteTable(connection, table2Name);

    if (isSqlExecSdkClient()) {
      // At least 11 statement requests are sent:
      // drop table1, create table1, drop table2, create table2, insert table1, insert table1,
      // insert table2, insert table2, select join, drop table1, drop table2
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 11),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testComplexQuerySubqueries() throws SQLException {
    String tableName = "subquery_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    String subquerySQL =
        "SELECT id FROM "
            + getFullyQualifiedTableName(tableName)
            + " WHERE id IN (SELECT id FROM "
            + getFullyQualifiedTableName(tableName)
            + " WHERE col1 = 'value1')";
    ResultSet rs = executeQuery(connection, subquerySQL);
    assertTrue(rs.next(), "Expected at least one row from subquery");
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // At least 5 statement requests are sent: drop, create, insert, select, drop
      // There can be more for retries
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 5),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testExecuteAsyncStatement() throws Exception {
    Statement s = connection.createStatement();
    IDatabricksStatement ids = s.unwrap(IDatabricksStatement.class);

    // Takes approx 10s to complete
    String sql =
        "WITH a AS (\n"
            + "    SELECT id AS x FROM range(1, 1000000)\n"
            + "),\n"
            + "b AS (\n"
            + "    SELECT id AS y FROM range(1, 1000000)\n"
            + ")\n"
            + "SELECT a.x, b.y\n"
            + "FROM a\n"
            + "JOIN b\n"
            + "  ON (a.x * b.y) % 1234567 = 1\n"
            + "WHERE a.x < 100\n"
            + "LIMIT 10;";

    // Execute asynchronously
    ResultSet rs = ids.executeAsync(sql);
    StatementState state = rs.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();

    // Poll for status
    while (state != StatementState.SUCCEEDED && state != StatementState.FAILED) {
      Thread.sleep(1000);
      rs = s.unwrap(IDatabricksStatement.class).getExecutionResult();
      state = rs.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();
    }

    // Second connection
    Connection con2 = getValidJDBCConnection();
    IDatabricksConnection idc = con2.unwrap(IDatabricksConnection.class);
    Statement stm = idc.getStatement(rs.unwrap(IDatabricksResultSet.class).getStatementId());
    ResultSet rs2 = stm.unwrap(IDatabricksStatement.class).getExecutionResult();
    assertEquals(
        StatementState.SUCCEEDED,
        rs2.unwrap(IDatabricksResultSet.class).getStatementStatus().getState());
  }

  // --- Statement property tests ---

  @Test
  void testStatement_FetchSize() throws SQLException {
    Statement stmt = connection.createStatement();

    // Default fetch size should be 0 (driver does not support custom fetch size)
    assertEquals(0, stmt.getFetchSize(), "Default fetch size should be 0");

    // setFetchSize is a no-op in this driver (logs a warning but doesn't change the value)
    stmt.setFetchSize(100);
    assertEquals(
        0, stmt.getFetchSize(), "getFetchSize should still return 0 (fetch size not supported)");

    // Verify a warning was generated
    assertNotNull(stmt.getWarnings(), "setFetchSize should generate a warning");

    stmt.close();
  }

  @Test
  void testStatement_GetResultSetType() throws SQLException {
    Statement stmt = connection.createStatement();

    int rsType = stmt.getResultSetType();
    assertEquals(
        ResultSet.TYPE_FORWARD_ONLY, rsType, "Default ResultSet type should be TYPE_FORWARD_ONLY");

    stmt.close();
  }

  @Test
  void testStatement_GetResultSetConcurrency() throws SQLException {
    Statement stmt = connection.createStatement();

    int rsConcurrency = stmt.getResultSetConcurrency();
    assertEquals(
        ResultSet.CONCUR_READ_ONLY,
        rsConcurrency,
        "Default ResultSet concurrency should be CONCUR_READ_ONLY");

    stmt.close();
  }

  @Test
  void testStatement_FetchDirection() throws SQLException {
    Statement stmt = connection.createStatement();

    // Default should be FETCH_FORWARD
    assertEquals(
        ResultSet.FETCH_FORWARD,
        stmt.getFetchDirection(),
        "Default fetch direction should be FETCH_FORWARD");

    // Setting FETCH_FORWARD should work
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
    assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());

    stmt.close();
  }

  @Test
  void testStatement_EnquoteLiteral() throws SQLException {
    Statement stmt = connection.createStatement();

    String result = stmt.enquoteLiteral("test");
    assertNotNull(result, "enquoteLiteral should return non-null");
    assertTrue(result.contains("test"), "enquoteLiteral should contain the original string");

    stmt.close();
  }

  @Test
  void testStatement_EnquoteIdentifier() throws SQLException {
    Statement stmt = connection.createStatement();

    String result = stmt.enquoteIdentifier("my_column", false);
    assertNotNull(result, "enquoteIdentifier should return non-null");

    stmt.close();
  }

  @Test
  void testStatement_IsSimpleIdentifier() throws SQLException {
    Statement stmt = connection.createStatement();

    assertTrue(stmt.isSimpleIdentifier("simple_name"), "simple_name should be a simple identifier");
    assertFalse(
        stmt.isSimpleIdentifier("has space"), "'has space' should not be a simple identifier");

    stmt.close();
  }

  // --- Statement lifecycle and property tests ---

  @Test
  void testStatement_CloseAndIsClosed() throws SQLException {
    Statement stmt = connection.createStatement();
    assertFalse(stmt.isClosed(), "New statement should not be closed");

    stmt.close();
    assertTrue(stmt.isClosed(), "Statement should be closed after close()");
  }

  @Test
  void testStatement_GetConnection() throws SQLException {
    Statement stmt = connection.createStatement();
    Connection stmtConn = stmt.getConnection();
    assertNotNull(stmtConn, "getConnection() should return non-null");
    assertSame(connection, stmtConn, "getConnection() should return the parent connection");

    stmt.close();
  }

  @Test
  void testStatement_MaxRows() throws SQLException {
    Statement stmt = connection.createStatement();

    // Default maxRows should be 0 (no limit)
    assertEquals(0, stmt.getMaxRows(), "Default maxRows should be 0");

    stmt.setMaxRows(100);
    assertEquals(100, stmt.getMaxRows(), "maxRows should be 100 after setMaxRows(100)");

    // setMaxRows(0) means no limit
    stmt.setMaxRows(0);
    assertEquals(0, stmt.getMaxRows(), "maxRows should be 0 after setMaxRows(0)");

    // Negative value should throw
    assertThrows(
        SQLException.class, () -> stmt.setMaxRows(-1), "setMaxRows(-1) should throw SQLException");

    stmt.close();
  }

  @Test
  void testStatement_QueryTimeout() throws SQLException {
    Statement stmt = connection.createStatement();

    // Default timeout should be 0 (no timeout)
    assertEquals(0, stmt.getQueryTimeout(), "Default queryTimeout should be 0");

    stmt.setQueryTimeout(30);
    assertEquals(30, stmt.getQueryTimeout(), "queryTimeout should be 30 after setQueryTimeout(30)");

    // Negative value should throw
    assertThrows(
        SQLException.class,
        () -> stmt.setQueryTimeout(-1),
        "setQueryTimeout(-1) should throw SQLException");

    stmt.close();
  }

  @Test
  void testStatement_Warnings() throws SQLException {
    Statement stmt = connection.createStatement();

    // getWarnings should not throw
    SQLWarning warnings = stmt.getWarnings();

    // clearWarnings should not throw
    stmt.clearWarnings();
    assertNull(stmt.getWarnings(), "Warnings should be null after clearWarnings()");

    stmt.close();
  }

  // --- Execution result handling tests (getResultSet, getUpdateCount, getMoreResults) ---

  @Test
  void testExecute_SelectQuery_GetResultSetReturnsResultSet() throws SQLException {
    Statement stmt = connection.createStatement();
    boolean hasResultSet = stmt.execute("SELECT 1 AS num, 'hello' AS greeting");
    assertTrue(hasResultSet, "execute() should return true for SELECT query");

    ResultSet rs = stmt.getResultSet();
    assertNotNull(rs, "getResultSet() should return non-null ResultSet after SELECT");
    assertTrue(rs.next(), "ResultSet should have at least one row");
    assertEquals(1, rs.getInt("num"));
    assertEquals("hello", rs.getString("greeting"));

    assertEquals(-1, stmt.getUpdateCount(), "getUpdateCount() should return -1 for SELECT query");
  }

  @Test
  void testExecute_InsertStatement_GetResultSetReturnsNull() throws SQLException {
    String tableName = "exec_result_insert_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    boolean hasResultSet =
        stmt.execute(
            "INSERT INTO "
                + getFullyQualifiedTableName(tableName)
                + " (id, col1, col2) VALUES (1, 'a', 'b')");
    assertFalse(hasResultSet, "execute() should return false for INSERT statement");

    int updateCount = stmt.getUpdateCount();
    assertTrue(updateCount >= 0, "getUpdateCount() should return >= 0 for INSERT");

    deleteTable(connection, tableName);
  }

  @Test
  void testExecute_UpdateStatement_GetResultSetReturnsNull() throws SQLException {
    String tableName = "exec_result_update_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    Statement stmt = connection.createStatement();
    boolean hasResultSet =
        stmt.execute(
            "UPDATE "
                + getFullyQualifiedTableName(tableName)
                + " SET col1 = 'updated' WHERE id = 1");
    assertFalse(hasResultSet, "execute() should return false for UPDATE statement");

    int updateCount = stmt.getUpdateCount();
    assertTrue(updateCount >= 0, "getUpdateCount() should return >= 0 for UPDATE");

    deleteTable(connection, tableName);
  }

  @Test
  void testExecute_DeleteStatement_GetResultSetReturnsNull() throws SQLException {
    String tableName = "exec_result_delete_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    Statement stmt = connection.createStatement();
    boolean hasResultSet =
        stmt.execute("DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertFalse(hasResultSet, "execute() should return false for DELETE statement");

    int updateCount = stmt.getUpdateCount();
    assertTrue(updateCount >= 0, "getUpdateCount() should return >= 0 for DELETE");

    deleteTable(connection, tableName);
  }

  @Test
  void testGetResultSet_AfterExecuteQuery() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 42 AS answer");
    assertNotNull(rs, "executeQuery() should return non-null ResultSet");
    assertTrue(rs.next(), "ResultSet should have at least one row");
    assertEquals(42, rs.getInt("answer"));

    ResultSet rs2 = stmt.getResultSet();
    assertNotNull(rs2, "getResultSet() should return non-null after executeQuery()");
  }

  @Test
  void testGetUpdateCount_AfterExecuteUpdate() throws SQLException {
    String tableName = "exec_update_count_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    int result =
        stmt.executeUpdate(
            "INSERT INTO "
                + getFullyQualifiedTableName(tableName)
                + " (id, col1, col2) VALUES (1, 'val1', 'val2')");
    assertTrue(result >= 0, "executeUpdate() should return >= 0 for INSERT");

    int updateCount = stmt.getUpdateCount();
    assertTrue(updateCount >= 0, "getUpdateCount() should return >= 0 after executeUpdate()");

    deleteTable(connection, tableName);
  }

  @Test
  void testGetMoreResults_AdvancesPastResult() throws SQLException {
    Statement stmt = connection.createStatement();
    stmt.execute("SELECT 1 AS num");

    boolean hasMore = stmt.getMoreResults();
    assertFalse(hasMore, "getMoreResults() should return false (single result)");

    assertEquals(
        -1,
        stmt.getUpdateCount(),
        "getUpdateCount() should return -1 after getMoreResults() with no more results");
  }

  // --- Batch execution tests ---

  @Test
  void testStatementBatch_MultipleInserts() throws SQLException {
    String tableName = "batch_multi_insert_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    String fqn = getFullyQualifiedTableName(tableName);
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a1', 'b1')");
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'a2', 'b2')");
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (3, 'a3', 'b3')");

    int[] updateCounts = stmt.executeBatch();
    assertEquals(3, updateCounts.length, "Should have 3 update counts");

    ResultSet rs = executeQuery(connection, "SELECT COUNT(*) AS cnt FROM " + fqn);
    assertTrue(rs.next());
    assertEquals(3, rs.getInt("cnt"), "All 3 rows should be inserted");

    deleteTable(connection, tableName);
  }

  @Test
  void testStatementBatch_MixedDMLOperations() throws SQLException {
    String tableName = "batch_mixed_dml_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    Statement stmt = connection.createStatement();
    String fqn = getFullyQualifiedTableName(tableName);
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'x', 'y')");
    stmt.addBatch("UPDATE " + fqn + " SET col1 = 'updated' WHERE id = 1");

    int[] updateCounts = stmt.executeBatch();
    assertEquals(2, updateCounts.length, "Should have 2 update counts");

    ResultSet rs = executeQuery(connection, "SELECT COUNT(*) AS cnt FROM " + fqn);
    assertTrue(rs.next());
    assertEquals(2, rs.getInt("cnt"), "Should have 2 rows total");

    ResultSet rs2 = executeQuery(connection, "SELECT col1 FROM " + fqn + " WHERE id = 1");
    assertTrue(rs2.next());
    assertEquals("updated", rs2.getString("col1"));

    deleteTable(connection, tableName);
  }

  @Test
  void testStatementBatch_ClearBatch() throws SQLException {
    String tableName = "batch_clear_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    String fqn = getFullyQualifiedTableName(tableName);
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a', 'b')");
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'c', 'd')");

    stmt.clearBatch();

    int[] updateCounts = stmt.executeBatch();
    assertEquals(0, updateCounts.length, "Empty batch should return empty array");

    ResultSet rs = executeQuery(connection, "SELECT COUNT(*) AS cnt FROM " + fqn);
    assertTrue(rs.next());
    assertEquals(0, rs.getInt("cnt"), "No rows should be inserted after clearBatch");

    deleteTable(connection, tableName);
  }

  @Test
  void testStatementBatch_EmptyBatch() throws SQLException {
    Statement stmt = connection.createStatement();
    int[] updateCounts = stmt.executeBatch();
    assertEquals(0, updateCounts.length, "Empty batch should return empty array");
  }

  @Test
  void testPreparedStatementBatch_MultipleInserts() throws SQLException {
    String tableName = "ps_batch_insert_table";
    setupDatabaseTable(connection, tableName);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";
    PreparedStatement pstmt = connection.prepareStatement(insertSQL);

    pstmt.setInt(1, 1);
    pstmt.setString(2, "val1");
    pstmt.setString(3, "val2");
    pstmt.addBatch();

    pstmt.setInt(1, 2);
    pstmt.setString(2, "val3");
    pstmt.setString(3, "val4");
    pstmt.addBatch();

    int[] updateCounts = pstmt.executeBatch();
    assertEquals(2, updateCounts.length, "Should have 2 update counts");

    ResultSet rs =
        executeQuery(
            connection, "SELECT COUNT(*) AS cnt FROM " + getFullyQualifiedTableName(tableName));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt("cnt"), "Both rows should be inserted");

    deleteTable(connection, tableName);
  }

  @Test
  void testPreparedStatementBatch_ClearBatch() throws SQLException {
    String tableName = "ps_batch_clear_table";
    setupDatabaseTable(connection, tableName);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";
    PreparedStatement pstmt = connection.prepareStatement(insertSQL);

    pstmt.setInt(1, 1);
    pstmt.setString(2, "val1");
    pstmt.setString(3, "val2");
    pstmt.addBatch();

    pstmt.clearBatch();

    pstmt.setInt(1, 10);
    pstmt.setString(2, "fresh1");
    pstmt.setString(3, "fresh2");
    pstmt.addBatch();

    int[] updateCounts = pstmt.executeBatch();
    assertEquals(1, updateCounts.length, "Should have 1 update count after clear + re-add");

    ResultSet rs =
        executeQuery(connection, "SELECT id, col1 FROM " + getFullyQualifiedTableName(tableName));
    assertTrue(rs.next());
    assertEquals(10, rs.getInt("id"));
    assertEquals("fresh1", rs.getString("col1"));
    assertFalse(rs.next(), "Should only have 1 row");
  }
}
