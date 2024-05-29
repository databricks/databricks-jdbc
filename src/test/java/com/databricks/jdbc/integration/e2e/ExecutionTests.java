package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.*;

public class ExecutionTests {

  @Test
  void testInsertStatement() throws SQLException {
    String tableName = "insert_test_table";
    setupDatabaseTable(tableName);
    String SQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    assertDoesNotThrow(() -> executeSQL(SQL), "Error executing SQL");

    ResultSet rs = executeQuery("SELECT * FROM " + getFullyQualifiedTableName(tableName));
    int rows = 0;
    while (rs != null && rs.next()) {
      rows++;
    }
    assertTrue(rows == 1, "Expected 1 row, got " + rows);
    deleteTable(tableName);
  }

  @Test
  void testUpdateStatement() throws SQLException {
    // Insert initial test data
    String tableName = "update_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    String updateSQL =
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col1 = 'updatedValue1' WHERE id = 1";
    executeSQL(updateSQL);

    ResultSet rs =
        executeQuery("SELECT col1 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(
        rs.next() && "updatedValue1".equals(rs.getString("col1")),
        "Expected 'updatedValue1', got " + rs.getString("col1"));
    deleteTable(tableName);
  }

  @Test
  void testDeleteStatement() throws SQLException {
    // Insert initial test data
    String tableName = "delete_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    executeSQL(deleteSQL);

    ResultSet rs = executeQuery("SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertFalse(rs.next(), "Expected no rows after delete");
    deleteTable(tableName);
  }

  @Test
  void testCompoundStatements() throws SQLException {
    // Insert for compound test
    String tableName = "compound_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    // Update operation as part of compound test
    String updateSQL =
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col2 = 'updatedValue2' WHERE id = 1";
    executeSQL(updateSQL);

    // Verify update operation
    ResultSet rs =
        executeQuery("SELECT col2 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(
        rs.next() && "updatedValue2".equals(rs.getString("col2")),
        "Expected 'updatedValue2', got " + rs.getString("col2"));

    // Delete operation as part of compound test
    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    executeSQL(deleteSQL);

    // Verify delete operation
    rs = executeQuery("SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertFalse(rs.next(), "Expected no rows after delete");
    deleteTable(tableName);
  }

  @Test
  void testComplexQueryJoins() throws SQLException {
    String table1Name = "table1_cqj";
    String table2Name = "table2_cqj";
    setupDatabaseTable(table1Name);
    setupDatabaseTable(table2Name);
    insertTestDataForJoins(table1Name, table2Name);

    String joinSQL =
        "SELECT t1.id, t2.col2 FROM "
            + getFullyQualifiedTableName(table1Name)
            + " t1 "
            + "JOIN "
            + getFullyQualifiedTableName(table2Name)
            + " t2 "
            + "ON t1.id = t2.id";
    ResultSet rs = executeQuery(joinSQL);
    assertTrue(rs.next(), "Expected at least one row from JOIN query");
    deleteTable(table1Name);
    deleteTable(table2Name);
  }

  @Test
  void testComplexQuerySubqueries() throws SQLException {
    String tableName = "subquery_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    String subquerySQL =
        "SELECT id FROM "
            + getFullyQualifiedTableName(tableName)
            + " WHERE id IN (SELECT id FROM "
            + getFullyQualifiedTableName(tableName)
            + " WHERE col1 = 'value1')";
    ResultSet rs = executeQuery(subquerySQL);
    assertTrue(rs.next(), "Expected at least one row from subquery");
    deleteTable(tableName);
  }
}
