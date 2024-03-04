package com.databricks.jdbc.integration.execution;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.*;

public class ExecutionIntegrationTests {

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

  private static void insertTestDataForJoins(String table1Name, String table2Name)
      throws SQLException {
    // Insert data into the first table
    String insertTable1SQL1 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table1Name)
            + " (id, col1, col2) VALUES (1, 'value1_table1', 'value2_table1')";
    executeSQL(insertTable1SQL1);

    String insertTable1SQL2 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table1Name)
            + " (id, col1, col2) VALUES (2, 'value3_table1', 'value4_table1')";
    executeSQL(insertTable1SQL2);

    // Insert related data into the second table
    String insertTable2SQL1 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table2Name)
            + " (id, col1, col2) VALUES (1, 'related_value1_table2', 'related_value2_table2')";
    executeSQL(insertTable2SQL1);

    String insertTable2SQL2 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table2Name)
            + " (id, col1, col2) VALUES (2, 'related_value3_table2', 'related_value4_table2')";
    executeSQL(insertTable2SQL2);
  }

  private static void insertTestData(String tableName) throws SQLException {
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(insertSQL);
  }
}
