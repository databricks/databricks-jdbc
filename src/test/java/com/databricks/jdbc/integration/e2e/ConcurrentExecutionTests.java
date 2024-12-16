package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

public class ConcurrentExecutionTests {
  private static final int NUM_THREADS = 20;

  @Test
  void testConcurrentExecution() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<Boolean>> futures = new ArrayList<>();

    for (int i = 0; i < NUM_THREADS; i++) {
      final int threadNum = i;
      Future<Boolean> future =
          executorService.submit(
              () -> {
                try {
                  runThreadQueries(threadNum);
                  return true;
                } catch (Exception e) {
                  e.printStackTrace();
                  return false;
                }
              });
      futures.add(future);
    }

    executorService.shutdown();

    boolean allSuccess = true;
    for (Future<Boolean> future : futures) {
      try {
        if (!future.get()) {
          allSuccess = false;
        }
      } catch (ExecutionException e) {
        e.printStackTrace();
        allSuccess = false;
      }
    }

    assertTrue(allSuccess, "Not all threads completed successfully");
  }

  private void runThreadQueries(int threadNum) throws SQLException {
    try (Connection connection = getValidJDBCConnection()) {
      // Use a unique table name per thread to avoid conflicts
      String tableName = "concurrent_test_table_" + threadNum;
      setupDatabaseTable(connection, tableName);

      // Insert data
      String insertSQL =
          "INSERT INTO "
              + getFullyQualifiedTableName(tableName)
              + " (id, col1, col2) VALUES ("
              + threadNum
              + ", 'value"
              + threadNum
              + "', 'value"
              + threadNum
              + "')";

      try (Statement statement = connection.createStatement()) {
        statement.execute(insertSQL);
      }

      // Update data
      String updateSQL =
          "UPDATE "
              + getFullyQualifiedTableName(tableName)
              + " SET col1 = 'updatedValue"
              + threadNum
              + "' WHERE id = "
              + threadNum;
      try (Statement statement = connection.createStatement()) {
        statement.execute(updateSQL);
      }

      // Select data
      String selectSQL =
          "SELECT col1 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = " + threadNum;
      try (Statement statement = connection.createStatement()) {
        try (ResultSet rs = statement.executeQuery(selectSQL)) {
          if (rs.next()) {
            String col1 = rs.getString("col1");
            assertEquals(
                "updatedValue" + threadNum, col1, "Expected updated value in thread " + threadNum);
          } else {
            fail("No data found in thread " + threadNum);
          }
        }
      }

      // Delete data
      String deleteSQL =
          "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = " + threadNum;
      try (Statement statement = connection.createStatement()) {
        statement.execute(deleteSQL);
      }

      // Clean up table
      deleteTable(connection, tableName);
    }
  }
}
