package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ConcurrentExecutionTests {
  private static final int NUM_THREADS = 100;
  private static final int NUM_ITERATIONS = 1;

  private static class QueryMetrics {
    final long executionTimeMs;
    final int threadNum;
    final int rowCount;

    QueryMetrics(long executionTimeMs, int threadNum, int rowCount) {
      this.executionTimeMs = executionTimeMs;
      this.threadNum = threadNum;
      this.rowCount = rowCount;
    }
  }

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

  @Test
  void testConcurrentExecutionWithMetrics() throws InterruptedException {
    List<List<QueryMetrics>> allIterationMetrics = new ArrayList<>();

    for (int iteration = 0; iteration < NUM_ITERATIONS; iteration++) {
      System.out.println("\nStarting iteration " + (iteration + 1) + " of " + NUM_ITERATIONS);

      ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
      List<Future<QueryMetrics>> futures = new ArrayList<>();
      Instant iterationStart = Instant.now();

      // Launch all threads
      for (int i = 0; i < NUM_THREADS; i++) {
        final int threadNum = i;
        Future<QueryMetrics> future =
            executorService.submit(() -> runThreadQueriesWithMetrics(threadNum));
        futures.add(future);
      }

      executorService.shutdown();

      // Collect metrics from all threads
      List<QueryMetrics> iterationMetrics = new ArrayList<>();
      boolean allSuccess = true;

      for (Future<QueryMetrics> future : futures) {
        try {
          QueryMetrics metrics = future.get();
          iterationMetrics.add(metrics);
        } catch (ExecutionException e) {
          e.printStackTrace();
          allSuccess = false;
        }
      }

      long iterationDurationMs = Duration.between(iterationStart, Instant.now()).toMillis();

      // Print metrics for this iteration
      printIterationMetrics(iterationMetrics, iterationDurationMs, iteration + 1);

      allIterationMetrics.add(iterationMetrics);
      assertTrue(allSuccess, "Not all threads completed successfully");
    }

    // Print aggregate statistics across all iterations
    printAggregateMetrics(allIterationMetrics);
  }

  private QueryMetrics runThreadQueriesWithMetrics(int threadNum) throws SQLException {
    Instant start = Instant.now();
    int rowCount = 0;

    try (Connection connection = getValidJDBCConnection()) {
      final String table = "main.tpcds_sf100_delta.catalog_sales";
      final int maxRows = 100000;
      final String sql = "SELECT * FROM " + table + " limit " + maxRows;

      try (Statement statement = connection.createStatement()) {
        statement.setMaxRows(maxRows);
        try (ResultSet rs = statement.executeQuery(sql)) {
          rowCount = processResultSet(rs);
        }
      }
    }

    long executionTimeMs = Duration.between(start, Instant.now()).toMillis();
    return new QueryMetrics(executionTimeMs, threadNum, rowCount);
  }

  private int processResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int columnsNumber = rsmd.getColumnCount();
    int rowCount = 0;

    while (resultSet.next()) {
      rowCount++;
      for (int i = 1; i <= columnsNumber; i++) {
        try {
          Object columnValue = resultSet.getObject(i);
        } catch (Exception e) {
          fail("Error processing result set: " + e.getMessage());
        }
      }
    }
    return rowCount;
  }

  private void printIterationMetrics(
      List<QueryMetrics> metrics, long iterationDurationMs, int iterationNum) {
    System.out.println("\nIteration " + iterationNum + " Metrics:");
    System.out.println("Total iteration time: " + iterationDurationMs + "ms");
    System.out.println(
        "Average query time: "
            + metrics.stream().mapToLong(m -> m.executionTimeMs).average().orElse(0)
            + "ms");
    System.out.println("Thread execution times (ms):");

    metrics.forEach(
        m ->
            System.out.printf(
                "Thread %d: %dms (%d rows)\n", m.threadNum, m.executionTimeMs, m.rowCount));

    long minTime = metrics.stream().mapToLong(m -> m.executionTimeMs).min().orElse(0);
    long maxTime = metrics.stream().mapToLong(m -> m.executionTimeMs).max().orElse(0);
    System.out.println("Min execution time: " + minTime + "ms");
    System.out.println("Max execution time: " + maxTime + "ms");
    System.out.println("Max/Min ratio: " + String.format("%.2f", (double) maxTime / minTime));
  }

  private void printAggregateMetrics(List<List<QueryMetrics>> allMetrics) {
    System.out.println("\nAggregate Metrics Across All Iterations:");

    // Calculate aggregate statistics
    DoubleSummaryStatistics executionStats =
        allMetrics.stream()
            .flatMap(List::stream)
            .mapToDouble(m -> m.executionTimeMs)
            .summaryStatistics();

    System.out.printf("Average query time: %.2fms\n", executionStats.getAverage());
    System.out.printf("Min query time: %dms\n", (long) executionStats.getMin());
    System.out.printf("Max query time: %dms\n", (long) executionStats.getMax());
    System.out.printf("Standard deviation: %.2fms\n", calculateStdDev(allMetrics));

    // Calculate throughput
    double avgQueriesPerSecond =
        allMetrics.stream()
            .mapToDouble(
                metrics ->
                    1000.0
                        * metrics.size()
                        / metrics.stream().mapToLong(m -> m.executionTimeMs).max().orElse(1))
            .average()
            .orElse(0.0);

    System.out.printf("Average throughput: %.2f queries/second\n", avgQueriesPerSecond);
  }

  private double calculateStdDev(List<List<QueryMetrics>> allMetrics) {
    List<Long> allTimes =
        allMetrics.stream()
            .flatMap(List::stream)
            .map(m -> m.executionTimeMs)
            .collect(Collectors.toList());

    double mean = allTimes.stream().mapToLong(t -> t).average().orElse(0.0);
    double variance =
        allTimes.stream().mapToDouble(t -> Math.pow(t - mean, 2)).average().orElse(0.0);

    return Math.sqrt(variance);
  }

  private void runThreadQueries(int threadNum) throws SQLException {
    try (Connection connection = getValidJDBCConnection()) {
      // Use a unique table name per thread to avoid conflicts
      //      String tableName = "concurrent_test_table_" + threadNum;
      //      setupDatabaseTable(connection, tableName);
      //
      //      // Insert data
      //      String insertSQL =
      //          "INSERT INTO "
      //              + getFullyQualifiedTableName(tableName)
      //              + " (id, col1, col2) VALUES ("
      //              + threadNum
      //              + ", 'value"
      //              + threadNum
      //              + "', 'value"
      //              + threadNum
      //              + "')";
      //
      //      try (Statement statement = connection.createStatement()) {
      //        statement.execute(insertSQL);
      //      }
      //
      //      // Update data
      //      String updateSQL =
      //          "UPDATE "
      //              + getFullyQualifiedTableName(tableName)
      //              + " SET col1 = 'updatedValue"
      //              + threadNum
      //              + "' WHERE id = "
      //              + threadNum;
      //      try (Statement statement = connection.createStatement()) {
      //        statement.execute(updateSQL);
      //      }
      //
      //      // Select data
      //      String selectSQL =
      //          "SELECT col1 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = " +
      // threadNum;
      //      try (Statement statement = connection.createStatement()) {
      //        try (ResultSet rs = statement.executeQuery(selectSQL)) {
      //          if (rs.next()) {
      //            String col1 = rs.getString("col1");
      //            assertEquals(
      //                "updatedValue" + threadNum, col1, "Expected updated value in thread " +
      // threadNum);
      //          } else {
      //            fail("No data found in thread " + threadNum);
      //          }
      //        }
      //      }
      //
      //      // Delete data
      //      String deleteSQL =
      //          "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = " +
      // threadNum;
      //      try (Statement statement = connection.createStatement()) {
      //        statement.execute(deleteSQL);
      //      }
      //
      //      // Clean up table
      //      deleteTable(connection, tableName);

      final String table = "main.tpcds_sf100_delta.catalog_sales";
      final int maxRows = 100000;
      final String sql = "SELECT * FROM " + table + " limit " + maxRows;
      try (Statement statement = connection.createStatement()) {
        statement.setMaxRows(maxRows);
        try (ResultSet rs = statement.executeQuery(sql)) {
          printResultSet(rs);
        }
      }
    }
  }

  private void printResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int rowCount = 0;
    while (resultSet.next()) {
      //      for (int i = 1; i <= columnsNumber; i++) {
      //        try {
      //          Object columnValue = resultSet.getObject(i);
      //        } catch (Exception ignored) {
      //          fail("Fight son.");
      //        }
      //      }
      rowCount++;
    }
    System.out.println("Row count: " + rowCount);
  }
}
