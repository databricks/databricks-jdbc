package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ConcurrentExecutionTests {
  private static final int NUM_THREADS = 150;
  private static final int NUM_ITERATIONS = 1;
  private static final int ROW_COUNT = 2000000;

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
  void testConcurrentExecutionWithMetrics() throws InterruptedException {
    printMemoryStats();
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
      final int maxRows = ROW_COUNT;
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
    assertEquals(ROW_COUNT, rowCount);
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

    List<QueryMetrics> sortedMetrics = new ArrayList<>(metrics);
    sortedMetrics.sort(Comparator.comparingLong(a -> a.executionTimeMs));
    System.out.println("\nTop 3 Fastest Queries:");
    for (int i = 0; i < Math.min(3, sortedMetrics.size()); i++) {
      QueryMetrics m = sortedMetrics.get(i);
      System.out.printf("Thread %d: %dms (%d rows)\n", m.threadNum, m.executionTimeMs, m.rowCount);
    }
    System.out.println("\nTop 3 Slowest Queries:");
    for (int i = 1; i <= Math.min(3, sortedMetrics.size()); i++) {
      QueryMetrics m = sortedMetrics.get(sortedMetrics.size() - i);
      System.out.printf("Thread %d: %dms (%d rows)\n", m.threadNum, m.executionTimeMs, m.rowCount);
    }

    long minTime = metrics.stream().mapToLong(m -> m.executionTimeMs).min().orElse(0);
    long maxTime = metrics.stream().mapToLong(m -> m.executionTimeMs).max().orElse(0);
    System.out.println("Max/Min ratio: " + String.format("%.2f", (double) maxTime / minTime));
  }

  private void printAggregateMetrics(List<List<QueryMetrics>> allMetrics) {
    System.out.println("\nAggregate Metrics Across All Iterations:");

    List<QueryMetrics> allQueryMetrics =
        allMetrics.stream()
            .flatMap(List::stream)
            .sorted(Comparator.comparingLong(a -> a.executionTimeMs))
            .collect(Collectors.toList());

    // Calculate aggregate statistics
    DoubleSummaryStatistics executionStats =
        allQueryMetrics.stream().mapToDouble(m -> m.executionTimeMs).summaryStatistics();

    System.out.printf("Average query time: %.2fms\n", executionStats.getAverage());
    System.out.println("\nTop 3 Fastest Queries Across All Iterations:");
    for (int i = 0; i < Math.min(3, allQueryMetrics.size()); i++) {
      QueryMetrics m = allQueryMetrics.get(i);
      System.out.printf("Thread %d: %dms (%d rows)\n", m.threadNum, m.executionTimeMs, m.rowCount);
    }
    System.out.println("\nTop 3 Slowest Queries Across All Iterations:");
    for (int i = 1; i <= Math.min(3, allQueryMetrics.size()); i++) {
      QueryMetrics m = allQueryMetrics.get(allQueryMetrics.size() - i);
      System.out.printf("Thread %d: %dms (%d rows)\n", m.threadNum, m.executionTimeMs, m.rowCount);
    }

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

  public static void printMemoryStats() {
    Runtime runtime = Runtime.getRuntime();

    long totalMemory = runtime.totalMemory() / (1024 * 1024); // Convert to MB
    long freeMemory = runtime.freeMemory() / (1024 * 1024); // Convert to MB
    long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    long maxMemory = runtime.maxMemory() / (1024 * 1024); // Convert to MB

    System.out.println("Memory Stats:");
    System.out.println("Total Memory: " + totalMemory + " MB");
    System.out.println("Free Memory: " + freeMemory + " MB");
    System.out.println("Used Memory: " + usedMemory + " MB");
    System.out.println("Max Memory: " + maxMemory + " MB");
    System.out.println("Available processors: " + runtime.availableProcessors());
  }
}
