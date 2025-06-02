package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

/** Utility class for executing tasks in parallel with proper context handling. */
public class JdbcThreadUtils {

  /**
   * Executes tasks concurrently with appropriate context management, utilizing a provided executor
   * service (which can be null, in which case a new one will be created).
   *
   * @param items The items to process
   * @param connectionContext The connection context to propagate to worker threads
   * @param maxThreads Maximum number of threads to use (when creating internal executor)
   * @param timeoutSeconds Timeout in seconds
   * @param task The task to execute for each item
   * @param executor Optional executor service to use; if null, an internal one will be created
   * @param <T> Type of input items
   * @param <R> Type of result
   * @return List of results from all tasks
   * @throws SQLException If an error occurs during execution
   */
  public static <T, R> List<R> parallelMap(
      Collection<T> items,
      IDatabricksConnectionContext connectionContext,
      int maxThreads,
      int timeoutSeconds,
      Function<T, R> task,
      ExecutorService executor)
      throws SQLException {

    if (items.isEmpty()) {
      return Collections.emptyList();
    }

    boolean createdExecutor = false;
    ExecutorService executorToUse = executor;

    // Create an executor if one wasn't provided
    if (executorToUse == null) {
      int threadCount = Math.min(items.size(), maxThreads);
      executorToUse = Executors.newFixedThreadPool(threadCount);
      createdExecutor = true;
    }

    try {
      List<Future<R>> futures = new ArrayList<>();

      // Submit tasks to the executor
      for (T item : items) {
        futures.add(
            executorToUse.submit(
                () -> {
                  // Set connection context for this thread
                  DatabricksThreadContextHolder.setConnectionContext(connectionContext);
                  try {
                    // Execute the task
                    return task.apply(item);
                  } finally {
                    // Clear connection context
                    DatabricksThreadContextHolder.clearConnectionContext();
                  }
                }));
      }

      // Collect results
      List<R> results = new ArrayList<>(items.size());
      for (Future<R> future : futures) {
        try {
          results.add(future.get(timeoutSeconds, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabricksSQLException(
              "Parallel execution interrupted",
              e,
              DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
        } catch (ExecutionException e) {
          SQLException sqlEx = findSQLExceptionInCauseChain(e);
          if (sqlEx != null) {
            throw sqlEx;
          } else {
            throw new DatabricksSQLException(
                "Error in parallel execution", e, DatabricksDriverErrorCode.INVALID_STATE);
          }
        } catch (TimeoutException e) {
          throw new DatabricksSQLException(
              "Parallel execution timed out after " + timeoutSeconds + " seconds",
              e,
              DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR);
        }
      }

      return results;
    } finally {
      // Only shut down the executor if we created it
      if (createdExecutor && executorToUse != null) {
        executorToUse.shutdownNow();
      }
    }
  }

  /**
   * Executes tasks in parallel, collecting and flattening all results, utilizing a provided
   * executor service (which can be null, in which case a new one will be created).
   *
   * @param items The items to process
   * @param connectionContext The connection context to propagate to worker threads
   * @param maxThreads Maximum number of threads to use
   * @param timeoutSeconds Timeout in seconds
   * @param task The task to execute for each item, producing a collection of results
   * @param executor Optional executor service to use; if null, an internal one will be created
   * @param <T> Type of input items
   * @param <R> Type of result
   * @return Flattened list of all results
   * @throws SQLException If an error occurs during execution
   */
  public static <T, R> List<R> parallelFlatMap(
      Collection<T> items,
      IDatabricksConnectionContext connectionContext,
      int maxThreads,
      int timeoutSeconds,
      Function<T, Collection<R>> task,
      ExecutorService executor)
      throws SQLException {

    List<Collection<R>> collections =
        parallelMap(items, connectionContext, maxThreads, timeoutSeconds, task, executor);

    // Flatten the results
    List<R> allResults = new ArrayList<>();
    for (Collection<R> collection : collections) {
      if (collection != null) {
        allResults.addAll(collection);
      }
    }

    return allResults;
  }

  /**
   * Recursively searches for a SQLException in the exception cause chain.
   *
   * @param throwable The exception to search
   * @return The first SQLException found in the cause chain, or null if none
   */
  private static SQLException findSQLExceptionInCauseChain(Throwable throwable) {
    if (throwable == null) {
      return null;
    }

    if (throwable instanceof SQLException) {
      return (SQLException) throwable;
    }

    return findSQLExceptionInCauseChain(throwable.getCause());
  }
}
