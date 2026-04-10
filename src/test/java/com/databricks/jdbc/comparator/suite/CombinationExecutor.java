package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Executes DatabaseMetaData method combinations sequentially or in parallel.
 *
 * <p>Each combination is a self-contained unit: call Thrift, call SEA, compare, close ResultSets,
 * return result. No shared mutable state during execution.
 *
 * <p>Configure via system properties:
 *
 * <ul>
 *   <li>{@code -DMETADATA_PARALLEL_THREADS=8} — number of threads (default=1, sequential)
 *   <li>{@code -DMETADATA_SKIP_SCHEMAS=information_schema,global_temp} — schemas to filter
 * </ul>
 */
public class CombinationExecutor {

  private static final int PARALLEL_THREADS = parseParallelThreads();
  private static final Set<String> SKIP_SCHEMAS = parseSkipSchemas();
  private static final MetadataFilterConfig FILTER_CONFIG = MetadataFilterConfig.load();

  private static int parseParallelThreads() {
    String prop = System.getProperty("METADATA_PARALLEL_THREADS");
    if (prop == null || prop.isEmpty()) return 1;
    return Math.max(1, Integer.parseInt(prop));
  }

  private static Set<String> parseSkipSchemas() {
    String prop = System.getProperty("METADATA_SKIP_SCHEMAS");
    if (prop == null || prop.isEmpty()) return Collections.emptySet();
    return new HashSet<>(Arrays.asList(prop.split(",")));
  }

  /**
   * Executes all argument combinations for a method and returns results in order.
   *
   * @return list of CombinationResults in the same order as argCombos
   */
  public static List<CombinationResult> executeAll(
      String methodName,
      List<Object[]> argCombos,
      DatabaseMetaData md1,
      DatabaseMetaData md2,
      String label) {
    int total = argCombos.size();
    if (PARALLEL_THREADS <= 1) {
      return executeSequential(methodName, argCombos, md1, md2, label, total);
    } else {
      return executeParallel(methodName, argCombos, md1, md2, label, total);
    }
  }

  private static List<CombinationResult> executeSequential(
      String methodName,
      List<Object[]> argCombos,
      DatabaseMetaData md1,
      DatabaseMetaData md2,
      String label,
      int total) {
    List<CombinationResult> results = new ArrayList<>();
    for (int idx = 0; idx < total; idx++) {
      Object[] args = argCombos.get(idx);
      String argsLabel = formatArgs(args);
      String skipReason = FILTER_CONFIG.getSkipReason(methodName, args);
      if (skipReason != null) {
        System.out.printf(
            "[%s]   Skipped %s(%s) [%d/%d] — %s%n",
            Instant.now(), methodName, argsLabel, idx + 1, total, skipReason);
        results.add(CombinationResult.skipped(argsLabel, skipReason));
        continue;
      }
      System.out.printf(
          "[%s]   Started comparing %s(%s) [%d/%d]%n",
          Instant.now(), methodName, argsLabel, idx + 1, total);
      results.add(executeSingle(methodName, args, md1, md2, label));
    }
    return results;
  }

  private static List<CombinationResult> executeParallel(
      String methodName,
      List<Object[]> argCombos,
      DatabaseMetaData md1,
      DatabaseMetaData md2,
      String label,
      int total) {
    ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_THREADS);
    AtomicInteger completed = new AtomicInteger();
    List<Future<CombinationResult>> futures = new ArrayList<>();

    for (Object[] args : argCombos) {
      final String argsLabel = formatArgs(args);
      String skipReason = FILTER_CONFIG.getSkipReason(methodName, args);
      if (skipReason != null) {
        System.out.printf(
            "[%s]   Skipped %s(%s) — %s%n", Instant.now(), methodName, argsLabel, skipReason);
        futures.add(
            CompletableFuture.completedFuture(CombinationResult.skipped(argsLabel, skipReason)));
        continue;
      }
      futures.add(
          executor.submit(
              () -> {
                System.out.printf(
                    "[%s]   Started comparing %s(%s)%n", Instant.now(), methodName, argsLabel);
                CombinationResult result = executeSingle(methodName, args, md1, md2, label);
                int done = completed.incrementAndGet();
                System.out.printf(
                    "[%s]   Finished comparing %s(%s) [%d/%d]%n",
                    Instant.now(), methodName, argsLabel, done, total);
                return result;
              }));
    }

    // Collect in submission order for deterministic report
    List<CombinationResult> results = new ArrayList<>();
    for (Future<CombinationResult> f : futures) {
      try {
        results.add(f.get());
      } catch (Exception e) {
        throw new RuntimeException("Combination execution failed", e);
      }
    }

    executor.shutdown();
    return results;
  }

  private static CombinationResult executeSingle(
      String methodName, Object[] args, DatabaseMetaData md1, DatabaseMetaData md2, String label) {
    String argsLabel = formatArgs(args);
    try {
      Object result1 = ReflectionUtils.executeMethod(md1, methodName, args);
      Object result2 = ReflectionUtils.executeMethod(md2, methodName, args);
      try {
        ComparisonResult sub =
            ResultSetComparator.compare(label, methodName, args, result1, result2, SKIP_SCHEMAS);
        return new CombinationResult(argsLabel, sub.metadataDifferences, sub.dataDifferences);
      } finally {
        if (result1 instanceof ResultSet) ((ResultSet) result1).close();
        if (result2 instanceof ResultSet) ((ResultSet) result2).close();
      }
    } catch (Exception e) {
      return new CombinationResult(
          argsLabel,
          Collections.emptyList(),
          Collections.singletonList("Execution error: " + e.getMessage()));
    }
  }

  private static String formatArgs(Object[] args) {
    if (args.length == 0) return "no args";
    return Arrays.stream(args)
        .map(
            o -> {
              if (o == null) return "null";
              if (o instanceof Object[]) return Arrays.toString((Object[]) o);
              return o.toString();
            })
        .collect(Collectors.joining(", "));
  }
}
