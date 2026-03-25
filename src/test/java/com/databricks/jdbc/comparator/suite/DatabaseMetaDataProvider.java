package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Compares DatabaseMetaData method results between Thrift and SEA.
 *
 * <p>Each test case corresponds to one DatabaseMetaData method. The execute() method iterates all
 * argument combinations for that method, invokes via reflection on both connections' metadata
 * objects, and compares results using ResultSetComparator.
 *
 * <p>The test plan (which methods, which args) is defined in {@link DatabaseMetaDataParams}. This
 * class is purely about execution and comparison.
 */
public class DatabaseMetaDataProvider implements SuiteProvider {

  private static final Map<String, List<Object[]>> METHOD_REGISTRY =
      DatabaseMetaDataParams.buildRegistry();

  /** Schemas to exclude from ResultSet comparison. Set via -DMETADATA_SKIP_SCHEMAS. */
  private static final Set<String> SKIP_SCHEMAS = parseSkipSchemas();

  private static Set<String> parseSkipSchemas() {
    String prop = System.getProperty("METADATA_SKIP_SCHEMAS");
    if (prop == null || prop.isEmpty()) return Set.of();
    return new HashSet<>(Arrays.asList(prop.split(",")));
  }

  @Override
  public List<TestCase> getTestCases() {
    List<TestCase> cases = new ArrayList<>();
    for (Map.Entry<String, List<Object[]>> entry : METHOD_REGISTRY.entrySet()) {
      String method = entry.getKey();
      int comboCount = entry.getValue().size();
      cases.add(new TestCase(method, method + " — " + comboCount + " arg combo(s)"));
    }
    return cases;
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String methodName = testCase.getIdentifier();
    List<Object[]> argCombos = METHOD_REGISTRY.get(methodName);

    DatabaseMetaData md1 = conn1.getMetaData();
    DatabaseMetaData md2 = conn2.getMetaData();

    List<String> metadataDiffs = new ArrayList<>();
    List<String> dataDiffs = new ArrayList<>();

    int total = argCombos.size();
    for (int idx = 0; idx < total; idx++) {
      Object[] args = argCombos.get(idx);
      String argsLabel = formatArgs(args);
      System.out.printf(
          "[%s]   Comparing %s(%s) [%d/%d]%n",
          Instant.now(), methodName, argsLabel, idx + 1, total);

      Object result1 = ReflectionUtils.executeMethod(md1, methodName, args);
      Object result2 = ReflectionUtils.executeMethod(md2, methodName, args);

      try {
        ComparisonResult sub =
            ResultSetComparator.compare(label, methodName, args, result1, result2, SKIP_SCHEMAS);

        // Prefix each diff with the args for traceability
        for (String diff : sub.metadataDifferences) {
          metadataDiffs.add("[" + argsLabel + "] " + diff);
        }
        for (String diff : sub.dataDifferences) {
          dataDiffs.add("[" + argsLabel + "] " + diff);
        }
      } finally {
        // Close ResultSets to avoid resource leaks
        if (result1 instanceof ResultSet) ((ResultSet) result1).close();
        if (result2 instanceof ResultSet) ((ResultSet) result2).close();
      }
    }

    ComparisonResult result = new ComparisonResult(label, methodName, new Object[0]);
    result.metadataDifferences = metadataDiffs;
    result.dataDifferences = dataDiffs;
    return result;
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
