package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Compares DatabaseMetaData method results between Thrift and SEA.
 *
 * <p>Each test case corresponds to one DatabaseMetaData method. Delegates argument combination
 * execution to {@link CombinationExecutor} (sequential or parallel), then merges results.
 *
 * <p>The test plan (which methods, which args) is defined in {@link DatabaseMetaDataParams}. This
 * class is purely about orchestration.
 */
public class DatabaseMetaDataProvider implements SuiteProvider {

  private static final Map<String, List<Object[]>> METHOD_REGISTRY =
      DatabaseMetaDataParams.buildRegistry();

  @Override
  public List<TestCase> getTestCases() {
    List<TestCase> cases = new ArrayList<>();
    for (Map.Entry<String, List<Object[]>> entry : METHOD_REGISTRY.entrySet()) {
      String method = entry.getKey();
      int combinationCount = entry.getValue().size();
      cases.add(new TestCase(method, method + " — " + combinationCount + " combination(s)"));
    }
    return cases;
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String methodName = testCase.getIdentifier();
    List<Object[]> argCombinations = METHOD_REGISTRY.get(methodName);

    DatabaseMetaData md1 = conn1.getMetaData();
    DatabaseMetaData md2 = conn2.getMetaData();

    List<CombinationResult> results =
        CombinationExecutor.executeAll(methodName, argCombinations, md1, md2, label);

    // Merge results with [argsLabel] prefix for traceability
    List<String> metadataDiffs = new ArrayList<>();
    List<String> dataDiffs = new ArrayList<>();
    for (CombinationResult cr : results) {
      for (String diff : cr.metadataDiffs) {
        metadataDiffs.add("[" + cr.argsLabel + "] " + diff);
      }
      for (String diff : cr.dataDiffs) {
        dataDiffs.add("[" + cr.argsLabel + "] " + diff);
      }
    }

    ComparisonResult result = new ComparisonResult(label, methodName, new Object[0]);
    result.metadataDifferences = metadataDiffs;
    result.dataDifferences = dataDiffs;
    return result;
  }
}
