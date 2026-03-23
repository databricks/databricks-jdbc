package com.databricks.jdbc.comparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ComparisonResult {
  public List<String> metadataDifferences = new ArrayList<>();
  public List<String> dataDifferences = new ArrayList<>();
  public String queryType;
  public String queryOrMethod;
  public Object[] methodArgs;

  public ComparisonResult(String queryType, String queryOrMethod, Object[] methodArgs) {
    this.queryType = queryType;
    this.queryOrMethod = queryOrMethod;
    this.methodArgs = methodArgs;
  }

  public boolean hasDifferences() {
    return !metadataDifferences.isEmpty() || !dataDifferences.isEmpty();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Query Type: ").append(queryType).append("\n");
    sb.append("Query/Method: ").append(queryOrMethod).append("\n");
    if (methodArgs.length > 0) {
      sb.append("Method Arguments: ");
      sb.append(
          Arrays.stream(methodArgs)
              .map(
                  o -> {
                    if (o == null) return "null";
                    if (o instanceof Object[]) return Arrays.toString((Object[]) o);
                    return o.toString();
                  })
              .collect(Collectors.joining(", ")));
      sb.append("\n");
    }
    sb.append("============================\n\n");

    if (metadataDifferences.isEmpty() && dataDifferences.isEmpty()) {
      sb.append("No differences found. The ResultSets are identical.\n");
      return sb.toString();
    }

    if (!metadataDifferences.isEmpty()) {
      sb.append("Metadata Differences:\n");
      sb.append("---------------------\n");
      formatDifferences(sb, metadataDifferences);
      if (!dataDifferences.isEmpty()) {
        sb.append("\n");
      }
    }

    if (!dataDifferences.isEmpty()) {
      sb.append("Data Differences:\n");
      sb.append("-----------------\n");
      formatDifferences(sb, dataDifferences);
    }

    return sb.toString();
  }

  private void formatDifferences(StringBuilder sb, List<String> differences) {
    Map<String, List<String>> categorizedDifferences = new HashMap<>();

    for (String difference : differences) {
      String category = getCategoryFromDifference(difference);
      categorizedDifferences.computeIfAbsent(category, k -> new ArrayList<>()).add(difference);
    }

    for (Map.Entry<String, List<String>> entry : categorizedDifferences.entrySet()) {
      sb.append(entry.getKey()).append(":\n");
      for (String diff : entry.getValue()) {
        sb.append("  - ").append(diff).append("\n");
      }
      sb.append("\n");
    }
  }

  private String getCategoryFromDifference(String difference) {
    if (difference.startsWith("Column count mismatch")) return "Column Count";
    if (difference.startsWith("Extra columns")) return "Extra Columns";
    if (difference.startsWith("Column") && difference.contains("mismatch"))
      return "Column Metadata";
    if (difference.startsWith("Row") && difference.contains("mismatch")) return "Row Data";
    if (difference.contains("extra rows")) return "Extra Rows";
    if (difference.startsWith("Extra row")) return "Extra Row Data";
    return "Other";
  }
}
