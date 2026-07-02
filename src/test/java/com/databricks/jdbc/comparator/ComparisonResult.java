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

  /** Returns a concise one-line summary of diffs for CSV output. */
  public String csvSummary() {
    List<String> parts = new ArrayList<>();

    // Exception vs ResultSet — already concise, keep as is. Legacy diffs use the " vs class "
    // marker; deep error comparison uses the stable "Error one-sided: " prefix (which survives
    // regardless of whether the non-throwing side renders to a class, a scalar, or null).
    for (String d : metadataDifferences) {
      if (d.contains(" vs class ") || d.startsWith("Error one-sided: ")) {
        parts.add(d);
      }
    }

    // Count metadata mismatches (e.g., "Column name X mismatch: ...")
    long metaMismatches = metadataDifferences.stream().filter(d -> d.contains("mismatch")).count();
    if (metaMismatches > 0) parts.add(metaMismatches + " metadata mismatches");

    // Extra rows (e.g., "First ResultSet has N extra rows")
    for (String d : dataDifferences) {
      if (d.contains("extra rows")) parts.add(d);
    }

    // Count row data mismatches (e.g., "Row 1, Column X mismatch: ...")
    long rowMismatches =
        dataDifferences.stream()
            .filter(d -> d.startsWith("Row ") || d.startsWith("["))
            .filter(d -> d.contains("mismatch"))
            .count();
    if (rowMismatches > 0) parts.add(rowMismatches + " data mismatches");

    // Count error-field mismatches (e.g., "Error SQLState mismatch: ...") emitted by
    // ErrorComparator for both-threw cases.
    long errorMismatches =
        dataDifferences.stream()
            .filter(d -> d.startsWith("Error ") && d.contains("mismatch"))
            .count();
    if (errorMismatches > 0) parts.add(errorMismatches + " error mismatches");

    return parts.isEmpty() ? "" : String.join(", ", parts);
  }

  /** Returns a new ComparisonResult with diffs matching any skip pattern removed. */
  public ComparisonResult filterDiffs(List<String> skipPatterns) {
    if (skipPatterns.isEmpty()) return this;
    ComparisonResult filtered = new ComparisonResult(queryType, queryOrMethod, methodArgs);
    filtered.metadataDifferences =
        metadataDifferences.stream()
            .filter(d -> skipPatterns.stream().noneMatch(d::contains))
            .collect(Collectors.toList());
    filtered.dataDifferences =
        dataDifferences.stream()
            .filter(d -> skipPatterns.stream().noneMatch(d::contains))
            .collect(Collectors.toList());
    return filtered;
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
