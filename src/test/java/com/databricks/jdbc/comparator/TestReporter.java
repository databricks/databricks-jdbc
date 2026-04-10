package com.databricks.jdbc.comparator;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestReporter {
  private final Path outputPath;
  private final Path csvPath;
  private final List<String> skipDiffPatterns;

  public TestReporter(Path outputPath, List<String> headerLines) throws IOException {
    this.skipDiffPatterns = parseSkipDiffPatterns();
    this.outputPath = outputPath;
    this.csvPath =
        Path.of(
            outputPath.toString().replaceFirst("-report-", "-results-").replace(".txt", ".csv"));
    try (FileWriter writer = new FileWriter(outputPath.toFile())) {
      writer.write("Report generated at: " + Instant.now() + "\n");
      for (String line : headerLines) {
        writer.write(line + "\n");
      }
      writer.write("============================\n\n");
    }
    // Write CSV header
    try (FileWriter csv = new FileWriter(csvPath.toFile())) {
      csv.write("Suite,Config,Test Case,Result,Diff Summary\n");
    }
  }

  public void addResult(ComparisonResult result) {
    ComparisonResult filtered = result.filterDiffs(skipDiffPatterns);
    if (filtered.hasDifferences()) {
      try (FileWriter writer = new FileWriter(outputPath.toFile(), true)) {
        writer.write(filtered.toString());
        writer.write("\n============================\n\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /** Writes a single CSV row for a test case result. */
  public void addCsvRow(
      String suite, String config, String testCase, String result, String diffSummary) {
    try (FileWriter csv = new FileWriter(csvPath.toFile(), true)) {
      csv.write(
          escapeCsv(suite)
              + ","
              + escapeCsv(config)
              + ","
              + escapeCsv(testCase)
              + ","
              + escapeCsv(result)
              + ","
              + escapeCsv(diffSummary)
              + "\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Convenience method to add CSV rows from a ComparisonResult. Writes one row per diff, or a
   * single PASS/DIFF_FILTERED row.
   */
  public void addCsvFromResult(
      String suite, String config, String testCase, ComparisonResult result) {
    ComparisonResult filtered = result.filterDiffs(skipDiffPatterns);

    if (!result.hasDifferences()) {
      addCsvRow(suite, config, testCase, "PASS", "");
    } else if (!filtered.hasDifferences()) {
      // Had diffs but all were filtered
      addCsvRow(suite, config, testCase, "DIFF_FILTERED", "");
    } else {
      addCsvRow(suite, config, testCase, "DIFF", filtered.csvSummary());
    }
  }

  private static String escapeCsv(String value) {
    if (value == null || value.isEmpty()) return "";
    if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }

  private static List<String> parseSkipDiffPatterns() {
    String prop = System.getProperty("SKIP_DIFF_PATTERNS");
    if (prop == null || prop.isEmpty()) return Collections.emptyList();
    return Arrays.asList(prop.split("\\|"));
  }

  public Path getCsvPath() {
    return csvPath;
  }

  public void finish() {
    try (FileWriter writer = new FileWriter(outputPath.toFile(), true)) {
      writer.write("Comparator testing finished at: " + Instant.now() + "\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
