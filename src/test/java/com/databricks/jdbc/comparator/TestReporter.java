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
  private final List<String> skipDiffPatterns;

  public TestReporter(Path outputPath, List<String> headerLines) throws IOException {
    this.skipDiffPatterns = parseSkipDiffPatterns();
    this.outputPath = outputPath;
    try (FileWriter writer = new FileWriter(outputPath.toFile())) {
      writer.write("Report generated at: " + Instant.now() + "\n");
      for (String line : headerLines) {
        writer.write(line + "\n");
      }
      writer.write("============================\n\n");
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

  private static List<String> parseSkipDiffPatterns() {
    String prop = System.getProperty("SKIP_DIFF_PATTERNS");
    if (prop == null || prop.isEmpty()) return Collections.emptyList();
    return Arrays.asList(prop.split("\\|"));
  }

  public void finish() {
    try (FileWriter writer = new FileWriter(outputPath.toFile(), true)) {
      writer.write("Comparator testing finished at: " + Instant.now() + "\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
