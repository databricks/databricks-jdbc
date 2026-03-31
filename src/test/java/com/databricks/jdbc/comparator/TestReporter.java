package com.databricks.jdbc.comparator;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class TestReporter {
  private final Path outputPath;

  public TestReporter(Path outputPath, List<String> headerLines) throws IOException {
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
    if (result.hasDifferences()) {
      try (FileWriter writer = new FileWriter(outputPath.toFile(), true)) {
        writer.write(result.toString());
        writer.write("\n============================\n\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void finish() {
    try (FileWriter writer = new FileWriter(outputPath.toFile(), true)) {
      writer.write("Comparator testing finished at: " + Instant.now() + "\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
