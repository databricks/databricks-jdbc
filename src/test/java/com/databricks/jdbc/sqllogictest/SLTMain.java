// This file is a copy of the file from the sqllogictest project
// (net/hydromatic/sqllogictest/Main.java)
package com.databricks.jdbc.sqllogictest;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.TestLoader;
import net.hydromatic.sqllogictest.TestStatistics;
import net.hydromatic.sqllogictest.executors.NoExecutor;
import org.apache.commons.io.FileUtils;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

/** Execute all SqlLogicTest tests. */
public class SLTMain {
  private SLTMain() {}

  /** Command-line entry point. */
  public static void main(String[] args) throws IOException, URISyntaxException {
    OptionsParser optionParser = new OptionsParser(true, System.out, System.err);
    execute(optionParser, args);
  }

  /** Get the list of all test files. */
  public static Set<String> getTestList() throws IOException, URISyntaxException {
    File directory = new File("src/test/resources/sqllogictest");

    if (!directory.exists()) {
      System.err.println("SQL Logic tests directory not found");
      return null;
    }

    return FileUtils.listFiles(directory, new String[]{"test"}, true).stream().map(File::getPath)
            .map(s -> s.replaceFirst("src/test/resources/", ""))
            .collect(Collectors.toSet());
  }

  /**
   * Execute the program using the specified command-line options.
   *
   * @param optionParser Parser that will be used to parse the command-line options.
   * @param args Command-line options.
   * @return A description of the outcome of the tests. null when tests cannot even be started.
   */
  public static TestStatistics execute(OptionsParser optionParser, String... args)
          throws IOException, URISyntaxException {
    optionParser.setBinaryName("slt");
    NoExecutor.register(optionParser);
    DbsqlExecutor.register(optionParser);
    OptionsParser.SuppliedOptions options = optionParser.parse(args);
    if (options.exitCode != 0) {
      return null;
    }

    Set<String> allTests = getTestList();
    TestLoader loader = new TestLoader(options);
    for (String testPath : allTests) {
      boolean runTest = options.getDirectories().stream().anyMatch(testPath::contains);
      if (!runTest) {
        continue;
      }
      if (!loader.visitFile(testPath)) {
        break;
      }
    }
    return loader.statistics;
  }
}
