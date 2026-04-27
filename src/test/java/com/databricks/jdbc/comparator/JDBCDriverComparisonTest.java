package com.databricks.jdbc.comparator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.databricks.jdbc.comparator.config.ConnectionConfig;
import com.databricks.jdbc.comparator.config.ConnectionManager;
import com.databricks.jdbc.comparator.config.Endpoint;
import com.databricks.jdbc.comparator.config.TestSuite;
import com.databricks.jdbc.comparator.setup.WorkspaceSetup;
import com.databricks.jdbc.comparator.suite.CombinationResult;
import com.databricks.jdbc.comparator.suite.DatabaseMetaDataProvider;
import com.databricks.jdbc.comparator.suite.SuiteProvider;
import com.databricks.jdbc.comparator.suite.TestCase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class JDBCDriverComparisonTest {
  private static final String DEFAULT_HOST = "adb-7405613695221181.1.azuredatabricks.net";
  private static final String DEFAULT_WAREHOUSE = "6feab30b476abfa4";

  private static final String BASE_HOST_URL =
      "jdbc:databricks://"
          + System.getProperty("COMPARATOR_HOST", DEFAULT_HOST)
          + ":443/default;ssl=1;authMech=3";

  private static final Endpoint LEFT;
  private static final Endpoint RIGHT;

  static {
    Endpoint left = Endpoint.fromSystemProperties("LEFT");
    Endpoint right = Endpoint.fromSystemProperties("RIGHT");
    if (left == null && right == null) {
      // Legacy mode: single warehouse, dual transport.
      String warehouse = System.getProperty("COMPARATOR_WAREHOUSE", DEFAULT_WAREHOUSE);
      Endpoint[] pair = Endpoint.legacyPair(warehouse);
      LEFT = pair[0];
      RIGHT = pair[1];
    } else if (left == null || right == null) {
      throw new IllegalStateException(
          "Both LEFT_* and RIGHT_* must be configured. Set one of "
              + "LEFT_WAREHOUSE / LEFT_CLUSTER / LEFT_HTTP_PATH and the same for RIGHT, "
              + "or omit all of them to fall back to legacy single-warehouse Thrift-vs-SEA mode.");
    } else {
      LEFT = left;
      RIGHT = right;
    }
  }

  private static final String BASE_LEFT_URL = LEFT.toUrl(BASE_HOST_URL);
  private static final String BASE_RIGHT_URL = RIGHT.toUrl(BASE_HOST_URL);

  /** Returns the configured endpoint for the named side. Used by suite providers for labeling. */
  public static Endpoint endpointFor(String side) {
    if ("LEFT".equalsIgnoreCase(side)) return LEFT;
    if ("RIGHT".equalsIgnoreCase(side)) return RIGHT;
    throw new IllegalArgumentException("side must be LEFT or RIGHT, got: " + side);
  }

  private static ConnectionManager connectionManager;
  private static TestReporter reporter;

  @BeforeAll
  static void setup() throws Exception {
    String token = System.getenv("DATABRICKS_COMPARATOR_TOKEN");
    connectionManager = new ConnectionManager(token);

    // Workspace validation/setup — only runs when -DWORKSPACE_SETUP=validate|create.
    // Setup runs against LEFT by convention; LEFT must be the side that supports the suite DDL.
    WorkspaceSetup.run(connectionManager.getConnection(BASE_LEFT_URL));

    String timestamp = Instant.now().toString().replaceAll("[:.]+", "-");
    List<String> headerLines =
        List.of(
            "LEFT  (" + LEFT.getLabel() + "): " + BASE_LEFT_URL,
            "RIGHT (" + RIGHT.getLabel() + "): " + BASE_RIGHT_URL,
            "CONNECTION_CONFIG: " + System.getProperty("CONNECTION_CONFIG", "(all)"),
            "SUITES_RUN_ONLY: " + System.getProperty("SUITES_RUN_ONLY", "(all)"),
            "METADATA_RUN_ONLY_METHODS: "
                + System.getProperty("METADATA_RUN_ONLY_METHODS", "(all)"),
            "METADATA_SKIP_METHODS: " + System.getProperty("METADATA_SKIP_METHODS", "(none)"),
            "METADATA_SKIP_SCHEMAS: " + System.getProperty("METADATA_SKIP_SCHEMAS", "(none)"),
            "METADATA_PARALLEL_THREADS: " + System.getProperty("METADATA_PARALLEL_THREADS", "1"),
            "METADATA_FILTER_CONFIG: " + System.getProperty("METADATA_FILTER_CONFIG", "(none)"),
            "Filter config:\n" + readFilterConfig());
    reporter =
        new TestReporter(Path.of("jdbc-comparison-report-" + timestamp + ".txt"), headerLines);
  }

  private static String readFilterConfig() {
    String path = System.getProperty("METADATA_FILTER_CONFIG");
    if (path == null || path.isEmpty()) return "(none)";
    try {
      return Files.readString(Path.of(path));
    } catch (IOException e) {
      return "(failed to read: " + e.getMessage() + ")";
    }
  }

  @AfterAll
  static void teardown() throws Exception {
    if (connectionManager != null) connectionManager.close();
    if (reporter != null) {
      reporter.finish();
      System.out.println("CSV results: " + reporter.getCsvPath());
    }
  }

  /**
   * Generates all test arguments by iterating: config → suite → test case.
   *
   * <p>Configs with no applicable suites or suites with no provider/test cases are skipped.
   */
  static Stream<Arguments> provideAllTests() {
    List<Arguments> allTests = new ArrayList<>();

    String suiteFilter = System.getProperty("SUITES_RUN_ONLY");
    Set<String> allowedSuites =
        (suiteFilter == null || suiteFilter.isEmpty())
            ? null
            : new HashSet<>(Arrays.asList(suiteFilter.split(",")));

    for (ConnectionConfig config : ConnectionConfig.activeConfigs()) {
      Connection leftConn;
      Connection rightConn;
      try {
        leftConn = connectionManager.getConnection(config.buildUrl(BASE_LEFT_URL));
        rightConn = connectionManager.getConnection(config.buildUrl(BASE_RIGHT_URL));
      } catch (SQLException e) {
        throw new RuntimeException(
            "Failed to create connections for config: " + config.getDisplayName(), e);
      }

      for (TestSuite suite : config.getApplicableSuites()) {
        if (allowedSuites != null && !allowedSuites.contains(suite.name())) continue;

        SuiteProvider provider = suite.getProvider();
        if (provider == null) {
          continue;
        }

        List<TestCase> testCases = provider.getTestCases();
        String label = config.getDisplayName() + " | " + suite.name();

        for (TestCase tc : testCases) {
          allTests.add(Arguments.of(label, leftConn, rightConn, suite, tc));
        }
      }
    }
    return allTests.stream();
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("provideAllTests")
  @DisplayName("JDBC Driver Comparison")
  void compareDriverResults(
      String comparisonName,
      Connection conn1,
      Connection conn2,
      TestSuite suite,
      TestCase testCase) {
    assertDoesNotThrow(
        () -> {
          System.out.printf(
              "[%s] [%s] Running: %s%n", Instant.now(), comparisonName, testCase.getDescription());

          String label = suite.name() + " [" + comparisonName + "]";
          // comparisonName is "Config | Suite", extract config part
          String configName =
              comparisonName.contains(" | ")
                  ? comparisonName.substring(0, comparisonName.indexOf(" | "))
                  : comparisonName;
          try {
            ComparisonResult result = suite.getProvider().execute(conn1, conn2, testCase, label);
            reporter.addResult(result);

            // CSV: per-combo rows for metadata, single row for other suites
            SuiteProvider provider = suite.getProvider();
            if (provider instanceof DatabaseMetaDataProvider) {
              String methodName = testCase.getIdentifier();
              for (CombinationResult cr :
                  ((DatabaseMetaDataProvider) provider).getLastComboResults()) {
                String comboTestCase = methodName + "(" + cr.argsLabel + ")";
                if (cr.skipped) {
                  reporter.addCsvRow(
                      suite.name(),
                      configName,
                      comboTestCase,
                      "SKIPPED",
                      cr.skipReason != null ? cr.skipReason : "");
                } else if (cr.metadataDiffs.isEmpty() && cr.dataDiffs.isEmpty()) {
                  reporter.addCsvRow(suite.name(), configName, comboTestCase, "PASS", "");
                } else {
                  ComparisonResult temp = new ComparisonResult("", "", new Object[0]);
                  temp.metadataDifferences = cr.metadataDiffs;
                  temp.dataDifferences = cr.dataDiffs;
                  reporter.addCsvRow(
                      suite.name(), configName, comboTestCase, "DIFF", temp.csvSummary());
                }
              }
            } else {
              reporter.addCsvFromResult(
                  suite.name(), configName, testCase.getDescription(), result);
            }

            if (result.hasDifferences()) {
              System.err.println(
                  "[" + comparisonName + "] Differences found for: " + testCase.getDescription());
              System.err.println(result);
            }
          } catch (Throwable e) {
            System.err.printf(
                "[%s] [%s] ERROR in %s: %s%n",
                Instant.now(), comparisonName, testCase.getDescription(), e.getMessage());
            reporter.addCsvRow(
                suite.name(), configName, testCase.getDescription(), "ERROR", e.getMessage());
            throw e;
          }
        });
  }
}
