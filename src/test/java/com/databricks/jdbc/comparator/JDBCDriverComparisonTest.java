package com.databricks.jdbc.comparator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.databricks.jdbc.comparator.config.ConnectionConfig;
import com.databricks.jdbc.comparator.config.ConnectionManager;
import com.databricks.jdbc.comparator.config.TestSuite;
import com.databricks.jdbc.comparator.setup.WorkspaceSetup;
import com.databricks.jdbc.comparator.suite.SuiteProvider;
import com.databricks.jdbc.comparator.suite.TestCase;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

  private static final String BASE_JDBC_URL =
      "jdbc:databricks://"
          + System.getProperty("COMPARATOR_HOST", DEFAULT_HOST)
          + ":443/default;ssl=1;authMech=3;httpPath=/sql/1.0/warehouses/"
          + System.getProperty("COMPARATOR_WAREHOUSE", DEFAULT_WAREHOUSE);
  private static final String BASE_THRIFT_URL = BASE_JDBC_URL + ";useThriftClient=1";
  private static final String BASE_SEA_URL = BASE_JDBC_URL + ";useThriftClient=0";

  private static ConnectionManager connectionManager;
  private static TestReporter reporter;

  @BeforeAll
  static void setup() throws Exception {
    String token = System.getenv("DATABRICKS_COMPARATOR_TOKEN");
    connectionManager = new ConnectionManager(token);

    // Workspace validation/setup — only runs when -DWORKSPACE_SETUP=validate|create
    WorkspaceSetup.run(connectionManager.getConnection(BASE_THRIFT_URL));

    String timestamp = Instant.now().toString().replaceAll("[:.]+", "-");
    List<String> connectionUrls =
        List.of("Base Thrift URL: " + BASE_THRIFT_URL, "Base SEA URL: " + BASE_SEA_URL);
    reporter =
        new TestReporter(Path.of("jdbc-comparison-report-" + timestamp + ".txt"), connectionUrls);
  }

  @AfterAll
  static void teardown() throws Exception {
    if (connectionManager != null) connectionManager.close();
    if (reporter != null) reporter.finish();
  }

  /**
   * Generates all test arguments by iterating: config → suite → test case.
   *
   * <p>Configs with no applicable suites or suites with no provider/test cases are skipped.
   */
  static Stream<Arguments> provideAllTests() {
    List<Arguments> allTests = new ArrayList<>();

    for (ConnectionConfig config : ConnectionConfig.activeConfigs()) {
      Connection thriftConn;
      Connection seaConn;
      try {
        thriftConn = connectionManager.getConnection(config.buildUrl(BASE_THRIFT_URL));
        seaConn = connectionManager.getConnection(config.buildUrl(BASE_SEA_URL));
      } catch (SQLException e) {
        throw new RuntimeException(
            "Failed to create connections for config: " + config.getDisplayName(), e);
      }

      for (TestSuite suite : config.getApplicableSuites()) {
        SuiteProvider provider = suite.getProvider();
        if (provider == null) {
          continue;
        }

        List<TestCase> testCases = provider.getTestCases();
        String label = config.getDisplayName() + " | " + suite.name();

        for (TestCase tc : testCases) {
          allTests.add(Arguments.of(label, thriftConn, seaConn, suite, tc));
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
          try {
            ComparisonResult result = suite.getProvider().execute(conn1, conn2, testCase, label);
            reporter.addResult(result);

            if (result.hasDifferences()) {
              System.err.println(
                  "[" + comparisonName + "] Differences found for: " + testCase.getDescription());
              System.err.println(result);
            }
          } catch (Exception e) {
            System.err.printf(
                "[%s] [%s] ERROR in %s: %s%n",
                Instant.now(), comparisonName, testCase.getDescription(), e.getMessage());
            throw e;
          }
        });
  }
}
