package com.jayant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.jayant.testparams.DatabaseMetaDataTestParams;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class JDBCDriverComparisonTest {
  private static final String SIMBA_JDBC_URL =
      "jdbc:databricks://benchmarking-prod-aws-us-west-2.cloud.databricks.com:443/default;ssl=1;authMech=3;httpPath=/sql/1.0/warehouses/7e635336d748166a;UID=token;";
  private static final String OSS_JDBC_URL =
      "jdbc:databricks://benchmarking-prod-aws-us-west-2.cloud.databricks.com:443/default;ssl=1;authMech=3;httpPath=/sql/1.0/warehouses/7e635336d748166a";
  private static Connection simbaConnection;
  private static Connection ossConnection;
  private static Path tempDir;
  private static TestReporter reporter;

  @BeforeAll
  static void setup() throws Exception {
    // Create temporary directory for extracted JARs
    tempDir = Files.createTempDirectory("jdbc-drivers");

    // Extract and load drivers
    URL simbaJarUrl = extractJarToTemp("databricks-jdbc-2.6.38.jar", tempDir);

    if (simbaJarUrl == null) {
      throw new RuntimeException("Unable to find JDBC driver JARs in the classpath");
    }

    // Initialize class loaders and drivers
    URLClassLoader simbaClassLoader =
        new CustomClassLoader(
            new URL[] {simbaJarUrl}, JDBCDriverComparisonTest.class.getClassLoader());

    Class<?> simbaDriverClass =
        Class.forName("com.databricks.client.jdbc.Driver", true, simbaClassLoader);

    Driver simbaDriver = (Driver) simbaDriverClass.getDeclaredConstructor().newInstance();

    // Initialize connections
    String pwd = System.getenv("DATABRICKS_COMPARATOR_TOKEN");
    Properties props = new Properties();
    ossConnection = DriverManager.getConnection(OSS_JDBC_URL, "token", pwd);
    simbaConnection = simbaDriver.connect(SIMBA_JDBC_URL + "PWD=" + pwd, props);
    reporter = new TestReporter(Path.of("jdbc-comparison-report.txt"));
  }

  @AfterAll
  static void teardown() throws Exception {
    if (simbaConnection != null) simbaConnection.close();
    if (ossConnection != null) ossConnection.close();
    // Clean up temp directory
    if (tempDir != null) {
      Files.walk(tempDir)
          .sorted((p1, p2) -> -p1.compareTo(p2))
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              });
    }
    reporter.generateReport();
  }

  @ParameterizedTest
  @MethodSource("provideSQLQueries")
  @DisplayName("Compare SQL Query Results")
  void compareSQLQueryResults(String query, String description) {
    assertDoesNotThrow(
        () -> {
          ResultSet simbaRs = simbaConnection.createStatement().executeQuery(query);
          ResultSet ossRs = ossConnection.createStatement().executeQuery(query);

          ComparisonResult result =
              ResultSetComparator.compare("sql", query, new String[] {}, simbaRs, ossRs);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println("Differences found in query results for: " + description);
            System.err.println(result);
          }
        });
  }

  @ParameterizedTest
  @MethodSource("provideMetadataMethods")
  @DisplayName("Compare Metadata API Results")
  void compareMetadataResults(String methodName, Object[] args) {
    assertDoesNotThrow(
        () -> {
          DatabaseMetaData simbaMetadata = simbaConnection.getMetaData();
          DatabaseMetaData ossMetadata = ossConnection.getMetaData();

          Object simbaRs = ReflectionUtils.executeMethod(simbaMetadata, methodName, args);
          Object ossRs = ReflectionUtils.executeMethod(ossMetadata, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare("metadata", methodName, args, simbaRs, ossRs);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println("Differences found in metadata results for method: " + methodName);
            System.err.println(
                "Args: "
                    + Arrays.stream(args).map(Object::toString).collect(Collectors.joining(", ")));
            System.err.println(result);
          }
        });
  }

  private static Stream<Arguments> provideSQLQueries() {
    return Stream.of(
        Arguments.of("SELECT * FROM main.tpcds_sf100_delta.catalog_sales limit 5", "TPC-DS query"));
  }

  private static Stream<Arguments> provideMetadataMethods() {
    DatabaseMetaDataTestParams params = new DatabaseMetaDataTestParams();
    return ReflectionUtils.provideMethodsForClass(DatabaseMetaData.class, params);
  }

  private static URL extractJarToTemp(String jarName, Path tempDir) {
    try {
      try (InputStream in = JDBCDriverComparisonTest.class.getResourceAsStream("/" + jarName)) {
        if (in == null) {
          throw new RuntimeException("Could not find " + jarName + " in resources");
        }
        Path targetPath = tempDir.resolve(jarName);
        Files.copy(in, targetPath, StandardCopyOption.REPLACE_EXISTING);
        return targetPath.toUri().toURL();
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
