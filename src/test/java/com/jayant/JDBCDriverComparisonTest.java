package com.jayant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.jayant.testparams.ConnectionTestParams;
import com.jayant.testparams.DatabaseMetaDataTestParams;
import com.jayant.testparams.ResultSetMetaDataTestParams;
import com.jayant.testparams.ResultSetTestParams;
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
  private static final String OLD_DRIVER_JDBC_URL =
      "jdbc:databricks://benchmarking-prod-aws-us-west-2.cloud.databricks.com:443/default;ssl=1;authMech=3;httpPath=/sql/1.0/warehouses/7e635336d748166a;UID=token;";
  private static final String OSS_DRIVER_JDBC_URL =
      "jdbc:databricks://benchmarking-prod-aws-us-west-2.cloud.databricks.com:443/default;ssl=1;authMech=3;httpPath=/sql/1.0/warehouses/7e635336d748166a";
  private static Connection oldDriverConnection;
  private static Connection ossDriverConnection;
  private static Path tempDir;
  private static TestReporter reporter;
  private static ResultSet oldDriverResultSet;
  private static ResultSet ossDriverResultSet;

  @BeforeAll
  static void setup() throws Exception {
    // Create temporary directory for extracted JARs
    tempDir = Files.createTempDirectory("jdbc-drivers");

    // Extract and load drivers
    URL oldDriverJarUrl = extractJarToTemp("databricks-jdbc-2.6.38.jar", tempDir);

    if (oldDriverJarUrl == null) {
      throw new RuntimeException("Unable to find JDBC driver JARs in the classpath");
    }

    // Initialize class loaders and drivers
    URLClassLoader oldDriverClassLoader =
        new CustomClassLoader(
            new URL[] {oldDriverJarUrl}, JDBCDriverComparisonTest.class.getClassLoader());

    Class<?> oldDriverClass =
        Class.forName("com.databricks.client.jdbc.Driver", true, oldDriverClassLoader);

    Driver oldDriver = (Driver) oldDriverClass.getDeclaredConstructor().newInstance();

    // Initialize connections
    String pwd = System.getenv("DATABRICKS_COMPARATOR_TOKEN");
    Properties props = new Properties();
    ossDriverConnection = DriverManager.getConnection(OSS_DRIVER_JDBC_URL, "token", pwd);
    oldDriverConnection = oldDriver.connect(OLD_DRIVER_JDBC_URL + "PWD=" + pwd, props);
    reporter = new TestReporter(Path.of("jdbc-comparison-report.txt"));

    String queryResultSetTypesTable =
        "select * from main.oss_jdbc_tests.test_result_set_types limit 100";
    oldDriverResultSet =
        oldDriverConnection.createStatement().executeQuery(queryResultSetTypesTable);
    oldDriverResultSet.next();
    ossDriverResultSet =
        ossDriverConnection.createStatement().executeQuery(queryResultSetTypesTable);
    ossDriverResultSet.next();
  }

  @AfterAll
  static void teardown() throws Exception {
    if (oldDriverConnection != null) oldDriverConnection.close();
    if (ossDriverConnection != null) ossDriverConnection.close();
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
          ResultSet oldDriverRs = oldDriverConnection.createStatement().executeQuery(query);
          ResultSet ossDriverRs = ossDriverConnection.createStatement().executeQuery(query);

          ComparisonResult result =
              ResultSetComparator.compare("sql", query, new String[] {}, oldDriverRs, ossDriverRs);
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
          DatabaseMetaData oldDriverMetadata = oldDriverConnection.getMetaData();
          DatabaseMetaData ossDriverMetadata = ossDriverConnection.getMetaData();

          Object oldDriverRs = ReflectionUtils.executeMethod(oldDriverMetadata, methodName, args);
          Object ossDriverRs = ReflectionUtils.executeMethod(ossDriverMetadata, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "DatabaseMetaData", methodName, args, oldDriverRs, ossDriverRs);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println("Differences found in metadata results for method: " + methodName);
            System.err.println("Args: " + getStringForArgs(args));
            System.err.println(result);
          }
        });
  }

  @ParameterizedTest
  @MethodSource("provideResultSetMethods")
  @DisplayName("Compare ResultSet API Results")
  void compareResultSetResults(String methodName, Object[] args) {
    assertDoesNotThrow(
        () -> {
          Object oldDriverResult =
              ReflectionUtils.executeMethod(oldDriverResultSet, methodName, args);
          Object ossDriverResult =
              ReflectionUtils.executeMethod(ossDriverResultSet, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "ResultSet", methodName, args, oldDriverResult, ossDriverResult);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println("Differences found in ResultSet results for method: " + methodName);
            System.err.println("Args: " + getStringForArgs(args));
            System.err.println(result);
          }
        });
  }

  @ParameterizedTest
  @MethodSource("provideResultSetMetaDataMethods")
  @DisplayName("Compare ResultSetMetaData API Results")
  void compareResultSetMetaDataResults(String methodName, Object[] args) {
    assertDoesNotThrow(
        () -> {
          ResultSetMetaData oldDriverRsMd = oldDriverResultSet.getMetaData();
          ResultSetMetaData ossDriverRsMd = ossDriverResultSet.getMetaData();
          Object oldDriverResult = ReflectionUtils.executeMethod(oldDriverRsMd, methodName, args);
          Object ossDriverResult = ReflectionUtils.executeMethod(ossDriverRsMd, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "ResultSetMetaData", methodName, args, oldDriverResult, ossDriverResult);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println(
                "Differences found in ResultSetMetaData results for method: " + methodName);
            System.err.println("Args: " + getStringForArgs(args));
            System.err.println(result);
          }
        });
  }

  @ParameterizedTest
  @MethodSource("provideConnectionMethods")
  @DisplayName("Compare Connection API Results")
  void compareConnectionResults(String methodName, Object[] args) {
    assertDoesNotThrow(
        () -> {
          Object oldDriverResult =
              ReflectionUtils.executeMethod(oldDriverConnection, methodName, args);
          Object ossDriverResult =
              ReflectionUtils.executeMethod(ossDriverConnection, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "Connection", methodName, args, oldDriverResult, ossDriverResult);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println("Differences found in Connection results for method: " + methodName);
            System.err.println("Args: " + getStringForArgs(args));
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

  private static Stream<Arguments> provideResultSetMethods() {
    ResultSetTestParams params = new ResultSetTestParams();
    return ReflectionUtils.provideMethodsForClass(ResultSet.class, params);
  }

  private static Stream<Arguments> provideResultSetMetaDataMethods() {
    ResultSetMetaDataTestParams params = new ResultSetMetaDataTestParams();
    return ReflectionUtils.provideMethodsForClass(ResultSetMetaData.class, params);
  }

  private static Stream<Arguments> provideConnectionMethods() {
    ConnectionTestParams params = new ConnectionTestParams();
    return ReflectionUtils.provideMethodsForClass(Connection.class, params);
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

  private static String getStringForArgs(Object[] args) {
    return Arrays.stream(args)
        .map(o -> o == null ? String.valueOf(o) : o.toString())
        .collect(Collectors.joining(", "));
  }
}
