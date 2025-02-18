package com.jayant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
  void compareMetadataResults(String methodName, String[] args) {
    assertDoesNotThrow(
        () -> {
          DatabaseMetaData simbaMetadata = simbaConnection.getMetaData();
          DatabaseMetaData ossMetadata = ossConnection.getMetaData();

          Object simbaRs = executeMetadataMethod(simbaMetadata, methodName, args);
          Object ossRs = executeMetadataMethod(ossMetadata, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare("metadata", methodName, args, simbaRs, ossRs);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println("Differences found in metadata results for method: " + methodName);
            System.err.println("Args: " + String.join(", ", args));
            System.err.println(result);
          }
        });
  }

  private static Stream<Arguments> provideSQLQueries() {
    return Stream.of(
        Arguments.of("SELECT * FROM main.tpcds_sf100_delta.catalog_sales limit 5", "TPC-DS query"));
  }

  private static Stream<Arguments> provideMetadataMethods() {
    List<Arguments> argumentsStream = new ArrayList<>(List.of(
        Arguments.of(
            "getTables", new String[] {"main", "tpcds_sf100_delta", "%", null}),
        Arguments.of("getTableTypes", new String[] {}),
        Arguments.of("getCatalogs", new String[] {}),
        Arguments.of("getSchemas", new String[] {"main", "tpcds_%"}),
        Arguments.of(
            "getColumns",
            new String[] {"main", "tpcds_sf100_delta", "catalog_sales", "%"}),
        Arguments.of("getTypeInfo", new String[] {}),
        Arguments.of(
            "getFunctions",
            new String[] {"main", "tpcds_sf100_delta", "aggregate"}),
        Arguments.of(
            "getProcedures", new String[] {"main", "tpcds_sf100_delta", "%"})));

    // Use reflection to get methods with 0 arguments
    // TODO: can enhance this to support multiple args
    try {
      Method[] methods = DatabaseMetaData.class.getMethods();
      for (Method method : methods) {
        if (method.getParameterCount() == 0) {
          // Add the method to the stream if it has 0 arguments
          String methodName = method.getName();
          if (methodName.equals("getSchemas")) {
            // getSchemas with empty args is generating a lot of diff, need to investigate first before including in the email report
            continue;
          }
          Arguments arguments = Arguments.of(methodName, new String[] {});
          if (!argumentsStream.contains(arguments)) {
            argumentsStream.add(arguments);
          }
        }
      }
    } catch (SecurityException e) {
      e.printStackTrace();
    }

    return argumentsStream.stream();
  }

  private Object executeMetadataMethod(
      DatabaseMetaData metadata, String methodName, String[] args) throws SQLException {
    switch (methodName) {
      case "getTables":
        return metadata.getTables(
            args[0], args[1], args[2], args[3] == null ? null : args[3].split(","));
      default:
        return executeGenericMetadataMethod(metadata, methodName, args);
    }
  }

  private Object executeGenericMetadataMethod(DatabaseMetaData metadata, String methodName, String[] args) throws SQLException {
    try {
      // Get the method by its name and parameter types
      Method method;
      if (args == null || args.length == 0) {
        method = metadata.getClass().getMethod(methodName);
      } else {
        // Create an array of parameter types to match the method signature
        Class<?>[] paramTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
          paramTypes[i] = String.class;  // Assuming all arguments are Strings
        }

        method = metadata.getClass().getMethod(methodName, paramTypes);
      }

      // Invoke the method dynamically with the arguments
      Object result;
      try {
        result = method.invoke(metadata, (Object[]) args);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof UnsupportedOperationException) {
          result = "UnsupportedOperationException";
        } else {
          throw e;
        }
      }
      return result;
    } catch (NoSuchMethodException e) {
      throw new SQLException("No such method: " + methodName, e);
    } catch (Exception e) {
      throw new SQLException("Error executing method: " + methodName, e);
    }
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
