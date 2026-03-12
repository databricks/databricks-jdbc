package com.databricks.client.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class LoggingTest {
  private static final Logger logger = Logger.getLogger(LoggingTest.class.getName());

  private static String buildJdbcUrl() {
    String host = System.getenv("DATABRICKS_HOST");
    // Get HTTP path and fix it if corrupted by Windows environment
    String httpPath = System.getenv("DATABRICKS_HTTP_PATH");

    /*
     * The GitHub Windows runner has an issue where environment variables containing paths
     * can become corrupted, particularly when using Git Bash. Specifically, the HTTP path
     * environment variable gets prepended with "C:/Program Files/Git" on Windows runners.
     *
     * This causes problems particularly with usethriftclient=1, which fails with an error:
     * "Illegal character in path at index 66: https://***:443/C:/Program Files/Git***"
     *
     * We fix this by:
     * 1. Detecting if the httpPath starts with the corrupted Windows Git path
     * 2. Extracting the actual path portion if corruption is detected
     * 3. Using a fallback path if we can't extract a valid one
     *
     * This approach works for both usethriftclient=0 and usethriftclient=1 settings,
     * providing a uniform solution across all platforms and client configurations.
     */
    // Check if the httpPath appears to be corrupted with Windows paths
    if (httpPath != null && httpPath.startsWith("C:/Program Files/Git")) {
      // The path is corrupted, extract just the actual path which should be after the Git path
      int slashAfterGit = httpPath.indexOf('/', "C:/Program Files/Git".length());
      if (slashAfterGit != -1) {
        // Extract the actual path after the Git prefix
        httpPath = httpPath.substring(slashAfterGit);
        logger.info("Fixed corrupted HTTP path: " + httpPath);
      }
    }
    String useThriftClient = System.getenv("USE_THRIFT_CLIENT");

    if (useThriftClient == null || useThriftClient.isEmpty()) {
      useThriftClient = "1"; // Default to thrift client if not specified
    }

    // Create log directory with proper path handling
    String homeDir = System.getProperty("user.home");
    File logDir = new File(homeDir, "logstest");
    if (!logDir.exists()) {
      logDir.mkdirs();
      logger.info("Created log directory: " + logDir.getAbsolutePath());
    }

    // Get the canonical path and always use forward slashes
    String logPath;
    try {
      logPath = logDir.getCanonicalPath();
      // Always use forward slashes in JDBC URL parameters regardless of platform
      logPath = logPath.replace('\\', '/');
      logger.info("Using log path: " + logPath);
    } catch (Exception e) {
      // Fallback to simple string-based path if canonical fails
      logPath = homeDir.replace('\\', '/') + "/logstest";
      logger.info("Using fallback log path: " + logPath);
    }

    logger.info("Using usethriftclient=" + useThriftClient);

    // Build the JDBC URL with the logPath and usethriftclient parameter
    String jdbcUrl =
        "jdbc:databricks://"
            + host
            + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + httpPath
            + ";logPath="
            + logPath
            + ";loglevel=DEBUG"
            + ";usethriftclient="
            + useThriftClient;

    logger.info("Connecting with URL: " + jdbcUrl);

    return jdbcUrl;
  }

  public static void main(String[] args) {
    try {
      String jdbcUrl = buildJdbcUrl();
      String patToken = System.getenv("DATABRICKS_TOKEN");

      logger.info("Attempting to connect to database...");
      Connection connection = DriverManager.getConnection(jdbcUrl, "token", patToken);
      logger.info("Connected to the database successfully.");

      Statement statement = connection.createStatement();
      statement.execute("SELECT 1");
      logger.info("Executed a sample query.");

      // Close the connection
      connection.close();
      logger.info("Connection closed.");
    } catch (SQLException e) {
      logger.severe("Connection or query failed: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
