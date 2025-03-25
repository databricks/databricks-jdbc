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
    String httpPath = System.getenv("DATABRICKS_HTTP_PATH");
    String useThriftClient = System.getenv("USE_THRIFT_CLIENT");

    if (useThriftClient == null || useThriftClient.isEmpty()) {
      useThriftClient = "0"; // Default to non-Thrift client if not specified
    }

    // Create log directory
    String homeDir = System.getProperty("user.home");
    File logDir = new File(homeDir, "logstest");
    if (!logDir.exists()) {
      logDir.mkdirs();
      logger.info("Created log directory: " + logDir.getAbsolutePath());
    }

    // Use the canonical path with forward slashes and URL encoding for all platforms
    String logPath;
    try {
      // Create a very simple path with no spaces or special characters
      // Use only alphanumeric characters to avoid URL encoding issues
      File simpleLogDir = new File(homeDir, "dblogtest");
      if (!simpleLogDir.exists()) {
        simpleLogDir.mkdirs();
        logger.info("Created simple log directory: " + simpleLogDir.getAbsolutePath());
      }

      logPath = simpleLogDir.getCanonicalPath();
      // Always use forward slashes in JDBC URL parameters regardless of platform
      logPath = logPath.replace('\\', '/');
      logger.info("Using simple canonical log path: " + logPath);
    } catch (Exception e) {
      // Fallback to a hardcoded path if all else fails
      if (System.getProperty("os.name").toLowerCase().contains("windows")) {
        logPath = "C:/temp/dblogs";
      } else {
        logPath = "/tmp/dblogs";
      }
      new File(logPath).mkdirs();
      logger.info("Using fallback log path: " + logPath);
    }

    logger.info("Using usethriftclient=" + useThriftClient);

    // Build the JDBC URL with properly formatted path
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
