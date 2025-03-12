package com.databricks.client.jdbc;

import java.nio.file.Paths;
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
    // Use the user's directory for logging
    String logDir = Paths.get(System.getProperty("user.home"), "logstest").toString();
    logger.info("Logging to: " + logDir);
    // Build the JDBC URL with the new logPath
    String jdbcUrl =
        "jdbc:databricks://"
            + host
            + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + httpPath
            + ";logPath="
            + logDir
            + ";loglevel=DEBUG"
            + ";usethriftclient=0";
    return jdbcUrl;
  }

  public static void main(String[] args) throws SQLException {
    String jdbcUrl = buildJdbcUrl();
    String patToken = System.getenv("DATABRICKS_TOKEN");

    Connection connection = DriverManager.getConnection(jdbcUrl, "token", patToken);
    logger.info("Connected to the database.");

    Statement statement = connection.createStatement();
    statement.execute("SELECT 1");
    logger.info("Executed a sample query.");

    // Close the connection
    connection.close();
    logger.info("Connection closed.");
  }
}
