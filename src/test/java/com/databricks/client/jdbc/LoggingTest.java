package com.databricks.client.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.logging.Logger;

public class LoggingTest {
  private static final Logger logger = Logger.getLogger(LoggingTest.class.getName());

  private static String buildJdbcUrl() {
    String host = System.getenv("DATABRICKS_HOST");
    String httpPath = System.getenv("DATABRICKS_HTTP_PATH");
    String jdbcUrl =
        "jdbc:databricks://"
            + host
            + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + httpPath
            + ";logPath=/tmp/logtest;loglevel=DEBUG";
    return jdbcUrl;
  }

  public static void main(String[] args) {
    String jdbcUrl = buildJdbcUrl();
    String patToken = System.getenv("DATABRICKS_TOKEN");

    try {
      Connection connection = DriverManager.getConnection(jdbcUrl, "token", patToken);
      logger.info("Connected to the database.");

      Statement statement = connection.createStatement();
      statement.execute("SELECT 1");
      logger.info("Executed a sample query.");

      // Close the connection
      connection.close();
      logger.info("Connection closed.");
    } catch (Exception e) {
      logger.severe("Error during logging test: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
