package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.PASSWORD;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.USER;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class OAuthTests {
  @Test
  void testM2M() throws SQLException {
    Connection connection = getValidM2MConnection();
    assertDoesNotThrow(() -> connection.createStatement().execute("select 1"));
  }

  @Test
  void testPAT() throws SQLException {
    Properties connectionProperties = new Properties();
    connectionProperties.put(USER, getDatabricksUser());
    connectionProperties.put(PASSWORD, getDatabricksToken());
    Connection connection = DriverManager.getConnection(getJDBCUrl(), connectionProperties);
    assertDoesNotThrow(() -> connection.createStatement().execute("select 1"));
  }

  @Test
  void testSPTokenFederation() throws SQLException {
    Connection connection = getValidSPTokenFedConnection();
    System.out.println("TOKEN CREDS");
    System.out.println(System.getenv("DATABRICKS_JDBC_SP_TOKEN_FED_HOST"));
    System.out.println(System.getenv("DATABRICKS_JDBC_SP_TOKEN_FED_HTTP_PATH"));
    System.out.println(System.getenv("DATABRICKS_JDBC_SP_TOKEN_FED_CLIENT_ID"));
    System.out.println(System.getenv("DATABRICKS_JDBC_SP_TOKEN_FED_CLIENT_SECRET"));
    System.out.println(System.getenv("DATABRICKS_SP_TOKEN_FED_FEDERATION_ID"));
    System.out.println(System.getenv("DATABRICKS_SP_TOKEN_FED_AZURE_TENANT_ID"));
    assertDoesNotThrow(() -> connection.createStatement().execute("select 1"));
  }
}
