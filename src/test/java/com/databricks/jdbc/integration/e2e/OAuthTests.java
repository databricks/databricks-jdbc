package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.PASSWORD;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.USER;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class OAuthTests {
  @Test
  void testM2M() throws SQLException {
    Connection connection = DriverManager.getConnection(getDatabricksUrlForM2M());
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
}
