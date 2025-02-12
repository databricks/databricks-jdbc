package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.PASSWORD;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.USER;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.utils.Cloud;
import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class OAuthTests {
  @Test
  void test_M2M_Azure() throws SQLException {
    Connection connection = getValidM2MConnection(Cloud.AZURE);
    assertDoesNotThrow(() -> connection.createStatement().execute("select 1"));
  }

  @Test
  void test_M2M_Aws() throws SQLException {
    Connection connection = getValidM2MConnection(Cloud.AWS);
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
