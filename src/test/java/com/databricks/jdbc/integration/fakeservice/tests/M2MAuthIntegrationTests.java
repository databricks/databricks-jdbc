package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration tests for M2M OAuth authentication flow. */
public class M2MAuthIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private static final String TEST_CLIENT_ID = System.getenv("DATABRICKS_JDBC_M2M_CLIENT_ID");
  private static final String TEST_CLIENT_SECRET =
      System.getenv("DATABRICKS_JDBC_M2M_CLIENT_SECRET");

  @BeforeAll
  static void setup() {
    setDatabricksApiTargetUrl(getM2MHost());
  }

  @Test
  void testSuccessfulM2MConnection() throws SQLException {
    Connection conn = getValidM2MConnection();
    assert ((conn != null) && !conn.isClosed());
    conn.close();
  }

  @Test
  void testIncorrectCredentialsForM2M() {
    String url = getFakeServiceM2MUrl();
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                DriverManager.getConnection(
                    url, createFakeServiceM2MConnectionProperties("invalid-secret")));

    assert e.getMessage()
        .contains("Connection failure while using the OSS Databricks JDBC driver.");
  }

  private Connection getValidM2MConnection() throws SQLException {
    return DriverManager.getConnection(
        getFakeServiceM2MUrl(), createFakeServiceM2MConnectionProperties(TEST_CLIENT_SECRET));
  }

  private Properties createFakeServiceM2MConnectionProperties(String clientSecret) {
    Properties connProps = new Properties();
    connProps.put("OAuth2ClientId", TEST_CLIENT_ID);
    connProps.put("OAuth2Secret", clientSecret);
    connProps.put(
        DatabricksJdbcUrlParams.CONN_CATALOG.getParamName(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_CATALOG.getParamName()));
    connProps.put(
        DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName()));

    return connProps;
  }
}
