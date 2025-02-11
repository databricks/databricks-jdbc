package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getFakeServiceM2MPrivateKeyCredentialsUrl;
import static com.databricks.jdbc.integration.IntegrationTestUtil.getM2MPrivateKeyCredentialsHost;

import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class M2MPrivateKeyCredentialsIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private static final String TEST_CLIENT_ID =
      System.getenv("DATABRICKS_JDBC_M2M_PRIVATE_KEY_CLIENT_ID");
  private static final String TEST_TOKEN_ENDPOINT =
      System.getenv("DATABRICKS_JDBC_M2M_PRIVATE_KEY_TOKEN_ENDPOINT");
  private static final String TEST_AUTH_KID =
      System.getenv("DATABRICKS_JDBC_M2M_PRIVATE_KEY_AUTH_KID");
  private static final String TEST_JWT_KEY_FILE = "/tmp/jdbc-testing-enc.pem";
  private static final String TEST_JWT_KEY_PASSPHRASE =
      System.getenv("DATABRICKS_JDBC_M2M_PRIVATE_KEY_JWT_KEY_PASSPHRASE");

  @BeforeAll
  static void setup() {
    System.setProperty(
        "FAKE_SERVICE_TEST_MODE", "REPLAY"); // DO NOT change. Not sending req to production server.
    setDatabricksApiTargetUrl(getM2MPrivateKeyCredentialsHost());
  }

  @Test
  void testSuccessfulM2MPrivateKeyCredentialsConnection() throws SQLException {
    Connection conn = getValidJDBCPrivateKeyCredentialsConnection();
    assert ((conn != null) && !conn.isClosed());
    conn.close();
  }

  private Connection getValidJDBCPrivateKeyCredentialsConnection() throws SQLException {
    return DriverManager.getConnection(
        getFakeServiceM2MPrivateKeyCredentialsUrl(), createFakeServiceM2MConnectionProperties());
  }

  private Properties createFakeServiceM2MConnectionProperties() {
    Properties connProps = new Properties();
    connProps.put("OAuth2ClientId", TEST_CLIENT_ID);
    connProps.put("OAuth2TokenEndpoint", TEST_TOKEN_ENDPOINT);
    connProps.put("Auth_KID", TEST_AUTH_KID);
    connProps.put("Auth_JWT_Key_File", TEST_JWT_KEY_FILE);
    connProps.put("Auth_JWT_Key_Passphrase", TEST_JWT_KEY_PASSPHRASE);
    connProps.put(
        DatabricksJdbcUrlParams.CATALOG.getParamName(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CATALOG.getParamName()));
    connProps.put(
        DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName()));

    return connProps;
  }
}
