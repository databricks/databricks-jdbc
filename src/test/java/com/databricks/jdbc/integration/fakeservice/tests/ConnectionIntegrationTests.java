package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

/** Integration tests for connection to Databricks service. */
public class ConnectionIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testSuccessfulConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();
    assert ((conn != null) && !conn.isClosed());

    conn.close();
  }

  @Test
  void testIncorrectCredentialsForPAT() {
    String url = getFakeServiceJDBCUrl();
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, createConnectionProperties("bad_token_1")));

    assert e.getMessage().contains("Communication link failure. Failed to connect to server.");
  }

  @Test
  void testIncorrectCredentialsForOAuth() {
    // SSL is disabled as embedded web server of fake service uses HTTP protocol.
    // Note that in RECORD mode, the web server interacts with production services over HTTPS.
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=11;AuthFlow=0;httpPath=%s";
    String url =
        String.format(
            template, getFakeServiceHost(), FakeServiceConfigLoader.getProperty(HTTP_PATH));
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, createConnectionProperties("bad_token_2")));

    assert e.getMessage().contains("Communication link failure. Failed to connect to server.");
  }

  private Properties createConnectionProperties(String password) {
    Properties connProps = new Properties();
    connProps.put(USER, getDatabricksUser());
    connProps.put(PASSWORD, password);
    connProps.put(CATALOG, FakeServiceConfigLoader.getProperty(CATALOG));
    connProps.put(CONN_SCHEMA, FakeServiceConfigLoader.getProperty(CONN_SCHEMA));

    return connProps;
  }
}
