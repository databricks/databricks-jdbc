package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.integration.fakeservice.DatabricksWireMockExtension;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.databricks.jdbc.integration.fakeservice.StubMappingCredentialsCleaner;
import com.github.tomakehurst.wiremock.extension.Extension;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Integration tests for connection to Databricks service. */
public class ConnectionIntegrationTests {

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#SQL_EXEC}.
   * Intercepts all requests to SQL Execution API.
   */
  @RegisterExtension
  private static final FakeServiceExtension sqlExecApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          DatabricksJdbcConstants.FakeServiceType.SQL_EXEC,
          "https://" + System.getenv("DATABRICKS_HOST"));

  @Test
  void testSuccessfulConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();
    assert ((conn != null) && !conn.isClosed());

    conn.close();
  }

  @Test
  void testIncorrectCredentialsForPAT() {
    String url = getJDBCUrl();
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, getDatabricksUser(), "bad_token"));

    assert (e.getMessage().contains("Invalid or unknown token or hostname provided"));
  }

  @Test
  void testIncorrectCredentialsForOAuth() {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=%s";
    String url = String.format(template, getDatabricksHost(), getDatabricksHTTPPath());
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, getDatabricksUser(), "bad_token"));

    assert (e.getMessage().contains("Invalid or unknown token or hostname provided"));
  }

  /** Returns the extensions to be used for stubbing. */
  private static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }
}
