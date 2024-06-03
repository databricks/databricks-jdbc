package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    String url = getJDBCUrl();
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, getDatabricksUser(), "bad_token"));

    assert e.getMessage().contains("Invalid or unknown token or hostname provided");
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

    assert e.getMessage().contains("Invalid or unknown token or hostname provided");
  }
}
