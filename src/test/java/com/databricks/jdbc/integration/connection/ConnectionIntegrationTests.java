package com.databricks.jdbc.integration.connection;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class ConnectionIntegrationTests {

  @Test
  void testSuccessfulConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();
    assert ((conn != null) && !conn.isClosed());

    if (conn != null) conn.close();
  }

  @Test
  void testIncorrectCredentialsForPAT() throws SQLException {
    String url = getJDBCUrl();
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> {
              DriverManager.getConnection(url, getDatabricksUser(), "bad_token");
            });

    assert (e.getMessage().contains("Invalid or unknown token or hostname provided"));
  }

  @Test
  void testIncorrectCredentialsForOAuth() throws SQLException {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=%s";
    String url = String.format(template, getDatabricksHost(), getDatabricksHTTPPath());
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> {
              DriverManager.getConnection(url, getDatabricksUser(), "bad_token");
            });

    assert (e.getMessage().contains("Invalid or unknown token or hostname provided"));
  }
}
