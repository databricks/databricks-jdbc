package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.Test;

public class OAuthTests {
  @Test
  void testM2M() throws SQLException {
    Connection connection = DriverManager.getConnection(getDatabricksUrlForM2M());
    assertDoesNotThrow(() -> connection.createStatement().execute("select 1"));
  }
}
