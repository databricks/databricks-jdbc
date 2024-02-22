package com.databricks.jdbc.integration.connection;

import com.databricks.jdbc.integration.IntegrationTestUtil;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class ConnectionIntegrationTests {

  @Test
  void testSuccessfulConnection() throws SQLException {
    Connection conn = IntegrationTestUtil.getValidJDBCConnection();
    assert ((conn != null) && !conn.isClosed());

    if (conn != null) conn.close();
  }
}
