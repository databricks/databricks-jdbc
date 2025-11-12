package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class SqlExecApiIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testJsonInlineChunkedResults_withoutArrow() throws SQLException {
    final String table = "samples.tpch.lineitem";
    // Limit set such that atleast 2 chunks are present for results
    final int maxRows = 64000;
    final String sql = "SELECT * FROM " + table + " limit " + maxRows;

    Properties properties = new Properties();
    properties.setProperty("EnableArrow", "0");
    Connection connection = getValidJDBCConnection(properties);

    final Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    int rowCount = 0;
    while (rs.next()) {
      rowCount++;
    }
    assertEquals(maxRows, rowCount);
  }
}
