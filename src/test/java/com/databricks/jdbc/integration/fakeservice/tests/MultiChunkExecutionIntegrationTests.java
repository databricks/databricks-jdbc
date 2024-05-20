package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.integration.fakeservice.DatabricksWireMockExtension;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.databricks.jdbc.integration.fakeservice.StubMappingCredentialsCleaner;
import com.github.tomakehurst.wiremock.extension.Extension;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Test SQL execution with results spanning multiple chunks. */
public class MultiChunkExecutionIntegrationTests {

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
          "https://e2-dogfood.staging.cloud.databricks.com");

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#CLOUD_FETCH}.
   * Intercepts all requests to Cloud Fetch API.
   */
  @RegisterExtension
  private static final FakeServiceExtension cloudFetchApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH,
          "https://e2-dogfood-core.s3.us-west-2.amazonaws.com");

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    // close the connection
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testMultiChunkSelect() throws SQLException {
    final String table = "samples.tpch.lineitem";
    final int maxRows = 122900;
    final String sql = "SELECT * FROM " + table + " limit " + maxRows;

    final Statement statement = connection.createStatement();
    statement.setMaxRows(maxRows);

    final ResultSet rs = statement.executeQuery(sql);
    int cnt = 0;
    while (rs.next()) {
      cnt++;
    }

    assertEquals(maxRows, cnt);
    assertEquals(16, rs.getMetaData().getColumnCount());

    rs.close();
  }

  /** Returns the extensions to be used for stubbing. */
  private static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }
}
