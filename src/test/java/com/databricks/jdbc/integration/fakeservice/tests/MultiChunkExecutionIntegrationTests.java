package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.client.impl.sdk.PathConstants.RESULT_CHUNK_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksResultSetMetaData;
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

    try (ResultSet rs = statement.executeQuery(sql)) {
      DatabricksResultSetMetaData metaData = (DatabricksResultSetMetaData) rs.getMetaData();

      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }

      // The result should have the same number of rows as the limit
      assertEquals(maxRows, rowCount);
      assertEquals(maxRows, metaData.getTotalRows());

      // The result should be split into multiple chunks
      assertTrue(metaData.getChunkCount() > 1, "Chunk count should be greater than 1");

      // The number of cloud fetch calls should be equal to the number of chunks
      final int cloudFetchCalls =
          cloudFetchApiExtension
              .countRequestsMatching(getRequestedFor(urlPathMatching(".*")).build())
              .getCount();
      assertEquals(metaData.getChunkCount(), cloudFetchCalls);

      // Number of requests to fetch external links should be one less than the total number of
      // chunks as first chunk link is already fetched
      final String statementId = ((DatabricksResultSet) rs).statementId();
      final String resultChunkPathRegex = String.format(RESULT_CHUNK_PATH, statementId, ".*");
      sqlExecApiExtension.verify(
          (int) (metaData.getChunkCount() - 1),
          getRequestedFor(urlPathMatching(resultChunkPathRegex)));
    }
  }

  /** Returns the extensions to be used for stubbing. */
  private static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }
}
