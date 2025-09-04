package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.RESULT_CHUNK_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.*;
import org.junit.jupiter.api.*;

public class SqlExecApiHybridResultsIntegrationTests extends AbstractFakeServiceIntegrationTests {

  /** JDBC URL where hybrid results are enabled. */
  private static final String jdbcUrlTemplate =
      "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=3;httpPath=%s;EnableSQLExecHybridResults=1;useThriftClient=0";

  private static final String e2BenchfoodHttpPath = "/sql/1.0/warehouses/7e635336d748166a";
  private static final String e2BenchfoodHost =
      "https://benchmarking-prod-aws-us-west-2.cloud.databricks.com:443";
  private static final String e2BenchfoodRootBucketHost =
      "https://root-benchmarking-prod-aws-us-west-2.s3.us-west-2.amazonaws.com";
  private Connection connection;

  @BeforeAll
  static void beforeAll() {
    setDatabricksApiTargetUrl(e2BenchfoodHost);
    setCloudFetchApiTargetUrl(e2BenchfoodRootBucketHost);
  }

  @BeforeEach
  void setUp() throws SQLException {
    String jdbcUrl = String.format(jdbcUrlTemplate, getFakeServiceHost(), e2BenchfoodHttpPath);
    connection = DriverManager.getConnection(jdbcUrl, getDatabricksUser(), getDatabricksToken());
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testHybridSmallQuery() throws SQLException {
    final String table = "main.tpcds_sf100_delta.catalog_sales";
    // Small query (< 5 MB)
    final int maxRows = 10;
    final String sql = "SELECT * FROM " + table + " limit " + maxRows;

    final Statement statement = connection.createStatement();
    statement.setMaxRows(maxRows);

    try (ResultSet rs = statement.executeQuery(sql)) {
      DatabricksResultSetMetaData metaData = (DatabricksResultSetMetaData) rs.getMetaData();

      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }

      assertEquals(maxRows, rowCount);
      assertEquals(maxRows, metaData.getTotalRows());
      // For small query, arrow results are received inline in hybrid mode
      assertFalse(metaData.getIsCloudFetchUsed());

      // For small query, arrow results are received inline in hybrid mode so no cloud fetch calls
      // are made
      final int cloudFetchCalls =
          getCloudFetchApiExtension()
              .countRequestsMatching(getRequestedFor(urlPathMatching(".*")).build())
              .getCount();
      assertEquals(0, cloudFetchCalls);

      if (isSqlExecSdkClient()) {
        // For small query, no result chunks are fetched
        final String statementId = ((DatabricksResultSet) rs).getStatementId();
        final String resultChunkPathRegex = String.format(RESULT_CHUNK_PATH, statementId, ".*");
        getDatabricksApiExtension()
            .verify(0, getRequestedFor(urlPathMatching(resultChunkPathRegex)));
      }
    }
  }

  @Test
  void testHybridLargeQuery() throws SQLException {
    final String table = "main.tpcds_sf100_delta.catalog_sales";
    // Large query (> 5 MB)
    final int maxRows = 61000;
    final String sql = "SELECT * FROM " + table + " limit " + maxRows;

    final Statement statement = connection.createStatement();
    statement.setMaxRows(maxRows);

    try (ResultSet rs = statement.executeQuery(sql)) {
      DatabricksResultSetMetaData metaData = (DatabricksResultSetMetaData) rs.getMetaData();

      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }

      assertEquals(maxRows, rowCount);
      assertEquals(maxRows, metaData.getTotalRows());
      // For large query, arrow results are fetched using cloud fetch
      assertTrue(metaData.getIsCloudFetchUsed());

      // The number of cloud fetch calls should be equal to the number of chunks
      final int cloudFetchCalls =
          getCloudFetchApiExtension()
              .countRequestsMatching(getRequestedFor(urlPathMatching(".*")).build())
              .getCount();
      // cloud fetch calls can be retried
      assertTrue(cloudFetchCalls >= metaData.getChunkCount());

      if (isSqlExecSdkClient()) {
        final String statementId = ((DatabricksResultSet) rs).getStatementId();
        final String resultChunkPathRegex = String.format(RESULT_CHUNK_PATH, statementId, ".*");
        // No need to fetch links as a set of links are available in the manifest
        getDatabricksApiExtension()
            .verify(0, getRequestedFor(urlPathMatching(resultChunkPathRegex)));
      }
    }
  }
}
