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

public class SqlExecApiDirectResultsIntegrationTests extends AbstractFakeServiceIntegrationTests {

  /** JDBC URL where direct results are enabled. */
  private static final String jdbcUrlTemplate =
      "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=3;httpPath=%s;EnableSQLExecDirectResults=1;useThriftClient=0";

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
  void testDirectResultsSmallQuery() throws SQLException {
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
      // For direct results mode, cloud fetch should not be used for small queries
      assertFalse(metaData.getIsCloudFetchUsed());

      // For direct results mode, no cloud fetch calls should be made for small queries
      final int cloudFetchCalls =
          getCloudFetchApiExtension()
              .countRequestsMatching(getRequestedFor(urlPathMatching(".*")).build())
              .getCount();
      assertEquals(0, cloudFetchCalls);

      if (isSqlExecSdkClient()) {
        // For direct results small query, no result chunks should be fetched
        final String statementId = ((DatabricksResultSet) rs).getStatementId();
        final String resultChunkPathRegex = String.format(RESULT_CHUNK_PATH, statementId, ".*");
        getDatabricksApiExtension()
            .verify(0, getRequestedFor(urlPathMatching(resultChunkPathRegex)));
      }
    }
  }

  @Test
  void testDirectResultsLargeQuery() throws SQLException {
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

      // In direct results mode, the behavior for large queries may vary:
      // - If results are provided directly in response, cloud fetch should not be used
      // - If results are too large, they might still use cloud fetch or external links
      // We'll verify the actual behavior rather than making assumptions

      if (isSqlExecSdkClient()) {
        final String statementId = ((DatabricksResultSet) rs).getStatementId();
        final String resultChunkPathRegex = String.format(RESULT_CHUNK_PATH, statementId, ".*");

        if (metaData.getIsCloudFetchUsed()) {
          // If cloud fetch is used, verify cloud fetch calls were made
          final int cloudFetchCalls =
              getCloudFetchApiExtension()
                  .countRequestsMatching(getRequestedFor(urlPathMatching(".*")).build())
                  .getCount();
          assertTrue(cloudFetchCalls > 0, "Expected cloud fetch calls when cloud fetch is used");

          // For large queries using cloud fetch, no result chunks should be fetched via API
          getDatabricksApiExtension()
              .verify(0, getRequestedFor(urlPathMatching(resultChunkPathRegex)));
        } else {
          // If cloud fetch is not used in direct results mode, no cloud fetch calls should be made
          final int cloudFetchCalls =
              getCloudFetchApiExtension()
                  .countRequestsMatching(getRequestedFor(urlPathMatching(".*")).build())
                  .getCount();
          assertEquals(
              0, cloudFetchCalls, "No cloud fetch calls expected when not using cloud fetch");
        }
      }
    }
  }

  @Test
  void testDirectResultsWithTimeout() throws SQLException {
    // Test that direct results mode works correctly with query timeout
    final String sql = "SELECT * FROM main.tpcds_sf100_delta.catalog_sales limit 100";

    final Statement statement = connection.createStatement();
    statement.setQueryTimeout(30); // 30 seconds timeout
    statement.setMaxRows(100);

    try (ResultSet rs = statement.executeQuery(sql)) {
      assertNotNull(rs);
      assertTrue(rs.next(), "Should have at least one row");

      DatabricksResultSetMetaData metaData = (DatabricksResultSetMetaData) rs.getMetaData();
      assertNotNull(metaData);

      // In direct results mode with timeout, verify the query completes successfully
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertTrue(rowCount > 0, "Should have retrieved some rows");
    }
  }

  @Test
  void testDirectResultsParameterValidation() throws SQLException {
    // Verify that the direct results parameter is properly recognized
    DatabaseMetaData dbMetaData = connection.getMetaData();
    String url = dbMetaData.getURL();
    assertTrue(
        url.contains("EnableSQLExecDirectResults=1"),
        "JDBC URL should contain direct results parameter");
  }

  @Test
  void testDirectResultsSimpleQuery() throws SQLException {
    // Test a very simple query to ensure direct results mode works for basic cases
    final String sql = "SELECT 1 as test_column";

    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {

      assertTrue(rs.next(), "Should have exactly one row");
      assertEquals(1, rs.getInt("test_column"));
      assertFalse(rs.next(), "Should have only one row");

      DatabricksResultSetMetaData metaData = (DatabricksResultSetMetaData) rs.getMetaData();
      assertEquals(1, metaData.getTotalRows());

      // For a simple query like this, cloud fetch should definitely not be used
      assertFalse(metaData.getIsCloudFetchUsed());
    }
  }
}
