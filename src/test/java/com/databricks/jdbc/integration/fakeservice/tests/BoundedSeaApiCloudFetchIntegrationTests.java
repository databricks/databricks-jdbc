package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.*;

public class BoundedSeaApiCloudFetchIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    // These tests use SEA-only WireMock stubs. Skip when running in Thrift-server fake-service
    // mode (CI sets FAKE_SERVICE_TYPE=THRIFT_SERVER for the Thrift integration test suite).
    assumeTrue(
        isSqlExecSdkClient(),
        "BoundedSeaApiCloudFetchIntegrationTests require SQL_EXEC fake service type");

    System.setProperty(DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP, "true");

    Properties props = new Properties();
    props.setProperty("UseBoundedSeaApi", "1");
    // UseThriftClient=0 (SEA) is injected by getValidJDBCConnection via
    // FakeServiceConfigLoader.shouldUseThriftClient() when FAKE_SERVICE_TYPE=SQL_EXEC.
    connection = getValidJDBCConnection(props);
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testBoundedSeaApiSingleChunkQuery() throws SQLException {
    final String table = "main.tpcds_sf100_delta.catalog_sales";
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
      assertTrue(metaData.getIsCloudFetchUsed());
      assertTrue(rs.isAfterLast());
    }
  }

  @Test
  void testBoundedSeaApiMultiChunkQuery() throws SQLException {
    final String table = "main.tpcds_sf100_delta.catalog_sales";
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

      assertEquals(maxRows, rowCount);
      assertTrue(metaData.getIsCloudFetchUsed());
      assertFalse(metaData.getIsTruncated());

      assertTrue(rs.isAfterLast());
    }
  }

  @Test
  void testBoundedSeaApiEmptyResult() throws SQLException {
    final String sql = "SELECT 1 WHERE 1 = 0";

    final Statement statement = connection.createStatement();

    try (ResultSet rs = statement.executeQuery(sql)) {
      assertFalse(rs.next());

      final int cloudFetchCalls =
          getCloudFetchApiExtension()
              .countRequestsMatching(getRequestedFor(urlPathMatching(".*")).build())
              .getCount();
      assertEquals(0, cloudFetchCalls);
    }
  }
}
