package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.RESULT_CHUNK_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Test SQL execution with results spanning multiple chunks. */
public class MultiChunkExecutionIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testMultiChunkSelect() throws SQLException, InterruptedException {
    // Ensure fake service property is set to skip link expiry checks in tests
    System.setProperty(DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP, "true");

    final String table = "samples.tpch.lineitem";

    // To save on the size of stub mappings, the test uses just enough rows to span multiple chunks.
    // That minimum threshold is different for SQL Exec and SQL Gateway clients.
    final int maxRows = isSqlExecSdkClient() ? 122900 : 147500;
    final String sql = "SELECT * FROM " + table + " limit " + maxRows;

    Properties properties = new Properties();
    properties.setProperty("RowsFetchedPerBlock", String.valueOf(maxRows));
    properties.setProperty("EnableSQLExecHybridResults", "0");
    Connection connection = getValidJDBCConnection(properties);

    final Statement statement = connection.createStatement();
    statement.setMaxRows(maxRows);

    final AtomicReference<Throwable> threadException = new AtomicReference<>();

    // Iterate through the result set in a different thread to surface any 1st-level thread-safety
    // issues
    Thread thread =
        new Thread(
            () -> {
              try (ResultSet rs = statement.executeQuery(sql)) {
                DatabricksResultSetMetaData metaData =
                    (DatabricksResultSetMetaData) rs.getMetaData();

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
                    getCloudFetchApiExtension()
                        .countRequestsMatching(getRequestedFor(urlPathMatching(".*")).build())
                        .getCount();
                // cloud fetch calls can be retried
                assertTrue(cloudFetchCalls >= metaData.getChunkCount());

                if (isSqlExecSdkClient()) {
                  // Number of requests to fetch external links should be one less than the total
                  // number of chunks as first chunk link is already fetched
                  final String statementId = ((DatabricksResultSet) rs).getStatementId();
                  final String resultChunkPathRegex =
                      String.format(RESULT_CHUNK_PATH, statementId, ".*");
                  getDatabricksApiExtension()
                      .verify(
                          (int) (metaData.getChunkCount() - 1),
                          getRequestedFor(urlPathMatching(resultChunkPathRegex)));
                }
              } catch (Throwable e) {
                threadException.set(e);
              }
            });

    thread.start();
    thread.join(10_000);

    // Check if the thread had an exception
    if (threadException.get() != null) {
      if (threadException.get() instanceof AssertionError) {
        throw (AssertionError) threadException.get();
      } else {
        fail("Test thread failed with exception: " + threadException.get());
      }
    }

    connection.close();
  }
}
