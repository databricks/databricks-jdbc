package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.FAKE_SERVICE_URI_PROP_SUFFIX;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.STATEMENT_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.deleteTable;
import static com.databricks.jdbc.integration.IntegrationTestUtil.getFullyQualifiedTableName;
import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static com.databricks.jdbc.integration.IntegrationTestUtil.setupDatabaseTable;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Replays real backend responses for native PreparedStatement batching. */
public class NativePreparedStatementBatchIntegrationTests
    extends AbstractFakeServiceIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    if (!isSqlExecSdkClient()) {
      String targetUri = System.getProperty("thrift_server.targetURI");
      String routeProperty = targetUri + FAKE_SERVICE_URI_PROP_SUFFIX;
      System.setProperty(
          routeProperty, System.getProperty(routeProperty).replace("localhost", "127.0.0.1"));
    }
    Properties properties = new Properties();
    properties.setProperty(DatabricksJdbcUrlParams.ENABLE_NATIVE_BATCHING.getParamName(), "1");
    properties.setProperty(DatabricksJdbcUrlParams.ENABLE_BATCHED_INSERTS.getParamName(), "0");
    connection = getValidJDBCConnection(properties);
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection == null) {
      return;
    }
    if (((DatabricksConnection) connection).getConnectionContext().getClientType()
            != DatabricksClientType.THRIFT
        || getFakeServiceMode() != FakeServiceExtension.FakeServiceMode.REPLAY) {
      connection.close();
    }
  }

  @Test
  void testNativeBatchSuccess() throws SQLException {
    String tableName = "native_batch_success_table";
    setupDatabaseTable(connection, tableName);
    String sql =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      addRow(statement, 1, "first-a", "first-b");
      addRow(statement, 2, "second-a", "second-b");
      assertArrayEquals(new int[] {1, 1}, statement.executeBatch());
    } finally {
      deleteTable(connection, tableName);
    }

    if (isSqlExecSdkClient()) {
      getDatabricksApiExtension()
          .verify(
              1,
              postRequestedFor(urlEqualTo(STATEMENT_PATH))
                  .withRequestBody(
                      equalToJson(
                          "{"
                              + "\"statement\":\""
                              + sql
                              + "\","
                              + "\"parameter_sets\":["
                              + "{\"parameters\":["
                              + "{\"ordinal\":0,\"type\":\"INT\",\"value\":\"1\"},"
                              + "{\"ordinal\":1,\"type\":\"STRING\",\"value\":\"first-a\"},"
                              + "{\"ordinal\":2,\"type\":\"STRING\",\"value\":\"first-b\"}]},"
                              + "{\"parameters\":["
                              + "{\"ordinal\":0,\"type\":\"INT\",\"value\":\"2\"},"
                              + "{\"ordinal\":1,\"type\":\"STRING\",\"value\":\"second-a\"},"
                              + "{\"ordinal\":2,\"type\":\"STRING\",\"value\":\"second-b\"}]}"
                              + "]"
                              + "}",
                          false,
                          true)));
    }
  }

  private void addRow(PreparedStatement statement, int id, String col1, String col2)
      throws SQLException {
    statement.setInt(1, id);
    statement.setString(2, col1);
    statement.setString(3, col2);
    statement.addBatch();
  }
}
