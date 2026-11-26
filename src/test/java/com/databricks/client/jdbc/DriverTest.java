package com.databricks.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.impl.common.SessionId;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class DriverTest {

  private static final String APC_URL =
      "jdbc:databricks://sample-host.cloud.databricks.com:443/default;"
          + "httpPath=sql/protocolv1/o/123456/0123-456789-abcdef;AuthMech=3;UID=token;PWD=x;";

  @Test
  void closeConnectionRoutesApcSeaSessionToSeaClientRegardlessOfContextRouting() throws Exception {
    String connectionId = "a|123456|0123-456789-abcdef|sea-session-id";

    try (MockedConstruction<DatabricksSdkClient> sea =
            Mockito.mockConstruction(DatabricksSdkClient.class);
        MockedConstruction<DatabricksThriftServiceClient> thrift =
            Mockito.mockConstruction(DatabricksThriftServiceClient.class)) {
      Driver.getInstance()
          .closeConnection(APC_URL + "UseThriftClient=1;", new Properties(), connectionId);

      assertEquals(1, sea.constructed().size());
      assertEquals(0, thrift.constructed().size());
      Mockito.verify(sea.constructed().get(0))
          .deleteSession(
              ArgumentMatchers.argThat(
                  info ->
                      "sea-session-id".equals(info.sessionId())
                          && new AllPurposeCluster("123456", "0123-456789-abcdef")
                              .equals(info.computeResource())));
    }
  }

  @Test
  void closeConnectionRoutesThriftSessionToThriftClientRegardlessOfContextRouting()
      throws Exception {
    String connectionId =
        new SessionId(
                new THandleIdentifier()
                    .setGuid(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
                    .setSecret(
                        new byte[] {
                          17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
                        }),
                null)
            .toString();

    try (MockedConstruction<DatabricksSdkClient> sea =
            Mockito.mockConstruction(DatabricksSdkClient.class);
        MockedConstruction<DatabricksThriftServiceClient> thrift =
            Mockito.mockConstruction(DatabricksThriftServiceClient.class)) {
      Driver.getInstance()
          .closeConnection(APC_URL + "UseThriftClient=0;", new Properties(), connectionId);

      assertEquals(0, sea.constructed().size());
      assertEquals(1, thrift.constructed().size());
      Mockito.verify(thrift.constructed().get(0))
          .deleteSession(ArgumentMatchers.argThat(info -> info.sessionHandle() != null));
    }
  }

  @Test
  void closeConnectionRoutesWarehouseThriftConnectionIdToThriftClient() throws Exception {
    byte[] guid = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    byte[] secret = new byte[] {17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
    TSessionHandle sessionHandle =
        new TSessionHandle(new THandleIdentifier().setGuid(guid).setSecret(secret));
    String connectionId =
        SessionId.create(
                ImmutableSessionInfo.builder()
                    .sessionId(DatabricksThriftUtil.byteBufferToString(ByteBuffer.wrap(guid)))
                    .computeResource(new Warehouse("warehouse-id"))
                    .sessionHandle(sessionHandle)
                    .build())
            .toString();
    assertTrue(connectionId.startsWith("t|"), connectionId);

    try (MockedConstruction<DatabricksSdkClient> sea =
            Mockito.mockConstruction(DatabricksSdkClient.class);
        MockedConstruction<DatabricksThriftServiceClient> thrift =
            Mockito.mockConstruction(DatabricksThriftServiceClient.class)) {
      Driver.getInstance()
          .closeConnection(
              "jdbc:databricks://sample-host.cloud.databricks.com:443/default;"
                  + "httpPath=/sql/1.0/warehouses/warehouse-id;AuthMech=3;UID=token;PWD=x;",
              new Properties(),
              connectionId);

      assertEquals(0, sea.constructed().size());
      assertEquals(1, thrift.constructed().size());
      Mockito.verify(thrift.constructed().get(0))
          .deleteSession(
              ArgumentMatchers.argThat(info -> sessionHandle.equals(info.sessionHandle())));
    }
  }

  @Test
  void connectRejectsMissingRequiredConnectionParameters() {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                Driver.getInstance().connect("jdbc:databricks://localhost:8080", new Properties()));

    assertEquals("INPUT_VALIDATION_ERROR", exception.getSQLState());
    assertTrue(exception.getMessage().contains("httppath"));
  }
}
