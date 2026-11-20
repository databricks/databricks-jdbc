package com.databricks.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.common.SessionId;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class DriverTest {

  private static final String URL =
      "jdbc:databricks://localhost:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/protocolv1/o/org/cluster;";

  @Test
  void closeConnectionUsesSavedSeaTypeAfterRoutingChanges() throws Exception {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getConnectionUuid()).thenReturn("saved-sea");
    when(context.getClientType()).thenReturn(DatabricksClientType.THRIFT);
    Properties properties = new Properties();
    try (MockedStatic<DatabricksConnectionContextFactory> factory =
            mockStatic(DatabricksConnectionContextFactory.class);
        MockedConstruction<DatabricksSdkClient> sdk = mockConstruction(DatabricksSdkClient.class);
        MockedConstruction<DatabricksThriftServiceClient> thrift =
            mockConstruction(DatabricksThriftServiceClient.class)) {
      factory
          .when(() -> DatabricksConnectionContextFactory.create(URL, properties))
          .thenReturn(context);
      Driver.getInstance().closeConnection(URL, properties, "s|a|org|cluster|session|8");
      assertEquals(1, sdk.constructed().size());
      assertTrue(thrift.constructed().isEmpty());
      verify(sdk.constructed().get(0))
          .deleteSession(
              org.mockito.ArgumentMatchers.argThat(
                  session ->
                      session.clientType() == DatabricksClientType.SEA
                          && session
                              .computeResource()
                              .equals(new AllPurposeCluster("org", "cluster"))
                          && session.sessionId().equals("session")));
    }
  }

  @Test
  void closeConnectionUsesSavedThriftTypeAfterRoutingChanges() throws Exception {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getConnectionUuid()).thenReturn("saved-thrift");
    when(context.getClientType()).thenReturn(DatabricksClientType.SEA);
    Properties properties = new Properties();
    byte[] guid = new byte[16];
    byte[] secret = new byte[16];
    String savedId =
        new SessionId(
                new THandleIdentifier().setGuid(guid).setSecret(secret),
                new AllPurposeCluster("org", "cluster"))
            .toString();
    try (MockedStatic<DatabricksConnectionContextFactory> factory =
            mockStatic(DatabricksConnectionContextFactory.class);
        MockedConstruction<DatabricksSdkClient> sdk = mockConstruction(DatabricksSdkClient.class);
        MockedConstruction<DatabricksThriftServiceClient> thrift =
            mockConstruction(DatabricksThriftServiceClient.class)) {
      factory
          .when(() -> DatabricksConnectionContextFactory.create(URL, properties))
          .thenReturn(context);
      Driver.getInstance().closeConnection(URL, properties, savedId);
      assertEquals(1, thrift.constructed().size());
      assertTrue(sdk.constructed().isEmpty());
      verify(thrift.constructed().get(0))
          .deleteSession(
              org.mockito.ArgumentMatchers.argThat(
                  session -> session.clientType() == DatabricksClientType.THRIFT));
    }
  }

  @Test
  void closeConnectionPreservesSavedWarehouseSeaRoute() throws Exception {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getConnectionUuid()).thenReturn("saved-warehouse");
    Properties properties = new Properties();
    try (MockedStatic<DatabricksConnectionContextFactory> factory =
            mockStatic(DatabricksConnectionContextFactory.class);
        MockedConstruction<DatabricksSdkClient> sdk = mockConstruction(DatabricksSdkClient.class)) {
      factory
          .when(() -> DatabricksConnectionContextFactory.create(URL, properties))
          .thenReturn(context);
      Driver.getInstance().closeConnection(URL, properties, "s|warehouse|session|r");
      assertEquals(1, sdk.constructed().size());
      verify(sdk.constructed().get(0))
          .deleteSession(
              org.mockito.ArgumentMatchers.argThat(
                  session ->
                      session.computeResource().equals(new Warehouse("warehouse"))
                          && session.realtimeSession()
                          && session.sessionId().equals("session")));
    }
  }

  @Test
  void closeConnectionReleasesTemporaryClientsAfterSuccessfulDelete() throws Exception {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    DatabricksClientConfiguratorManager configurators =
        mock(DatabricksClientConfiguratorManager.class);
    DatabricksHttpClientFactory httpClients = mock(DatabricksHttpClientFactory.class);
    Properties properties = new Properties();
    try (MockedStatic<DatabricksConnectionContextFactory> factory =
            mockStatic(DatabricksConnectionContextFactory.class);
        MockedStatic<DatabricksClientConfiguratorManager> manager =
            mockStatic(DatabricksClientConfiguratorManager.class);
        MockedStatic<DatabricksHttpClientFactory> httpFactory =
            mockStatic(DatabricksHttpClientFactory.class);
        MockedConstruction<DatabricksSdkClient> sdk = mockConstruction(DatabricksSdkClient.class)) {
      factory
          .when(() -> DatabricksConnectionContextFactory.create(URL, properties))
          .thenReturn(context);
      manager.when(DatabricksClientConfiguratorManager::getInstance).thenReturn(configurators);
      httpFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClients);

      Driver.getInstance().closeConnection(URL, properties, "s|a|org|cluster|session|8");

      verify(sdk.constructed().get(0)).deleteSession(any());
      verify(configurators).removeInstance(context);
      verify(httpClients).removeClient(context);
    }
  }

  @Test
  void closeConnectionReleasesTemporaryClientsWhenDeleteFails() throws Exception {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    DatabricksClientConfiguratorManager configurators =
        mock(DatabricksClientConfiguratorManager.class);
    DatabricksHttpClientFactory httpClients = mock(DatabricksHttpClientFactory.class);
    Properties properties = new Properties();
    DatabricksSQLException deleteFailure =
        new DatabricksSQLException("delete failed", DatabricksDriverErrorCode.CONNECTION_ERROR);
    try (MockedStatic<DatabricksConnectionContextFactory> factory =
            mockStatic(DatabricksConnectionContextFactory.class);
        MockedStatic<DatabricksClientConfiguratorManager> manager =
            mockStatic(DatabricksClientConfiguratorManager.class);
        MockedStatic<DatabricksHttpClientFactory> httpFactory =
            mockStatic(DatabricksHttpClientFactory.class);
        MockedConstruction<DatabricksSdkClient> sdk =
            mockConstruction(
                DatabricksSdkClient.class,
                (client, construction) ->
                    doThrow(deleteFailure).when(client).deleteSession(any()))) {
      factory
          .when(() -> DatabricksConnectionContextFactory.create(URL, properties))
          .thenReturn(context);
      manager.when(DatabricksClientConfiguratorManager::getInstance).thenReturn(configurators);
      httpFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClients);

      assertEquals(
          deleteFailure,
          assertThrows(
              SQLException.class,
              () ->
                  Driver.getInstance()
                      .closeConnection(URL, properties, "s|a|org|cluster|session|8")));

      verify(sdk.constructed().get(0)).deleteSession(any());
      verify(configurators).removeInstance(context);
      verify(httpClients).removeClient(context);
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
