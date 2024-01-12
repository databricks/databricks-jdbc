package com.databricks.jdbc.pooling;

import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.client.sqlexec.Session;
import com.databricks.jdbc.core.DatabricksConnection;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.StatementExecutionService;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksConnectionPoolingTest {

  private static final String JDBC_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
  private static final String WAREHOUSE_ID = "791ba2a31c7fd70a";
  private static final String SESSION_ID = "session_id";
  private static StatementExecutionService statementExecutionService =
      Mockito.mock(StatementExecutionService.class);
  private static ApiClient apiClient = Mockito.mock(ApiClient.class);
  private static final String CLIENT_PATH = "/api/2.0/sql/statements/sessions";
  private static final Map<String, String> headers =
      new HashMap<>() {
        {
          put("Accept", "application/json");
          put("Content-Type", "application/json");
        }
      };

  @BeforeAll
  public static void setUp() {
    CreateSessionRequest createSessionRequest =
        new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
    Session session = new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID);
    Mockito.when(apiClient.POST(CLIENT_PATH, createSessionRequest, Session.class, headers))
        .thenReturn(session);
  }

  @Test
  public void testPooledConnection() throws SQLException {
    DatabricksConnectionPoolDataSource poolDataSource =
        Mockito.mock(DatabricksConnectionPoolDataSource.class);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection databricksConnection =
        new DatabricksConnection(
            connectionContext,
            new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    Mockito.when(poolDataSource.getPooledConnection())
        .thenReturn(new DatabricksPooledConnection(databricksConnection));

    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    TestListener listener = new TestListener();
    pooledConnection.addConnectionEventListener(listener);

    Connection connection = pooledConnection.getConnection();

    // TODO(PECO-1328): Add back when connection closing is fixed.
    //    connection.close();
    //    List<ConnectionEvent> connectionClosedEvents = listener.getConnectionClosedEvents();
    //    Assertions.assertEquals(connectionClosedEvents.size(), 1);
    Connection actualConnection =
        ((DatabricksPooledConnection) pooledConnection).getPhysicalConnection();
    Assertions.assertFalse(actualConnection.isClosed());

    pooledConnection.removeConnectionEventListener(listener);
    pooledConnection.close();
    //    Assertions.assertTrue(actualConnection.isClosed());
  }

  @Test
  public void testPooledConnectionReuse() throws SQLException {
    DatabricksConnectionPoolDataSource poolDataSource =
        Mockito.mock(DatabricksConnectionPoolDataSource.class);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection databricksConnection =
        new DatabricksConnection(
            connectionContext,
            new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    Mockito.when(poolDataSource.getPooledConnection())
        .thenReturn(new DatabricksPooledConnection(databricksConnection));

    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DatabricksPooledConnection pooledConnection =
        (DatabricksPooledConnection) poolDataSource.getPooledConnection();
    TestListener listener = new TestListener();
    pooledConnection.addConnectionEventListener(listener);
    Connection c1 = pooledConnection.getConnection();
    Connection pc1 = pooledConnection.getPhysicalConnection();
    c1.close(); // calling close on this should not close the underlying physical connection
    Connection c2 = pooledConnection.getConnection();
    Connection pc2 = pooledConnection.getPhysicalConnection();
    Assertions.assertEquals(pc1, pc2);
    Assertions.assertFalse(pc1.isClosed());
    Assertions.assertEquals(listener.getConnectionClosedEvents().size(), 1);
    c2.close();
    Assertions.assertEquals(listener.getConnectionClosedEvents().size(), 2);
    pooledConnection.close();
  }
}

class TestListener implements ConnectionEventListener {
  List<ConnectionEvent> connectionClosedEvents = new ArrayList<>();
  List<ConnectionEvent> connectionErrorEvents = new ArrayList<>();

  @Override
  public void connectionClosed(ConnectionEvent event) {
    connectionClosedEvents.add(event);
  }

  @Override
  public void connectionErrorOccurred(ConnectionEvent event) {
    connectionErrorEvents.add(event);
  }

  public List<ConnectionEvent> getConnectionClosedEvents() {
    return connectionClosedEvents;
  }

  public List<ConnectionEvent> getConnectionErrorEvents() {
    return connectionErrorEvents;
  }
}
