package com.databricks.jdbc.pooling;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.databricks.client.jdbc.DataSource;
import com.databricks.client.jdbc.Driver;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksPooledConnectionTest {

  private static final String JDBC_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
  private static final String WAREHOUSE_ID = "791ba2a31c7fd70a";
  private static final IDatabricksComputeResource warehouse = new Warehouse(WAREHOUSE_ID);
  private static final String SESSION_ID = "session_id";
  @Mock private static DatabricksSdkClient databricksClient;
  private static IDatabricksConnectionContext connectionContext;

  @BeforeAll
  public static void setUp() throws DatabricksSQLException {
    connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
  }

  @Test
  public void testPooledConnection() throws SQLException {
    DataSource poolDataSource = Mockito.mock(DataSource.class);
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
    when(databricksClient.createSession(eq(new Warehouse(WAREHOUSE_ID)), any(), any(), any()))
        .thenReturn(session);

    DatabricksConnection databricksConnection =
        new DatabricksConnection(connectionContext, databricksClient);
    Mockito.when(poolDataSource.getPooledConnection())
        .thenReturn(new DatabricksPooledConnection(databricksConnection));

    // Get a pooled connection
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    TestListener listener = new TestListener();
    pooledConnection.addConnectionEventListener(listener);

    // Check that the virtual connection is not closed
    Connection connection = pooledConnection.getConnection();
    assertFalse(connection.isClosed());
    connection.close();

    // Check that the physical connection is not closed
    List<ConnectionEvent> connectionClosedEvents = listener.getConnectionClosedEvents();
    assertEquals(connectionClosedEvents.size(), 1);
    Connection actualConnection =
        ((DatabricksPooledConnection) pooledConnection).getPhysicalConnection();
    assertFalse(actualConnection.isClosed());

    // Statement listeners do nothing
    pooledConnection.addStatementEventListener(null);
    pooledConnection.removeStatementEventListener(null);

    // Confirm closing of physical connection
    pooledConnection.removeConnectionEventListener(listener);
    pooledConnection.close();
    assertTrue(actualConnection.isClosed());

    // An error should be thrown when underlying physical connection is closed
    assertThrows(DatabricksSQLException.class, pooledConnection::getConnection);
  }

  @Test
  public void testPooledConnectionInvoke() throws SQLException {
    DataSource poolDataSource = Mockito.mock(DataSource.class);
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
    when(databricksClient.createSession(eq(new Warehouse(WAREHOUSE_ID)), any(), any(), any()))
        .thenReturn(session);

    DatabricksConnection databricksConnection =
        new DatabricksConnection(connectionContext, databricksClient);
    Mockito.when(poolDataSource.getPooledConnection())
        .thenReturn(new DatabricksPooledConnection(databricksConnection));

    // Get a pooled connection
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    Connection virtualConnection = pooledConnection.getConnection();
    Connection physicalConnection =
        ((DatabricksPooledConnection) pooledConnection).getPhysicalConnection();
    // Check invoke methods
    assertNotEquals(0, virtualConnection.hashCode());
    assertEquals(
        "Pooled connection wrapping physical connection " + physicalConnection,
        virtualConnection.toString());
    assertInstanceOf(PreparedStatement.class, virtualConnection.prepareStatement("SELECT 1"));

    pooledConnection.close();
  }

  @Test
  public void testPooledConnectionReuse() throws SQLException {
    DataSource poolDataSource = Mockito.mock(DataSource.class);
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
    when(databricksClient.createSession(eq(new Warehouse(WAREHOUSE_ID)), any(), any(), any()))
        .thenReturn(session);

    DatabricksConnection databricksConnection =
        new DatabricksConnection(connectionContext, databricksClient);
    Mockito.when(poolDataSource.getPooledConnection())
        .thenReturn(new DatabricksPooledConnection(databricksConnection));

    // Get a pooled connection
    DriverManager.registerDriver(new Driver());
    DatabricksPooledConnection pooledConnection =
        (DatabricksPooledConnection) poolDataSource.getPooledConnection();
    TestListener listener = new TestListener();
    pooledConnection.addConnectionEventListener(listener);
    Connection c1 = pooledConnection.getConnection();
    Connection pc1 = pooledConnection.getPhysicalConnection();
    // Calling close on this should not close the underlying physical connection
    c1.close();
    Connection c2 = pooledConnection.getConnection();
    Connection pc2 = pooledConnection.getPhysicalConnection();
    assertEquals(pc1, pc2);
    assertFalse(pc1.isClosed());
    assertEquals(listener.getConnectionClosedEvents().size(), 1);
    c2.close();
    assertEquals(listener.getConnectionClosedEvents().size(), 2);
    pooledConnection.close();
  }

  @Test
  public void testPooledConnectionStatement() throws SQLException {
    DataSource poolDataSource = Mockito.mock(DataSource.class);
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
    when(databricksClient.createSession(eq(new Warehouse(WAREHOUSE_ID)), any(), any(), any()))
        .thenReturn(session);
    DatabricksConnection databricksConnection =
        new DatabricksConnection(connectionContext, databricksClient);
    Mockito.when(poolDataSource.getPooledConnection())
        .thenReturn(new DatabricksPooledConnection(databricksConnection));

    // Get a pooled connection
    DriverManager.registerDriver(new Driver());
    DatabricksPooledConnection pooledConnection =
        (DatabricksPooledConnection) poolDataSource.getPooledConnection();
    Connection connection = pooledConnection.getConnection();
    // Check statement commands
    Statement statement = connection.createStatement();
    assertFalse(statement.isClosed());
    assertEquals(connection, statement.getConnection());
    statement.close();
    assertTrue(statement.isClosed());
  }

  @Test
  public void testPooledConnectionStatementInvoke() throws SQLException {
    DataSource poolDataSource = Mockito.mock(DataSource.class);
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
    when(databricksClient.createSession(eq(new Warehouse(WAREHOUSE_ID)), any(), any(), any()))
        .thenReturn(session);
    DatabricksConnection databricksConnection =
        new DatabricksConnection(connectionContext, databricksClient);
    Mockito.when(poolDataSource.getPooledConnection())
        .thenReturn(new DatabricksPooledConnection(databricksConnection));

    // Get a pooled connection
    DriverManager.registerDriver(new Driver());
    DatabricksPooledConnection pooledConnection =
        (DatabricksPooledConnection) poolDataSource.getPooledConnection();
    Connection connection = pooledConnection.getConnection();
    // Check statement commands
    Statement statement = connection.createStatement();
    // Check invokes
    assertNotEquals(0, statement.hashCode());
    assertTrue(statement.toString().startsWith("Pooled statement wrapping physical statement "));
    // Check delegated invoke
    assertEquals(300, statement.getQueryTimeout());
    statement.close();
    assertThrows(DatabricksSQLException.class, statement::getConnection);
    assertTrue(statement.isClosed());
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
}
