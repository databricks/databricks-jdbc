package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.core.types.Warehouse;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksStatementTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String STATEMENT = "select 1";
  private static final String STATEMENT_ID = "statement_id";
  private static final String SESSION_ID = "session_id";
  private static final ComputeResource WAREHOUSE_COMPUTE = new Warehouse(WAREHOUSE_ID);
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";

  @Mock DatabricksSdkClient client;
  @Mock DatabricksResultSet resultSet;

  @Test
  public void testExecuteQueryStatement() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    when(client.executeStatement(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<Integer, ImmutableSqlParameter>()),
            eq(StatementType.QUERY),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    ResultSet newResultSet = statement.executeQuery(STATEMENT);

    assertFalse(statement.isClosed());
    assertEquals(resultSet, newResultSet);
    statement.close(true);
    assertTrue(statement.isClosed());
  }

  @Test
  public void testExecuteStatement() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    when(client.executeStatement(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<Integer, ImmutableSqlParameter>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    assertTrue(statement.execute(STATEMENT));

    assertTrue(statement.execute(STATEMENT, Statement.NO_GENERATED_KEYS));

    assertFalse(statement.isClosed());
    statement.cancel();

    statement.close();
    assertThrows(DatabricksSQLException.class, () -> statement.cancel());
  }

  @Test
  public void testExecuteUpdateStatement() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    when(resultSet.getUpdateCount()).thenReturn(2L);
    when(client.executeStatement(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<Integer, ImmutableSqlParameter>()),
            eq(StatementType.UPDATE),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    int updateCount = statement.executeUpdate(STATEMENT);
    assertEquals(2, updateCount);
    assertFalse(statement.isClosed());
    statement.handleResultSetClose(resultSet);
    assertEquals(2, statement.executeUpdate(STATEMENT, Statement.NO_GENERATED_KEYS));
    statement.closeOnCompletion();
    assertTrue(statement.isCloseOnCompletion());
    statement.close();
    assertTrue(statement.isClosed());
  }

  @Test
  public void testFetchSizeAndWarnings() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    assertNull(statement.getWarnings());
    statement.setFetchSize(10);
    assertEquals(0, statement.getFetchSize());
    SQLWarning warnings = statement.getWarnings();
    assertEquals(
        warnings.getMessage(), "As FetchSize is not supported in the Databricks JDBC, ignoring it");
    assertEquals(
        warnings.getNextWarning().getMessage(),
        "As FetchSize is not supported in the Databricks JDBC, we don't set it in the first place");

    statement.clearWarnings();
    assertNull(statement.getWarnings());
  }

  @Test
  public void testSessionStatement() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();
    when(client.createSession(any(), any(), any(), any())).thenReturn(sessionInfo);
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    when(client.executeStatement(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<Integer, ImmutableSqlParameter>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    assertTrue(statement.execute(STATEMENT));

    statement.setStatementId(STATEMENT_ID);
    statement.setQueryTimeout(10);
    statement.setEscapeProcessing(true);
    assertEquals(statement.getQueryTimeout(), 10);
    assertEquals(statement.getStatement(), statement);
    assertEquals(statement.getSessionId(), SESSION_ID);
    assertEquals(statement.getStatementId(), STATEMENT_ID);
    doNothing().when(client).closeStatement(STATEMENT_ID);
    statement.close(true);
  }

  @Test
  public void testFeatureNotSupported() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> statement.addBatch("sql"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, statement::clearBatch);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, statement::executeBatch);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> statement.executeUpdate("sql", 23));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.executeUpdate("sql", new int[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.executeUpdate("sql", new String[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> statement.execute("sql", 23));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.execute("sql", new int[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.execute("sql", new String[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> statement.setPoolable(true));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.unwrap(java.sql.Connection.class));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.isWrapperFor(java.sql.Connection.class));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> statement.setCursorName("name"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, statement::getMaxFieldSize);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, statement::getMoreResults);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> statement.getMoreResults(5));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> statement.setMaxFieldSize(5));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
  }

  @Test
  public void testThrowErrorIfClosed() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.close();
    assertThrows(DatabricksSQLException.class, statement::getMaxRows);
  }

  @Test
  public void testStaticReturns() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    assertFalse(statement.isPoolable());
    assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, statement.getResultSetHoldability());
    assertFalse(statement.getGeneratedKeys().next());
    assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
    assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
  }
}
