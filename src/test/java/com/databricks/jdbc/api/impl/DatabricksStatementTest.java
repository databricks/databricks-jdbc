package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.StatementState;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.http.entity.InputStreamEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksStatementTest {

  private static final String WAREHOUSE_ID = "99999999";
  private static final String STATEMENT = "select 1";
  private static final StatementId STATEMENT_ID = new StatementId("statement_id");
  private static final String SESSION_ID = "session_id";
  private static final IDatabricksComputeResource WAREHOUSE_COMPUTE = new Warehouse(WAREHOUSE_ID);
  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;";
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
            eq(new HashMap<>()),
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
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    assertTrue(statement.execute(STATEMENT));

    assertTrue(statement.execute(STATEMENT, Statement.NO_GENERATED_KEYS));

    assertFalse(statement.isClosed());
    statement.cancel();

    statement.close();
    assertThrows(DatabricksSQLException.class, statement::cancel);
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
            eq(new HashMap<>()),
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
  public void testGetUpdateCountForQueries() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    when(client.executeStatement(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.QUERY),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    statement.executeQuery(STATEMENT);
    assertEquals(-1, statement.getUpdateCount());
  }

  @Test
  public void testGetUpdateCountAfterMoreResultsForDMLQueries() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Mock DML statement (UPDATE) that returns update count
    when(resultSet.hasUpdateCount()).thenReturn(true);
    when(resultSet.getUpdateCount()).thenReturn(5L);
    when(client.executeStatement(
            eq("UPDATE table SET col = 'value'"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.UPDATE),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    // Execute DML statement
    int updateCount = statement.executeUpdate("UPDATE table SET col = 'value'");
    assertEquals(5, updateCount);

    assertEquals(5, statement.getUpdateCount());

    // Call getMoreResults() - this should set noMoreResults flag and close the ResultSet
    assertFalse(statement.getMoreResults());

    // After getMoreResults(), getUpdateCount() should return -1 (indicating no more results)
    assertEquals(-1, statement.getUpdateCount());
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
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    when(client.executeStatement(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
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
    assertEquals(statement.getStatementId(), STATEMENT_ID);
    doNothing().when(client).closeStatement(STATEMENT_ID);
    statement.close(true);
    assertTrue(statement.isWrapperFor(Statement.class));
  }

  @Test
  public void testFeatureNotSupported() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.executeUpdate("sql", Statement.RETURN_GENERATED_KEYS));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.executeUpdate("sql", new int[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.executeUpdate("sql", new String[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.execute("sql", Statement.RETURN_GENERATED_KEYS));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.execute("sql", new int[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.execute("sql", new String[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.executeLargeUpdate("sql", Statement.RETURN_GENERATED_KEYS));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.executeLargeUpdate("sql", new int[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.executeLargeUpdate("sql", new String[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> statement.setPoolable(true));
    assertThrows(DatabricksSQLException.class, () -> statement.unwrap(java.sql.Connection.class));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> statement.setCursorName("name"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
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

  @Test
  public void testExecuteInternalWithZeroTimeout() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection mockConnection = mock(DatabricksConnection.class);
    when(mockConnection.getConnectionContext()).thenReturn(connectionContext);
    DatabricksStatement statement = new DatabricksStatement(mockConnection);

    // Set timeout to 0 for infinite wait
    statement.setQueryTimeout(0);

    CompletableFuture<DatabricksResultSet> mockFuture = mock(CompletableFuture.class);

    when(mockFuture.get()).thenReturn(resultSet);

    DatabricksStatement spyStatement = spy(statement);
    doReturn(mockFuture).when(spyStatement).getFutureResult(anyString(), anyMap(), any());

    // Execute query using statement
    spyStatement.executeInternal("SELECT * FROM table", new HashMap<>(), StatementType.QUERY);

    // Verify that get() is called instead of get(long, TimeUnit) for infinite wait
    verify(mockFuture, times(1)).get();
    verify(mockFuture, never()).get(anyLong(), any(TimeUnit.class));
  }

  @Test
  public void testInputStreamForVolumeOperation() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection mockConnection = mock(DatabricksConnection.class);
    when(mockConnection.getConnectionContext()).thenReturn(connectionContext);
    InputStream mockStream = mock(InputStream.class);
    DatabricksStatement statement = new DatabricksStatement(mockConnection);

    assertFalse(statement.isAllowedInputStreamForVolumeOperation());
    assertNull(statement.getInputStreamForUCVolume());
    assertThrows(
        DatabricksSQLException.class,
        () -> statement.setInputStreamForUCVolume(new InputStreamEntity(mockStream, -1L)));

    statement.allowInputStreamForVolumeOperation(true);
    statement.setInputStreamForUCVolume(new InputStreamEntity(mockStream));

    assertTrue(statement.isAllowedInputStreamForVolumeOperation());
    assertNotNull(statement.getInputStreamForUCVolume());

    statement.close();
    assertThrows(DatabricksSQLException.class, statement::getInputStreamForUCVolume);
    assertThrows(
        DatabricksSQLException.class,
        () -> statement.setInputStreamForUCVolume(new InputStreamEntity(mockStream, -1L)));
    assertThrows(DatabricksSQLException.class, statement::isAllowedInputStreamForVolumeOperation);
    assertThrows(
        DatabricksSQLException.class, () -> statement.allowInputStreamForVolumeOperation(false));
  }

  @Test
  public void testGetStatementId() throws DatabricksSQLException {
    DatabricksConnection mockConnection = mock(DatabricksConnection.class);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    when(mockConnection.getConnectionContext()).thenReturn(connectionContext);
    DatabricksStatement statement = new DatabricksStatement(mockConnection, STATEMENT_ID);
    assertEquals(STATEMENT_ID, statement.getStatementId());
  }

  @Test
  public void testExecuteAsync() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    when(client.executeStatementAsync(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    when(resultSet.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.RUNNING));

    ResultSet newResultSet = statement.executeAsync(STATEMENT);
    assertEquals(resultSet, newResultSet);
    assertEquals(
        StatementState.RUNNING,
        ((IDatabricksResultSet) newResultSet).getStatementStatus().getState());
  }

  @Test
  public void testGetExecutionResult() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection, STATEMENT_ID);
    when(client.getStatementResult(eq(STATEMENT_ID), any(IDatabricksSession.class), eq(statement)))
        .thenReturn(resultSet);
    when(resultSet.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.RUNNING));

    ResultSet newResultSet = statement.getExecutionResult();
    assertEquals(resultSet, newResultSet);
    assertEquals(
        StatementState.RUNNING,
        ((IDatabricksResultSet) newResultSet).getStatementStatus().getState());
  }

  @Test
  public void testGetExecutionResult_statementIdNull() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    assertThrows(DatabricksSQLException.class, statement::getExecutionResult);
  }

  @Test
  public void testGetAndSetMaxRows() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, statement.getMaxRows());
    // "no limit"
    statement.setMaxRows(0);
    assertEquals(0, statement.getMaxRows());
  }

  @Test
  public void testGetAndSetLargeMaxRows() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setLargeMaxRows(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, statement.getLargeMaxRows());
    // "no limit"
    statement.setLargeMaxRows(0);
    assertEquals(0, statement.getLargeMaxRows());
  }

  @Test
  public void testEnquoteLiteral() throws Exception {
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement stmt = new DatabricksStatement(connection);
    // Normal string
    assertEquals("'hello'", stmt.enquoteLiteral("hello"));
    // Empty string
    assertEquals("''", stmt.enquoteLiteral(""));
    // String with single quote
    assertEquals("'It''s a test'", stmt.enquoteLiteral("It's a test"));
    // String with multiple single quotes
    assertEquals("'It''s a ''quoted'' test'", stmt.enquoteLiteral("It's a 'quoted' test"));
  }

  @Test
  public void testEnquoteIdentifier() throws Exception {
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement stmt = new DatabricksStatement(connection);
    // Valid identifier without forced quoting
    assertEquals("myTable", stmt.enquoteIdentifier("myTable", false));
    // Valid identifier with forced quoting
    assertEquals("\"myTable\"", stmt.enquoteIdentifier("myTable", true));
    // Identifier that requires quoting
    assertEquals("\"my-table\"", stmt.enquoteIdentifier("my-table", false));
    // Already quoted identifier
    assertEquals("\"my-table\"", stmt.enquoteIdentifier("\"my-table\"", false));
  }

  @Test
  public void testEnquoteNCharLiteral() throws Exception {
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement stmt = new DatabricksStatement(connection);
    // Normal string
    assertEquals("N'hello'", stmt.enquoteNCharLiteral("hello"));
    // Empty string
    assertEquals("N''", stmt.enquoteNCharLiteral(""));
    // String with single quote
    assertEquals("N'It''s a test'", stmt.enquoteNCharLiteral("It's a test"));
    // String with multiple single quotes
    assertEquals("N'It''s a ''quoted'' test'", stmt.enquoteNCharLiteral("It's a 'quoted' test"));
    // String with non-ASCII characters
    assertEquals("N'こんにちは'", stmt.enquoteNCharLiteral("こんにちは"));
  }

  @Test
  public void testIsSimpleIdentifier() throws Exception {
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement stmt = new DatabricksStatement(connection);
    // Valid identifier
    assertTrue(stmt.isSimpleIdentifier("validName"));
    // Valid identifier with underscores and numbers
    assertTrue(stmt.isSimpleIdentifier("valid_name_123"));
    // Invalid identifier starting with number
    assertFalse(stmt.isSimpleIdentifier("123name"));
    // Invalid identifier with special characters
    assertFalse(stmt.isSimpleIdentifier("invalid-name"));
    // Empty string
    assertFalse(stmt.isSimpleIdentifier(""));
    // Too long identifier
    String longIdentifier = "a".repeat(129);
    assertFalse(stmt.isSimpleIdentifier(longIdentifier));
  }

  @Test
  public void testShouldReturnResultSet_SelectQuery() {
    String query = "-- comment\nSELECT * FROM table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_ShowQuery() {
    String query = "SHOW TABLES;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_DescribeQuery() {
    String query = "DESCRIBE table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_ExplainQuery() {
    String query = "EXPLAIN SELECT * FROM table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_WithQuery() {
    String query = "WITH cte AS (SELECT * FROM table) SELECT * FROM cte;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_SetQuery() {
    String query = "SET @var = (SELECT COUNT(*) FROM table);";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_MapQuery() {
    String query = "MAP table USING some_mapping;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_FromQuery() {
    String query = "SELECT * FROM table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_ValuesQuery() {
    String query = "VALUES (1, 2, 3);";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_UnionQuery() {
    String query = "SELECT * FROM table1 UNION SELECT * FROM table2;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_IntersectQuery() {
    String query = "SELECT * FROM table1 INTERSECT SELECT * FROM table2;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_ExceptQuery() {
    String query = "SELECT * FROM table1 EXCEPT SELECT * FROM table2;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_DeclareQuery() {
    String query = "DECLARE @var INT;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_PutQuery() {
    String query = "PUT some_data INTO table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_GetQuery() {
    String query = "GET some_data FROM table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_RemoveQuery() {
    String query = "REMOVE some_data FROM table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_ListQuery() {
    String query = "LIST TABLES;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_UpdateQuery() {
    String query = "UPDATE table SET column = value;";
    // Without prefix configuration, UPDATE should not return result set
    assertFalse(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
    // With UPDATE prefix, it should return result set
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Arrays.asList("UPDATE")));
  }

  @Test
  public void testShouldReturnResultSet_DeleteQuery() {
    String query = "DELETE FROM table WHERE condition;";
    // Without prefix configuration, DELETE should not return result set
    assertFalse(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
    // With DELETE prefix, it should return result set
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Arrays.asList("DELETE")));
  }

  @Test
  public void testShouldReturnResultSet_SingleLineCommentAtStart() {
    String query = "-- This is a comment\nSELECT * FROM table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_SingleLineCommentAtEnd() {
    String query = "SELECT * FROM table; -- This is a comment";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_SingleLineCommentInMiddle() {
    String query = "SELECT * FROM table -- This is a comment\nWHERE id = 1;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_MultiLineCommentAtStart() {
    String query = "/* This is a comment */ SELECT * FROM table;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_MultiLineCommentAtEnd() {
    String query = "SELECT * FROM table; /* This is a comment */";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_MultiLineCommentInMiddle() {
    String query = "SELECT * FROM table /* This is a comment */ WHERE id = 1;";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_MultipleSingleLineComments() {
    String query = "-- Comment 1\nSELECT * FROM table; -- Comment 2\n-- Comment 3";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_MultipleMultiLineComments() {
    String query = "/* Comment 1 */ SELECT * FROM table; /* Comment 2 */ /* Comment 3 */";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_SingleAndMultiLineComments() {
    String query = "-- Single-line comment\nSELECT * FROM table; /* Multi-line comment */";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_CommentSurroundingQuery() {
    String query =
        "-- Single-line comment\n/* Multi-line comment */ SELECT * FROM table; /* Another comment */ -- End comment";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_CallStatement() {
    String query =
        "-- Single-line comment\n/* Multi-line comment */ CALL send_notifications(12); /* Another comment */ -- End comment";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, Collections.emptyList()));
    assertTrue(
        DatabricksStatement.shouldReturnResultSet(
            "CALL send_notifications(12);", Collections.emptyList()));
  }

  @Test
  public void testShouldReturnResultSet_StartWithBegin() {
    assertTrue(DatabricksStatement.shouldReturnResultSet("BEGIN", Collections.emptyList()));
    assertTrue(DatabricksStatement.shouldReturnResultSet("   begin   ", Collections.emptyList()));
    assertTrue(
        DatabricksStatement.shouldReturnResultSet("BEGIN; WORK; END", Collections.emptyList()));
    assertTrue(
        DatabricksStatement.shouldReturnResultSet("BEGIN; SELECT 1", Collections.emptyList()));
    // Not supporting for transaction statements
    assertFalse(
        DatabricksStatement.shouldReturnResultSet("BEGIN TRANSACTION", Collections.emptyList()));
    assertFalse(
        DatabricksStatement.shouldReturnResultSet(
            "   BeGiN    TRANSACTION", Collections.emptyList()));
    assertFalse(
        DatabricksStatement.shouldReturnResultSet(
            "BEGIN    transaction ", Collections.emptyList()));
  }

  @Test
  public void testIsSelectQuery() {
    String query =
        "-- Single-line comment\n/* Multi-line comment */ SELECT * FROM table; /* Another comment */ -- End comment";
    assertTrue(DatabricksStatement.isSelectQuery(query));

    query = "REMOVE some_data FROM table;";
    assertFalse(DatabricksStatement.isSelectQuery(query));
  }

  @Test
  public void testIsInsertQuery() {
    // Test basic INSERT statements
    assertTrue(DatabricksStatement.isInsertQuery("INSERT INTO users (id, name) VALUES (?, ?)"));
    assertTrue(DatabricksStatement.isInsertQuery("insert into users (id, name) values (?, ?)"));
    assertTrue(
        DatabricksStatement.isInsertQuery(
            "   INSERT   INTO   users   (id, name)   VALUES   (?, ?)"));

    // Test INSERT with comments
    String queryWithComments =
        "-- Comment\n/* Multi-line */ INSERT INTO users (id) VALUES (?); -- End";
    assertTrue(DatabricksStatement.isInsertQuery(queryWithComments));

    // Test non-INSERT statements
    assertFalse(DatabricksStatement.isInsertQuery("SELECT * FROM users"));
    assertFalse(DatabricksStatement.isInsertQuery("UPDATE users SET name = ?"));
    assertFalse(DatabricksStatement.isInsertQuery("DELETE FROM users"));
    assertFalse(DatabricksStatement.isInsertQuery("CREATE TABLE users (id INT)"));
    assertFalse(DatabricksStatement.isInsertQuery(""));
    assertFalse(DatabricksStatement.isInsertQuery(null));

    // Test INSERT with schema prefix
    assertTrue(DatabricksStatement.isInsertQuery("INSERT INTO schema.users (id) VALUES (?)"));

    // Test with parentheses at the beginning
    assertTrue(DatabricksStatement.isInsertQuery("(INSERT INTO users (id) VALUES (?))"));
  }

  @Test
  public void testMarkAsClosed() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    when(client.executeStatement(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.QUERY),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    // Execute a query to set up result set
    statement.executeQuery(STATEMENT);
    assertFalse(statement.isClosed());

    // Mark statement as closed without attempting to close on server or clean up resources
    statement.markAsClosed();

    // Verify statement is marked as closed
    assertTrue(statement.isClosed());

    // Verify that closeStatement was NOT called on the client (server already closed it)
    verify(client, never()).closeStatement(any(StatementId.class));

    // Verify that result set is NOT closed yet by markAsClosed
    verify(resultSet, never()).close();

    // Verify that the statement cannot be used anymore
    assertThrows(DatabricksSQLException.class, () -> statement.executeQuery(STATEMENT));

    // Now call close() - it should clean up the result set without trying to close on server
    statement.close();

    // Verify that result set was closed by close()
    verify(resultSet, times(1)).close();

    // Verify that closeStatement was still NOT called on the client (already closed on server)
    verify(client, never()).closeStatement(any(StatementId.class));
  }

  @Test
  public void testMarkAsClosedThenCloseWithResultSetError() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Create a mock result set that throws an exception on close
    DatabricksResultSet mockResultSet = mock(DatabricksResultSet.class);
    doThrow(new DatabricksSQLException("Error closing result set", "HY000"))
        .when(mockResultSet)
        .close();

    when(client.executeStatement(
            eq(STATEMENT),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.QUERY),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(mockResultSet);

    // Execute a query to set up result set
    statement.executeQuery(STATEMENT);
    assertFalse(statement.isClosed());

    // Mark statement as closed - should not throw since it doesn't close result set
    assertDoesNotThrow(() -> statement.markAsClosed());

    // Verify statement is marked as closed
    assertTrue(statement.isClosed());

    // Verify result set was NOT closed by markAsClosed
    verify(mockResultSet, never()).close();

    // Now call close() - it should attempt to close result set and throw the exception
    assertThrows(DatabricksSQLException.class, () -> statement.close());

    // Verify that result set close was attempted during close()
    verify(mockResultSet, times(1)).close();

    // Verify that closeStatement was NOT called on the client
    verify(client, never()).closeStatement(any(StatementId.class));
  }

  @Test
  public void testMarkAsClosedWithoutResultSet() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Mark statement as closed without executing any query (no result set)
    assertFalse(statement.isClosed());
    statement.markAsClosed();

    // Verify statement is marked as closed
    assertTrue(statement.isClosed());

    // Verify that closeStatement was NOT called on the client
    verify(client, never()).closeStatement(any(StatementId.class));

    // Calling close() after markAsClosed should not throw
    assertDoesNotThrow(() -> statement.close());

    // Statement should still be closed
    assertTrue(statement.isClosed());
  }

  @Test
  public void testRemoveEmptyEscapeClauseFromQuery() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    String sqlWithEmptyEscape = "SELECT * FROM table WHERE name LIKE 'pattern%' ESCAPE ''";
    String expectedSql = "SELECT * FROM table WHERE name LIKE 'pattern%'";

    when(client.executeStatement(
            eq(expectedSql),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.QUERY),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    // Execute query with empty ESCAPE clause
    statement.executeQuery(sqlWithEmptyEscape);

    // Verify that the SQL sent to client has the ESCAPE clause removed
    verify(client)
        .executeStatement(
            eq(expectedSql),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.QUERY),
            any(IDatabricksSession.class),
            eq(statement));
  }

  private DatabricksConnection getTestConnection() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    return connection;
  }

  // Tests for Issue #1063: getLargeUpdateCount() should return -1 instead of throwing exception

  @Test
  public void testGetLargeUpdateCountAfterNoMoreResults() throws Exception {
    // Setup
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement statement = new DatabricksStatement(connection);

    when(client.executeStatement(
            eq(STATEMENT),
            eq(WAREHOUSE_COMPUTE),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    when(resultSet.hasUpdateCount()).thenReturn(false); // SELECT query

    // Execute and get result set
    statement.execute(STATEMENT);
    statement.getResultSet();

    // Advance past results
    assertFalse(statement.getMoreResults());

    // Should return -1, not throw exception
    assertEquals(-1, statement.getLargeUpdateCount());
    assertEquals(-1, statement.getUpdateCount());
  }

  @Test
  public void testGetLargeUpdateCountForSelect() throws Exception {
    // Setup
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement statement = new DatabricksStatement(connection);

    when(client.executeStatement(
            eq(STATEMENT),
            eq(WAREHOUSE_COMPUTE),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    when(resultSet.hasUpdateCount()).thenReturn(false); // SELECT query

    // Execute SELECT query
    statement.execute(STATEMENT);

    // Should return -1 for SELECT statements
    assertEquals(-1, statement.getLargeUpdateCount());
    assertEquals(-1, statement.getUpdateCount());
  }

  @Test
  public void testGetLargeUpdateCountForUpdate() throws Exception {
    // Setup
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement statement = new DatabricksStatement(connection);
    String updateSql = "UPDATE test_table SET col = 'value'";

    when(client.executeStatement(
            eq(updateSql),
            eq(WAREHOUSE_COMPUTE),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    when(resultSet.hasUpdateCount()).thenReturn(true);
    when(resultSet.getUpdateCount()).thenReturn(42L);

    // Execute UPDATE query
    statement.execute(updateSql);

    // Should return actual update count
    assertEquals(42L, statement.getLargeUpdateCount());
    assertEquals(42, statement.getUpdateCount());
  }

  @Test
  public void testUpdateCountConsistency() throws Exception {
    // Setup
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement statement = new DatabricksStatement(connection);

    when(client.executeStatement(
            eq(STATEMENT),
            eq(WAREHOUSE_COMPUTE),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    when(resultSet.hasUpdateCount()).thenReturn(false);

    // Execute and advance past results
    statement.execute(STATEMENT);
    statement.getResultSet();
    statement.getMoreResults();

    // Both methods should return the same value (-1)
    assertEquals(statement.getUpdateCount(), (int) statement.getLargeUpdateCount());
  }

  @Test
  public void testGetUpdateCountOverflow() throws Exception {
    // Setup
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement statement = new DatabricksStatement(connection);
    String updateSql = "UPDATE large_table SET col = 'value'";
    long largeCount = ((long) Integer.MAX_VALUE) + 100L;

    when(client.executeStatement(
            eq(updateSql),
            eq(WAREHOUSE_COMPUTE),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    when(resultSet.hasUpdateCount()).thenReturn(true);
    when(resultSet.getUpdateCount()).thenReturn(largeCount);

    // Execute UPDATE with large count
    statement.execute(updateSql);

    // getLargeUpdateCount should return actual count
    assertEquals(largeCount, statement.getLargeUpdateCount());

    // getUpdateCount should return SUCCESS_NO_INFO for overflow
    assertEquals(Statement.SUCCESS_NO_INFO, statement.getUpdateCount());
  }

  @Test
  public void testJdbcStopCondition() throws Exception {
    // Setup - test the standard JDBC stop condition pattern
    DatabricksConnection connection = getTestConnection();
    DatabricksStatement statement = new DatabricksStatement(connection);

    when(client.executeStatement(
            eq(STATEMENT),
            eq(WAREHOUSE_COMPUTE),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    when(resultSet.hasUpdateCount()).thenReturn(false);

    // Execute query
    statement.execute(STATEMENT);
    statement.getResultSet();

    // Standard JDBC stop condition should work without exception
    boolean hasMoreResults = false;
    do {
      hasMoreResults = statement.getMoreResults();
    } while (hasMoreResults || statement.getUpdateCount() != -1);

    // After loop, should be able to call getLargeUpdateCount() without exception
    assertEquals(-1, statement.getLargeUpdateCount());
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_Insert() {
    List<String> prefixes = Arrays.asList("INSERT", "UPDATE", "DELETE");
    String query = "INSERT INTO table VALUES (1, 2, 3)";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_Update() {
    List<String> prefixes = Arrays.asList("INSERT", "UPDATE", "DELETE");
    String query = "UPDATE table SET col = 1 WHERE id = 2";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_Delete() {
    List<String> prefixes = Arrays.asList("INSERT", "UPDATE", "DELETE");
    String query = "DELETE FROM table WHERE id = 1";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_Merge() {
    List<String> prefixes = Arrays.asList("MERGE");
    String query = "MERGE INTO target USING source ON target.id = source.id";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_CaseInsensitive() {
    List<String> prefixes = Arrays.asList("INSERT", "UPDATE");
    String query = "insert into table values (1, 2, 3)";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_WithComments() {
    List<String> prefixes = Arrays.asList("INSERT", "UPDATE", "DELETE");
    String query = "-- Comment\n/* Block comment */ INSERT INTO table VALUES (1, 2, 3)";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_NoMatch() {
    List<String> prefixes = Arrays.asList("INSERT", "UPDATE", "DELETE");
    String query = "CREATE TABLE test (id INT)";
    assertFalse(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_EmptyList() {
    List<String> prefixes = Collections.emptyList();
    String query = "INSERT INTO table VALUES (1, 2, 3)";
    assertFalse(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testShouldReturnResultSet_WithNonRowcountQueryPrefixes_SelectStillWorks() {
    List<String> prefixes = Arrays.asList("INSERT", "UPDATE", "DELETE");
    String query = "SELECT * FROM table";
    assertTrue(DatabricksStatement.shouldReturnResultSet(query, prefixes));
  }

  @Test
  public void testExecuteInsertWithNonRowcountQueryPrefixes() throws Exception {
    // Create connection with NonRowcountQueryPrefixes=INSERT
    String jdbcUrlWithPrefix = JDBC_URL + "NonRowcountQueryPrefixes=INSERT";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(jdbcUrlWithPrefix, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    String insertStatement = "INSERT INTO table VALUES (1, 2, 3)";

    when(client.executeStatement(
            eq(insertStatement),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    // Execute INSERT statement
    boolean hasResultSet = statement.execute(insertStatement);

    // With NonRowcountQueryPrefixes=INSERT, execute() should return true (has result set)
    assertTrue(hasResultSet, "INSERT with NonRowcountQueryPrefixes=INSERT should return true");
    assertNotNull(statement.getResultSet(), "ResultSet should be available");
    assertEquals(
        -1, statement.getUpdateCount(), "Update count should be -1 when result set is returned");

    statement.close();
  }

  @Test
  public void testExecuteInsertWithoutNonRowcountQueryPrefixes() throws Exception {
    // Create connection without NonRowcountQueryPrefixes
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    String insertStatement = "INSERT INTO table VALUES (1, 2, 3)";

    when(client.executeStatement(
            eq(insertStatement),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    // Execute INSERT statement
    boolean hasResultSet = statement.execute(insertStatement);

    // Without NonRowcountQueryPrefixes, execute() should return false (update count, not result
    // set)
    assertFalse(hasResultSet, "INSERT without NonRowcountQueryPrefixes should return false");

    statement.close();
  }
}
