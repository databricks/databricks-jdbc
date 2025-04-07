package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.TEMPORARY_REDIRECT_STATUS_CODE;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksTemporaryRedirectException;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.model.client.sqlexec.*;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.model.core.Disposition;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksSdkClientTest {
  @Mock StatementExecutionService statementExecutionService;
  @Mock ApiClient apiClient;
  @Mock ResultData resultData;
  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final IDatabricksComputeResource warehouse = new Warehouse(WAREHOUSE_ID);
  private static final String SESSION_ID = "session_id";
  private static final StatementId STATEMENT_ID = new StatementId("statementId");
  private static final String STATEMENT =
      "SELECT * FROM orders WHERE user_id = ? AND shard = ? AND region_code = ? AND namespace = ?";
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final Map<String, String> headers =
      new HashMap<>() {
        {
          put("Accept", "application/json");
          put("Content-Type", "application/json");
        }
      };
  private static final Map<Integer, ImmutableSqlParameter> sqlParams =
      new HashMap<>() {
        {
          put(1, getSqlParam(1, 100, DatabricksTypeUtil.BIGINT));
          put(2, getSqlParam(2, (short) 10, DatabricksTypeUtil.SMALLINT));
          put(3, getSqlParam(3, (byte) 15, DatabricksTypeUtil.TINYINT));
          put(4, getSqlParam(4, "value", DatabricksTypeUtil.STRING));
        }
      };

  private void setupSessionMocks() {
    CreateSessionResponse response = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.POST(eq(SESSION_PATH), any(), eq(CreateSessionResponse.class), eq(headers)))
        .thenReturn(response);
  }

  private void setupClientMocks(boolean includeResults, boolean async) {
    List<StatementParameterListItem> params =
        new ArrayList<>() {
          {
            add(getParam("LONG", "100", 1));
            add(getParam("SHORT", "10", 2));
            add(getParam("SHORT", "15", 3));
            add(getParam("STRING", "value", 4));
          }
        };

    StatementStatus statementStatus = new StatementStatus().setState(StatementState.SUCCEEDED);
    ExecuteStatementRequest executeStatementRequest =
        new ExecuteStatementRequest()
            .setSessionId(SESSION_ID)
            .setWarehouseId(WAREHOUSE_ID)
            .setStatement(STATEMENT)
            .setDisposition(Disposition.EXTERNAL_LINKS)
            .setFormat(Format.ARROW_STREAM)
            .setRowLimit(100L)
            .setParameters(params);
    if (async) {
      executeStatementRequest.setWaitTimeout("0s");
    } else {
      executeStatementRequest
          .setWaitTimeout("10s")
          .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE);
    }
    ExecuteStatementResponse response =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(statementStatus);
    if (includeResults) {
      response
          .setResult(resultData)
          .setManifest(
              new ResultManifest()
                  .setFormat(Format.JSON_ARRAY)
                  .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L))
                  .setTotalRowCount(0L));
    }

    when(apiClient.POST(anyString(), any(), any(), any()))
        .thenAnswer(
            invocationOnMock -> {
              String path = (String) invocationOnMock.getArguments()[0];
              if (path.equals(STATEMENT_PATH)) {
                ExecuteStatementRequest request =
                    (ExecuteStatementRequest) invocationOnMock.getArguments()[1];
                assertEquals(request, executeStatementRequest);
                return response;
              } else if (path.equals(SESSION_PATH)) {
                CreateSessionRequest request =
                    (CreateSessionRequest) invocationOnMock.getArguments()[1];
                assertEquals(request.getWarehouseId(), WAREHOUSE_ID);
                return new CreateSessionResponse().setSessionId(SESSION_ID);
              }
              return null;
            });
  }

  @Test
  public void testCreateSession() throws DatabricksSQLException {
    setupSessionMocks();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSessionInfo sessionInfo =
        databricksSdkClient.createSession(warehouse, null, null, null);
    assertEquals(sessionInfo.sessionId(), SESSION_ID);
    assertEquals(sessionInfo.computeResource(), warehouse);
  }

  @Test
  public void testCreateSessionRedirect() throws DatabricksSQLException {
    // Create a DatabricksError with 307 status code to simulate the temporary redirect.
    DatabricksError redirectError =
        new DatabricksError("307", "Redirect to Thrift Client", TEMPORARY_REDIRECT_STATUS_CODE);

    // When the POST is called with the SESSION_PATH, throw the redirect error.
    when(apiClient.POST(eq(SESSION_PATH), any(), eq(CreateSessionResponse.class), eq(headers)))
        .thenThrow(redirectError);

    // Set up the connection context and the client.
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    // Assert that createSession throws a DatabricksTemporaryRedirectException.
    assertThrows(
        DatabricksTemporaryRedirectException.class,
        () -> databricksSdkClient.createSession(warehouse, null, null, null));
  }

  @Test
  public void testDeleteSession() throws DatabricksSQLException {
    String path = String.format(SESSION_PATH_WITH_ID, SESSION_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(new Warehouse(WAREHOUSE_ID))
            .build();
    databricksSdkClient.deleteSession(sessionInfo);
    DeleteSessionRequest request =
        new DeleteSessionRequest().setSessionId(SESSION_ID).setWarehouseId(WAREHOUSE_ID);
    verify(apiClient).DELETE(eq(path), eq(request), eq(Void.class), eq(headers));
  }

  @Test
  public void testExecuteStatement() throws Exception {
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);

    DatabricksResultSet resultSet =
        databricksSdkClient.executeStatement(
            STATEMENT,
            warehouse,
            sqlParams,
            StatementType.QUERY,
            connection.getSession(),
            statement);
    assertEquals(STATEMENT_ID, statement.getStatementId());
    assertNotNull(resultSet.getMetaData());
  }

  @Test
  public void testExecuteStatementAsync() throws Exception {
    setupClientMocks(false, true);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);

    DatabricksResultSet resultSet =
        databricksSdkClient.executeStatementAsync(
            STATEMENT, warehouse, sqlParams, connection.getSession(), statement);
    assertEquals(STATEMENT_ID, statement.getStatementId());
    assertNull(resultSet.getMetaData());
  }

  @Test
  public void testCloseStatement() throws DatabricksSQLException {
    String path = String.format(STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    CloseStatementRequest request =
        new CloseStatementRequest().setStatementId(STATEMENT_ID.toSQLExecStatementId());
    databricksSdkClient.closeStatement(STATEMENT_ID);

    verify(apiClient).DELETE(eq(path), eq(request), eq(Void.class), eq(headers));
  }

  @Test
  public void testCancelStatement() throws DatabricksSQLException {
    String path = String.format(CANCEL_STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    CancelStatementRequest request =
        new CancelStatementRequest().setStatementId(STATEMENT_ID.toSQLExecStatementId());
    databricksSdkClient.cancelStatement(STATEMENT_ID);
    verify(apiClient).POST(eq(path), eq(request), eq(Void.class), eq(headers));
  }

  @Test
  public void testGetDatabricksConfig() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    assertNotNull(databricksSdkClient.getDatabricksConfig());
  }

  @Test
  public void testExecuteStatementWithTimeout() throws Exception {
    // Set up connection context and client
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.POST(eq(SESSION_PATH), any(), eq(CreateSessionResponse.class), eq(headers)))
        .thenReturn(sessionResponse);
    connection.open();

    // Create statement with a 10-second timeout (long enough)
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    statement.setQueryTimeout(10);

    // Create statement execution mocks
    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse runningStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse successStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));
    // First POST will create the statement, subsequent GETs will be for status checks
    when(apiClient.POST(eq(STATEMENT_PATH), any(), eq(ExecuteStatementResponse.class), eq(headers)))
        .thenReturn(executeResponse);
    when(apiClient.GET(any(), any(), eq(GetStatementResponse.class), eq(headers)))
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(successStatementResponse);

    assertDoesNotThrow(
        () ->
            databricksSdkClient.executeStatement(
                STATEMENT,
                warehouse,
                sqlParams,
                StatementType.QUERY,
                connection.getSession(),
                statement));

    // Verify that a POST was made exactly once (no cancellation due to timeout)
    verify(apiClient, times(2))
        .POST(anyString(), any(), any(), any()); // Once for session, once for statement
  }

  @Test
  public void testExecuteStatementWithTimeoutExpired() throws Exception {
    // Set up connection context and client. Async exec poll interval is set to 1 second to
    // facilitate timeout
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(
            JDBC_URL,
            new Properties() {
              {
                setProperty("asyncExecPollInterval", "1000");
              }
            });
    DatabricksSdkClient databricksSdkClient =
        spy(new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.POST(eq(SESSION_PATH), any(), eq(CreateSessionResponse.class), eq(headers)))
        .thenReturn(sessionResponse);
    connection.open();

    // Create statement with a very short timeout (1 second)
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    statement.setQueryTimeout(1);

    // Create statement execution mocks
    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse runningStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse successStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));
    // First POST will create the statement, subsequent GETs will be for status checks
    when(apiClient.POST(eq(STATEMENT_PATH), any(), eq(ExecuteStatementResponse.class), eq(headers)))
        .thenReturn(executeResponse);
    when(apiClient.GET(any(), any(), eq(GetStatementResponse.class), eq(headers)))
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(successStatementResponse);

    // Verify that the timeout exception (1 second) is thrown due to repeated polling, where each
    // poll occurs at an interval of 1 second
    DatabricksTimeoutException exception =
        assertThrows(
            DatabricksTimeoutException.class,
            () ->
                databricksSdkClient.executeStatement(
                    STATEMENT,
                    warehouse,
                    sqlParams,
                    StatementType.QUERY,
                    connection.getSession(),
                    statement));

    assertTrue(exception.getMessage().contains("timed-out after 1 seconds"));

    // Verify cancel was called
    verify(databricksSdkClient).cancelStatement(eq(STATEMENT_ID));
  }

  private static ImmutableSqlParameter getSqlParam(
      int parameterIndex, Object x, String databricksType) {
    return ImmutableSqlParameter.builder()
        .type(DatabricksTypeUtil.getColumnInfoType(databricksType))
        .value(x)
        .cardinal(parameterIndex)
        .build();
  }

  private StatementParameterListItem getParam(String type, String value, int ordinal) {
    return new PositionalStatementParameterListItem()
        .setOrdinal(ordinal)
        .setType(type)
        .setValue(value);
  }
}
