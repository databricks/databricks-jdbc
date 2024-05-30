package com.databricks.jdbc.core;

import static com.databricks.jdbc.client.impl.sdk.PathConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.*;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.core.types.Warehouse;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/*DatabricksSdkClientTest is kept in the core folder as test DatabricksConnection constructor (visible for tests) needs to be used*/
@ExtendWith(MockitoExtension.class)
public class DatabricksSdkClientTest {
  @Mock StatementExecutionService statementExecutionService;
  @Mock ApiClient apiClient;
  @Mock ResultData resultData;
  @Mock DatabricksSession session;

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final ComputeResource warehouse = new Warehouse(WAREHOUSE_ID);
  private static final String SESSION_ID = "session_id";
  private static final String STATEMENT_ID = "statement_id";
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

  private void setupSessionMocks() {
    CreateSessionResponse response = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.POST(eq(SESSION_PATH), any(), eq(CreateSessionResponse.class), eq(headers)))
        .thenReturn(response);
  }

  private void setupClientMocks() {
    List<StatementParameterListItem> params =
        new ArrayList<>() {
          {
            add(getParam("BIGINT", "100", 1));
            add(getParam("SHORT", "10", 2));
            add(getParam("TINYINT", "15", 3));
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
            .setWaitTimeout("10s")
            .setRowLimit(100L)
            .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE)
            .setParameters(params);
    ExecuteStatementResponse response =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID)
            .setStatus(statementStatus)
            .setResult(resultData)
            .setManifest(
                new ResultManifest()
                    .setFormat(Format.JSON_ARRAY)
                    .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L))
                    .setTotalRowCount(0L));

    when(apiClient.POST(anyString(), any(), any(), any()))
        .thenAnswer(
            invocationOnMock -> {
              String path = (String) invocationOnMock.getArguments()[0];
              if (path.equals(STATEMENT_PATH)) {
                ExecuteStatementRequest request =
                    (ExecuteStatementRequest) invocationOnMock.getArguments()[1];
                assertTrue(request.equals(executeStatementRequest));
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
  public void testDeleteSession() throws DatabricksSQLException {
    String path = String.format(DELETE_SESSION_PATH_WITH_ID, SESSION_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    when(session.getSessionId()).thenReturn(SESSION_ID);
    databricksSdkClient.deleteSession(session, warehouse);
    DeleteSessionRequest request =
        new DeleteSessionRequest().setSessionId(SESSION_ID).setWarehouseId(WAREHOUSE_ID);
    verify(apiClient).DELETE(eq(path), eq(request), eq(Void.class), eq(new HashMap<>()));
  }

  @Test
  public void testExecuteStatement() throws Exception {
    setupClientMocks();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    HashMap<Integer, ImmutableSqlParameter> sqlParams =
        new HashMap<>() {
          {
            put(1, getSqlParam(1, 100, DatabricksTypeUtil.BIGINT));
            put(2, getSqlParam(2, (short) 10, DatabricksTypeUtil.SMALLINT));
            put(3, getSqlParam(3, (byte) 15, DatabricksTypeUtil.TINYINT));
            put(4, getSqlParam(4, "value", DatabricksTypeUtil.STRING));
          }
        };

    DatabricksResultSet resultSet =
        databricksSdkClient.executeStatement(
            STATEMENT,
            warehouse,
            sqlParams,
            StatementType.QUERY,
            connection.getSession(),
            statement);
    assertEquals(STATEMENT_ID, statement.getStatementId());
  }

  @Test
  public void testCloseStatement() throws DatabricksSQLException {
    String path = String.format(STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    CloseStatementRequest request = new CloseStatementRequest().setStatementId(STATEMENT_ID);
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
    CancelStatementRequest request = new CancelStatementRequest().setStatementId(STATEMENT_ID);
    databricksSdkClient.cancelStatement(STATEMENT_ID);
    verify(apiClient).POST(eq(path), eq(request), eq(Void.class), eq(headers));
  }

  private StatementParameterListItem getParam(String type, String value, int ordinal) {
    return new PositionalStatementParameterListItem()
        .setOrdinal(ordinal)
        .setType(type)
        .setValue(value);
  }

  private ImmutableSqlParameter getSqlParam(int parameterIndex, Object x, String databricksType) {
    return ImmutableSqlParameter.builder()
        .type(databricksType)
        .value(x)
        .cardinal(parameterIndex)
        .build();
  }
}
