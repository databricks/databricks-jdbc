package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.sqlexec.*;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.service.sql.*;
import java.io.IOException;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/* DatabricksSdkClientTest is kept in the impl folder as test DatabricksConnection constructor (visible for tests) needs to be used. */
@ExtendWith(MockitoExtension.class)
public class DatabricksSdkClientTest {
  @Mock StatementExecutionService statementExecutionService;
  @Mock ApiClient apiClient;
  @Mock ResultData resultData;
  @Mock DatabricksSession session;

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final IDatabricksComputeResource warehouse = new Warehouse(WAREHOUSE_ID);
  private static final String SESSION_ID = "session_id";
  private static final StatementId STATEMENT_ID = new StatementId("statementId");
  private static final String STATEMENT =
      "SELECT * FROM orders WHERE user_id = ? AND shard = ? AND region_code = ? AND namespace = ?";
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final Map<String, String> headers;

  static {
    headers =
        new HashMap<String, String>() {
          {
            put("Accept", "application/json");
            put("Content-Type", "application/json");
          }
        };
  }

  private void setupSessionMocks() throws IOException {
    CreateSessionResponse response = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(response);
  }

  private void setupClientMocks(boolean includeResults, boolean async) throws IOException {
    List<StatementParameterListItem> params = new ArrayList<StatementParameterListItem>();
    {
      params.add(getParam("LONG", "100", 1));
      params.add(getParam("SHORT", "10", 2));
      params.add(getParam("SHORT", "15", 3));
      params.add(getParam("STRING", "value", 4));
    }

    com.databricks.jdbc.model.core.StatementStatus statementStatus =
        new com.databricks.jdbc.model.core.StatementStatus().setState(StatementState.SUCCEEDED);
    ExecuteStatementRequest executeStatementRequest = new ExecuteStatementRequest();
    executeStatementRequest.setSessionId(SESSION_ID);
    executeStatementRequest.setWarehouseId(WAREHOUSE_ID);
    executeStatementRequest.setStatement(STATEMENT);
    executeStatementRequest.setDisposition(
        com.databricks.jdbc.model.core.Disposition.EXTERNAL_LINKS);
    executeStatementRequest.setFormat(Format.ARROW_STREAM);
    executeStatementRequest.setRowLimit(100L);
    executeStatementRequest.setParameters(params);
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

    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(response);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(new CreateSessionResponse().setSessionId(SESSION_ID));
  }

  @Test
  public void testCreateSession() throws DatabricksSQLException, IOException {
    setupSessionMocks();
    IDatabricksConnectionContext connectionContext =
        (IDatabricksConnectionContext)
            DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(
            (com.databricks.jdbc.api.internal.IDatabricksConnectionContext) connectionContext,
            statementExecutionService,
            apiClient);
    ImmutableSessionInfo sessionInfo =
        databricksSdkClient.createSession(warehouse, null, null, null);
    assertEquals(sessionInfo.sessionId(), SESSION_ID);
    assertEquals(sessionInfo.computeResource(), warehouse);
  }

  @Test
  public void testExecuteStatement() throws Exception {
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        (IDatabricksConnectionContext)
            DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(
            (com.databricks.jdbc.api.internal.IDatabricksConnectionContext) connectionContext,
            statementExecutionService,
            apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(
            (com.databricks.jdbc.api.internal.IDatabricksConnectionContext) connectionContext,
            databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    HashMap<Integer, ImmutableSqlParameter> sqlParams;
    {
      sqlParams =
          new HashMap<Integer, ImmutableSqlParameter>() {
            {
              put(1, getSqlParam(1, 100, DatabricksTypeUtil.BIGINT));
              put(2, getSqlParam(2, (short) 10, DatabricksTypeUtil.SMALLINT));
              put(3, getSqlParam(3, (byte) 15, DatabricksTypeUtil.TINYINT));
              put(4, getSqlParam(4, "value", DatabricksTypeUtil.STRING));
            }
          };
    }

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
        (IDatabricksConnectionContext)
            DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(
            (com.databricks.jdbc.api.internal.IDatabricksConnectionContext) connectionContext,
            statementExecutionService,
            apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(
            (com.databricks.jdbc.api.internal.IDatabricksConnectionContext) connectionContext,
            databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    HashMap<Integer, ImmutableSqlParameter> sqlParams;
    {
      sqlParams =
          new HashMap<Integer, ImmutableSqlParameter>() {
            {
              put(1, getSqlParam(1, 100, DatabricksTypeUtil.BIGINT));
              put(2, getSqlParam(2, (short) 10, DatabricksTypeUtil.SMALLINT));
              put(3, getSqlParam(3, (byte) 15, DatabricksTypeUtil.TINYINT));
              put(4, getSqlParam(4, "value", DatabricksTypeUtil.STRING));
            }
          };
    }

    DatabricksResultSet resultSet =
        databricksSdkClient.executeStatementAsync(
            STATEMENT, warehouse, sqlParams, connection.getSession(), statement);
    assertEquals(STATEMENT_ID, statement.getStatementId());
    assertNull(resultSet.getMetaData());
  }

  @Test
  public void testCloseStatement() throws DatabricksSQLException, IOException {
    String path = String.format(STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        (IDatabricksConnectionContext)
            DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(
            (com.databricks.jdbc.api.internal.IDatabricksConnectionContext) connectionContext,
            statementExecutionService,
            apiClient);
    CloseStatementRequest request =
        new CloseStatementRequest().setStatementId(STATEMENT_ID.toSQLExecStatementId());
    databricksSdkClient.closeStatement(STATEMENT_ID);

    verify(apiClient).execute(any(Request.class), eq(Void.class));
  }

  @Test
  public void testCancelStatement() throws DatabricksSQLException, IOException {
    String path = String.format(CANCEL_STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        (IDatabricksConnectionContext)
            DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(
            (com.databricks.jdbc.api.internal.IDatabricksConnectionContext) connectionContext,
            statementExecutionService,
            apiClient);
    CancelStatementRequest request =
        new CancelStatementRequest().setStatementId(STATEMENT_ID.toSQLExecStatementId());
    databricksSdkClient.cancelStatement(STATEMENT_ID);
    verify(apiClient).execute(any(Request.class), eq(Void.class));
  }

  private StatementParameterListItem getParam(String type, String value, int ordinal) {
    return new PositionalStatementParameterListItem()
        .setOrdinal(ordinal)
        .setType(type)
        .setValue(value);
  }

  private ImmutableSqlParameter getSqlParam(int parameterIndex, Object x, String databricksType) {
    return ImmutableSqlParameter.builder()
        .type(DatabricksTypeUtil.getColumnInfoType(databricksType))
        .value(x)
        .cardinal(parameterIndex)
        .build();
  }
}
