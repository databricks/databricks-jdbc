package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementRequestWithSession;
import com.databricks.jdbc.client.sqlexec.PositionalStatementParameterListItem;
import com.databricks.jdbc.client.sqlexec.Session;
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

  private static final String WAREHOUSE_ID = "erg6767gg";
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

  private static final String CLIENT_PATH = "/api/2.0/sql/statements/sessions";

  @Test
  public void testCreateSession() {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    CreateSessionRequest createSessionRequest =
        new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
    Session session = new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID);
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    when(apiClient.POST(CLIENT_PATH, createSessionRequest, Session.class, headers))
        .thenReturn(session);

    ImmutableSessionInfo sessionInfo = databricksSdkClient.createSession(WAREHOUSE_ID);
    assertEquals(sessionInfo.sessionId(), SESSION_ID);
    assertEquals(sessionInfo.warehouseId(), WAREHOUSE_ID);
  }

  @Test
  public void testExecuteStatement() throws Exception {
    List<StatementParameterListItem> params =
        new ArrayList<>() {
          {
            add(getParam("BIGINT", "100", 1));
            add(getParam("SMALLINT", "10", 2));
            add(getParam("TINYINT", "15", 3));
            add(getParam("STRING", "value", 4));
          }
        };

    GetStatementResponse getResponsePending =
        new GetStatementResponse()
            .setStatementId(STATEMENT_ID)
            .setStatus(new StatementStatus().setState(StatementState.PENDING));
    GetStatementResponse getResponseSuccessful =
        new GetStatementResponse()
            .setStatementId(STATEMENT_ID)
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setManifest(
                new ResultManifest()
                    .setFormat(Format.ARROW_STREAM)
                    .setTotalRowCount(0L)
                    .setTotalChunkCount(0L)
                    .setChunks(new ArrayList<>())
                    .setSchema(new ResultSchema().setColumns(new ArrayList<>())))
            .setResult(new ResultData().setExternalLinks(new ArrayList<>()));
    GetStatementRequest getStatementRequest =
        new GetStatementRequest().setStatementId(STATEMENT_ID);

    when(statementExecutionService.getStatement(getStatementRequest))
        .thenReturn(getResponsePending, getResponseSuccessful);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    CreateSessionRequest createSessionRequest =
        new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
    Session session = new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID);
    when(apiClient.POST(CLIENT_PATH, createSessionRequest, Session.class, headers))
        .thenReturn(session);

    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    ExecuteStatementRequestWithSession executeStatementRequest =
        (ExecuteStatementRequestWithSession)
            new ExecuteStatementRequestWithSession()
                .setSessionId(connection.getSession().getSessionId())
                .setWarehouseId(WAREHOUSE_ID)
                .setStatement(STATEMENT)
                .setDisposition(Disposition.EXTERNAL_LINKS)
                .setFormat(Format.ARROW_STREAM)
                .setWaitTimeout("10s")
                .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE)
                .setParameters(params);

    when(statementExecutionService.executeStatement(executeStatementRequest))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId(STATEMENT_ID)
                .setStatus(new StatementStatus().setState(StatementState.PENDING)));

    DatabricksStatement statement = new DatabricksStatement(connection);
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
            WAREHOUSE_ID,
            sqlParams,
            StatementType.QUERY,
            connection.getSession(),
            statement);
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
