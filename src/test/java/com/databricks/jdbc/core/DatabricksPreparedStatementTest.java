package com.databricks.jdbc.core;

import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementRequestWithSession;
import com.databricks.jdbc.client.sqlexec.PositionalStatementParameterListItem;
import com.databricks.jdbc.client.sqlexec.Session;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.*;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DatabricksPreparedStatementTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";
  private static final String STATEMENT_ID = "statement_id";
  private static final String STATEMENT = "SELECT * FROM orders WHERE user_id = ? AND shard = ? AND region_code = ? AND namespace = ?";
  private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";


  @Mock
  StatementExecutionService statementExecutionService;
  @Mock
  ApiClient apiClient;

  @Test
  public void testExecuteStatement() throws Exception {
    CreateSessionRequest createSessionRequest = new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    headers.put("Content-Type", "application/json");
    when(apiClient.POST("/api/2.0/sql/statements/sessions", createSessionRequest,
        Session.class, headers)).thenReturn(new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID));

    List<StatementParameterListItem> params = new ArrayList<>();
    params.add(getParam("BIGINT", "100", 1));
    params.add(getParam("SMALLINT", "10", 2));
    params.add(getParam("TINYINT", "15", 3));
    params.add(getParam("STRING", "value", 4));
    ExecuteStatementRequestWithSession executeStatementRequest = (ExecuteStatementRequestWithSession)
        new ExecuteStatementRequestWithSession()
            .setSessionId(SESSION_ID)
            .setWarehouseId(WAREHOUSE_ID)
            .setStatement(STATEMENT)
            .setDisposition(Disposition.EXTERNAL_LINKS)
            .setFormat(Format.ARROW_STREAM)
            .setWaitTimeout("10s")
            .setOnWaitTimeout(TimeoutAction.CONTINUE)
            .setParameters(params);
    when(statementExecutionService.executeStatement(executeStatementRequest))
        .thenReturn(new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID)
            .setStatus(new StatementStatus().setState(StatementState.PENDING)));

    GetStatementResponse getResponsePending = new GetStatementResponse()
        .setStatementId(STATEMENT_ID)
        .setStatus(new StatementStatus().setState(StatementState.PENDING));
    GetStatementResponse getResponseSuccessful = new GetStatementResponse()
        .setStatementId(STATEMENT_ID)
        .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
        .setManifest(new ResultManifest()
            .setFormat(Format.ARROW_STREAM)
            .setTotalRowCount(0L).setTotalChunkCount(0L)
            .setChunks(new ArrayList<>())
            .setSchema(new ResultSchema()
                .setColumns(new ArrayList<>())))
        .setResult(new ResultData().setExternalLinks(new ArrayList<>()));
    GetStatementRequest getStatementRequest = new GetStatementRequest().setStatementId(STATEMENT_ID);
    when(statementExecutionService.getStatement(getStatementRequest))
        .thenReturn(getResponsePending, getResponseSuccessful);

    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext,
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    DatabricksPreparedStatement statement = (DatabricksPreparedStatement) connection.prepareStatement(STATEMENT);
    statement.setLong(1, 100);
    statement.setShort(2, (short) 10);
    statement.setByte(3, (byte) 15);
    statement.setString(4, "value");

    DatabricksResultSet resultSet = (DatabricksResultSet) statement.executeQuery();
    assertFalse(resultSet.hasUpdateCount());
    assertFalse(statement.isClosed());
    assertFalse(resultSet.isClosed());

    statement.close();
    assertTrue(statement.isClosed());
    assertTrue(resultSet.isClosed());

    // TODO: add more assertions
    verify(statementExecutionService, Mockito.times(1))
        .executeStatement(executeStatementRequest);
    verify(statementExecutionService, Mockito.times(2))
        .getStatement(getStatementRequest);
  }

  @Test
  public void testExecuteUpdateStatement() throws Exception {
    CreateSessionRequest createSessionRequest = new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    headers.put("Content-Type", "application/json");
    when(apiClient.POST("/api/2.0/sql/statements/sessions", createSessionRequest,
        Session.class, headers)).thenReturn(new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID));

    List<StatementParameterListItem> params = new ArrayList<>();
    ExecuteStatementRequestWithSession executeStatementRequest = (ExecuteStatementRequestWithSession)
        new ExecuteStatementRequestWithSession()
            .setSessionId(SESSION_ID)
            .setWarehouseId(WAREHOUSE_ID)
            .setStatement(STATEMENT)
            .setDisposition(Disposition.INLINE)
            .setFormat(Format.JSON_ARRAY)
            .setWaitTimeout("10s")
            .setOnWaitTimeout(TimeoutAction.CONTINUE)
            .setParameters(params);
    when(statementExecutionService.executeStatement(executeStatementRequest))
        .thenReturn(new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID)
            .setStatus(new StatementStatus().setState(StatementState.PENDING)));

    GetStatementResponse getResponsePending = new GetStatementResponse()
        .setStatementId(STATEMENT_ID)
        .setStatus(new StatementStatus().setState(StatementState.PENDING));
    GetStatementResponse getResponseSuccessful = new GetStatementResponse()
        .setStatementId(STATEMENT_ID)
        .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
        .setManifest(new ResultManifest()
            .setFormat(Format.JSON_ARRAY)
            .setTotalRowCount(1L).setTotalChunkCount(1L)
            .setChunks(new ArrayList<>())
            .setSchema(new ResultSchema()
                .setColumns(ImmutableList.of(new ColumnInfo()
                    .setName("num_affected_rows")
                    .setTypeText("Long")
                    .setTypeName(ColumnInfoTypeName.LONG)
                    .setPosition(0L)))))
        .setResult(new ResultData()
            .setDataArray(ImmutableList.of(ImmutableList.of("2"))));
    GetStatementRequest getStatementRequest = new GetStatementRequest().setStatementId(STATEMENT_ID);
    when(statementExecutionService.getStatement(getStatementRequest))
        .thenReturn(getResponsePending, getResponseSuccessful);

    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext,
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    DatabricksPreparedStatement statement = (DatabricksPreparedStatement) connection.prepareStatement(STATEMENT);
    int updateCount = statement.executeUpdate();

    assertEquals(2, updateCount);
    assertTrue(statement.resultSet.hasUpdateCount());
    assertFalse(statement.isClosed());
    // TODO: add more assertions
    verify(statementExecutionService, Mockito.times(1))
        .executeStatement(executeStatementRequest);
    verify(statementExecutionService, Mockito.times(2))
        .getStatement(getStatementRequest);

    // close the statement
    statement.close();
    assertTrue(statement.isClosed());
  }

  private StatementParameterListItem getParam(String type, String value, int ordinal) {
    return new PositionalStatementParameterListItem()
        .setOrdinal(ordinal)
        .setType(type)
        .setValue(value);
  }
}
