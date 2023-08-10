package com.databricks.jdbc.core;

import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.service.sql.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DatabricksStatementTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";
  private static final String STATEMENT_ID = "statement_id";
  private static final String STATEMENT = "select 1";
  private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";


  @Mock
  StatementExecutionService statementExecutionService;

  @Test
  public void testExecuteStatement() throws Exception {
    CreateSessionRequest createSessionRequest = new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
    when(statementExecutionService.createSession(createSessionRequest))
        .thenReturn(new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID));

    ExecuteStatementRequest executeStatementRequest =
        new ExecuteStatementRequest()
            .setWarehouseId(WAREHOUSE_ID)
            .setSessionId(SESSION_ID)
            .setStatement(STATEMENT)
            .setDisposition(Disposition.EXTERNAL_LINKS)
            .setFormat(Format.ARROW_STREAM)
            .setWaitTimeout("0s");
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
            .setTotalRowCount(1L).setTotalChunkCount(1L)
            .setChunks(new ArrayList<>())
            .setSchema(new ResultSchema()
                .setColumns(new ArrayList<>())))
        .setResult(new ResultData().setExternalLinks(new ArrayList<>()));
    GetStatementRequest getStatementRequest = new GetStatementRequest().setStatementId(STATEMENT_ID);
    when(statementExecutionService.getStatement(getStatementRequest))
        .thenReturn(getResponsePending, getResponseSuccessful);

    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext,
        new DatabricksSdkClient(connectionContext, statementExecutionService));
    ResultSet resultSet = connection.createStatement().executeQuery(STATEMENT);

    // TODO: add more assertions
    verify(statementExecutionService, Mockito.times(1))
        .executeStatement(executeStatementRequest);
    verify(statementExecutionService, Mockito.times(2))
        .getStatement(getStatementRequest);
  }
}
