package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.QUERY_EXECUTION_TIMEOUT_SQLSTATE;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_BYTE_LIMIT;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_ROW_LIMIT_PER_BLOCK;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftAccessorTest {

  @Mock TCLIService.Client thriftClient;
  @Mock IDatabricksSession session;
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock IDatabricksStatementInternal parentStatement;
  private static DatabricksThriftAccessor accessor;
  private static final String TEST_STMT_ID =
      "01efc77c-7c8b-1a8e-9ecb-a9a6e6aa050a|338d529d-8272-46eb-8482-cb419466839d";
  private static final THandleIdentifier handleIdentifier =
      StatementId.deserialize(TEST_STMT_ID).toOperationIdentifier();
  private static final TOperationHandle tOperationHandle =
      new TOperationHandle().setOperationId(handleIdentifier).setHasResultSet(false);
  private static final TRowSet rowSet = new TRowSet().setResultLinks(new ArrayList<>(2));
  private static final TFetchResultsResp fetchResultsResponse =
      new TFetchResultsResp()
          .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
          .setResultSetMetadata(
              new TGetResultSetMetadataResp().setResultFormat(TSparkRowSetType.COLUMN_BASED_SET))
          .setResults(rowSet);
  private static final TSparkDirectResults directResults =
      new TSparkDirectResults()
          .setResultSet(fetchResultsResponse)
          .setResultSetMetadata(
              new TGetResultSetMetadataResp()
                  .setResultFormat(TSparkRowSetType.COLUMN_BASED_SET)
                  .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)))
          .setOperationStatus(
              new TGetOperationStatusResp()
                  .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
                  .setOperationState(TOperationState.FINISHED_STATE));
  private static final TGetOperationStatusReq operationStatusReq =
      new TGetOperationStatusReq().setOperationHandle(tOperationHandle).setGetProgressUpdate(false);
  private static final TGetOperationStatusResp operationStatusFinishedResp =
      new TGetOperationStatusResp()
          .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
          .setOperationState(TOperationState.FINISHED_STATE);
  private static final TGetOperationStatusResp operationStatusRunningResp =
      new TGetOperationStatusResp()
          .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
          .setOperationState(TOperationState.RUNNING_STATE);

  private MockedStatic<DatabricksClientConfiguratorManager> configuratorManagerStatic;
  private DatabricksClientConfiguratorManager configuratorManager;

  @BeforeEach
  void initConfiguratorManager() throws DatabricksParsingException, DatabricksValidationException {
    configuratorManagerStatic = mockStatic(DatabricksClientConfiguratorManager.class);
    configuratorManager = mock(DatabricksClientConfiguratorManager.class);
    configuratorManagerStatic
        .when(DatabricksClientConfiguratorManager::getInstance)
        .thenReturn(configuratorManager);
    ClientConfigurator mockConfigurator = mock(ClientConfigurator.class);
    lenient().when(mockConfigurator.getDatabricksConfig()).thenReturn(new DatabricksConfig());
    lenient()
        .when(configuratorManager.getConfigurator(any(IDatabricksConnectionContext.class)))
        .thenReturn(mockConfigurator);
    // Provide common defaults used in constructor and various tests
    lenient()
        .when(connectionContext.getRowsFetchedPerBlock())
        .thenReturn(DEFAULT_ROW_LIMIT_PER_BLOCK);
    lenient().when(connectionContext.getAsyncExecPollInterval()).thenReturn(1000);
    lenient().when(connectionContext.getEndpointURL()).thenReturn("http://localhost");
  }

  @AfterEach
  void cleanupConfiguratorManager() {
    if (configuratorManagerStatic != null) {
      configuratorManagerStatic.close();
    }
  }

  void setup(Boolean directResultsEnabled)
      throws DatabricksParsingException, DatabricksValidationException {
    lenient().when(connectionContext.getDirectResultMode()).thenReturn(directResultsEnabled);
    lenient()
        .when(connectionContext.getRowsFetchedPerBlock())
        .thenReturn(DEFAULT_ROW_LIMIT_PER_BLOCK);
    accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();
  }

  @Test
  void testOpenSession() throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TOpenSessionReq request = new TOpenSessionReq();
    TOpenSessionResp response = new TOpenSessionResp();
    when(thriftClient.OpenSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request), response);
  }

  @Test
  void testCloseSession() throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TCloseSessionReq request = new TCloseSessionReq();
    TCloseSessionResp response = new TCloseSessionResp();
    when(thriftClient.CloseSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request), response);
  }

  @Test
  void testExecute() throws TException, SQLException, DatabricksValidationException {
    setup(false);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(getFetchResultsRequest(true))).thenReturn(fetchResultsResponse);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    Statement statement = mock(Statement.class);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(0);
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);
    DatabricksResultSet resultSet =
        accessor.execute(request, parentStatement, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteAsync() throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);
    DatabricksResultSet resultSet =
        accessor.executeAsync(request, parentStatement, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.RUNNING);
  }

  @Test
  void testExecuteAsync_error()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(true);

    TExecuteStatementReq request = new TExecuteStatementReq();
    when(thriftClient.ExecuteStatement(request)).thenThrow(new TException("failed"));
    assertThrows(
        DatabricksHttpException.class,
        () -> accessor.executeAsync(request, null, session, StatementType.SQL));
  }

  @Test
  void testExecuteAsync_SQLState()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(true);

    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.ERROR_STATUS).setSqlState("42601"));
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> accessor.executeAsync(request, null, session, StatementType.SQL));
    assertEquals("42601", exception.getSQLState());
  }

  @Test
  void testExecuteThrowsThriftError()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(true);
    TExecuteStatementReq request = new TExecuteStatementReq();
    when(thriftClient.ExecuteStatement(request)).thenThrow(TException.class);
    assertThrows(
        DatabricksHttpException.class,
        () -> accessor.execute(request, null, session, StatementType.SQL));
  }

  @Test
  void testExecuteWithParentStatement()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    Statement statement = mock(Statement.class);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(0);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);
    DatabricksResultSet resultSet =
        accessor.execute(request, parentStatement, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);
    DatabricksResultSet resultSet = accessor.execute(request, null, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteWithoutDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    lenient().when(connectionContext.getDirectResultMode()).thenReturn(false);
    lenient()
        .when(connectionContext.getRowsFetchedPerBlock())
        .thenReturn(DEFAULT_ROW_LIMIT_PER_BLOCK);
    accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);
    DatabricksResultSet resultSet = accessor.execute(request, null, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecute_throwsException()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    when(connectionContext.getDirectResultMode()).thenReturn(false);
    when(connectionContext.getRowsFetchedPerBlock()).thenReturn(DEFAULT_ROW_LIMIT_PER_BLOCK);
    accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(new TOperationHandle())
            .setStatus(
                new TStatus()
                    .setStatusCode(TStatusCode.ERROR_STATUS)
                    .setErrorMessage("Test Error Message"));
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> accessor.execute(request, null, session, StatementType.SQL));
    assert (e.getMessage().contains("Test Error Message"));
  }

  @Test
  void testExecuteThrowsSQLExceptionWithSqlState()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(true);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(
                new TStatus()
                    .setStatusCode(TStatusCode.ERROR_STATUS) // Simulate an error
                    .setErrorMessage("Error executing statement")
                    .setSqlState("42000")); // Example SQL state

    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> accessor.execute(request, null, session, StatementType.SQL));

    assertEquals("Error executing statement", exception.getMessage());
    assertEquals("42000", exception.getSQLState());
  }

  @Test
  void testCancelOperation() throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TCancelOperationReq request =
        new TCancelOperationReq()
            .setOperationHandle(
                new TOperationHandle()
                    .setOperationId(handleIdentifier)
                    .setOperationType(TOperationType.UNKNOWN));
    TCancelOperationResp response =
        new TCancelOperationResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.CancelOperation(request)).thenReturn(response);
    assertEquals(accessor.cancelOperation(request), response);
  }

  @Test
  void testCloseOperation() throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TCloseOperationReq request =
        new TCloseOperationReq()
            .setOperationHandle(
                new TOperationHandle()
                    .setOperationId(handleIdentifier)
                    .setOperationType(TOperationType.UNKNOWN));
    TCloseOperationResp response =
        new TCloseOperationResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.CloseOperation(request)).thenReturn(response);
    assertEquals(accessor.closeOperation(request), response);
  }

  @Test
  void testCancelOperation_error()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(true);

    TCancelOperationReq request =
        new TCancelOperationReq()
            .setOperationHandle(
                new TOperationHandle()
                    .setOperationId(handleIdentifier)
                    .setOperationType(TOperationType.UNKNOWN));
    when(thriftClient.CancelOperation(request)).thenThrow(new TException("failed"));
    assertThrows(DatabricksHttpException.class, () -> accessor.cancelOperation(request));
  }

  @Test
  void testCloseOperation_error()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(true);

    TCloseOperationReq request =
        new TCloseOperationReq()
            .setOperationHandle(
                new TOperationHandle()
                    .setOperationId(handleIdentifier)
                    .setOperationType(TOperationType.UNKNOWN));
    when(thriftClient.CloseOperation(request)).thenThrow(new TException("failed"));
    assertThrows(DatabricksHttpException.class, () -> accessor.closeOperation(request));
  }

  @Test
  void testIncludeResultSetMetadataNotSetForOldProtocol() throws TException, SQLException {
    TOperationHandle operationHandle =
        new TOperationHandle()
            .setOperationId(handleIdentifier)
            .setHasResultSet(false)
            .setOperationType(TOperationType.UNKNOWN);
    DatabricksThriftAccessor accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();
    accessor.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4);
    TFetchResultsReq expectedReq = getFetchResultsRequest(false);
    expectedReq.setOperationHandle(operationHandle);

    when(thriftClient.FetchResults(expectedReq))
        .thenReturn(fetchResultsResponse); // request has no includeResultSetMetadata
    when(parentStatement.getStatementId()).thenReturn(StatementId.deserialize(TEST_STMT_ID));
    accessor.getMoreResults(parentStatement);

    accessor.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9);
    expectedReq = getFetchResultsRequest(true);
    expectedReq.setOperationHandle(operationHandle);
    when(thriftClient.FetchResults(expectedReq))
        .thenReturn(fetchResultsResponse); // request has includeResultSetMetadata
    accessor.getMoreResults(parentStatement);
  }

  @Test
  void testGetStatementResult_success() throws Exception {
    when(connectionContext.getDirectResultMode()).thenReturn(false);
    accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    TFetchResultsReq fetchReq =
        new TFetchResultsReq()
            .setOperationHandle(tOperationHandle)
            .setFetchType((short) 0) // 0 represents Query output. 1 represents Log
            .setMaxRows(-1)
            .setIncludeResultSetMetadata(true)
            .setMaxBytes(DEFAULT_BYTE_LIMIT);
    when(thriftClient.FetchResults(fetchReq)).thenReturn(fetchResultsResponse);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);
    DatabricksResultSet resultSet = accessor.getStatementResult(tOperationHandle, null, session);
    assertEquals(StatementState.SUCCEEDED, resultSet.getStatementStatus().getState());
    assertNotNull(resultSet.getMetaData());
  }

  @Test
  void testGetStatementResult_pending() throws Exception {
    when(connectionContext.getDirectResultMode()).thenReturn(false);
    accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();
    TGetOperationStatusResp resp =
        new TGetOperationStatusResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.STILL_EXECUTING_STATUS))
            .setOperationState(TOperationState.RUNNING_STATE);
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(resp);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);
    DatabricksResultSet resultSet = accessor.getStatementResult(tOperationHandle, null, session);
    assertEquals(StatementState.RUNNING, resultSet.getStatementStatus().getState());
    assertNull(resultSet.getMetaData());
  }

  @Test
  void testListPrimaryKeys() throws TException, SQLException, DatabricksValidationException {
    setup(false);
    TGetPrimaryKeysReq request = new TGetPrimaryKeysReq();
    TGetPrimaryKeysResp tGetPrimaryKeysResp =
        new TGetPrimaryKeysResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(fetchResultsResponse);
    when(thriftClient.GetPrimaryKeys(request)).thenReturn(tGetPrimaryKeysResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListPrimaryKeysWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetPrimaryKeysReq request = new TGetPrimaryKeysReq();
    TGetPrimaryKeysResp tGetPrimaryKeysResp =
        new TGetPrimaryKeysResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetPrimaryKeys(request)).thenReturn(tGetPrimaryKeysResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListFunctions() throws TException, SQLException, DatabricksValidationException {
    setup(false);
    TGetFunctionsReq request = new TGetFunctionsReq();
    TGetFunctionsResp tGetFunctionsResp =
        new TGetFunctionsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(fetchResultsResponse);
    when(thriftClient.GetFunctions(request)).thenReturn(tGetFunctionsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListFunctionsWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetFunctionsReq request = new TGetFunctionsReq();
    TGetFunctionsResp tGetFunctionsResp =
        new TGetFunctionsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetFunctions(request)).thenReturn(tGetFunctionsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListSchemas() throws TException, SQLException, DatabricksValidationException {
    setup(false);
    TGetSchemasReq request = new TGetSchemasReq();
    TGetSchemasResp tGetSchemasResp =
        new TGetSchemasResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(fetchResultsResponse);
    when(thriftClient.GetSchemas(request)).thenReturn(tGetSchemasResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListSchemasWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetSchemasReq request = new TGetSchemasReq();
    TGetSchemasResp tGetSchemasResp =
        new TGetSchemasResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetSchemas(request)).thenReturn(tGetSchemasResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListColumns() throws TException, SQLException, DatabricksValidationException {
    setup(false);
    TGetColumnsReq request = new TGetColumnsReq();
    TGetColumnsResp tGetColumnsResp =
        new TGetColumnsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(fetchResultsResponse);
    when(thriftClient.GetColumns(request)).thenReturn(tGetColumnsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListColumnsWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetColumnsReq request = new TGetColumnsReq();
    TGetColumnsResp tGetColumnsResp =
        new TGetColumnsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetColumns(request)).thenReturn(tGetColumnsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListCatalogs() throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetCatalogsReq request = new TGetCatalogsReq();
    TGetCatalogsResp tGetCatalogsResp =
        new TGetCatalogsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(fetchResultsResponse);
    when(thriftClient.GetCatalogs(request)).thenReturn(tGetCatalogsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListCatalogsWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetCatalogsReq request = new TGetCatalogsReq();
    TGetCatalogsResp tGetCatalogsResp =
        new TGetCatalogsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetCatalogs(request)).thenReturn(tGetCatalogsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListTables() throws TException, SQLException, DatabricksValidationException {
    setup(false);
    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(fetchResultsResponse);
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListTablesWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListTableTypes() throws TException, SQLException, DatabricksValidationException {
    setup(false);
    TGetTableTypesReq request = new TGetTableTypesReq();
    TGetTableTypesResp tGetTableTypesResp =
        new TGetTableTypesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(fetchResultsResponse);
    when(thriftClient.GetTableTypes(request)).thenReturn(tGetTableTypesResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testListTableTypesWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetTableTypesReq request = new TGetTableTypesReq();
    TGetTableTypesResp tGetTableTypesResp =
        new TGetTableTypesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetTableTypes(request)).thenReturn(tGetTableTypesResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testTypeInfo() throws TException, SQLException, DatabricksValidationException {
    setup(false);
    TGetTypeInfoReq request = new TGetTypeInfoReq();
    TGetTypeInfoResp tGetTypeInfoResp =
        new TGetTypeInfoResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(fetchResultsResponse);
    when(thriftClient.GetTypeInfo(request)).thenReturn(tGetTypeInfoResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testTypeInfoWithDirectResults()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TGetTypeInfoReq request = new TGetTypeInfoReq();
    TGetTypeInfoResp tGetTypeInfoResp =
        new TGetTypeInfoResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetTypeInfo(request)).thenReturn(tGetTypeInfoResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, fetchResultsResponse);
  }

  @Test
  void testAccessorWhenFetchResultsThrowsError()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(false);

    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenThrow(new TException());
    assertThrows(DatabricksSQLException.class, () -> accessor.getThriftResponse(request));
  }

  @Test
  void testAccessorDuringThriftError()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(true);

    TGetTablesReq request = new TGetTablesReq();
    when(thriftClient.GetTables(request)).thenThrow(new TException());
    assertThrows(DatabricksSQLException.class, () -> accessor.getThriftResponse(request));
  }

  @Test
  void testAccessorDuringHTTPError()
      throws TException, DatabricksParsingException, DatabricksValidationException {
    setup(true);

    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.ERROR_STATUS).setSqlState("08000"));
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    DatabricksSQLException sqlException =
        assertThrows(DatabricksSQLException.class, () -> accessor.getThriftResponse(request));
    assertEquals("08000", sqlException.getSQLState());
  }

  @Test
  void testExecute_setsStatementIdEvenIfStatusRequestFails()
      throws TException, SQLException, DatabricksValidationException {
    setup(true);
    TExecuteStatementReq request = new TExecuteStatementReq();

    // Prepare successful execute statement response
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));

    // Make execute statement succeed but get operation status fail
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(thriftClient.GetOperationStatus(any(TGetOperationStatusReq.class)))
        .thenThrow(new TException("Failed to get status"));
    Statement statement = mock(Statement.class);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(0);

    // Prepare parent statement for verification
    StatementId expectedStatementId = new StatementId(tOperationHandle.getOperationId());

    try {
      accessor.execute(request, parentStatement, session, StatementType.SQL);
      fail("Expected exception due to GetOperationStatus failure");
    } catch (DatabricksHttpException e) {
      // Verify that statement ID was set on parent statement despite the failure
      verify(parentStatement).setStatementId(eq(expectedStatementId));

      // Verify the error was from GetOperationStatus
      assertTrue(e.getMessage().contains("Failed to get status"));
    }
  }

  @Test
  void testExecuteWithTimeout() throws TException, SQLException, DatabricksValidationException {
    // Set the async poll interval to 200 ms
    when(connectionContext.getAsyncExecPollInterval()).thenReturn(200);

    accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();

    // Create statement execution mocks
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(true))).thenReturn(fetchResultsResponse);
    // Mock the behavior where the first few status checks show the operation is still running
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusRunningResp)
        .thenReturn(operationStatusRunningResp)
        .thenReturn(operationStatusFinishedResp);

    // Set a 10-second (long enough) timeout on the statement
    Statement statement = mock(Statement.class);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(10);

    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);

    DatabricksResultSet resultSet =
        accessor.execute(request, parentStatement, session, StatementType.SQL);

    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);

    // Verify that cancelStatement was not called (no timeout occurred)
    verify(thriftClient, never()).CancelOperation(any());
  }

  @Test
  void testExecuteWithTimeoutExpired() throws TException, SQLException {
    // Set the async poll interval to 1 second to facilitate testing
    when(connectionContext.getAsyncExecPollInterval()).thenReturn(1000);

    accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();

    // Create statement execution mocks
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    // Mock the behavior where the first few status checks show the operation is still running
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusRunningResp)
        .thenReturn(operationStatusRunningResp)
        .thenReturn(operationStatusFinishedResp);

    // Set a short timeout on the statement
    Statement statement = mock(Statement.class);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(1);

    // Create statement cancel mock
    TCancelOperationResp cancelResp =
        new TCancelOperationResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.CancelOperation(any(TCancelOperationReq.class))).thenReturn(cancelResp);

    // The execute method should throw a timeout exception since the operation does not complete
    // within 1 second. The polling interval is 1 second, and multiple polling attempts are made
    DatabricksTimeoutException exception =
        assertThrows(
            DatabricksTimeoutException.class,
            () -> accessor.execute(request, parentStatement, session, StatementType.SQL));

    assertTrue(exception.getMessage().contains("timed-out after 1 seconds"));

    // Verify that cancel was called
    verify(thriftClient).CancelOperation(any(TCancelOperationReq.class));
  }

  @Test
  void testServerSideTimeoutThrowsTimeoutException() throws TException, SQLException {

    accessor = spy(new DatabricksThriftAccessor(connectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();

    // Create statement execution mocks
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);

    // Mock server-side timeout: server returns ERROR_STATE with sqlState="57KD0"
    TGetOperationStatusResp operationStatusErrorResp =
        new TGetOperationStatusResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setOperationState(TOperationState.ERROR_STATE)
            .setSqlState(QUERY_EXECUTION_TIMEOUT_SQLSTATE) // Server-side timeout SQL state
            .setErrorMessage("Statement has timed out after 10 seconds.");

    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusErrorResp);

    Statement statement = mock(Statement.class);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(300); // Long timeout, server times out first

    // Verify that DatabricksTimeoutException is thrown
    assertThrows(
        DatabricksTimeoutException.class,
        () -> accessor.execute(request, parentStatement, session, StatementType.SQL));
  }

  @Test
  void testFetchResultsWithCustomMaxRowsPerBlock()
      throws TException, SQLException, DatabricksValidationException {
    int customMaxRows = 500000;
    IDatabricksConnectionContext mockConnectionContext = mock(IDatabricksConnectionContext.class);
    when(mockConnectionContext.getDirectResultMode()).thenReturn(true);
    when(mockConnectionContext.getRowsFetchedPerBlock()).thenReturn(customMaxRows);
    // Ensure configurator manager returns a configurator for this separate mock context
    ClientConfigurator customMockConfigurator = mock(ClientConfigurator.class);
    when(customMockConfigurator.getDatabricksConfig()).thenReturn(new DatabricksConfig());
    when(configuratorManager.getConfigurator(mockConnectionContext))
        .thenReturn(customMockConfigurator);
    lenient().when(mockConnectionContext.getAsyncExecPollInterval()).thenReturn(1000);
    lenient().when(mockConnectionContext.getEndpointURL()).thenReturn("http://localhost");
    accessor = spy(new DatabricksThriftAccessor(mockConnectionContext));
    doReturn(thriftClient).when(accessor).getThriftClient();

    TExecuteStatementReq executeRequest = new TExecuteStatementReq();
    TExecuteStatementResp executeResponse =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));

    TFetchResultsReq expectedFetchRequest =
        new TFetchResultsReq()
            .setOperationHandle(tOperationHandle)
            .setFetchType((short) 0)
            .setMaxRows(customMaxRows)
            .setMaxBytes(DEFAULT_BYTE_LIMIT)
            .setIncludeResultSetMetadata(true);

    Statement statement = mock(Statement.class);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(0);
    when(thriftClient.ExecuteStatement(executeRequest)).thenReturn(executeResponse);
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(operationStatusFinishedResp);
    when(thriftClient.FetchResults(expectedFetchRequest)).thenReturn(fetchResultsResponse);
    when(session.getConnectionContext()).thenReturn(mockConnectionContext);
    when(mockConnectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);

    accessor.execute(executeRequest, parentStatement, session, StatementType.SQL);

    // Verify that FetchResults was called with the correct maxRows value
    verify(thriftClient).FetchResults(expectedFetchRequest);
  }

  private TFetchResultsReq getFetchResultsRequest(boolean includeMetadata)
      throws DatabricksValidationException {
    TFetchResultsReq request =
        new TFetchResultsReq()
            .setOperationHandle(tOperationHandle)
            .setFetchType((short) 0)
            .setMaxRows(connectionContext.getRowsFetchedPerBlock())
            .setMaxBytes(DEFAULT_BYTE_LIMIT);
    if (includeMetadata) {
      request.setIncludeResultSetMetadata(true);
    }
    return request;
  }
}
