package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_BYTE_LIMIT;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_ROW_LIMIT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftAccessorTest {

  @Mock TCLIService.Client thriftClient;
  @Mock IDatabricksSession session;
  @Mock IDatabricksStatementInternal statement;
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
  private static final TFetchResultsResp response =
      new TFetchResultsResp()
          .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
          .setResultSetMetadata(
              new TGetResultSetMetadataResp().setResultFormat(TSparkRowSetType.COLUMN_BASED_SET))
          .setResults(rowSet);
  private static final TSparkDirectResults directResults =
      new TSparkDirectResults()
          .setResultSet(response)
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
  private static final TGetOperationStatusResp operationStatusResp =
      new TGetOperationStatusResp()
          .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
          .setOperationState(TOperationState.FINISHED_STATE);

  void setup(Boolean directResultsEnabled) {
    when(connectionContext.getDirectResultMode()).thenReturn(directResultsEnabled);
    accessor = new DatabricksThriftAccessor(thriftClient, connectionContext);
  }

  @Test
  void testOpenSession() throws TException, DatabricksSQLException {
    setup(true);
    TOpenSessionReq request = new TOpenSessionReq();
    TOpenSessionResp response = new TOpenSessionResp();
    when(thriftClient.OpenSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request), response);
  }

  @Test
  void testCloseSession() throws TException, DatabricksSQLException {
    setup(true);
    TCloseSessionReq request = new TCloseSessionReq();
    TCloseSessionResp response = new TCloseSessionResp();
    when(thriftClient.CloseSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request), response);
  }

  @Test
  void testExecute() throws TException, SQLException {
    setup(false);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(getFetchResultsRequest(true))).thenReturn(response);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(parentStatement.getMaxRows()).thenReturn(DEFAULT_ROW_LIMIT);
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    DatabricksResultSet resultSet =
        accessor.execute(request, parentStatement, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteAsync() throws TException, SQLException {
    setup(true);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    DatabricksResultSet resultSet =
        accessor.executeAsync(request, parentStatement, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.RUNNING);
  }

  @Test
  void testExecuteAsync_error() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    TExecuteStatementReq request = new TExecuteStatementReq();
    when(thriftClient.ExecuteStatement(request)).thenThrow(new TException("failed"));
    assertThrows(
        DatabricksHttpException.class,
        () -> accessor.executeAsync(request, null, session, StatementType.SQL));
  }

  @Test
  void testExecuteAsync_SQLState() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
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
  void testExecuteThrowsThriftError() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    accessor = new DatabricksThriftAccessor(thriftClient, connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    when(thriftClient.ExecuteStatement(request)).thenThrow(TException.class);
    assertThrows(
        DatabricksHttpException.class,
        () -> accessor.execute(request, null, session, StatementType.SQL));
  }

  @Test
  void testExecuteWithParentStatement() throws TException, SQLException {
    setup(true);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    accessor = new DatabricksThriftAccessor(thriftClient, connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(statement.getMaxRows()).thenReturn(25);
    DatabricksResultSet resultSet =
        accessor.execute(request, statement, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteWithDirectResults() throws TException, SQLException {
    setup(true);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    accessor = new DatabricksThriftAccessor(thriftClient, connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    DatabricksResultSet resultSet = accessor.execute(request, null, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteWithoutDirectResults() throws TException, SQLException {
    setup(false);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    accessor = new DatabricksThriftAccessor(thriftClient, connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    DatabricksResultSet resultSet = accessor.execute(request, null, session, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecute_throwsException() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    accessor = new DatabricksThriftAccessor(thriftClient, connectionContext);
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
  void testExecuteThrowsSQLExceptionWithSqlState() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
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
  void testCancelOperation() throws TException, DatabricksSQLException {
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
  void testCloseOperation() throws TException, DatabricksSQLException {
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
  void testCancelOperation_error() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
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
  void testCloseOperation_error() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
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
  void testGetStatementResult_success() throws Exception {
    when(connectionContext.getDirectResultMode()).thenReturn(false);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    accessor = new DatabricksThriftAccessor(thriftClient, connectionContext);
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    TFetchResultsReq fetchReq =
        new TFetchResultsReq()
            .setOperationHandle(tOperationHandle)
            .setFetchType((short) 0) // 0 represents Query output. 1 represents Log
            .setMaxRows(-1)
            .setIncludeResultSetMetadata(true)
            .setMaxBytes(DEFAULT_BYTE_LIMIT);
    when(thriftClient.FetchResults(fetchReq)).thenReturn(response);

    DatabricksResultSet resultSet = accessor.getStatementResult(tOperationHandle, null, session);
    assertEquals(StatementState.SUCCEEDED, resultSet.getStatementStatus().getState());
    assertNotNull(resultSet.getMetaData());
  }

  @Test
  void testGetStatementResult_pending() throws Exception {
    when(connectionContext.getDirectResultMode()).thenReturn(false);
    accessor = new DatabricksThriftAccessor(thriftClient, connectionContext);
    TGetOperationStatusResp resp =
        new TGetOperationStatusResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.STILL_EXECUTING_STATUS))
            .setOperationState(TOperationState.RUNNING_STATE);
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(resp);

    DatabricksResultSet resultSet = accessor.getStatementResult(tOperationHandle, null, session);
    assertEquals(StatementState.RUNNING, resultSet.getStatementStatus().getState());
    assertNull(resultSet.getMetaData());
  }

  @Test
  void testListPrimaryKeys() throws TException, DatabricksSQLException {
    setup(false);
    TGetPrimaryKeysReq request = new TGetPrimaryKeysReq();
    TGetPrimaryKeysResp tGetPrimaryKeysResp =
        new TGetPrimaryKeysResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(response);
    when(thriftClient.GetPrimaryKeys(request)).thenReturn(tGetPrimaryKeysResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListPrimaryKeysWithDirectResults() throws TException, DatabricksSQLException {
    setup(true);
    TGetPrimaryKeysReq request = new TGetPrimaryKeysReq();
    TGetPrimaryKeysResp tGetPrimaryKeysResp =
        new TGetPrimaryKeysResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetPrimaryKeys(request)).thenReturn(tGetPrimaryKeysResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListFunctions() throws TException, DatabricksSQLException {
    setup(false);
    TGetFunctionsReq request = new TGetFunctionsReq();
    TGetFunctionsResp tGetFunctionsResp =
        new TGetFunctionsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(response);
    when(thriftClient.GetFunctions(request)).thenReturn(tGetFunctionsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListFunctionsWithDirectResults() throws TException, DatabricksSQLException {
    setup(true);
    TGetFunctionsReq request = new TGetFunctionsReq();
    TGetFunctionsResp tGetFunctionsResp =
        new TGetFunctionsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetFunctions(request)).thenReturn(tGetFunctionsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListSchemas() throws TException, DatabricksSQLException {
    setup(false);
    TGetSchemasReq request = new TGetSchemasReq();
    TGetSchemasResp tGetSchemasResp =
        new TGetSchemasResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(response);
    when(thriftClient.GetSchemas(request)).thenReturn(tGetSchemasResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListSchemasWithDirectResults() throws TException, DatabricksSQLException {
    setup(true);
    TGetSchemasReq request = new TGetSchemasReq();
    TGetSchemasResp tGetSchemasResp =
        new TGetSchemasResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetSchemas(request)).thenReturn(tGetSchemasResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListColumns() throws TException, DatabricksSQLException {
    setup(false);
    TGetColumnsReq request = new TGetColumnsReq();
    TGetColumnsResp tGetColumnsResp =
        new TGetColumnsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(response);
    when(thriftClient.GetColumns(request)).thenReturn(tGetColumnsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListColumnsWithDirectResults() throws TException, DatabricksSQLException {
    setup(true);
    TGetColumnsReq request = new TGetColumnsReq();
    TGetColumnsResp tGetColumnsResp =
        new TGetColumnsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetColumns(request)).thenReturn(tGetColumnsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListCatalogs() throws TException, DatabricksSQLException {
    setup(true);
    TGetCatalogsReq request = new TGetCatalogsReq();
    TGetCatalogsResp tGetCatalogsResp =
        new TGetCatalogsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(response);
    when(thriftClient.GetCatalogs(request)).thenReturn(tGetCatalogsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListCatalogsWithDirectResults() throws TException, DatabricksSQLException {
    setup(true);
    TGetCatalogsReq request = new TGetCatalogsReq();
    TGetCatalogsResp tGetCatalogsResp =
        new TGetCatalogsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetCatalogs(request)).thenReturn(tGetCatalogsResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListTables() throws TException, DatabricksSQLException {
    setup(false);
    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(response);
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListTablesWithDirectResults() throws TException, DatabricksSQLException {
    setup(true);
    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListTableTypes() throws TException, DatabricksSQLException {
    setup(false);
    TGetTableTypesReq request = new TGetTableTypesReq();
    TGetTableTypesResp tGetTableTypesResp =
        new TGetTableTypesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(response);
    when(thriftClient.GetTableTypes(request)).thenReturn(tGetTableTypesResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListTableTypesWithDirectResults() throws TException, DatabricksSQLException {
    setup(true);
    TGetTableTypesReq request = new TGetTableTypesReq();
    TGetTableTypesResp tGetTableTypesResp =
        new TGetTableTypesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetTableTypes(request)).thenReturn(tGetTableTypesResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testTypeInfo() throws TException, DatabricksSQLException {
    setup(false);
    TGetTypeInfoReq request = new TGetTypeInfoReq();
    TGetTypeInfoResp tGetTypeInfoResp =
        new TGetTypeInfoResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenReturn(response);
    when(thriftClient.GetTypeInfo(request)).thenReturn(tGetTypeInfoResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testTypeInfoWithDirectResults() throws TException, DatabricksSQLException {
    setup(true);
    TGetTypeInfoReq request = new TGetTypeInfoReq();
    TGetTypeInfoResp tGetTypeInfoResp =
        new TGetTypeInfoResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.GetTypeInfo(request)).thenReturn(tGetTypeInfoResp);
    TFetchResultsResp actualResponse = (TFetchResultsResp) accessor.getThriftResponse(request);
    assertEquals(actualResponse, response);
  }

  @Test
  void testAccessorWhenFetchResultsThrowsError() throws TException {
    setup(false);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetOperationStatus(operationStatusReq)).thenReturn(operationStatusResp);
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    when(thriftClient.FetchResults(getFetchResultsRequest(false))).thenThrow(new TException());
    assertThrows(DatabricksSQLException.class, () -> accessor.getThriftResponse(request));
  }

  @Test
  void testAccessorDuringThriftError() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    TGetTablesReq request = new TGetTablesReq();
    when(thriftClient.GetTables(request)).thenThrow(new TException());
    assertThrows(DatabricksSQLException.class, () -> accessor.getThriftResponse(request));
  }

  @Test
  void testAccessorDuringHTTPError() throws TException {
    setup(true);
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
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

  private TFetchResultsReq getFetchResultsRequest(boolean includeMetadata) {
    TFetchResultsReq request =
        new TFetchResultsReq()
            .setOperationHandle(tOperationHandle)
            .setFetchType((short) 0)
            .setMaxRows(DEFAULT_ROW_LIMIT)
            .setMaxBytes(DEFAULT_BYTE_LIMIT);
    if (includeMetadata) {
      request.setIncludeResultSetMetadata(true);
    }
    return request;
  }
}
