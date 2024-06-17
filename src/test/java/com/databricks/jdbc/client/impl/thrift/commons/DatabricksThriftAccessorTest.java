package com.databricks.jdbc.client.impl.thrift.commons;

import static com.databricks.jdbc.TestConstants.TEST_BYTES;
import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_BYTE_LIMIT;
import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_ROW_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksStatement;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftAccessorTest {
  @Mock TCLIService.Client thriftClient;
  @Mock TProtocol protocol;
  @Mock DatabricksHttpTTransport transport;
  @Mock DatabricksConfig config;
  @Mock IDatabricksStatement statement;
  @Mock IDatabricksConnectionContext connectionContext;
  static DatabricksThriftAccessor accessor;
  static THandleIdentifier handleIdentifier = new THandleIdentifier().setGuid(TEST_BYTES);
  private static final TOperationHandle tOperationHandle =
      new TOperationHandle().setOperationId(handleIdentifier).setHasResultSet(false);
  private static final TFetchResultsReq fetchResultsReq =
      new TFetchResultsReq()
          .setOperationHandle(tOperationHandle)
          .setIncludeResultSetMetadata(true)
          .setFetchType((short) 0)
          .setMaxRows(DEFAULT_ROW_LIMIT)
          .setMaxBytes(DEFAULT_BYTE_LIMIT);
  private static final TGetResultSetMetadataReq resultSetMetadataReq =
      new TGetResultSetMetadataReq().setOperationHandle(tOperationHandle);

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
                  .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)));
  private static final TGetResultSetMetadataResp metadataResp =
      new TGetResultSetMetadataResp().setResultFormat(TSparkRowSetType.COLUMN_BASED_SET);

  void setup(Boolean directResultsEnabled) {
    when(connectionContext.getDirectResultMode()).thenReturn(directResultsEnabled);
    accessor = new DatabricksThriftAccessor(thriftClient, config, connectionContext);
    when(thriftClient.getInputProtocol()).thenReturn(protocol);
    when(protocol.getTransport()).thenReturn(transport);
    doNothing().when(transport).setCustomHeaders(Collections.emptyMap());
    when(config.authenticate()).thenReturn(Collections.emptyMap());
  }

  @Test
  void testOpenSession() throws TException, DatabricksSQLException {
    setup(true);
    TOpenSessionReq request = new TOpenSessionReq();
    TOpenSessionResp response = new TOpenSessionResp();
    when(thriftClient.OpenSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request, CommandName.OPEN_SESSION, null), response);
  }

  @Test
  void testCloseSession() throws TException, DatabricksSQLException {
    setup(true);
    TCloseSessionReq request = new TCloseSessionReq();
    TCloseSessionResp response = new TCloseSessionResp();
    when(thriftClient.CloseSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request, CommandName.CLOSE_SESSION, null), response);
  }

  @Test
  void testExecute() throws TException, SQLException {
    setup(true);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetResultSetMetadata(resultSetMetadataReq)).thenReturn(metadataResp);
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    TGetOperationStatusReq operationStatusReq =
        new TGetOperationStatusReq()
            .setOperationHandle(tOperationHandle)
            .setGetProgressUpdate(false);
    when(thriftClient.GetOperationStatus(operationStatusReq))
        .thenReturn(
            new TGetOperationStatusResp()
                .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)));
    DatabricksResultSet resultSet = accessor.execute(request, null, null, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteThrowsThriftError() throws TException {
    setup(true);
    accessor = new DatabricksThriftAccessor(thriftClient, config, connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    when(thriftClient.ExecuteStatement(request)).thenThrow(TException.class);
    assertThrows(
        DatabricksHttpException.class,
        () -> accessor.execute(request, null, null, StatementType.SQL));
  }

  @Test
  void testExecuteWithParentStatement() throws TException, SQLException {
    setup(true);
    accessor = new DatabricksThriftAccessor(thriftClient, config, connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(statement.getMaxRows()).thenReturn(25);
    DatabricksResultSet resultSet = accessor.execute(request, statement, null, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteWithDirectResults() throws TException, SQLException {
    setup(true);
    accessor = new DatabricksThriftAccessor(thriftClient, config, connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    DatabricksResultSet resultSet = accessor.execute(request, null, null, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testExecuteWithoutDirectResults() throws TException, SQLException {
    setup(false);
    accessor = new DatabricksThriftAccessor(thriftClient, config, connectionContext);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    DatabricksResultSet resultSet = accessor.execute(request, null, null, StatementType.SQL);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListPrimaryKeys() throws TException, DatabricksSQLException {
    setup(true);
    TGetPrimaryKeysReq request = new TGetPrimaryKeysReq();
    TGetPrimaryKeysResp tGetPrimaryKeysResp =
        new TGetPrimaryKeysResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.GetPrimaryKeys(request)).thenReturn(tGetPrimaryKeysResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp)
            accessor.getThriftResponse(request, CommandName.LIST_PRIMARY_KEYS, null);
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
    TFetchResultsResp actualResponse =
        (TFetchResultsResp)
            accessor.getThriftResponse(request, CommandName.LIST_PRIMARY_KEYS, null);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListFunctions() throws TException, DatabricksSQLException {
    setup(true);
    TGetFunctionsReq request = new TGetFunctionsReq();
    TGetFunctionsResp tGetFunctionsResp =
        new TGetFunctionsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.GetFunctions(request)).thenReturn(tGetFunctionsResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_FUNCTIONS, null);
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
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_FUNCTIONS, null);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListSchemas() throws TException, DatabricksSQLException {
    setup(true);
    TGetSchemasReq request = new TGetSchemasReq();
    TGetSchemasResp tGetSchemasResp =
        new TGetSchemasResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.GetSchemas(request)).thenReturn(tGetSchemasResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_SCHEMAS, null);
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
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_SCHEMAS, null);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListColumns() throws TException, DatabricksSQLException {
    setup(true);
    TGetColumnsReq request = new TGetColumnsReq();
    TGetColumnsResp tGetColumnsResp =
        new TGetColumnsResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.GetColumns(request)).thenReturn(tGetColumnsResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_COLUMNS, null);
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
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_COLUMNS, null);
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
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.GetCatalogs(request)).thenReturn(tGetCatalogsResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_CATALOGS, null);
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
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_CATALOGS, null);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListTables() throws TException, DatabricksSQLException {
    setup(true);
    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_TABLES, null);
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
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_TABLES, null);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListTableTypes() throws TException, DatabricksSQLException {
    setup(true);
    TGetTableTypesReq request = new TGetTableTypesReq();
    TGetTableTypesResp tGetTableTypesResp =
        new TGetTableTypesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.GetTableTypes(request)).thenReturn(tGetTableTypesResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_TABLE_TYPES, null);
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
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_TABLE_TYPES, null);
    assertEquals(actualResponse, response);
  }

  @Test
  void testTypeInfo() throws TException, DatabricksSQLException {
    setup(true);
    TGetTypeInfoReq request = new TGetTypeInfoReq();
    TGetTypeInfoResp tGetTypeInfoResp =
        new TGetTypeInfoResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.GetTypeInfo(request)).thenReturn(tGetTypeInfoResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_TYPE_INFO, null);
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
    TFetchResultsResp actualResponse =
        (TFetchResultsResp) accessor.getThriftResponse(request, CommandName.LIST_TYPE_INFO, null);
    assertEquals(actualResponse, response);
  }

  @Test
  void testAccessorWhenFetchResultsThrowsError() throws TException {
    setup(true);
    TGetTablesReq request = new TGetTablesReq();
    TGetTablesResp tGetTablesResp =
        new TGetTablesResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetTables(request)).thenReturn(tGetTablesResp);
    when(thriftClient.FetchResults(fetchResultsReq)).thenThrow(new TException());
    assertThrows(
        DatabricksSQLException.class,
        () -> accessor.getThriftResponse(request, CommandName.LIST_TABLES, null));
  }

  @Test
  void testAccessorDuringThriftError() throws TException {
    setup(true);
    TGetTablesReq request = new TGetTablesReq();
    when(thriftClient.GetTables(request)).thenThrow(new TException());
    assertThrows(
        DatabricksSQLException.class,
        () -> accessor.getThriftResponse(request, CommandName.LIST_TABLES, null));
  }
}
