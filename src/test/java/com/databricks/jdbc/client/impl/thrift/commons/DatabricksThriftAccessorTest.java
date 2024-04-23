package com.databricks.jdbc.client.impl.thrift.commons;

import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_BYTE_LIMIT;
import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_ROW_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksStatement;
import java.sql.SQLException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftAccessorTest {
  @Mock TCLIService.Client thriftClient;
  DatabricksThriftAccessor accessor;
  private static final TOperationHandle tOperationHandle = new TOperationHandle();
  private static final TFetchResultsReq fetchResultsReq =
      new TFetchResultsReq()
          .setOperationHandle(tOperationHandle)
          .setIncludeResultSetMetadata(true)
          .setFetchType((short) 0)
          .setMaxRows(DEFAULT_ROW_LIMIT)
          .setMaxBytes(DEFAULT_BYTE_LIMIT);
  private static final TGetResultSetMetadataReq resultSetMetadataReq =
      new TGetResultSetMetadataReq().setOperationHandle(tOperationHandle);
  private static final TFetchResultsResp response =
      new TFetchResultsResp().setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
  private static final TSparkDirectResults directResults =
      new TSparkDirectResults().setResultSet(response);
  private static final TGetResultSetMetadataResp metadataResp = new TGetResultSetMetadataResp();

  @Mock IDatabricksStatement statement;

  @Test
  void testOpenSession() throws TException, DatabricksSQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
    TOpenSessionReq request = new TOpenSessionReq();
    TOpenSessionResp response = new TOpenSessionResp();
    when(thriftClient.OpenSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request, CommandName.OPEN_SESSION, null), response);
  }

  @Test
  void testCloseSession() throws TException, DatabricksSQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
    TCloseSessionReq request = new TCloseSessionReq();
    TCloseSessionResp response = new TCloseSessionResp();
    when(thriftClient.CloseSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request, CommandName.CLOSE_SESSION, null), response);
  }

  @Test
  void testExecute() throws TException, DatabricksSQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftClient.GetResultSetMetadata(resultSetMetadataReq)).thenReturn(metadataResp);
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp)
            accessor.getThriftResponse(request, CommandName.EXECUTE_STATEMENT, null);
    assertEquals(actualResponse, response.setResultSetMetadata(metadataResp));
  }

  @Test
  void testExecuteWithParentStatement() throws TException, SQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    when(statement.getMaxRows()).thenReturn(25);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp)
            accessor.getThriftResponse(request, CommandName.EXECUTE_STATEMENT, statement);
    assertEquals(actualResponse, response);
  }

  @Test
  void testExecuteWithDirectResults() throws TException, DatabricksSQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setDirectResults(directResults);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp)
            accessor.getThriftResponse(request, CommandName.EXECUTE_STATEMENT, null);
    assertEquals(actualResponse, response);
  }

  @Test
  void testListPrimaryKeys() throws TException, DatabricksSQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
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
    accessor = new DatabricksThriftAccessor(thriftClient);
    TGetTablesReq request = new TGetTablesReq();
    when(thriftClient.GetTables(request)).thenThrow(new TException());
    assertThrows(
        DatabricksSQLException.class,
        () -> accessor.getThriftResponse(request, CommandName.LIST_TABLES, null));
  }
}
