package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.CATALOG;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.SCHEMA;
import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getNamespace;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.GET_TABLE_TYPE_STATEMENT_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultColumn;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.SQLException;
import java.util.*;
import org.apache.thrift.protocol.TProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftServiceClientTest {

  private static final String NEW_ACCESS_TOKEN = "new-access-token";
  private static final StatementId TEST_STMT_ID =
      StatementId.deserialize(
          "01efc77c-7c8b-1a8e-9ecb-a9a6e6aa050a|338d529d-8272-46eb-8482-cb419466839d");
  @Mock DatabricksThriftAccessor thriftAccessor;
  @Mock IDatabricksSession session;
  @Mock TRowSet resultData;
  @Mock TGetResultSetMetadataResp resultMetadataData;
  @Mock DatabricksResultSet resultSet;
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock IDatabricksStatementInternal parentStatement;
  @Mock DatabricksStatement statement;

  @Test
  void testCreateSession() throws DatabricksSQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    TOpenSessionReq openSessionReq =
        new TOpenSessionReq()
            .setInitialNamespace(getNamespace(CATALOG, SCHEMA))
            .setConfiguration(EMPTY_MAP)
            .setCanUseMultipleCatalogs(true)
            .setClient_protocol_i64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9.getValue());
    TOpenSessionResp openSessionResp =
        new TOpenSessionResp()
            .setSessionHandle(SESSION_HANDLE)
            .setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftAccessor.getThriftResponse(openSessionReq)).thenReturn(openSessionResp);
    ImmutableSessionInfo actualResponse =
        client.createSession(CLUSTER_COMPUTE, CATALOG, SCHEMA, EMPTY_MAP);
    assertEquals(actualResponse.sessionHandle(), SESSION_HANDLE);
  }

  @Test
  void testCloseSession() throws DatabricksSQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    TCloseSessionReq closeSessionReq = new TCloseSessionReq().setSessionHandle(SESSION_HANDLE);
    TCloseSessionResp closeSessionResp =
        new TCloseSessionResp().setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftAccessor.getThriftResponse(closeSessionReq)).thenReturn(closeSessionResp);
    assertDoesNotThrow(() -> client.deleteSession(SESSION_INFO));
  }

  @Test
  void testExecute() throws SQLException {
    when(connectionContext.shouldEnableArrow()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(10);
    TSparkArrowTypes arrowNativeTypes =
        new TSparkArrowTypes()
            .setComplexTypesAsArrow(true)
            .setIntervalTypesAsArrow(true)
            .setNullTypeAsArrow(true)
            .setDecimalAsArrow(true)
            .setTimestampAsArrow(true);
    TExecuteStatementReq executeStatementReq =
        new TExecuteStatementReq()
            .setStatement(TEST_STRING)
            .setSessionHandle(SESSION_HANDLE)
            .setCanReadArrowResult(true)
            .setQueryTimeout(10)
            .setCanDecompressLZ4Result(true)
            .setCanDownloadResult(true)
            .setRunAsync(true)
            .setUseArrowNativeTypes(arrowNativeTypes);
    when(thriftAccessor.execute(executeStatementReq, parentStatement, session, StatementType.SQL))
        .thenReturn(resultSet);
    DatabricksResultSet actualResultSet =
        client.executeStatement(
            TEST_STRING,
            CLUSTER_COMPUTE,
            Collections.emptyMap(),
            StatementType.SQL,
            session,
            parentStatement);
    assertEquals(resultSet, actualResultSet);
  }

  @Test
  void testExecuteAsync() throws SQLException {
    when(connectionContext.shouldEnableArrow()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(statement.getQueryTimeout()).thenReturn(20);
    TSparkArrowTypes arrowNativeTypes =
        new TSparkArrowTypes()
            .setComplexTypesAsArrow(true)
            .setIntervalTypesAsArrow(true)
            .setNullTypeAsArrow(true)
            .setDecimalAsArrow(true)
            .setTimestampAsArrow(true);
    TExecuteStatementReq executeStatementReq =
        new TExecuteStatementReq()
            .setStatement(TEST_STRING)
            .setQueryTimeout(20)
            .setSessionHandle(SESSION_HANDLE)
            .setCanReadArrowResult(true)
            .setCanDecompressLZ4Result(true)
            .setRunAsync(true)
            .setCanDownloadResult(true)
            .setUseArrowNativeTypes(arrowNativeTypes);
    when(thriftAccessor.executeAsync(
            executeStatementReq, parentStatement, session, StatementType.SQL))
        .thenReturn(resultSet);
    DatabricksResultSet actualResultSet =
        client.executeStatementAsync(
            TEST_STRING, CLUSTER_COMPUTE, Collections.emptyMap(), session, parentStatement);
    assertEquals(resultSet, actualResultSet);
  }

  @Test
  void testCloseStatement() throws Exception {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(thriftAccessor.closeOperation(any()))
        .thenReturn(
            new TCloseOperationResp()
                .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)));
    client.closeStatement(TEST_STMT_ID);
    verify(thriftAccessor).closeOperation(any());
  }

  @Test
  void testListCatalogs() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetCatalogsReq request =
        new TGetCatalogsReq().setSessionHandle(SESSION_HANDLE).setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    TColumn tColumn = new TColumn();
    tColumn.setStringVal(new TStringColumn().setValues(Collections.singletonList(TEST_CATALOG)));
    when(resultData.getColumns()).thenReturn(Collections.singletonList(tColumn));
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet = client.listCatalogs(session);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testGetResultChunks() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(thriftAccessor.getResultSetResp(any(), any())).thenReturn(response);
    when(resultData.getResultLinks())
        .thenReturn(
            Collections.singletonList(new TSparkArrowResultLink().setFileLink(TEST_STRING)));
    Collection<ExternalLink> resultChunks = client.getResultChunks(TEST_STMT_ID, 0);
    assertEquals(resultChunks.size(), 1);
    assertEquals(resultChunks.stream().findFirst().get().getExternalLink(), TEST_STRING);
  }

  @Test
  void testGetResultChunksThrowsError() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(thriftAccessor.getResultSetResp(any(), any())).thenReturn(response);
    assertThrows(DatabricksSQLException.class, () -> client.getResultChunks(TEST_STMT_ID, -1));
    assertThrows(DatabricksSQLException.class, () -> client.getResultChunks(TEST_STMT_ID, 2));
    assertThrows(DatabricksSQLException.class, () -> client.getResultChunks(TEST_STMT_ID, 1));
  }

  @Test
  void testListTableTypes() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    DatabricksResultSet actualResult = client.listTableTypes(session);
    assertEquals(actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED);
    assertEquals(actualResult.getStatementId(), GET_TABLE_TYPE_STATEMENT_ID);
    assertEquals(((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 3);
  }

  @Test
  void testListTypeInfo() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    DatabricksResultSet resultSet = client.listTypeInfo(session);
    assertNotNull(resultSet);
    assertEquals(StatementState.SUCCEEDED, resultSet.getStatementStatus().getState());
  }

  @Test
  void testListSchemas() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetSchemasReq request =
        new TGetSchemasReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(Collections.emptyList());
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet = client.listSchemas(session, TEST_CATALOG, TEST_SCHEMA);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListTables() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    String[] tableTypes = {"testTableType"};
    TGetTablesReq request =
        new TGetTablesReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setTableTypes(Arrays.asList(tableTypes))
            .setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    TColumn tColumn = new TColumn();
    tColumn.setStringVal(new TStringColumn().setValues(Collections.singletonList("")));
    when(resultData.getColumns()).thenReturn(List.of(tColumn, tColumn, tColumn, tColumn));
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet =
        client.listTables(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, tableTypes);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListColumns() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetColumnsReq request =
        new TGetColumnsReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setColumnName(TEST_STRING)
            .setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(new ArrayList<>());
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet =
        client.listColumns(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, TEST_STRING);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
    DatabricksResultSetMetaData metaData = (DatabricksResultSetMetaData) resultSet.getMetaData();
    assertEquals(metaData.getColumnCount(), COLUMN_COLUMNS.size());
    for (int i = 0; i < COLUMN_COLUMNS.size(); i++) {
      ResultColumn resultColumn = COLUMN_COLUMNS.get(i);
      assertEquals(metaData.getColumnName(i + 1), resultColumn.getColumnName());
      assertEquals(metaData.getColumnType(i + 1), resultColumn.getColumnTypeInt());
      assertEquals(metaData.getColumnTypeName(i + 1), resultColumn.getColumnTypeString());
      if (LARGE_DISPLAY_COLUMNS.contains(resultColumn)) {
        assertEquals(254, metaData.getPrecision(i + 1));
      } else {
        assertEquals(metaData.getPrecision(i + 1), resultColumn.getColumnPrecision());
      }
    }
  }

  @Test
  void testListFunctions() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetFunctionsReq request =
        new TGetFunctionsReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setFunctionName(TEST_STRING)
            .setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet =
        client.listFunctions(session, TEST_CATALOG, TEST_SCHEMA, TEST_STRING);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListPrimaryKeys() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetPrimaryKeysReq request =
        new TGetPrimaryKeysReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet =
        client.listPrimaryKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListImportedKeys() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetCrossReferenceReq request =
        new TGetCrossReferenceReq()
            .setSessionHandle(SESSION_HANDLE)
            .setForeignCatalogName(TEST_FOREIGN_CATALOG)
            .setForeignSchemaName(TEST_FOREIGN_SCHEMA)
            .setForeignTableName(TEST_FOREIGN_TABLE)
            .setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet =
        client.listImportedKeys(
            session, TEST_FOREIGN_CATALOG, TEST_FOREIGN_SCHEMA, TEST_FOREIGN_TABLE);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListExportedKeys() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetCrossReferenceReq request =
        new TGetCrossReferenceReq()
            .setSessionHandle(SESSION_HANDLE)
            .setParentCatalogName(TEST_CATALOG)
            .setParentSchemaName(TEST_SCHEMA)
            .setParentTableName(TEST_TABLE)
            .setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet =
        client.listExportedKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListCrossReferences() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetCrossReferenceReq request =
        new TGetCrossReferenceReq()
            .setSessionHandle(SESSION_HANDLE)
            .setParentCatalogName(TEST_CATALOG)
            .setParentSchemaName(TEST_SCHEMA)
            .setParentTableName(TEST_TABLE)
            .setForeignCatalogName(TEST_FOREIGN_CATALOG)
            .setForeignSchemaName(TEST_FOREIGN_SCHEMA)
            .setForeignTableName(TEST_FOREIGN_TABLE)
            .setRunAsync(true);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    DatabricksResultSet resultSet =
        client.listCrossReferences(
            session,
            TEST_CATALOG,
            TEST_SCHEMA,
            TEST_TABLE,
            TEST_FOREIGN_CATALOG,
            TEST_FOREIGN_SCHEMA,
            TEST_FOREIGN_TABLE);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testCancelStatement() throws Exception {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(thriftAccessor.cancelOperation(any()))
        .thenReturn(
            new TCancelOperationResp()
                .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)));
    client.cancelStatement(TEST_STMT_ID);
    verify(thriftAccessor).cancelOperation(any());
  }

  @Test
  void testConnectionContext() {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    assertEquals(client.getConnectionContext(), connectionContext);
  }

  @Test
  void testResetAccessToken() {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    DatabricksHttpTTransport mockDatabricksHttpTTransport =
        Mockito.mock(DatabricksHttpTTransport.class);
    TCLIService.Client mockTCLIServiceClient = Mockito.mock(TCLIService.Client.class);
    TProtocol mockProtocol = Mockito.mock(TProtocol.class);
    when(thriftAccessor.getThriftClient()).thenReturn(mockTCLIServiceClient);
    when(mockTCLIServiceClient.getInputProtocol()).thenReturn(mockProtocol);
    when(mockProtocol.getTransport()).thenReturn(mockDatabricksHttpTTransport);
    client.resetAccessToken(NEW_ACCESS_TOKEN);
    verify(mockDatabricksHttpTTransport).resetAccessToken(NEW_ACCESS_TOKEN);
  }
}
