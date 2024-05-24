package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.getNamespace;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.CATALOG;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.SCHEMA;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.*;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftServiceClientTest {
  @Mock DatabricksThriftAccessor thriftAccessor;
  @Mock IDatabricksSession session;
  @Mock TRowSet resultData;
  @Mock TGetResultSetMetadataResp resultMetadataData;
  @Mock DatabricksResultSet resultSet;

  @Test
  void testCreateSession() throws DatabricksSQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    TOpenSessionReq openSessionReq =
        new TOpenSessionReq()
            .setInitialNamespace(getNamespace(CATALOG, SCHEMA))
            .setConfiguration(EMPTY_MAP)
            .setCanUseMultipleCatalogs(true)
            .setClient_protocol(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9);
    TOpenSessionResp openSessionResp =
        new TOpenSessionResp()
            .setSessionHandle(SESSION_HANDLE)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftAccessor.getThriftResponse(openSessionReq, CommandName.OPEN_SESSION, null))
        .thenReturn(openSessionResp);

    ImmutableSessionInfo actualResponse =
        client.createSession(CLUSTER_COMPUTE, CATALOG, SCHEMA, EMPTY_MAP);
    assertEquals(actualResponse.sessionHandle(), SESSION_HANDLE);
  }

  @Test
  void testCloseSession() throws DatabricksSQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TCloseSessionReq closeSessionReq = new TCloseSessionReq().setSessionHandle(SESSION_HANDLE);
    TCloseSessionResp closeSessionResp =
        new TCloseSessionResp().setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftAccessor.getThriftResponse(closeSessionReq, CommandName.CLOSE_SESSION, null))
        .thenReturn(closeSessionResp);
    assertDoesNotThrow(() -> client.deleteSession(session, CLUSTER_COMPUTE));
  }

  @Test
  void testExecute() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TExecuteStatementReq executeStatementReq =
        new TExecuteStatementReq()
            .setStatement(TEST_STRING)
            .setSessionHandle(SESSION_HANDLE)
            .setCanReadArrowResult(true)
            .setCanDownloadResult(true);
    when(thriftAccessor.execute(executeStatementReq, null, session, StatementType.SQL))
        .thenReturn(resultSet);
    DatabricksResultSet actualResultSet =
        client.executeStatement(
            TEST_STRING, CLUSTER_COMPUTE, Collections.emptyMap(), StatementType.SQL, session, null);
    assertEquals(resultSet, actualResultSet);
  }

  @Test
  void testUnsupportedFunctions() {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> client.closeStatement(TEST_STRING));
  }

  @Test
  void testListCatalogs() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetCatalogsReq request = new TGetCatalogsReq().setSessionHandle(SESSION_HANDLE);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(Collections.singletonList(new TColumn()));
    when(thriftAccessor.getThriftResponse(request, CommandName.LIST_CATALOGS, null))
        .thenReturn(response);
    DatabricksResultSet resultSet = client.listCatalogs(session);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testGetResultChunks() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(thriftAccessor.getResultSetResp(any(), any())).thenReturn(response);
    when(resultData.getResultLinksSize()).thenReturn(1);
    when(resultData.getResultLinks())
        .thenReturn(
            Collections.singletonList(new TSparkArrowResultLink().setFileLink(TEST_STRING)));
    Collection<ExternalLink> resultChunks = client.getResultChunks(TEST_STATEMENT_ID, 0);
    assertEquals(resultChunks.size(), 1);
    assertEquals(resultChunks.stream().findFirst().get().getExternalLink(), TEST_STRING);
  }

  @Test
  void testGetResultChunksThrowsError() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(thriftAccessor.getResultSetResp(any(), any())).thenReturn(response);
    when(resultData.getResultLinksSize()).thenReturn(1);
    assertThrows(DatabricksSQLException.class, () -> client.getResultChunks(TEST_STATEMENT_ID, -1));
    assertThrows(DatabricksSQLException.class, () -> client.getResultChunks(TEST_STATEMENT_ID, 2));
    assertThrows(DatabricksSQLException.class, () -> client.getResultChunks(TEST_STATEMENT_ID, 1));
  }

  @Test
  void testListTableTypes() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetTableTypesReq request = new TGetTableTypesReq().setSessionHandle(SESSION_HANDLE);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(Collections.singletonList(new TColumn()));
    when(thriftAccessor.getThriftResponse(request, CommandName.LIST_TABLE_TYPES, null))
        .thenReturn(response);
    DatabricksResultSet resultSet = client.listTableTypes(session);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListTypeInfo() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetTypeInfoReq request = new TGetTypeInfoReq().setSessionHandle(SESSION_HANDLE);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(Collections.emptyList());
    when(thriftAccessor.getThriftResponse(request, CommandName.LIST_TYPE_INFO, null))
        .thenReturn(response);
    DatabricksResultSet resultSet = client.listTypeInfo(session);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListSchemas() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetSchemasReq request =
        new TGetSchemasReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(Collections.emptyList());
    when(thriftAccessor.getThriftResponse(request, CommandName.LIST_SCHEMAS, null))
        .thenReturn(response);
    DatabricksResultSet resultSet = client.listSchemas(session, TEST_CATALOG, TEST_SCHEMA);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListTables() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    String[] tableTypes = {"testTableType"};
    TGetTablesReq request =
        new TGetTablesReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setTableTypes(Arrays.asList(tableTypes));
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(Collections.emptyList());
    when(thriftAccessor.getThriftResponse(request, CommandName.LIST_TABLES, null))
        .thenReturn(response);
    DatabricksResultSet resultSet =
        client.listTables(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, tableTypes);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListColumns() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetColumnsReq request =
        new TGetColumnsReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setColumnName(TEST_STRING);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(Collections.emptyList());
    when(thriftAccessor.getThriftResponse(request, CommandName.LIST_COLUMNS, null))
        .thenReturn(response);
    DatabricksResultSet resultSet =
        client.listColumns(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, TEST_STRING);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListFunctions() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetFunctionsReq request =
        new TGetFunctionsReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setFunctionName(TEST_STRING);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request, CommandName.LIST_FUNCTIONS, null))
        .thenReturn(response);
    DatabricksResultSet resultSet =
        client.listFunctions(session, TEST_CATALOG, TEST_SCHEMA, TEST_STRING);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListPrimaryKeys() throws SQLException {
    DatabricksThriftServiceClient client = new DatabricksThriftServiceClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TGetPrimaryKeysReq request =
        new TGetPrimaryKeysReq()
            .setSessionHandle(SESSION_HANDLE)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request, CommandName.LIST_PRIMARY_KEYS, null))
        .thenReturn(response);
    DatabricksResultSet resultSet =
        client.listPrimaryKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }
}
