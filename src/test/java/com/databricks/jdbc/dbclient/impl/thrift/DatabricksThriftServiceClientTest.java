package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.CATALOG;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.SCHEMA;
import static com.databricks.jdbc.common.EnvironmentVariables.JDBC_THRIFT_VERSION;
import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getNamespace;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.GET_TABLE_TYPE_STATEMENT_ID;
import static com.databricks.jdbc.model.core.ColumnInfoTypeName.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultColumn;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.StatementState;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
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
  @Mock DatabricksConfig databricksConfig;
  @Mock ResultSetMetaData mockedMetaData;

  @Test
  void testCreateSession() throws DatabricksSQLException {
    when(connectionContext.getEnableMultipleCatalogSupport()).thenReturn(true);
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
  void testCreateSessionHandlesProtocolVersion() throws DatabricksSQLException {
    when(connectionContext.getEnableMultipleCatalogSupport()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);

    TOpenSessionReq openSessionReq =
        new TOpenSessionReq()
            .setInitialNamespace(getNamespace(CATALOG, SCHEMA))
            .setConfiguration(EMPTY_MAP)
            .setCanUseMultipleCatalogs(true)
            .setClient_protocol_i64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9.getValue());

    // Case 1: Server returns unsupported protocol version (too old)
    TOpenSessionResp unsupportedVersionResp =
        new TOpenSessionResp()
            .setSessionHandle(SESSION_HANDLE)
            .setServerProtocolVersion(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));

    when(thriftAccessor.getThriftResponse(openSessionReq)).thenReturn(unsupportedVersionResp);

    // Verify that attempting to connect with an unsupported protocol throws the expected exception
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> client.createSession(CLUSTER_COMPUTE, CATALOG, SCHEMA, EMPTY_MAP));

    assertEquals(
        "Attempting to connect to a non Databricks compute using the Databricks driver.",
        exception.getMessage());

    // Case 2: Server returns supported protocol version
    TOpenSessionResp supportedVersionResp =
        new TOpenSessionResp()
            .setSessionHandle(SESSION_HANDLE)
            .setServerProtocolVersion(JDBC_THRIFT_VERSION)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));

    when(thriftAccessor.getThriftResponse(openSessionReq)).thenReturn(supportedVersionResp);

    ImmutableSessionInfo sessionInfo =
        client.createSession(CLUSTER_COMPUTE, CATALOG, SCHEMA, EMPTY_MAP);

    verify(thriftAccessor).setServerProtocolVersion(JDBC_THRIFT_VERSION);

    // Verify returned session info
    assertEquals(SESSION_HANDLE, sessionInfo.sessionHandle());
    assertEquals(CLUSTER_COMPUTE, sessionInfo.computeResource());
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

  private static Stream<Arguments> protocolVersionProvider() {
    return Stream.of(
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1),
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V2),
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3),
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4),
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5),
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6),
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7),
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8),
        Arguments.of(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9));
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  void testGetRequestWithDifferentProtocolVersions(TProtocolVersion protocolVersion)
      throws SQLException {
    when(connectionContext.shouldEnableArrow()).thenReturn(true);
    // Use lenient() because isCloudFetchEnabled() is only called for protocols that support
    // CloudFetch
    lenient().when(connectionContext.isCloudFetchEnabled()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(parentStatement.getMaxRows()).thenReturn(10);
    when(statement.getQueryTimeout()).thenReturn(10);
    client.setServerProtocolVersion(protocolVersion);

    // Execute the method that will internally call getRequest
    try {
      client.executeStatement(
          TEST_STRING,
          CLUSTER_COMPUTE,
          Collections.emptyMap(),
          StatementType.SQL,
          session,
          parentStatement);
    } catch (Exception e) {
      // expect this to throw since thriftAccessor is mocked
      // only interested in capturing the request
    }

    // Capture the request
    ArgumentCaptor<TExecuteStatementReq> requestCaptor =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(thriftAccessor)
        .execute(requestCaptor.capture(), eq(parentStatement), eq(session), eq(StatementType.SQL));

    // Get the captured request
    TExecuteStatementReq request = requestCaptor.getValue();

    // Common assertions for all protocol versions
    assertNotNull(request);
    assertEquals(TEST_STRING, request.getStatement());
    assertEquals(10, request.getQueryTimeout());
    assertEquals(SESSION_HANDLE, request.getSessionHandle());
    assertTrue(request.isCanReadArrowResult());
    assertEquals(10, request.getResultRowLimit());

    // Protocol-specific assertions
    boolean supportsParameterizedQueries =
        protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8) >= 0;
    boolean supportsCompressedArrowBatches =
        protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6) >= 0;
    boolean supportsCloudFetch =
        protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3) >= 0;
    boolean supportsAdvancedArrowTypes =
        protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5) >= 0;

    if (supportsParameterizedQueries) {
      assertNotNull(request);
    } else {
      assertFalse(request.isSetParameters());
    }

    assertEquals(supportsCompressedArrowBatches, request.isCanDecompressLZ4Result());
    assertEquals(supportsCloudFetch, request.isCanDownloadResult());

    // Check advanced arrow types settings
    TSparkArrowTypes arrowTypes = request.getUseArrowNativeTypes();
    assertNotNull(arrowTypes);
    assertTrue(arrowTypes.isTimestampAsArrow());

    if (supportsAdvancedArrowTypes) {
      assertTrue(arrowTypes.isComplexTypesAsArrow());
      assertTrue(arrowTypes.isIntervalTypesAsArrow());
      assertTrue(arrowTypes.isNullTypeAsArrow());
      assertTrue(arrowTypes.isDecimalAsArrow());
    } else {
      assertFalse(arrowTypes.isComplexTypesAsArrow());
      assertFalse(arrowTypes.isIntervalTypesAsArrow());
      assertFalse(arrowTypes.isNullTypeAsArrow());
      assertFalse(arrowTypes.isDecimalAsArrow());
    }
  }

  @Test
  void testExecute() throws SQLException {
    when(connectionContext.shouldEnableArrow()).thenReturn(true);
    when(connectionContext.isCloudFetchEnabled()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(parentStatement.getMaxRows()).thenReturn(10);
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
            .setResultRowLimit(10)
            .setCanDecompressLZ4Result(true)
            .setCanDownloadResult(true)
            .setParameters(Collections.emptyList())
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
  void testExecuteWithCloudFetchDisabled() throws SQLException {
    when(connectionContext.shouldEnableArrow()).thenReturn(true);
    when(connectionContext.isCloudFetchEnabled()).thenReturn(false);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(parentStatement.getMaxRows()).thenReturn(10);
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
            .setResultRowLimit(10)
            .setCanDecompressLZ4Result(true)
            .setCanDownloadResult(false)
            .setParameters(Collections.emptyList())
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
    when(connectionContext.isCloudFetchEnabled()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    when(parentStatement.getStatement()).thenReturn(statement);
    when(parentStatement.getMaxRows()).thenReturn(10);
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
            .setResultRowLimit(10)
            .setSessionHandle(SESSION_HANDLE)
            .setCanReadArrowResult(true)
            .setCanDecompressLZ4Result(true)
            .setRunAsync(true)
            .setCanDownloadResult(true)
            .setParameters(Collections.emptyList())
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
    when(connectionContext.getEnableMultipleCatalogSupport()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
    TGetCatalogsReq request = new TGetCatalogsReq().setSessionHandle(SESSION_HANDLE);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);
    client.listCatalogs(session);

    ArgumentCaptor<TGetCatalogsReq> captor = ArgumentCaptor.forClass(TGetCatalogsReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    TGetCatalogsReq capturedRequest = captor.getValue();
    assertFalse(
        capturedRequest.isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION); // latest version
    TColumn tColumn = new TColumn();
    tColumn.setStringVal(new TStringColumn().setValues(Collections.singletonList(TEST_CATALOG)));
    when(resultData.getColumns()).thenReturn(Collections.singletonList(tColumn));
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);
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
    when(thriftAccessor.fetchResultsWithAbsoluteOffset(any(), anyLong())).thenReturn(response);
    when(resultData.getResultLinks())
        .thenReturn(
            Collections.singletonList(new TSparkArrowResultLink().setFileLink(TEST_STRING)));
    // Pass chunkIndex=0 and rowOffset=0 for the first chunk
    ChunkLinkFetchResult result = client.getResultChunks(TEST_STMT_ID, 0, 0);
    List<ExternalLink> chunkLinks = result.getChunkLinks();
    assertEquals(1, chunkLinks.size());
    assertEquals(TEST_STRING, chunkLinks.get(0).getExternalLink());
  }

  @Test
  void testGetResultChunksReturnsEmptyWhenNoLinks() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(thriftAccessor.fetchResultsWithAbsoluteOffset(any(), anyLong())).thenReturn(response);
    when(resultData.getResultLinks()).thenReturn(null);
    ChunkLinkFetchResult result = client.getResultChunks(TEST_STMT_ID, 0, 0);
    assertTrue(result.isEndOfStream());
    assertEquals(0, result.getChunkLinks().size());
  }

  @Test
  void testGetResultChunksRowOffsetMismatchThrowsException() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);

    long requestedStartRowOffset = 1000L;
    long actualStartRowOffset = 500L;

    TSparkArrowResultLink resultLink =
        new TSparkArrowResultLink()
            .setFileLink(TEST_STRING)
            .setStartRowOffset(actualStartRowOffset)
            .setRowCount(100)
            .setBytesNum(1024)
            .setExpiryTime(System.currentTimeMillis() + 3600000);

    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setHasMoreRows(false)
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(thriftAccessor.fetchResultsWithAbsoluteOffset(any(), eq(requestedStartRowOffset)))
        .thenReturn(response);
    when(resultData.getResultLinks()).thenReturn(Collections.singletonList(resultLink));

    assertThrows(
        DatabricksSQLException.class,
        () -> client.getResultChunks(TEST_STMT_ID, 5, requestedStartRowOffset));
  }

  @Test
  void testListTableTypes() throws SQLException {
    // Mock connection context to disable metric view metadata by default
    when(connectionContext.getEnableMetricViewMetadata()).thenReturn(false);

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

    // Test with older protocol version
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
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
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);

    client.listSchemas(session, TEST_CATALOG, TEST_SCHEMA);

    ArgumentCaptor<TGetSchemasReq> captor = ArgumentCaptor.forClass(TGetSchemasReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    assertFalse(
        captor.getValue().isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION);
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);

    DatabricksResultSet resultSet = client.listSchemas(session, TEST_CATALOG, TEST_SCHEMA);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListTables() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    String[] tableTypes = {"testTableType"};

    // Test with older protocol version
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
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
    TColumn tColumn = new TColumn();
    tColumn.setStringVal(new TStringColumn().setValues(Collections.singletonList("")));
    when(resultData.getColumns()).thenReturn(List.of(tColumn, tColumn, tColumn, tColumn));
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);

    client.listTables(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, tableTypes);

    ArgumentCaptor<TGetTablesReq> captor = ArgumentCaptor.forClass(TGetTablesReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    TGetTablesReq capturedRequestOlderVersion = captor.getValue();
    assertFalse(
        capturedRequestOlderVersion.isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION);
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);

    DatabricksResultSet resultSet =
        client.listTables(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, tableTypes);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListColumns() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);

    // Test with older protocol version
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
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
    when(resultData.getColumns()).thenReturn(new ArrayList<>());
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);

    client.listColumns(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, TEST_STRING);

    ArgumentCaptor<TGetColumnsReq> captor = ArgumentCaptor.forClass(TGetColumnsReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    TGetColumnsReq capturedRequestOlderVersion = captor.getValue();
    assertFalse(
        capturedRequestOlderVersion.isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION);
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);

    DatabricksResultSet resultSet =
        client.listColumns(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, TEST_STRING);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);

    // Test metadata properties
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

    // Test with older protocol version
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
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
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);

    client.listFunctions(session, TEST_CATALOG, TEST_SCHEMA, TEST_STRING);

    ArgumentCaptor<TGetFunctionsReq> captor = ArgumentCaptor.forClass(TGetFunctionsReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    TGetFunctionsReq capturedRequestOlderVersion = captor.getValue();
    assertFalse(
        capturedRequestOlderVersion.isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION);
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);

    DatabricksResultSet resultSet =
        client.listFunctions(session, TEST_CATALOG, TEST_SCHEMA, TEST_STRING);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListFunctionsWithSQLEnabled() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(connectionContext.enableShowCommandsForGetFunctions()).thenReturn(true);
    when(connectionContext.shouldEnableArrow()).thenReturn(true);
    when(connectionContext.isCloudFetchEnabled()).thenReturn(true);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TSparkArrowTypes arrowNativeTypes =
        new TSparkArrowTypes()
            .setComplexTypesAsArrow(true)
            .setIntervalTypesAsArrow(true)
            .setNullTypeAsArrow(true)
            .setDecimalAsArrow(true)
            .setTimestampAsArrow(true);
    TExecuteStatementReq executeStatementReq =
        new TExecuteStatementReq()
            .setStatement(
                "SHOW FUNCTIONS IN CATALOG `catalog1` SCHEMA LIKE 'testSchema' LIKE 'test'")
            .setSessionHandle(SESSION_HANDLE)
            .setCanReadArrowResult(true)
            .setCanDecompressLZ4Result(true)
            .setCanDownloadResult(true)
            .setQueryTimeout(0)
            .setParameters(Collections.emptyList())
            .setRunAsync(true)
            .setUseArrowNativeTypes(arrowNativeTypes);
    when(thriftAccessor.execute(executeStatementReq, null, session, StatementType.METADATA))
        .thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(mockedMetaData);
    when(mockedMetaData.getColumnCount()).thenReturn(6);
    when(mockedMetaData.getColumnName(1)).thenReturn("functionName");
    when(mockedMetaData.getColumnName(2)).thenReturn("namespace");
    when(mockedMetaData.getColumnName(3)).thenReturn("catalogName");
    when(mockedMetaData.getColumnName(4)).thenReturn("remarks");
    when(mockedMetaData.getColumnName(5)).thenReturn("functionType");
    when(mockedMetaData.getColumnName(6)).thenReturn("specificName");
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getObject("functionName")).thenReturn("my_fn");
    when(resultSet.getObject("namespace")).thenReturn(TEST_SCHEMA);
    when(resultSet.getObject("remarks")).thenReturn("remark");
    when(resultSet.getObject("functionType")).thenReturn(1);
    when(resultSet.getObject("specificName")).thenReturn("my_fn");

    ResultSet actualResultSet =
        client.listFunctions(session, TEST_CATALOG, TEST_SCHEMA, TEST_STRING);
    assertNotNull(actualResultSet);
    assertTrue(actualResultSet.next());
    assertEquals(TEST_CATALOG, actualResultSet.getString("FUNCTION_CAT"));
    assertEquals("my_fn", actualResultSet.getString("FUNCTION_NAME"));
  }

  @Test
  void testGetRequest_DefaultTimeoutAndNoRowLimit_WhenParentStatementNull() throws SQLException {
    when(connectionContext.shouldEnableArrow()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);

    when(thriftAccessor.execute(
            any(TExecuteStatementReq.class), eq(null), eq(session), eq(StatementType.SQL)))
        .thenReturn(resultSet);

    client.executeStatement(
        TEST_STRING, CLUSTER_COMPUTE, Collections.emptyMap(), StatementType.SQL, session, null);

    ArgumentCaptor<TExecuteStatementReq> requestCaptor =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(thriftAccessor)
        .execute(requestCaptor.capture(), eq(null), eq(session), eq(StatementType.SQL));
    TExecuteStatementReq request = requestCaptor.getValue();
    assertEquals(0, request.getQueryTimeout());
    assertFalse(request.isSetResultRowLimit());
  }

  @Test
  void testGetRequest_DefaultTimeout_WhenStatementNull() throws SQLException {
    when(connectionContext.shouldEnableArrow()).thenReturn(true);
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    when(parentStatement.getStatement()).thenReturn(null);
    when(parentStatement.getMaxRows()).thenReturn(0);

    when(thriftAccessor.execute(
            any(TExecuteStatementReq.class),
            eq(parentStatement),
            eq(session),
            eq(StatementType.SQL)))
        .thenReturn(resultSet);

    client.executeStatement(
        TEST_STRING,
        CLUSTER_COMPUTE,
        Collections.emptyMap(),
        StatementType.SQL,
        session,
        parentStatement);

    ArgumentCaptor<TExecuteStatementReq> requestCaptor =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(thriftAccessor)
        .execute(requestCaptor.capture(), eq(parentStatement), eq(session), eq(StatementType.SQL));
    TExecuteStatementReq request = requestCaptor.getValue();
    assertEquals(0, request.getQueryTimeout());
    assertFalse(request.isSetResultRowLimit());
  }

  @Test
  void testListPrimaryKeys() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);

    // Test with older protocol version
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
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
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);

    client.listPrimaryKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

    ArgumentCaptor<TGetPrimaryKeysReq> captor = ArgumentCaptor.forClass(TGetPrimaryKeysReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    TGetPrimaryKeysReq capturedRequestOlderVersion = captor.getValue();
    assertFalse(
        capturedRequestOlderVersion.isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION);
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);

    DatabricksResultSet resultSet =
        client.listPrimaryKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListImportedKeys() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);

    // Test with older protocol version
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
    TGetCrossReferenceReq request =
        new TGetCrossReferenceReq()
            .setSessionHandle(SESSION_HANDLE)
            .setForeignCatalogName(TEST_FOREIGN_CATALOG)
            .setForeignSchemaName(TEST_FOREIGN_SCHEMA)
            .setForeignTableName(TEST_FOREIGN_TABLE);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);

    client.listImportedKeys(session, TEST_FOREIGN_CATALOG, TEST_FOREIGN_SCHEMA, TEST_FOREIGN_TABLE);

    ArgumentCaptor<TGetCrossReferenceReq> captor =
        ArgumentCaptor.forClass(TGetCrossReferenceReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    TGetCrossReferenceReq capturedRequestOlderVersion = captor.getValue();
    assertFalse(
        capturedRequestOlderVersion.isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION);
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);

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

    // Test with older protocol version
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
    TGetCrossReferenceReq request =
        new TGetCrossReferenceReq()
            .setSessionHandle(SESSION_HANDLE)
            .setParentCatalogName(TEST_CATALOG)
            .setParentSchemaName(TEST_SCHEMA)
            .setParentTableName(TEST_TABLE);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);

    client.listExportedKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

    ArgumentCaptor<TGetCrossReferenceReq> captor =
        ArgumentCaptor.forClass(TGetCrossReferenceReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    TGetCrossReferenceReq capturedRequestOlderVersion = captor.getValue();
    assertFalse(
        capturedRequestOlderVersion.isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION);
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);

    DatabricksResultSet resultSet =
        client.listExportedKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testListCrossReferences() throws SQLException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);

    // Test with older protocol version
    client.setServerProtocolVersion(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1);
    TGetCrossReferenceReq request =
        new TGetCrossReferenceReq()
            .setSessionHandle(SESSION_HANDLE)
            .setParentCatalogName(TEST_CATALOG)
            .setParentSchemaName(TEST_SCHEMA)
            .setParentTableName(TEST_TABLE)
            .setForeignCatalogName(TEST_FOREIGN_CATALOG)
            .setForeignSchemaName(TEST_FOREIGN_SCHEMA)
            .setForeignTableName(TEST_FOREIGN_TABLE);
    TFetchResultsResp response =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(resultData.getColumns()).thenReturn(null);
    when(thriftAccessor.getThriftResponse(request)).thenReturn(response);

    client.listCrossReferences(
        session,
        TEST_CATALOG,
        TEST_SCHEMA,
        TEST_TABLE,
        TEST_FOREIGN_CATALOG,
        TEST_FOREIGN_SCHEMA,
        TEST_FOREIGN_TABLE);

    ArgumentCaptor<TGetCrossReferenceReq> captor =
        ArgumentCaptor.forClass(TGetCrossReferenceReq.class);
    verify(thriftAccessor).getThriftResponse(captor.capture());
    TGetCrossReferenceReq capturedRequestOlderVersion = captor.getValue();
    assertFalse(
        capturedRequestOlderVersion.isSetRunAsync(),
        "Expected runAsync to be unset for older protocol versions");

    client.setServerProtocolVersion(JDBC_THRIFT_VERSION);
    when(thriftAccessor.getThriftResponse(request.setRunAsync(true))).thenReturn(response);

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
  void testGetDatabricksConfig() {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(thriftAccessor.getDatabricksConfig()).thenReturn(databricksConfig);
    assertEquals(client.getDatabricksConfig(), databricksConfig);
  }

  @Test
  void testResetAccessToken() throws DatabricksParsingException {
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    when(thriftAccessor.getDatabricksConfig()).thenReturn(databricksConfig);
    when(databricksConfig.getHost()).thenReturn("test-host");
    client.resetAccessToken(NEW_ACCESS_TOKEN);
    verify(thriftAccessor).updateConfig(any(DatabricksConfig.class));
  }

  @Test
  public void testDecimalTypeWithValidPrecisionAndScale() {
    BigDecimal decimalValue = new BigDecimal("123.45"); // precision: 5, scale: 2
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(0).type(DECIMAL).value(decimalValue).build();

    TSparkParameter result = client.mapToSparkParameterListItem(parameter);

    assertEquals("DECIMAL(5,2)", result.getType());
    assertEquals("123.45", result.getValue().getStringValue());
    assertEquals(0, result.getOrdinal());
  }

  @Test
  public void testDecimalTypeWithScaleGreaterThanPrecision() {
    BigDecimal decimalValue = new BigDecimal("0.000123"); // scale: 6, precision: 3
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(1).type(DECIMAL).value(decimalValue).build();

    TSparkParameter result = client.mapToSparkParameterListItem(parameter);

    assertEquals("DECIMAL(6,6)", result.getType());
    assertEquals("0.000123", result.getValue().getStringValue());
    assertEquals(1, result.getOrdinal());
  }

  @Test
  public void testNonDecimalType() {
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(2).type(STRING).value(TEST_STRING).build();
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    TSparkParameter result = client.mapToSparkParameterListItem(parameter);

    assertEquals("STRING", result.getType());
    assertEquals(TEST_STRING, result.getValue().getStringValue());
    assertEquals(2, result.getOrdinal());
  }

  @Test
  public void testNullValue() {
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(3).type(INT).value(null).build();
    DatabricksThriftServiceClient client =
        new DatabricksThriftServiceClient(thriftAccessor, connectionContext);
    TSparkParameter result = client.mapToSparkParameterListItem(parameter);

    assertEquals("INT", result.getType());
    assertNull(result.getValue());
    assertEquals(3, result.getOrdinal());
  }
}
