package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.QUERY_EXECUTION_TIMEOUT_SQLSTATE;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;
import static com.databricks.jdbc.model.core.ColumnInfoTypeName.DECIMAL;
import static com.databricks.jdbc.model.core.ColumnInfoTypeName.INT;
import static com.databricks.jdbc.model.core.ColumnInfoTypeName.STRING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.MetadataOperationType;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.impl.common.ConfiguratorUtilsTest;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.common.TimeoutHandler;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksRateLimitException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.log.LogCapture;
import com.databricks.jdbc.model.client.sqlexec.*;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.Disposition;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.core.SessionVersion;
import com.databricks.jdbc.model.core.SqlExecResultFormat;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.service.sql.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksSdkClientTest {
  @Mock StatementExecutionService statementExecutionService;
  @Mock ApiClient apiClient;
  @Mock ResultData resultData;
  private static final String WAREHOUSE_ID = "99999999";
  private static final IDatabricksComputeResource warehouse = new Warehouse(WAREHOUSE_ID);
  // Reference to MetadataOperationType to ensure import is not removed
  private static final MetadataOperationType SAMPLE_OP_TYPE = MetadataOperationType.GET_CATALOGS;
  private static final String SESSION_ID = "session_id";
  private static final long INITIAL_SESSION_VERSION = 10L;
  private static final long UPDATED_SESSION_VERSION = 12L;
  private static final StatementId STATEMENT_ID = new StatementId("statementId");
  private static final String STATEMENT =
      "SELECT * FROM orders WHERE user_id = ? AND shard = ? AND region_code = ? AND namespace = ?";
  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;";
  private static final String DEFAULT_KEYSTORE_PASSWORD = "changeit";

  @Test
  void apcCreateSessionFailureKeepsUnsupportedMessageWithoutIdentifiers() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "UseThriftClient=0;TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(
            new DatabricksError(
                "UNSUPPORTED",
                "all-purpose clusters are not supported for private-org/private-cluster",
                400));

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException failure =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  client.createSession(
                      new AllPurposeCluster("private-org", "private-cluster"),
                      null,
                      null,
                      Map.of()));
      assertTrue(failure.getMessage().contains("all-purpose clusters are not supported"));
      assertEquals(DatabricksDriverErrorCode.CONNECTION_ERROR.name(), failure.getSQLState());
      assertEquals(DatabricksDriverErrorCode.CONNECTION_ERROR.getCode(), failure.getErrorCode());
      DatabricksError cause = assertInstanceOf(DatabricksError.class, failure.getCause());
      assertEquals("UNSUPPORTED", cause.getErrorCode());
      assertEquals(400, cause.getStatusCode());
      assertFalse(cause.getMessage().contains("private-org"));
      assertFalse(failure.getMessage().contains("private-org"));
      assertFalse(failure.getMessage().contains("private-cluster"));
      assertFalse(logs.getOutput().contains("private-org"));
      assertFalse(logs.getOutput().contains("private-cluster"));
    }
  }

  @Test
  void apcCreateSessionRateLimitUsesDriverErrorCode() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "UseThriftClient=0;TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(new DatabricksError("RATE_LIMITED", "private-cluster", 429));

    DatabricksRateLimitException failure =
        assertThrows(
            DatabricksRateLimitException.class,
            () ->
                client.createSession(
                    new AllPurposeCluster("private-org", "private-cluster"), null, null, Map.of()));
    assertEquals(DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED.name(), failure.getSQLState());
    assertEquals(DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED.getCode(), failure.getErrorCode());
    DatabricksError cause = assertInstanceOf(DatabricksError.class, failure.getCause());
    assertEquals("RATE_LIMITED", cause.getErrorCode());
    assertEquals(429, cause.getStatusCode());
    assertFalse(cause.getMessage().contains("private-cluster"));
    assertFalse(failure.getMessage().contains("private-cluster"));
  }

  @Test
  void apcRateLimitRedactsUnsafeServerCodeFromExceptionAndCause() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "UseThriftClient=0;TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(new DatabricksError("private-org/private-cluster", "private-secret", 429));

    DatabricksRateLimitException failure =
        assertThrows(
            DatabricksRateLimitException.class,
            () ->
                client.createSession(
                    new AllPurposeCluster("private-org", "private-cluster"), null, null, Map.of()));
    DatabricksError cause = assertInstanceOf(DatabricksError.class, failure.getCause());
    assertEquals("SERVER_ERROR", cause.getErrorCode());
    assertEquals(DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED.getCode(), failure.getErrorCode());
    for (String secret : List.of("private-org", "private-cluster", "private-secret")) {
      assertFalse(failure.getMessage().contains(secret));
      assertFalse(cause.getMessage().contains(secret));
    }
  }

  @Test
  void apcCreateSessionPreservesUnsupportedRegionWithoutEchoedIdentifiers() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(
            new DatabricksError(
                "UNSUPPORTED_REGION", "unsupported region for private-org/private-cluster", 400));

    DatabricksSQLException failure =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                client.createSession(
                    new AllPurposeCluster("private-org", "private-cluster"), null, null, null));

    assertEquals(DatabricksDriverErrorCode.CONNECTION_ERROR.name(), failure.getSQLState());
    assertTrue(failure.getMessage().contains("unsupported region"));
    assertTrue(failure.getMessage().contains("UNSUPPORTED_REGION"));
    assertTrue(failure.getMessage().contains("HTTP 400"));
    assertFalse(failure.getMessage().contains("private-org"));
    DatabricksError cause = assertInstanceOf(DatabricksError.class, failure.getCause());
    assertEquals("UNSUPPORTED_REGION", cause.getErrorCode());
    assertEquals(400, cause.getStatusCode());
    assertFalse(cause.getMessage().contains("private-cluster"));
  }

  @Test
  void apcCreateSessionHandlesMissingServerMessage() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(new DatabricksError("PERMISSION_DENIED", null, 403));

    DatabricksSQLException failure =
        assertThrows(
            DatabricksSQLException.class,
            () -> client.createSession(new AllPurposeCluster("org", "cluster"), null, null, null));

    assertTrue(failure.getMessage().contains("PERMISSION_DENIED"));
    assertTrue(failure.getMessage().contains("HTTP 403"));
    assertEquals(403, ((DatabricksError) failure.getCause()).getStatusCode());
  }

  @Test
  void warehouseCreateSessionHandlesMissingServerMessage() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    DatabricksError serverError = new DatabricksError("FAILED", null, 500);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(serverError);

    DatabricksSQLException failure =
        assertThrows(
            DatabricksSQLException.class, () -> client.createSession(warehouse, null, null, null));

    assertSame(serverError, failure.getCause());
    assertTrue(failure.getMessage().contains("Error while establishing a connection"));
  }

  @Test
  void apcTerminateSessionTransportFailureRedactsExceptionAndLogs() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .sessionId("private-session")
            .computeResource(new AllPurposeCluster("private-org", "private-cluster"))
            .clientType(DatabricksClientType.SEA)
            .build();
    when(apiClient.serialize(any())).thenReturn("{}");
    when(apiClient.execute(any(Request.class), eq(Void.class)))
        .thenThrow(new IOException("private-org private-cluster private-session"));

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException failure =
          assertThrows(DatabricksSQLException.class, () -> client.deleteSession(info));
      assertEquals(DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(), failure.getSQLState());
      assertNull(failure.getCause());
      for (String secret : List.of("private-org", "private-cluster", "private-session")) {
        assertFalse(failure.getMessage().contains(secret));
        assertFalse(logs.getOutput().contains(secret));
      }
    }
  }

  @Test
  void apcCreateSessionIoFailureUsesDriverErrorCode() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "UseThriftClient=0;TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(new IOException("private-cluster"));

    DatabricksSQLException failure =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                client.createSession(
                    new AllPurposeCluster("private-org", "private-cluster"), null, null, Map.of()));
    assertEquals(DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(), failure.getSQLState());
    assertEquals(DatabricksDriverErrorCode.SDK_CLIENT_ERROR.getCode(), failure.getErrorCode());
    assertFalse(failure.getMessage().contains("private-cluster"));
  }

  @Test
  void ordinaryApcSyncAndAsyncExecuteLogsOmitComputeAndSessionIds() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            "jdbc:databricks://sample-host.18.azuredatabricks.net:443/default;"
                + "AuthMech=3;httpPath=/sql/protocolv1/o/private-org-541/private-cluster-612;"
                + "UseThriftClient=0;TelemetryLogLevel=OFF;",
            new Properties());
    AllPurposeCluster cluster = new AllPurposeCluster("private-org-541", "private-cluster-612");
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .sessionId("private-session-725")
                .computeResource(cluster)
                .clientType(DatabricksClientType.SEA)
                .build());
    when(session.getSessionId()).thenReturn("private-session-725");
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("private-statement-904")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    try (LogCapture logs = new LogCapture()) {
      client.executeStatement(
          "select 1", cluster, Map.of(), StatementType.QUERY, session, null, null);
      client.executeStatementAsync("select 1", cluster, Map.of(), session, null);
      assertFalse(logs.getOutput().contains("private-org-541"));
      assertFalse(logs.getOutput().contains("private-cluster-612"));
      assertFalse(logs.getOutput().contains("private-session-725"));
      assertFalse(logs.getOutput().contains("private-statement-904"));
      assertTrue(logs.getOutput().contains("all-purpose cluster"));
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = StatementState.class,
      names = {"FAILED", "CANCELED"})
  void ordinaryApcSyncFailureRedactsStatementId(StatementState state) throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("private-org", "private-cluster");
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("private-session")
                .computeResource(cluster)
                .build());
    when(session.getSessionId()).thenReturn("private-session");
    String rawId = "private-ordinary-statement-id";
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId(rawId)
                .setStatus(new StatementStatus().setState(state).setSqlState("42000")));

    try (LogCapture logs = new LogCapture()) {
      SQLException failure =
          assertThrows(
              SQLException.class,
              () ->
                  client.executeStatement(
                      "SELECT 1", cluster, Map.of(), StatementType.QUERY, session, null, null));
      assertFalse(failure.getMessage().contains(rawId));
      assertTrue(failure.getMessage().contains("[REDACTED]"));
      assertFalse(logs.getOutput().contains(rawId));
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = StatementState.class,
      names = {"FAILED", "CANCELED"})
  void ordinaryApcAsyncResultFailureRedactsStatementId(StatementState state) throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("private-org", "private-cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("private-session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("private-session");
    String rawId = "private-ordinary-async-statement-id";
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatementId(rawId)
                .setStatus(new StatementStatus().setState(state).setSqlState("42000")));

    try (LogCapture logs = new LogCapture()) {
      SQLException failure =
          assertThrows(
              SQLException.class,
              () ->
                  client.getStatementResult(
                      StatementId.forCurrentSession(rawId, info), session, null));
      assertFalse(failure.getMessage().contains(rawId));
      assertTrue(failure.getMessage().contains("[REDACTED]"));
      assertFalse(logs.getOutput().contains(rawId));
    }
  }

  private static final Map<Integer, ImmutableSqlParameter> sqlParams =
      new HashMap<>() {
        {
          put(1, getSqlParam(1, 100, DatabricksTypeUtil.BIGINT));
          put(2, getSqlParam(2, (short) 10, DatabricksTypeUtil.SMALLINT));
          put(3, getSqlParam(3, (byte) 15, DatabricksTypeUtil.TINYINT));
          put(4, getSqlParam(4, "value", DatabricksTypeUtil.STRING));
        }
      };

  @Test
  void failedApcExecutionClosesRecordedOperation() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42247L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    IDatabricksStatementInternal parent = mock(IDatabricksStatementInternal.class);
    when(parent.getStatement()).thenReturn(mock(java.sql.Statement.class));
    when(apiClient.serialize(any())).thenReturn("{}");
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("failed-operation")
                .setStatus(new StatementStatus().setState(StatementState.FAILED)));

    assertThrows(
        SQLException.class,
        () ->
            client.executeStatement(
                "INSERT INTO t VALUES (7)",
                cluster,
                Map.of(),
                StatementType.UPDATE,
                session,
                parent,
                null));

    verify(parent).setStatementId(any(StatementId.class));
    verify(parent).closeServerOperation();
  }

  @Test
  void failedApcMetadataExecutionWithoutParentClosesRecordedOperation() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("failed-metadata")
                .setStatus(new StatementStatus().setState(StatementState.FAILED)));

    assertThrows(
        SQLException.class,
        () ->
            client.executeStatement(
                "SELECT 1", cluster, Map.of(), StatementType.METADATA, session, null, null));

    ArgumentCaptor<Request> requests = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(requests.capture(), eq(Void.class));
    assertEquals(Request.DELETE, requests.getValue().getMethod());
    assertTrue(requests.getValue().getUrl().contains("failed-metadata"));
  }

  @Test
  void failedApcMetadataExecutionPreservesSqlStateWhenCloseFails() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("failed-metadata")
                .setStatus(
                    new StatementStatus()
                        .setState(StatementState.FAILED)
                        .setSqlState("42601")
                        .setError(new ServiceError().setMessage("original SQL error"))));
    String secret = "bound-value-from-close";
    when(apiClient.execute(any(Request.class), eq(Void.class)))
        .thenThrow(new DatabricksException("DELETE echoed " + secret));

    try (LogCapture logs = new LogCapture()) {
      SQLException failure =
          assertThrows(
              SQLException.class,
              () ->
                  client.executeStatement(
                      "SELECT 1", cluster, Map.of(), StatementType.METADATA, session, null, null));
      assertEquals("42601", failure.getSQLState());
      assertFalse(failure.getMessage().contains("original SQL error"));
      assertFalse(logs.getOutput().contains("original SQL error"));
      assertFalse(logs.getOutput().contains(secret));
    }
    verify(apiClient).execute(any(Request.class), eq(Void.class));
  }

  @ParameterizedTest
  @EnumSource(
      value = StatementState.class,
      names = {"SUCCEEDED", "FAILED", "CANCELED"})
  void synchronousApcMetadataRedactsStatementIdInSuccessAndFailureDiagnostics(StatementState state)
      throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    if (state == StatementState.SUCCEEDED) {
      when(session.getConnectionContext()).thenReturn(context);
    }
    String rawId = "private-synchronous-metadata-id";
    ResultManifest successManifest =
        new ResultManifest()
            .setFormat(SqlExecResultFormat.ARROW_STREAM)
            .setTotalChunkCount(0L)
            .setSchema(new ResultSchema().setColumns(List.of()).setColumnCount(0L));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId(rawId)
                .setStatus(new StatementStatus().setState(state).setSqlState("42000"))
                .setManifest(state == StatementState.SUCCEEDED ? successManifest : null)
                .setResult(state == StatementState.SUCCEEDED ? new ResultData() : null));

    try (LogCapture logs = new LogCapture()) {
      if (state == StatementState.SUCCEEDED) {
        assertNotNull(
            client.executeStatement(
                "SHOW CATALOGS", cluster, Map.of(), StatementType.METADATA, session, null, null));
      } else {
        SQLException failure =
            assertThrows(
                SQLException.class,
                () ->
                    client.executeStatement(
                        "SHOW CATALOGS",
                        cluster,
                        Map.of(),
                        StatementType.METADATA,
                        session,
                        null,
                        null));
        assertFalse(failure.getMessage().contains(rawId));
        assertTrue(failure.getMessage().contains("[REDACTED]"));
      }
      assertFalse(logs.getOutput().contains(rawId));
      assertTrue(logs.getOutput().contains("[REDACTED]"));
      if (state == StatementState.SUCCEEDED) {
        assertTrue(logs.getOutput().contains("Empty result (total_chunk_count=0)"));
      }
    }
  }

  @Test
  void apcMetadataTimeoutRedactsStatementIdEvenWhenCancellationFails() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    String rawId = "private-timeout-metadata-id";
    StatementId id = StatementId.forCurrentSession(rawId, info);
    when(apiClient.execute(any(Request.class), eq(Void.class)))
        .thenThrow(new IOException("Cancel failed for " + rawId));
    TimeoutHandler timeout =
        TimeoutHandler.forStatement(
            1, id, client, DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR, true);

    Thread.sleep(2100);
    try (LogCapture logs = new LogCapture()) {
      DatabricksTimeoutException failure =
          assertThrows(DatabricksTimeoutException.class, timeout::checkTimeout);
      assertFalse(failure.getMessage().contains(rawId));
      assertTrue(failure.getMessage().contains("[REDACTED]"));
      assertFalse(logs.getOutput().contains(rawId));
    }
    ArgumentCaptor<Request> requests = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(requests.capture(), eq(Void.class));
    assertTrue(requests.getValue().getUrl().contains(rawId));
  }

  @Test
  void asyncApcMetadataFailureRedactsServerTextAndRetainsSqlState() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("session");
    String secret = "server-echoed-secret";
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatementId("metadata-id")
                .setStatus(
                    new StatementStatus()
                        .setState(StatementState.FAILED)
                        .setSqlState("42601")
                        .setError(new ServiceError().setMessage(secret))));
    StatementId id = StatementId.forCurrentSession("metadata-id", info);

    try (LogCapture logs = new LogCapture()) {
      SQLException failure =
          assertThrows(
              SQLException.class,
              () -> client.getStatementResult(id, session, null, StatementType.METADATA));
      assertEquals("42601", failure.getSQLState());
      assertFalse(failure.getMessage().contains(secret));
      assertFalse(logs.getOutput().contains(secret));
    }
  }

  @Test
  void asyncApcMetadataServerTimeoutRetainsNumericErrorCode() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("session");
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatementId("metadata-id")
                .setStatus(
                    new StatementStatus()
                        .setState(StatementState.FAILED)
                        .setSqlState(QUERY_EXECUTION_TIMEOUT_SQLSTATE)
                        .setError(new ServiceError().setMessage("server-echoed-secret"))));
    StatementId id = StatementId.forCurrentSession("metadata-id", info);

    DatabricksTimeoutException failure =
        assertThrows(
            DatabricksTimeoutException.class,
            () -> client.getStatementResult(id, session, null, StatementType.METADATA));
    assertEquals(DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR.name(), failure.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR.getCode(), failure.getErrorCode());
    assertFalse(failure.getMessage().contains("server-echoed-secret"));
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void apcChunkFailuresRedactSdkDetails(boolean externalLinks, boolean sdkFailure)
      throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(new AllPurposeCluster("org", "cluster"))
            .build();
    StatementId id = StatementId.forCurrentSession("statement", info);
    String secret = "echoed-bound-value";
    when(apiClient.execute(any(Request.class), eq(ResultData.class)))
        .thenThrow(
            sdkFailure
                ? new DatabricksException("server echoed " + secret)
                : new IOException("transport echoed " + secret));

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException failure =
          assertThrows(
              DatabricksSQLException.class,
              () -> {
                if (externalLinks) {
                  client.getResultChunks(id, 0, 0);
                } else {
                  client.getResultChunksData(id, 0, 0);
                }
              });
      assertEquals(DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(), failure.getSQLState());
      assertFalse(failure.getMessage().contains(secret));
      assertNull(failure.getCause());
      assertFalse(logs.getOutput().contains(secret));
    }
  }

  @Test
  void directApcDowngradeRejectsMissingFirstBindingBeforeRequest() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42247L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);

    assertThrows(
        com.databricks.jdbc.exception.DatabricksValidationException.class,
        () ->
            client.executeStatement(
                "SELECT ?",
                cluster,
                Map.of(2, getSqlParam(2, "wrong", DatabricksTypeUtil.STRING)),
                StatementType.METADATA,
                session,
                null,
                null));
    verify(apiClient, never()).execute(any(Request.class), eq(ExecuteStatementResponse.class));
  }

  @Test
  void warehouseBinaryParameterMappingRetainsExistingBytes() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    byte[] bytes = new byte[] {0x00, (byte) 0xFF};
    StatementParameterListItem wire =
        client.mapToParameterListItem(getSqlParam(1, bytes, DatabricksTypeUtil.BINARY));

    assertEquals("BINARY", wire.getType());
    assertEquals(bytes.toString(), wire.getValue());
  }

  private static SessionVersion sessionVersion(long versionId) {
    return new SessionVersion().setVersionId(versionId);
  }

  private static CreateSessionResponse createSessionResponse() {
    return new CreateSessionResponse()
        .setSessionId(SESSION_ID)
        .setSessionVersion(sessionVersion(INITIAL_SESSION_VERSION));
  }

  private void setupSessionMocks() throws IOException {
    CreateSessionResponse response = createSessionResponse();
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(response);
  }

  private void setupClientMocks(boolean includeResults, boolean async) throws IOException {
    setupClientMocks(includeResults, async, new ArrayList<>(), null, null, null);
  }

  private void setupClientMocks(
      boolean includeResults,
      boolean async,
      List<ColumnInfo> manifestColumns,
      Boolean isNativeMetadataResult)
      throws IOException {
    setupClientMocks(includeResults, async, manifestColumns, isNativeMetadataResult, null, null);
  }

  private void setupClientMocks(
      boolean includeResults,
      boolean async,
      Long initialSessionVersionId,
      Long statementSessionVersionId)
      throws IOException {
    setupClientMocks(
        includeResults,
        async,
        new ArrayList<>(),
        null,
        initialSessionVersionId,
        statementSessionVersionId);
  }

  private void setupClientMocks(
      boolean includeResults,
      boolean async,
      List<ColumnInfo> manifestColumns,
      Boolean isNativeMetadataResult,
      Long initialSessionVersionId,
      Long statementSessionVersionId)
      throws IOException {
    List<StatementParameterListItem> params =
        new ArrayList<>() {
          {
            add(getParam("LONG", "100", 1));
            add(getParam("SHORT", "10", 2));
            add(getParam("SHORT", "15", 3));
            add(getParam("STRING", "value", 4));
          }
        };

    StatementStatus statementStatus = new StatementStatus().setState(StatementState.SUCCEEDED);
    if (statementSessionVersionId != null) {
      statementStatus.setSessionVersion(sessionVersion(statementSessionVersionId));
    }
    ExecuteStatementRequest executeStatementRequest =
        new ExecuteStatementRequest()
            .setSessionId(SESSION_ID)
            .setWarehouseId(WAREHOUSE_ID)
            .setStatement(STATEMENT)
            .setDisposition(Disposition.INLINE_OR_EXTERNAL_LINKS)
            .setFormat(SqlExecResultFormat.ARROW_STREAM)
            .setRowLimit(100L)
            .setParameters(params);
    if (async) {
      executeStatementRequest.setWaitTimeout("0s");
    } else {
      executeStatementRequest
          .setWaitTimeout("10s")
          .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE);
    }
    ExecuteStatementResponse response =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(statementStatus);
    if (includeResults) {
      response
          .setResult(resultData)
          .setManifest(
              new ResultManifest()
                  .setFormat(SqlExecResultFormat.JSON_ARRAY)
                  .setSchema(
                      new ResultSchema()
                          .setColumns(manifestColumns)
                          .setColumnCount((long) manifestColumns.size()))
                  .setIsNativeMetadataResult(isNativeMetadataResult)
                  .setTotalRowCount(0L));
    }

    when(apiClient.execute(any(Request.class), any()))
        .thenAnswer(
            invocationOnMock -> {
              Request req = invocationOnMock.getArgument(0, Request.class);
              if (req.getUrl().equals(STATEMENT_PATH)) {
                return response;
              } else if (req.getUrl().equals(String.format(WAREHOUSE_SESSION_PATH, WAREHOUSE_ID))) {
                return initialSessionVersionId != null
                    ? new CreateSessionResponse()
                        .setSessionId(SESSION_ID)
                        .setSessionVersion(sessionVersion(initialSessionVersionId))
                    : new CreateSessionResponse().setSessionId(SESSION_ID);
              }
              return null;
            });
  }

  @Test
  public void testCreateSession() throws DatabricksSQLException, IOException {
    setupSessionMocks();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSessionInfo sessionInfo =
        databricksSdkClient.createSession(
            warehouse, "catalog", "schema", Map.of("timezone", "UTC"));
    assertEquals(sessionInfo.sessionId(), SESSION_ID);
    assertEquals(sessionInfo.computeResource(), warehouse);
    assertEquals(INITIAL_SESSION_VERSION, sessionInfo.sessionVersionId());
    assertFalse(sessionInfo.realtimeSession());
    verify(apiClient)
        .execute(
            argThat(
                req ->
                    req.getMethod().equals(Request.POST)
                        && req.getUrl()
                            .equals(String.format(WAREHOUSE_SESSION_PATH, WAREHOUSE_ID))),
            eq(CreateSessionResponse.class));
    verify(apiClient)
        .serialize(
            argThat(
                request ->
                    request instanceof CreateSessionRequest
                        && "FAST".equals(((CreateSessionRequest) request).getExecutionMode())
                        && ((CreateSessionRequest) request).getWarehouseId() == null
                        && "catalog".equals(((CreateSessionRequest) request).getCatalog())
                        && "schema".equals(((CreateSessionRequest) request).getSchema())
                        && Map.of("timezone", "UTC")
                            .equals(((CreateSessionRequest) request).getSessionConfigs())));
  }

  @ParameterizedTest
  @MethodSource("sessionRoutes")
  public void testSessionLifecycleWireContract(String realtimeHeader, boolean boundedSea)
      throws Exception {
    boolean realtime = realtimeHeader != null && "true".equalsIgnoreCase(realtimeHeader.trim());
    List<Request> sent = new ArrayList<>();
    HttpClient transport =
        request -> {
          String path = request.getUri().getPath();
          if (path.equals("/oidc/.well-known/oauth-authorization-server")) {
            return new Response(request, 200, "OK", Map.of(), "{}");
          }
          sent.add(request);
          if (path.equals(String.format(WAREHOUSE_SESSION_PATH, WAREHOUSE_ID))) {
            Map<String, List<String>> headers =
                realtimeHeader == null
                    ? Map.of()
                    : Map.of("X-Databricks-RT-Session", List.of(realtimeHeader));
            return new Response(
                request,
                200,
                "OK",
                headers,
                "{\"session_id\":\"session_id\",\"session_version\":{\"version_id\":10}}");
          }
          String body;
          if (Request.POST.equals(request.getMethod()) && !path.endsWith("/cancel")) {
            body = "{\"statement_id\":\"owned-statement\",\"status\":{\"state\":\"PENDING\"}}";
          } else if (path.endsWith("/status")) {
            body = "{\"state\":\"SUCCEEDED\"}";
          } else if (path.contains("/result/chunks/")) {
            body = "{\"external_links\":[]}";
          } else {
            body =
                "{\"statement_id\":\"owned-statement\",\"status\":{\"state\":\"SUCCEEDED\","
                    + "\"session_version\":{\"version_id\":12}}}";
          }
          return new Response(
              request,
              200,
              "OK",
              Map.of("X-Databricks-RT-Session", List.of(Boolean.toString(!realtime))),
              body);
        };
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost("https://example.com")
            .setAuthType("pat")
            .setToken("test-token")
            .setHttpClient(transport);
    ApiClient realApiClient = DatabricksSdkClient.buildApiClient(config, null, true);
    Properties properties = new Properties();
    properties.setProperty("UseBoundedSeaApi", boundedSea ? "1" : "0");
    IDatabricksConnectionContext context = DatabricksConnectionContext.parse(JDBC_URL, properties);
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, realApiClient);
    DatabricksConnection connection = new DatabricksConnection(context, client);
    connection.open();
    assertEquals(realtime, connection.getSession().getSessionInfo().realtimeSession());

    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    client.executeStatement(
        STATEMENT,
        warehouse,
        sqlParams,
        StatementType.QUERY,
        connection.getSession(),
        statement,
        null);
    client.executeStatementAsync(
        STATEMENT, warehouse, sqlParams, connection.getSession(), statement);
    StatementId owned = statement.getStatementId();
    client.checkStatementAlive(owned);
    client.getStatementResult(owned, connection.getSession(), statement);
    client.getResultChunks(owned, 0, 64);
    client.getResultChunksData(owned, 0, 32);
    client.cancelStatement(owned);
    client.closeStatement(owned);

    // Recover through the public connection API to retain the legacy warehouse wire route.
    StatementId detached =
        ((DatabricksStatement) connection.getStatement("detached-statement")).getStatementId();
    client.checkStatementAlive(detached);
    client.getStatementResult(detached, connection.getSession(), statement);
    client.getResultChunks(detached, 0, 64);
    client.getResultChunksData(detached, 0, 32);
    client.cancelStatement(detached);
    client.closeStatement(detached);
    assertEquals(realtime, connection.getSession().getSessionInfo().realtimeSession());
    connection.close();

    assertEquals(
        17,
        sent.size(),
        () ->
            sent.stream()
                .map(Request::getRequestLine)
                .collect(java.util.stream.Collectors.joining("\n")));
    assertWarehouseWireFixture(sent, realtime, boundedSea);
    assertWireRequest(
        sent.get(0), Request.POST, String.format(WAREHOUSE_SESSION_PATH, WAREHOUSE_ID), Map.of());
    ObjectMapper mapper = new ObjectMapper();
    JsonNode createBody = mapper.readTree(sent.get(0).getBodyString());
    assertEquals("FAST", createBody.path("execution_mode").asText());
    assertFalse(createBody.has("warehouse_id"));
    String collectionPath =
        realtime
            ? String.format(WAREHOUSE_STATEMENT_PATH, WAREHOUSE_ID, SESSION_ID)
            : STATEMENT_PATH;
    for (int index : List.of(1, 3)) {
      Request execute = sent.get(index);
      assertWireRequest(execute, Request.POST, collectionPath, Map.of());
      JsonNode body = mapper.readTree(execute.getBodyString());
      assertEquals(!realtime, body.has("warehouse_id"));
      assertEquals(!realtime, body.has("session_id"));
      assertFalse(body.has("statement_id"));
      if (!realtime) {
        assertEquals(WAREHOUSE_ID, body.path("warehouse_id").asText());
        assertEquals(SESSION_ID, body.path("session_id").asText());
      }
      assertEquals(STATEMENT, body.path("statement").asText());
      assertEquals(index == 1 ? 10 : 12, body.path("session_version").path("version_id").asInt());
    }
    String ownedPath =
        realtime
            ? String.format(
                WAREHOUSE_STATEMENT_PATH_WITH_ID, WAREHOUSE_ID, SESSION_ID, "owned-statement")
            : String.format(STATEMENT_PATH_WITH_ID, "owned-statement");
    assertWireRequest(sent.get(2), Request.GET, ownedPath, Map.of());
    if (realtime) {
      assertNull(sent.get(2).getBodyString());
    } else {
      assertEquals(
          mapper.readTree(sent.get(1).getBodyString()),
          mapper.readTree(sent.get(2).getBodyString()));
    }
    assertStatementFollowUps(
        sent.subList(4, 10), ownedPath, "owned-statement", realtime, boundedSea);
    assertStatementFollowUps(
        sent.subList(10, 16),
        String.format(STATEMENT_PATH_WITH_ID, "detached-statement"),
        "detached-statement",
        false,
        boundedSea);
    assertWireRequest(
        sent.get(16),
        Request.DELETE,
        realtime
            ? String.format(WAREHOUSE_SESSION_PATH_WITH_ID, WAREHOUSE_ID, SESSION_ID)
            : String.format(SESSION_PATH_WITH_ID, SESSION_ID),
        realtime ? Map.of() : Map.of("warehouse_id", List.of(WAREHOUSE_ID)));
    assertNull(sent.get(16).getBodyString());
    for (Request request : sent) {
      assertTrue(
          request.getHeaders().keySet().stream()
              .map(name -> name.toLowerCase(Locale.ROOT))
              .noneMatch(
                  Set.of(
                          "x-databricks-rt-session",
                          "x-databricks-session-id",
                          "x-databricks-statement-id")
                      ::contains));
    }
  }

  private static void assertStatementFollowUps(
      List<Request> requests, String path, String id, boolean warehouseRoute, boolean boundedSea)
      throws IOException {
    assertWireRequest(requests.get(0), Request.GET, path + "/status", Map.of());
    assertNull(requests.get(0).getBodyString());
    assertWireRequest(requests.get(1), Request.GET, path, Map.of());
    assertWireRequest(
        requests.get(2),
        Request.GET,
        path + "/result/chunks/0",
        boundedSea ? Map.of("row_offset", List.of("64")) : Map.of());
    assertWireRequest(
        requests.get(3),
        Request.GET,
        path + "/result/chunks/0",
        boundedSea ? Map.of("row_offset", List.of("32")) : Map.of());
    assertWireRequest(requests.get(4), Request.POST, path + "/cancel", Map.of());
    assertWireRequest(requests.get(5), Request.DELETE, path, Map.of());
    ObjectMapper mapper = new ObjectMapper();
    for (int index = 1; index < requests.size(); index++) {
      String body = requests.get(index).getBodyString();
      if (warehouseRoute) {
        assertNull(body);
      } else {
        String expected = index >= 4 ? "{\"statementId\":\"" + id + "\"}" : "{}";
        assertEquals(mapper.readTree(expected), mapper.readTree(body));
      }
    }
  }

  private static void assertWarehouseWireFixture(
      List<Request> requests, boolean realtime, boolean boundedSea) throws IOException {
    String fixture =
        "wire/warehouse-"
            + (realtime ? "realtime" : "standard")
            + (boundedSea ? "-bounded" : "")
            + ".txt";
    InputStream stream =
        DatabricksSdkClientTest.class.getClassLoader().getResourceAsStream(fixture);
    assertNotNull(stream, fixture);
    List<String> expected;
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      expected =
          reader
              .lines()
              .filter(line -> !line.startsWith("#"))
              .collect(java.util.stream.Collectors.toList());
    }
    assertEquals(requests.size(), expected.size(), fixture);
    Map<String, String> baselineHeaders =
        Map.of(
            "Authorization", "Bearer test-token",
            "User-Agent",
                "DatabricksJDBCDriverOSS/<driver-version> databricks-sdk-java/0.118.0"
                    + " jvm/<runtime> os/<platform> Java/THttpClient auth/pat",
            "Accept", "application/json",
            "Content-Type", "application/json");
    for (int index = 0; index < requests.size(); index++) {
      String[] fields = expected.get(index).split("\t", -1);
      assertEquals(3, fields.length, fixture + " line " + index);
      Request request = requests.get(index);
      assertEquals(fields[0], request.getMethod(), fixture + " method " + index);
      assertEquals(fields[1], request.getUrl(), fixture + " URL " + index);
      assertEquals(
          fields[2].equals("<null>") ? null : fields[2],
          request.getBodyString(),
          fixture + " body " + index);
      Map<String, String> headers = new HashMap<>(request.getHeaders());
      String userAgent = headers.get("User-Agent");
      assertNotNull(userAgent, fixture + " User-Agent " + index);
      // The SDK accumulates User-Agent entries from unrelated tests in the same JVM. The first
      // four segments, the Thrift client marker, and auth mode are the stable pre-APC fixture.
      String[] segments = userAgent.split(" ");
      assertTrue(segments.length >= 6, fixture + " User-Agent " + index);
      assertTrue(
          Arrays.asList(segments).contains("Java/THttpClient"), fixture + " client " + index);
      assertEquals("auth/pat", segments[segments.length - 1], fixture + " auth " + index);
      headers.put(
          "User-Agent",
          String.join(
                  " ",
                  segments[0],
                  segments[1],
                  segments[2],
                  segments[3],
                  "Java/THttpClient",
                  segments[segments.length - 1])
              .replaceAll(
                  "DatabricksJDBCDriverOSS/[^ ]+", "DatabricksJDBCDriverOSS/<driver-version>")
              .replaceAll("jvm/[^ ]+", "jvm/<runtime>")
              .replaceAll("os/[^ ]+", "os/<platform>"));
      assertEquals(baselineHeaders, headers, fixture + " headers " + index);
      assertEquals(
          requests.get(0).getHeaders(), request.getHeaders(), fixture + " header " + index);
    }
  }

  private static Stream<Arguments> sessionRoutes() {
    return Stream.of(
        Arguments.of("true", false),
        Arguments.of("true", true),
        Arguments.of(" TrUe ", false),
        Arguments.of("false", true),
        Arguments.of("unexpected", false),
        Arguments.of(null, false));
  }

  private static void assertWireRequest(
      Request request, String method, String path, Map<String, List<String>> query) {
    assertEquals(method, request.getMethod());
    assertEquals(path, request.getUri().getPath());
    Set<String> expected = new TreeSet<>();
    query.forEach((name, values) -> values.forEach(value -> expected.add(name + "=" + value)));
    String rawQuery = request.getUri().getRawQuery();
    assertEquals(
        expected, rawQuery == null ? Set.of() : new TreeSet<>(Arrays.asList(rawQuery.split("&"))));
  }

  @Test
  public void testCreateSessionWithoutInitialSessionVersion() throws Exception {
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(new CreateSessionResponse().setSessionId(SESSION_ID));
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    ImmutableSessionInfo sessionInfo =
        databricksSdkClient.createSession(warehouse, null, null, null);

    assertEquals(SESSION_ID, sessionInfo.sessionId());
    assertNull(sessionInfo.sessionVersionId());
  }

  @Test
  public void testCreateSessionRedirectPreservesErrorCodeAndCause() throws Exception {
    DatabricksError redirectError = new DatabricksError("307", "Redirect to Thrift Client", 307);

    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(redirectError);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    DatabricksSQLException error =
        assertThrows(
            DatabricksSQLException.class,
            () -> databricksSdkClient.createSession(warehouse, null, null, null));

    assertEquals("CONNECTION_ERROR", error.getSQLState());
    assertEquals(0, error.getErrorCode());
    assertEquals(1001, DatabricksDriverErrorCode.valueOf(error.getSQLState()).getCode());
    assertSame(redirectError, error.getCause());
    assertTrue(error.getMessage().contains("Redirect to Thrift Client"));
    assertTrue(error.getMessage().contains("HTTP 307"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSessionRedirectDoesNotFallBackWithRealSdk(boolean explicitSea) throws Exception {
    List<Request> sent = new ArrayList<>();
    HttpClient transport =
        request -> {
          if (request.getUri().getPath().equals("/oidc/.well-known/oauth-authorization-server")) {
            return new Response(request, 200, "OK", Map.of(), "{}");
          }
          sent.add(request);
          return new Response(
              request,
              307,
              "Temporary Redirect",
              Map.of(),
              "{\"error_code\":\"307\",\"message\":\"Redirect to Thrift Client\"}");
        };
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost("https://example.com")
            .setAuthType("pat")
            .setToken("test-token")
            .setHttpClient(transport);
    ApiClient realApiClient = DatabricksSdkClient.buildApiClient(config, null, true);
    Properties properties = new Properties();
    if (explicitSea) {
      properties.setProperty("UseThriftClient", "0");
    }
    IDatabricksConnectionContext context = DatabricksConnectionContext.parse(JDBC_URL, properties);
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(
        context,
        Map.of(
            "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc", "true"));

    try (MockedConstruction<DatabricksThriftServiceClient> thriftConstruction =
        mockConstruction(DatabricksThriftServiceClient.class)) {
      DatabricksSdkClient client =
          new DatabricksSdkClient(context, statementExecutionService, realApiClient);
      DatabricksSession session = new DatabricksSession(context, client);
      assertEquals(DatabricksClientType.SEA, context.getClientType());
      Object metadataClient = session.getDatabricksMetadataClient();

      DatabricksSQLException error = assertThrows(DatabricksSQLException.class, session::open);

      assertEquals("CONNECTION_ERROR", error.getSQLState());
      assertEquals(0, error.getErrorCode());
      assertEquals(1001, DatabricksDriverErrorCode.valueOf(error.getSQLState()).getCode());
      DatabricksError cause = assertInstanceOf(DatabricksError.class, error.getCause());
      assertEquals(307, cause.getStatusCode());
      assertTrue(error.getMessage().contains("HTTP 307"));
      assertFalse(session.isOpen());
      assertNull(session.getSessionInfo());
      assertNull(session.getSessionId());
      assertEquals(DatabricksClientType.SEA, context.getClientType());
      assertSame(client, session.getDatabricksClient());
      assertSame(metadataClient, session.getDatabricksMetadataClient());
      assertTrue(thriftConstruction.constructed().isEmpty());
      assertEquals(1, sent.size());
      assertWireRequest(
          sent.get(0), Request.POST, String.format(WAREHOUSE_SESSION_PATH, WAREHOUSE_ID), Map.of());
    } finally {
      DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(context, new HashMap<>());
    }
  }

  @Test
  public void testCreateSessionRateLimitPreservesErrorCodeAndCause() throws Exception {
    DatabricksError rateLimit = new DatabricksError("429", "Too many requests", 429);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(rateLimit);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    DatabricksRateLimitException error =
        assertThrows(
            DatabricksRateLimitException.class,
            () -> client.createSession(warehouse, null, null, null));

    assertEquals("RATE_LIMIT_EXCEEDED", error.getSQLState());
    assertEquals(0, error.getErrorCode());
    assertEquals(1044, DatabricksDriverErrorCode.valueOf(error.getSQLState()).getCode());
    assertEquals(429, error.getStatusCode());
    assertSame(rateLimit, error.getCause());
    assertTrue(error.getMessage().contains(WAREHOUSE_ID));
  }

  @Test
  public void testDeleteSession() throws DatabricksSQLException, IOException {
    String path = String.format(SESSION_PATH_WITH_ID, SESSION_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(new Warehouse(WAREHOUSE_ID))
            .build();
    databricksSdkClient.deleteSession(sessionInfo);

    // Verify a Request with DELETE method is created and executed
    verify(apiClient)
        .execute(
            argThat(req -> req.getMethod().equals(Request.DELETE) && req.getUrl().equals(path)),
            eq(Void.class));
  }

  @Test
  public void testDeleteSessionFromSerializedRealtimeConnectionId() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    ImmutableSessionInfo sessionInfo =
        com.databricks.jdbc.dbclient.impl.common.SessionId.deserialize(
                "s|" + WAREHOUSE_ID + "|" + SESSION_ID + "|r")
            .getSessionInfo();

    client.deleteSession(sessionInfo);

    verify(apiClient)
        .execute(
            argThat(
                req ->
                    req.getMethod().equals(Request.DELETE)
                        && req.getUrl()
                            .equals(
                                String.format(
                                    WAREHOUSE_SESSION_PATH_WITH_ID, WAREHOUSE_ID, SESSION_ID))
                        && req.getQuery().isEmpty()),
            eq(Void.class));
  }

  @Test
  public void testExecuteStatement() throws Exception {
    setupClientMocks(true, false, INITIAL_SESSION_VERSION, UPDATED_SESSION_VERSION);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);

    DatabricksResultSet resultSet =
        databricksSdkClient.executeStatement(
            STATEMENT,
            warehouse,
            sqlParams,
            StatementType.QUERY,
            connection.getSession(),
            statement,
            null);
    assertEquals(STATEMENT_ID, statement.getStatementId());
    assertNotNull(resultSet.getMetaData());
    assertEquals(
        UPDATED_SESSION_VERSION, connection.getSession().getSessionVersion().getVersionId());

    // Verify a Request with POST method is created and executed
    verify(apiClient, atLeastOnce())
        .serialize(
            argThat(
                request ->
                    request instanceof ExecuteStatementRequest
                        && sessionVersion(INITIAL_SESSION_VERSION)
                            .equals(((ExecuteStatementRequest) request).getSessionVersion())));
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> req.getMethod().equals(Request.POST) && req.getUrl().equals(STATEMENT_PATH)),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testExecuteStatementWithoutInitialSessionVersion() throws Exception {
    setupClientMocks(true, false, null, UPDATED_SESSION_VERSION);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    databricksSdkClient.executeStatement(
        STATEMENT,
        warehouse,
        sqlParams,
        StatementType.QUERY,
        connection.getSession(),
        statement,
        null);

    verify(apiClient, atLeastOnce())
        .serialize(
            argThat(
                request ->
                    request instanceof ExecuteStatementRequest
                        && ((ExecuteStatementRequest) request).getSessionVersion() == null));
    assertEquals(
        UPDATED_SESSION_VERSION, connection.getSession().getSessionVersion().getVersionId());

    clearInvocations(apiClient);
    DatabricksStatement nextStatement = new DatabricksStatement(connection);
    databricksSdkClient.executeStatement(
        STATEMENT,
        warehouse,
        sqlParams,
        StatementType.QUERY,
        connection.getSession(),
        nextStatement,
        null);

    verify(apiClient, atLeastOnce())
        .serialize(
            argThat(
                request ->
                    request instanceof ExecuteStatementRequest
                        && sessionVersion(UPDATED_SESSION_VERSION)
                            .equals(((ExecuteStatementRequest) request).getSessionVersion())));
  }

  @Test
  public void testNativeBatchCapabilityIsWarehouseOnly() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    assertTrue(databricksSdkClient.supportsNativeParameterBatching(warehouse));
    assertFalse(
        databricksSdkClient.supportsNativeParameterBatching(
            new AllPurposeCluster("org", "cluster")));
  }

  @Test
  public void testExecuteStatementBatchBuildsSeaParameterSets() throws Exception {
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    List<BatchParameterSet> parameterSets =
        List.of(
            BatchParameterSet.from(
                Map.of(
                    1,
                    getSqlParam(1, 1, DatabricksTypeUtil.INT),
                    2,
                    getSqlParam(2, "first", DatabricksTypeUtil.STRING))),
            BatchParameterSet.from(
                Map.of(
                    1,
                    getSqlParam(1, 2, DatabricksTypeUtil.INT),
                    2,
                    getSqlParam(2, "second", DatabricksTypeUtil.STRING))));

    databricksSdkClient.executeStatementBatch(
        "INSERT INTO target VALUES (?, ?)",
        warehouse,
        parameterSets,
        StatementType.UPDATE,
        connection.getSession(),
        statement);

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(apiClient, atLeastOnce()).serialize(captor.capture());
    ExecuteStatementRequest request = captor.getValue();
    assertNull(request.getParameters());
    assertNull(request.getRowLimit());
    assertEquals(2, request.getParameterSets().size());
    List<StatementParameterSet> capturedSets = new ArrayList<>(request.getParameterSets());
    List<StatementParameterListItem> firstSet =
        new ArrayList<>(capturedSets.get(0).getParameters());
    assertEquals(0, ((PositionalStatementParameterListItem) firstSet.get(0)).getOrdinal());
    assertEquals(1, ((PositionalStatementParameterListItem) firstSet.get(1)).getOrdinal());
    assertEquals("first", firstSet.get(1).getValue());
    JsonNode requestJson = JsonUtil.getMapper().valueToTree(request);
    assertTrue(requestJson.has("parameter_sets"));
    assertTrue(requestJson.get("parameters").isNull());
    assertTrue(requestJson.get("row_limit").isNull());
  }

  @Test
  public void testExecuteStatementAsyncSendsButDoesNotUpdateSessionVersion() throws Exception {
    setupClientMocks(false, true, INITIAL_SESSION_VERSION, UPDATED_SESSION_VERSION);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);

    DatabricksResultSet resultSet =
        databricksSdkClient.executeStatementAsync(
            STATEMENT, warehouse, sqlParams, connection.getSession(), statement);
    assertEquals(STATEMENT_ID, statement.getStatementId());
    assertNull(resultSet.getMetaData());
    assertEquals(
        INITIAL_SESSION_VERSION, connection.getSession().getSessionVersion().getVersionId());

    verify(apiClient)
        .serialize(
            argThat(
                request ->
                    request instanceof ExecuteStatementRequest
                        && sessionVersion(INITIAL_SESSION_VERSION)
                            .equals(((ExecuteStatementRequest) request).getSessionVersion())));
    verify(apiClient)
        .execute(
            argThat(
                req -> req.getMethod().equals(Request.POST) && req.getUrl().equals(STATEMENT_PATH)),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testCloseStatement() throws DatabricksSQLException, IOException {
    String path = String.format(STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    databricksSdkClient.closeStatement(STATEMENT_ID);

    // Verify a Request with DELETE method is created and executed
    verify(apiClient).serialize(any(CloseStatementRequest.class));
    verify(apiClient)
        .execute(
            argThat(req -> req.getMethod().equals(Request.DELETE) && req.getUrl().equals(path)),
            eq(Void.class));
  }

  @Test
  void apcCloseStatementRedactsIdInDiagnosticsButKeepsRequestTarget() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    String rawId = "raw-sensitive-apc-statement-id";
    StatementId id =
        StatementId.forCurrentSession(
            rawId,
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(new AllPurposeCluster("org", "cluster"))
                .build());

    try (LogCapture logs = new LogCapture()) {
      client.closeStatement(id);
      assertTrue(logs.getOutput().contains("closeStatement"));
      assertFalse(logs.getOutput().contains(rawId));
    }
    ArgumentCaptor<Request> requests = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(requests.capture(), eq(Void.class));
    assertEquals(Request.DELETE, requests.getValue().getMethod());
    assertEquals(
        String.format(STATEMENT_PATH_WITH_ID, rawId), requests.getValue().getUri().getPath());
    assertNull(requests.getValue().getBodyString());
  }

  @Test
  public void testCancelStatement() throws DatabricksSQLException, IOException {
    String path = String.format(CANCEL_STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    databricksSdkClient.cancelStatement(STATEMENT_ID);

    // Verify a Request with POST method is created and executed
    verify(apiClient).serialize(any(CancelStatementRequest.class));
    verify(apiClient)
        .execute(
            argThat(req -> req.getMethod().equals(Request.POST) && req.getUrl().equals(path)),
            eq(Void.class));
  }

  @Test
  public void testHandleFailedExecution_CancelledState_ThrowsWithHY008() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    StatementStatus cancelledStatus = new StatementStatus().setState(StatementState.CANCELED);
    ExecuteStatementResponse response =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(cancelledStatus);

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                databricksSdkClient.handleFailedExecution(
                    response, STATEMENT_ID.toSQLExecStatementId(), STATEMENT));

    assertEquals("HY008", exception.getSQLState());
    assertTrue(exception.getMessage().contains("was cancelled"));
    assertEquals(1008, exception.getErrorCode()); // EXECUTE_STATEMENT_CANCELLED stable code
  }

  @Test
  public void testHandleFailedExecution_FailedState_ThrowsWithoutHY008() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    StatementStatus failedStatus = new StatementStatus().setState(StatementState.FAILED);
    ExecuteStatementResponse response =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(failedStatus);

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                databricksSdkClient.handleFailedExecution(
                    response, STATEMENT_ID.toSQLExecStatementId(), STATEMENT));

    assertNotEquals("HY008", exception.getSQLState());
    assertTrue(exception.getMessage().contains("execution failed"));
  }

  @Test
  public void testHandleFailedExecutionPreservesUnboundParameterFallbackSignal() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    StatementStatus failedStatus =
        new StatementStatus()
            .setState(StatementState.FAILED)
            .setSqlState("42P02")
            .setError(
                new ServiceError()
                    .setMessage("[UNBOUND_SQL_PARAMETER] Found an unbound parameter")
                    .setErrorCode(ServiceErrorCode.BAD_REQUEST));
    ExecuteStatementResponse response =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(failedStatus);

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                databricksSdkClient.handleFailedExecution(
                    response, STATEMENT_ID.toSQLExecStatementId(), STATEMENT));

    assertEquals("42P02", exception.getSQLState());
    assertTrue(exception.getMessage().contains("[UNBOUND_SQL_PARAMETER]"));
  }

  @Test
  public void testHandleFailedExecution_unityCatalogError_remapsToCommunicationLinkFailure()
      throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    StatementStatus failedStatus =
        new StatementStatus()
            .setState(StatementState.FAILED)
            .setSqlState("XXUCC")
            .setError(
                new ServiceError()
                    .setMessage(
                        "[UC_CLIENT_EXCEPTION] Failed to contact the Unity Catalog server. "
                            + "HTTP/1.1 504 Gateway Timeout, DEADLINE_EXCEEDED"));
    ExecuteStatementResponse response =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(failedStatus);

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                databricksSdkClient.handleFailedExecution(
                    response, STATEMENT_ID.toSQLExecStatementId(), STATEMENT));

    assertEquals("08S01", exception.getSQLState(), "Expected XXUCC to be remapped to 08S01");
  }

  @Test
  public void testGetStatementResult_CancelledState_ThrowsWithHY008() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    // Server returns CANCELED with null result data
    StatementStatus cancelledStatus = new StatementStatus().setState(StatementState.CANCELED);
    GetStatementResponse cancelledResponse = new GetStatementResponse();
    cancelledResponse.setStatus(cancelledStatus);
    cancelledResponse.setStatementId(STATEMENT_ID.toSQLExecStatementId());

    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(cancelledResponse);

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                databricksSdkClient.getStatementResult(
                    STATEMENT_ID, mock(DatabricksSession.class), null));

    assertEquals("HY008", exception.getSQLState());
    assertTrue(exception.getMessage().contains("was cancelled"));
    assertEquals(1008, exception.getErrorCode()); // EXECUTE_STATEMENT_CANCELLED stable code
  }

  @Test
  public void testGetStatementResultDoesNotUpdateSessionVersion() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(createSessionResponse());
    connection.open();

    GetStatementResponse response =
        new GetStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(
                new StatementStatus()
                    .setState(StatementState.SUCCEEDED)
                    .setSessionVersion(sessionVersion(UPDATED_SESSION_VERSION)));
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(response);

    databricksSdkClient.getStatementResult(STATEMENT_ID, connection.getSession(), null);

    assertEquals(
        INITIAL_SESSION_VERSION, connection.getSession().getSessionVersion().getVersionId());
  }

  @ParameterizedTest(name = "arrow={0}, cloudFetch={1}, boundedSea={2}, hybrid={3}")
  @CsvSource({
    "false, false, false, false, JSON_ARRAY, INLINE",
    "false, false, true,  true,  JSON_ARRAY, INLINE",
    "false, true,  false, true,  JSON_ARRAY, INLINE",
    "false, true,  true,  false, JSON_ARRAY, INLINE",
    "true,  true,  false, false, ARROW_STREAM, EXTERNAL_LINKS",
    "true,  true,  true,  true,  ARROW_STREAM, INLINE_OR_EXTERNAL_LINKS",
    "true,  false, true,  false, ARROW_STREAM, INLINE_OR_EXTERNAL_LINKS",
    "true,  false, false, true,  JSON_ARRAY, INLINE"
  })
  public void testSeaRequestConstructionGrid(
      boolean arrowEnabled,
      boolean cloudFetchEnabled,
      boolean boundedSeaEnabled,
      boolean hybridResultsEnabled,
      Format expectedFormat,
      Disposition expectedDisposition)
      throws Exception {
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        spy(
            DatabricksConnectionContext.parse(
                JDBC_URL
                    + "EnableQueryResultDownload="
                    + (cloudFetchEnabled ? "1" : "0")
                    + ";UseBoundedSeaApi="
                    + (boundedSeaEnabled ? "1" : "0")
                    + ";EnableSQLExecHybridResults="
                    + (hybridResultsEnabled ? "1" : "0"),
                new Properties()));
    doReturn(arrowEnabled).when(connectionContext).shouldEnableArrow();
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();

    databricksSdkClient.executeStatement(
        STATEMENT,
        warehouse,
        sqlParams,
        StatementType.QUERY,
        connection.getSession(),
        new DatabricksStatement(connection),
        null);

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(apiClient, atLeastOnce()).serialize(captor.capture());
    ExecuteStatementRequest request = captor.getValue();
    assertEquals(expectedDisposition, request.getDisposition());
    assertEquals(SqlExecResultFormat.fromSdkFormat(expectedFormat), request.getFormat());
    if (expectedFormat == Format.JSON_ARRAY) {
      assertEquals(CompressionCodec.NONE, request.getResultCompression());
    }
  }

  @Test
  public void testGetDatabricksConfig() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    assertNotNull(databricksSdkClient.getDatabricksConfig());
  }

  @Test
  public void testExecuteStatementWithTimeout() throws Exception {
    // Set up connection context and client
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = createSessionResponse();
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    // Create statement with a 10-second timeout (long enough)
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    statement.setQueryTimeout(10);

    // Create statement execution mocks
    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse runningStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse successStatementResponse =
        new GetStatementResponse()
            .setStatus(
                new StatementStatus()
                    .setState(StatementState.SUCCEEDED)
                    .setSessionVersion(sessionVersion(UPDATED_SESSION_VERSION)));

    // Set up response sequence for execute() calls
    when(apiClient.execute(
            argThat(req -> req != null && STATEMENT_PATH.equals(req.getUrl())),
            eq(ExecuteStatementResponse.class)))
        .thenReturn(executeResponse);
    when(apiClient.execute(
            argThat(
                req ->
                    req != null
                        && req.getUrl() != null
                        && req.getUrl().contains(STATEMENT_ID.toSQLExecStatementId())),
            eq(GetStatementResponse.class)))
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(successStatementResponse);
    assertDoesNotThrow(
        () ->
            databricksSdkClient.executeStatement(
                STATEMENT,
                warehouse,
                sqlParams,
                StatementType.QUERY,
                connection.getSession(),
                statement,
                null));
    assertEquals(
        UPDATED_SESSION_VERSION, connection.getSession().getSessionVersion().getVersionId());

    // Verify no cancellation occurred due to timeout
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> req.getMethod().equals(Request.POST) && req.getUrl().equals(STATEMENT_PATH)),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testExecuteStatementWithTimeoutExpired() throws Exception {
    // Set up connection context and client. Async exec poll interval is set to 1 second to
    // facilitate timeout
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(
            JDBC_URL,
            new Properties() {
              {
                setProperty("asyncExecPollInterval", "1000");
              }
            });
    DatabricksSdkClient databricksSdkClient =
        spy(new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    // Create statement with a very short timeout (1 second)
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    statement.setQueryTimeout(1);

    // Create statement execution mocks
    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse runningStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse successStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));

    // Set up response sequence for execute() calls
    when(apiClient.execute(
            argThat(req -> req != null && STATEMENT_PATH.equals(req.getUrl())),
            eq(ExecuteStatementResponse.class)))
        .thenReturn(executeResponse);
    when(apiClient.execute(
            argThat(
                req ->
                    req != null
                        && req.getUrl() != null
                        && req.getUrl().contains(STATEMENT_ID.toSQLExecStatementId())),
            eq(GetStatementResponse.class)))
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(successStatementResponse);

    // Verify that the timeout exception (1 second) is thrown due to repeated polling, where each
    // poll occurs at an interval of 1 second
    DatabricksTimeoutException exception =
        assertThrows(
            DatabricksTimeoutException.class,
            () ->
                databricksSdkClient.executeStatement(
                    STATEMENT,
                    warehouse,
                    sqlParams,
                    StatementType.QUERY,
                    connection.getSession(),
                    statement,
                    null));

    assertTrue(exception.getMessage().contains("timed-out after 1 seconds"));

    // Verify cancel was called
    verify(databricksSdkClient).cancelStatement(eq(STATEMENT_ID));
  }

  @Test
  public void testMetadataOperationUsesMetadataTimeout() throws Exception {
    // MetadataOperationTimeout=1 with parentStatement=null (metadata path)
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(
            JDBC_URL,
            new Properties() {
              {
                setProperty("MetadataOperationTimeout", "1");
                setProperty("asyncExecPollInterval", "1000");
              }
            });
    DatabricksSdkClient databricksSdkClient =
        spy(new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse runningResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));

    when(apiClient.execute(
            argThat(req -> req != null && STATEMENT_PATH.equals(req.getUrl())),
            eq(ExecuteStatementResponse.class)))
        .thenReturn(executeResponse);
    when(apiClient.execute(
            argThat(
                req ->
                    req != null
                        && req.getUrl() != null
                        && req.getUrl().contains(STATEMENT_ID.toSQLExecStatementId())),
            eq(GetStatementResponse.class)))
        .thenReturn(runningResponse);

    // Metadata with parentStatement=null should use MetadataOperationTimeout (1s)
    DatabricksTimeoutException exception =
        assertThrows(
            DatabricksTimeoutException.class,
            () ->
                databricksSdkClient.executeStatement(
                    "SHOW SCHEMAS IN ALL CATALOGS",
                    warehouse,
                    new java.util.HashMap<>(),
                    StatementType.METADATA,
                    connection.getSession(),
                    null, // parentStatement=null (metadata path)
                    null));

    assertTrue(exception.getMessage().contains("timed-out after 1 seconds"));
    verify(databricksSdkClient).cancelStatement(eq(STATEMENT_ID));
  }

  @Test
  public void testNonMetadataWithNullParentHasNoTimeout() throws Exception {
    // Non-metadata with parentStatement=null should have timeout=0 (infinite)
    // Use a short poll interval so the test completes quickly
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(
            JDBC_URL,
            new Properties() {
              {
                setProperty("MetadataOperationTimeout", "1");
              }
            });
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    // Return SUCCEEDED immediately so the test completes
    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));

    when(apiClient.execute(
            argThat(req -> req != null && STATEMENT_PATH.equals(req.getUrl())),
            eq(ExecuteStatementResponse.class)))
        .thenReturn(executeResponse);

    // Non-METADATA with parentStatement=null: no timeout applied, should succeed
    assertDoesNotThrow(
        () ->
            databricksSdkClient.executeStatement(
                "SELECT 1",
                warehouse,
                new java.util.HashMap<>(),
                StatementType.SQL,
                connection.getSession(),
                null,
                null));
  }

  @Test
  public void testServerSideTimeoutThrowsTimeoutException() throws Exception {

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        spy(new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    // Create statement with a long client timeout (server times out first)
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    statement.setQueryTimeout(300); // 300 seconds - server will timeout before this

    // Mock server-side timeout: server returns FAILED state with sqlState="57KD0"
    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(
                new StatementStatus()
                    .setState(StatementState.FAILED)
                    .setSqlState(QUERY_EXECUTION_TIMEOUT_SQLSTATE) // Server-side timeout SQL state
                    .setError(
                        new ServiceError()
                            .setMessage("Statement has timed out after 10 seconds.")
                            .setErrorCode(ServiceErrorCode.BAD_REQUEST)));

    // Set up mock response
    when(apiClient.execute(
            argThat(req -> req != null && STATEMENT_PATH.equals(req.getUrl())),
            eq(ExecuteStatementResponse.class)))
        .thenReturn(executeResponse);

    // Existing SQL timeout behavior does not include a vendor code.
    DatabricksTimeoutException failure =
        assertThrows(
            DatabricksTimeoutException.class,
            () ->
                databricksSdkClient.executeStatement(
                    STATEMENT,
                    warehouse,
                    sqlParams,
                    StatementType.QUERY,
                    connection.getSession(),
                    statement,
                    null));
    assertEquals(0, failure.getErrorCode());
  }

  @Test
  public void testDecimalTypeWithValidPrecisionAndScale() throws DatabricksSQLException {
    BigDecimal decimalValue = new BigDecimal("123.45"); // precision: 5, scale: 2
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(0).type(DECIMAL).value(decimalValue).build();

    StatementParameterListItem result = databricksSdkClient.mapToParameterListItem(parameter);

    assertEquals("DECIMAL(5,2)", result.getType());
    assertEquals("123.45", result.getValue());
  }

  @Test
  public void testDecimalTypeWithScaleGreaterThanPrecision() throws DatabricksSQLException {
    BigDecimal decimalValue = new BigDecimal("0.000123"); // scale: 6, precision: 3
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(1).type(DECIMAL).value(decimalValue).build();

    StatementParameterListItem result = databricksSdkClient.mapToParameterListItem(parameter);

    assertEquals("DECIMAL(6,6)", result.getType());
    assertEquals("0.000123", result.getValue());
  }

  @Test
  public void testNonDecimalType() throws DatabricksSQLException {
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(2).type(STRING).value(TEST_STRING).build();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    StatementParameterListItem result = databricksSdkClient.mapToParameterListItem(parameter);

    assertEquals("STRING", result.getType());
    assertEquals(TEST_STRING, result.getValue());
  }

  @Test
  public void testNullValue() throws DatabricksSQLException {
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(3).type(INT).value(null).build();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    StatementParameterListItem result = databricksSdkClient.mapToParameterListItem(parameter);

    assertEquals("INT", result.getType());
    assertNull(result.getValue());
  }

  @Test
  public void testCreateSessionWithSSLCertificatePathError() throws Exception {

    File wrongTrustStore = File.createTempFile("wrong-trust-store", ".jks");
    wrongTrustStore.deleteOnExit();
    ConfiguratorUtilsTest.createDummyStore(
        wrongTrustStore.getAbsolutePath(), "JKS", DEFAULT_KEYSTORE_PASSWORD, "wrong-ca", false);

    SSLHandshakeException sslException =
        new SSLHandshakeException(
            "PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target");

    DatabricksError sslError = mock(DatabricksError.class);
    when(sslError.getMessage()).thenReturn(sslException.getMessage());
    when(sslError.getCause()).thenReturn(sslException);

    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(sslError);

    Properties props = new Properties();
    props.setProperty("SSLTrustStore", wrongTrustStore.getAbsolutePath());
    props.setProperty("SSLTrustStorePwd", DEFAULT_KEYSTORE_PASSWORD);
    props.setProperty("SSLTrustStoreType", "JKS");

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, props);
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    // Assert that createSession throws a DatabricksSQLException with actionable error message
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> databricksSdkClient.createSession(warehouse, null, null, null));

    String errorMessage = exception.getMessage();

    // Verify that we get the exact SSL error message
    String expectedErrorMessage =
        String.format(
            "Unable to find certification path to requested target in truststore: %s\n\n"
                + "SSL Error: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target\n\n"
                + "Details: TLS handshake failure due to TLS Certificate of server being connected is not in the configured truststore.\n\n"
                + "Next steps:\n"
                + "- Make sure that the connection string has the appropriate Databricks workspace FQDN.\n\n"
                + "- Verify the configured truststore path and make sure the required certificates are imported.\n"
                + "  .   PEM certificate chain of the warehouse endpoint can be fetched using \"openssl s_client -connect sample-host.18.azuredatabricks.net:443 -showcerts\"\n"
                + "  .   Reference KB article with troubleshooting steps.\n",
            wrongTrustStore.getAbsolutePath());
    assertEquals(expectedErrorMessage, errorMessage);

    // Clean up
    wrongTrustStore.delete();
  }

  @Test
  public void testCreateSessionWithNonSSLError() throws IOException, DatabricksSQLException {

    DatabricksError nonSSLError = new DatabricksError("500", "Some other error", 500);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(nonSSLError);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> databricksSdkClient.createSession(warehouse, null, null, null));

    assertEquals(
        "Error while establishing a connection in databricks: Some other error (HTTP 500)",
        exception.getMessage());
    assertEquals(DatabricksDriverErrorCode.CONNECTION_ERROR.name(), exception.getSQLState());
    assertSame(nonSSLError, exception.getCause());
  }

  private static ImmutableSqlParameter getSqlParam(
      int parameterIndex, Object x, String databricksType) {
    return ImmutableSqlParameter.builder()
        .type(DatabricksTypeUtil.getColumnInfoType(databricksType))
        .value(x)
        .cardinal(parameterIndex)
        .build();
  }

  private StatementParameterListItem getParam(String type, String value, int ordinal) {
    return new PositionalStatementParameterListItem()
        .setOrdinal(ordinal)
        .setType(type)
        .setValue(value);
  }

  @Test
  public void testSeaSyncMetadataHeaderIsAdded() throws Exception {
    // Test that x-databricks-sea-can-run-fully-sync header is added for SEA + METADATA + sync
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a metadata request (synchronous)
    databricksSdkClient.executeStatement(
        "SHOW CATALOGS",
        warehouse,
        new HashMap<>(),
        StatementType.METADATA,
        connection.getSession(),
        statement,
        null);

    // Verify that the request was made with the correct header
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers != null
                      && "true".equals(headers.get("x-databricks-sea-can-run-fully-sync"));
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testSeaSyncMetadataHeaderNotAddedForAsyncExecution() throws Exception {
    // Test that header is NOT added for async execution
    setupClientMocks(false, true);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute an async statement
    databricksSdkClient.executeStatementAsync(
        "SELECT * FROM table", warehouse, new HashMap<>(), connection.getSession(), statement);

    // Verify that the request was made WITHOUT the header
    verify(apiClient)
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers == null
                      || !headers.containsKey("x-databricks-sea-can-run-fully-sync");
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testSeaSyncMetadataHeaderNotAddedForQueryType() throws Exception {
    // Test that header is NOT added for non-METADATA statement types
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a regular query (not metadata)
    databricksSdkClient.executeStatement(
        "SELECT * FROM table",
        warehouse,
        new HashMap<>(),
        StatementType.QUERY,
        connection.getSession(),
        statement,
        null);

    // Verify that the request was made WITHOUT the header
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers == null
                      || !headers.containsKey("x-databricks-sea-can-run-fully-sync");
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testSeaSyncMetadataHeaderNotAddedWhenDisabled() throws Exception {
    // Test that header is NOT added when the URL parameter is disabled
    setupClientMocks(true, false);
    String urlWithDisabledFlag =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;EnableSeaSyncMetadata=0;";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(urlWithDisabledFlag, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a metadata request (should NOT add header because flag is disabled)
    databricksSdkClient.executeStatement(
        "SHOW CATALOGS",
        warehouse,
        new HashMap<>(),
        StatementType.METADATA,
        connection.getSession(),
        statement,
        null);

    // Verify that the request was made WITHOUT the header
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers == null
                      || !headers.containsKey("x-databricks-sea-can-run-fully-sync");
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testMetadataOperationTypeHeaderIsAdded() throws Exception {
    // Test that X-Databricks-Metadata-Operation-Type header is added when metadataOperationType is
    // provided
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a metadata request with MetadataOperationType
    databricksSdkClient.executeStatement(
        "SHOW CATALOGS",
        warehouse,
        new HashMap<>(),
        StatementType.METADATA,
        connection.getSession(),
        statement,
        MetadataOperationType.GET_CATALOGS);

    // Verify that the request was made with the correct metadata operation type header
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers != null
                      && "GetCatalogs".equals(headers.get("X-Databricks-Metadata-Operation-Type"));
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testMetadataOperationTypeHeaderNotAddedWhenNull() throws Exception {
    // Test that X-Databricks-Metadata-Operation-Type header is NOT added when metadataOperationType
    // is null
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a metadata request without MetadataOperationType
    databricksSdkClient.executeStatement(
        "SHOW CATALOGS",
        warehouse,
        new HashMap<>(),
        StatementType.METADATA,
        connection.getSession(),
        statement,
        null);

    // Verify that the request was made WITHOUT the metadata operation type header
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers == null
                      || !headers.containsKey("X-Databricks-Metadata-Operation-Type");
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testMetadataOperationTypeHeaderWithGetTables() throws Exception {
    // Test that header value matches the enum's getHeaderValue() for GET_TABLES
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a metadata request with GET_TABLES
    databricksSdkClient.executeStatement(
        "SHOW TABLES",
        warehouse,
        new HashMap<>(),
        StatementType.METADATA,
        connection.getSession(),
        statement,
        MetadataOperationType.GET_TABLES);

    // Verify that the request was made with the correct header value from the enum
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers != null
                      && MetadataOperationType.GET_TABLES
                          .getHeaderValue()
                          .equals(headers.get("X-Databricks-Metadata-Operation-Type"));
                }),
            eq(ExecuteStatementResponse.class));
  }

  @ParameterizedTest
  @EnumSource(
      value = MetadataOperationType.class,
      names = {
        "GET_CATALOGS",
        "GET_SCHEMAS",
        "GET_TABLES",
        "GET_COLUMNS",
        "GET_FUNCTIONS",
        "GET_PRIMARY_KEYS",
        "GET_CROSS_REFERENCE"
      })
  public void testSupportedOperationsRequestThriftNativeMetadata(
      MetadataOperationType operationType) throws Exception {
    DatabricksResultSet resultSet =
        executeMetadataOperation(operationType, true, Boolean.TRUE, new ArrayList<>());

    assertTrue(resultSet.isThriftNativeMetadataResult());
    verifyNativeMetadataHeader(operationType, true);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(booleans = false)
  public void testNativeMetadataSchemaDoesNotOverrideManifestFlag(Boolean manifestFlag)
      throws Exception {
    DatabricksResultSet resultSet =
        executeMetadataOperation(
            MetadataOperationType.GET_COLUMNS,
            true,
            manifestFlag,
            List.of(
                new ColumnInfo()
                    .setName("ORDINAL_POSITION")
                    .setTypeName(STRING)
                    .setTypeText("STRING")));

    assertFalse(resultSet.isThriftNativeMetadataResult());
  }

  @Test
  public void testNativeMetadataManifestFlagDoesNotDependOnRequestHeader() throws Exception {
    DatabricksResultSet resultSet =
        executeMetadataOperation(
            MetadataOperationType.GET_COLUMNS, false, Boolean.TRUE, new ArrayList<>());

    assertTrue(resultSet.isThriftNativeMetadataResult());
    verifyNativeMetadataHeader(MetadataOperationType.GET_COLUMNS, false);
  }

  @ParameterizedTest
  @EnumSource(
      value = MetadataOperationType.class,
      names = {"GET_PROCEDURES", "GET_PROCEDURE_COLUMNS"})
  public void testUnsupportedOperationDoesNotRequestThriftNativeMetadata(
      MetadataOperationType operationType) throws Exception {
    DatabricksResultSet resultSet =
        executeMetadataOperation(operationType, true, null, new ArrayList<>());

    assertFalse(resultSet.isThriftNativeMetadataResult());
    verifyNativeMetadataHeader(operationType, false);
  }

  private DatabricksResultSet executeMetadataOperation(
      MetadataOperationType operationType,
      boolean enableThriftNativeMetadata,
      Boolean manifestFlag,
      List<ColumnInfo> manifestColumns)
      throws Exception {
    setupClientMocks(true, false, manifestColumns, manifestFlag);
    String jdbcUrl =
        enableThriftNativeMetadata ? JDBC_URL + "EnableThriftNativeMetadata=1;" : JDBC_URL;
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(jdbcUrl, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();

    return databricksSdkClient.executeStatement(
        "metadata query",
        warehouse,
        new HashMap<>(),
        StatementType.METADATA,
        connection.getSession(),
        new DatabricksStatement(connection),
        operationType);
  }

  private void verifyNativeMetadataHeader(MetadataOperationType operationType, boolean expected)
      throws IOException {
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers != null
                      && operationType
                          .getHeaderValue()
                          .equals(headers.get("X-Databricks-Metadata-Operation-Type"))
                      && Objects.equals(
                          expected ? "true" : null,
                          headers.get("X-Databricks-Require-Thrift-Native-Metadata"));
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testExecuteStatementWithClosedStatus() throws Exception {
    // Set up connection and statement
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    DatabricksStatement statement = spy(new DatabricksStatement(connection));
    statement.setMaxRows(100);

    // Create a response with CLOSED status
    StatementStatus closedStatus = new StatementStatus().setState(StatementState.CLOSED);
    ExecuteStatementResponse closedResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(closedStatus)
            .setResult(resultData)
            .setManifest(
                new ResultManifest()
                    .setFormat(SqlExecResultFormat.JSON_ARRAY)
                    .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L))
                    .setTotalRowCount(0L));

    when(apiClient.execute(any(Request.class), any()))
        .thenAnswer(
            invocationOnMock -> {
              Request req = invocationOnMock.getArgument(0, Request.class);
              if (req.getUrl().equals(STATEMENT_PATH)) {
                return closedResponse;
              } else if (req.getUrl().equals(String.format(WAREHOUSE_SESSION_PATH, WAREHOUSE_ID))) {
                return sessionResponse;
              }
              return null;
            });

    // Execute statement
    databricksSdkClient.executeStatement(
        STATEMENT,
        warehouse,
        new HashMap<>(),
        StatementType.QUERY,
        connection.getSession(),
        statement,
        null);

    // Verify that markDirectResultsReceived was called on the statement
    verify(statement, times(1)).markDirectResultsReceived();
  }

  @Test
  public void testExecuteStatementWithClosedStatusAndNoParentStatement() throws Exception {
    // Set up connection without parent statement
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    // Create a response with CLOSED status
    StatementStatus closedStatus = new StatementStatus().setState(StatementState.CLOSED);
    ExecuteStatementResponse closedResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(closedStatus)
            .setResult(resultData)
            .setManifest(
                new ResultManifest()
                    .setFormat(SqlExecResultFormat.JSON_ARRAY)
                    .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L))
                    .setTotalRowCount(0L));

    when(apiClient.execute(any(Request.class), any()))
        .thenAnswer(
            invocationOnMock -> {
              Request req = invocationOnMock.getArgument(0, Request.class);
              if (req.getUrl().equals(STATEMENT_PATH)) {
                return closedResponse;
              } else if (req.getUrl().equals(String.format(WAREHOUSE_SESSION_PATH, WAREHOUSE_ID))) {
                return sessionResponse;
              }
              return null;
            });

    // Execute statement with null parent statement - should not throw
    assertDoesNotThrow(
        () ->
            databricksSdkClient.executeStatement(
                STATEMENT,
                warehouse,
                new HashMap<>(),
                StatementType.QUERY,
                connection.getSession(),
                null,
                null));
  }

  @Test
  public void testGetResultChunks_DatabricksError_throwsSQLException() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    // Simulate a 404 from the server (result expired)
    when(apiClient.execute(any(Request.class), eq(ResultData.class)))
        .thenThrow(new DatabricksError("404", "Results have expired", 404));

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> databricksSdkClient.getResultChunks(STATEMENT_ID, 0, 0));

    assertTrue(exception.getMessage().contains("Results have expired"));
    assertNotNull(exception.getCause());
  }

  @Test
  public void testGetResultChunksData_DatabricksError_throwsSQLException() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    // Simulate a 404 from the server (result expired)
    when(apiClient.execute(any(Request.class), eq(ResultData.class)))
        .thenThrow(new DatabricksError("404", "Results have expired", 404));

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> databricksSdkClient.getResultChunksData(STATEMENT_ID, 0));

    assertTrue(exception.getMessage().contains("Results have expired"));
    assertNotNull(exception.getCause());
  }

  // =========================================================================
  // checkStatementAlive
  // =========================================================================

  @Test
  public void testCheckStatementAlive_succeededState_returnsTrue() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    StatementStatus status = new StatementStatus().setState(StatementState.SUCCEEDED);

    when(apiClient.execute(any(Request.class), eq(StatementStatus.class))).thenReturn(status);

    assertTrue(databricksSdkClient.checkStatementAlive(STATEMENT_ID));
  }

  @Test
  public void testCheckStatementAlive_runningState_returnsTrue() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    StatementStatus status = new StatementStatus().setState(StatementState.RUNNING);

    when(apiClient.execute(any(Request.class), eq(StatementStatus.class))).thenReturn(status);

    assertTrue(databricksSdkClient.checkStatementAlive(STATEMENT_ID));
  }

  @Test
  public void testCheckStatementAlive_canceledState_returnsFalse() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    StatementStatus status = new StatementStatus().setState(StatementState.CANCELED);

    when(apiClient.execute(any(Request.class), eq(StatementStatus.class))).thenReturn(status);

    assertFalse(databricksSdkClient.checkStatementAlive(STATEMENT_ID));
  }

  @Test
  public void testCheckStatementAlive_closedState_returnsFalse() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    StatementStatus status = new StatementStatus().setState(StatementState.CLOSED);

    when(apiClient.execute(any(Request.class), eq(StatementStatus.class))).thenReturn(status);

    assertFalse(databricksSdkClient.checkStatementAlive(STATEMENT_ID));
  }

  @Test
  public void testCheckStatementAlive_failedState_returnsFalse() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    StatementStatus status = new StatementStatus().setState(StatementState.FAILED);

    when(apiClient.execute(any(Request.class), eq(StatementStatus.class))).thenReturn(status);

    assertFalse(databricksSdkClient.checkStatementAlive(STATEMENT_ID));
  }

  @Test
  public void testCheckStatementAlive_exceptionWrapped() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    when(apiClient.execute(any(Request.class), eq(StatementStatus.class)))
        .thenThrow(new RuntimeException("Network error"));

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> databricksSdkClient.checkStatementAlive(STATEMENT_ID));
    assertTrue(exception.getMessage().contains("Heartbeat status check failed"));
  }

  @Test
  public void testWaitTimeout_directResultsDisabled_usesAsyncZero() throws Exception {
    setupClientMocks(true, false);
    // EnableDirectResults=0 -> getDirectResultMode() is false
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL + "EnableDirectResults=0", new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    databricksSdkClient.executeStatement(
        STATEMENT,
        warehouse,
        sqlParams,
        StatementType.QUERY,
        connection.getSession(),
        statement,
        null);

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(apiClient, atLeastOnce()).serialize(captor.capture());
    // Direct results disabled -> async (0s), not the hybrid 10s path that truncates (ES-1714092).
    assertEquals("0s", captor.getValue().getWaitTimeout());
  }

  @Test
  public void testWaitTimeout_directResultsEnabled_leftUnset() throws Exception {
    setupClientMocks(true, false);
    // Default JDBC_URL has direct results enabled -> getDirectResultMode() is true
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    databricksSdkClient.executeStatement(
        STATEMENT,
        warehouse,
        sqlParams,
        StatementType.QUERY,
        connection.getSession(),
        statement,
        null);

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(apiClient, atLeastOnce()).serialize(captor.capture());
    // Direct results enabled -> WaitTimeout left unset (true SEA direct results).
    assertNull(captor.getValue().getWaitTimeout());
  }

  // =========================================================================
  // getResultChunks — row_offset bounded-SEA contract
  // =========================================================================

  @Test
  public void testGetResultChunks_boundedSeaEnabled_appendsRowOffset() throws Exception {
    Properties props = new Properties();
    props.setProperty("UseBoundedSeaApi", "1");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, props);
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    when(apiClient.execute(any(Request.class), eq(ResultData.class))).thenReturn(new ResultData());

    databricksSdkClient.getResultChunks(STATEMENT_ID, 2L, 450L);

    ArgumentCaptor<Request> reqCaptor = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(reqCaptor.capture(), eq(ResultData.class));
    String path = reqCaptor.getValue().getUrl();
    assertTrue(
        path.contains("?row_offset=450"),
        "Bounded SEA must append ?row_offset=<offset> to the chunk path, got: " + path);
  }

  @Test
  public void testGetResultChunks_boundedSeaDisabled_noRowOffset() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    when(apiClient.execute(any(Request.class), eq(ResultData.class))).thenReturn(new ResultData());

    databricksSdkClient.getResultChunks(STATEMENT_ID, 2L, 450L);

    ArgumentCaptor<Request> reqCaptor = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(reqCaptor.capture(), eq(ResultData.class));
    String path = reqCaptor.getValue().getUrl();
    assertFalse(
        path.contains("row_offset"), "Non-bounded path must NOT append row_offset, got: " + path);
  }

  @Test
  public void testApcCreateAndTerminateCarryJsonTargetAndLiveVersion() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(
            new CreateSessionResponse().setSessionId("session").setNegotiatedProtocolVersion(8L));

    ImmutableSessionInfo info = client.createSession(cluster, null, null, null);
    assertEquals(DatabricksClientType.SEA, info.clientType());
    assertEquals(8L, info.nativeProtocolVersion());
    assertFalse(info.realtimeSession());
    client.deleteSession(info);

    ArgumentCaptor<Request> create = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(create.capture(), eq(CreateSessionResponse.class));
    assertEquals(SESSION_PATH, create.getValue().getUrl());
    JsonNode createBody = new ObjectMapper().readTree(create.getValue().getBodyString());
    assertEquals(
        "cluster",
        createBody
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
    assertFalse(createBody.has("warehouse_id"));

    ArgumentCaptor<Request> terminate = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(terminate.capture(), eq(Void.class));
    assertEquals(String.format(SESSION_PATH_WITH_ID, "session"), terminate.getValue().getUrl());
    JsonNode deleteBody = new ObjectMapper().readTree(terminate.getValue().getBodyString());
    assertEquals(
        "cluster",
        deleteBody
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
    assertFalse(deleteBody.has("warehouse_id"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "1"})
  public void testApcInlineOnlySessionConfigIgnoresBoundedSeaFlag(String bounded) throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "EnableQueryResultDownload=0;UseBoundedSeaApi=" + bounded + ";",
            new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(new CreateSessionResponse().setSessionId("session"));

    client.createSession(new AllPurposeCluster("org", "cluster"), null, null, null);

    ArgumentCaptor<Request> request = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(request.capture(), eq(CreateSessionResponse.class));
    assertEquals(
        "false",
        new ObjectMapper()
            .readTree(request.getValue().getBodyString())
            .path("session_confs")
            .path("can_cloud_download")
            .asText());
  }

  @Test
  public void testApcGetChunkCancelAndCloseRequestTargets() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(new AllPurposeCluster("org", "cluster /+"))
            .build();
    StatementId id = StatementId.forCurrentSession("statement", info);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ResultData.class))).thenReturn(new ResultData());
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatus(new StatementStatus().setState(StatementState.CANCELED)));

    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("session");
    assertThrows(DatabricksSQLException.class, () -> client.getStatementResult(id, session, null));
    client.getResultChunks(id, 2, 7);
    client.getResultChunksData(id, 3, 0);
    client.cancelStatement(id);
    client.closeStatement(id);

    ArgumentCaptor<Request> get = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(get.capture(), eq(GetStatementResponse.class));
    assertNull(get.getValue().getBodyString());
    assertEquals(
        String.format(STATEMENT_PATH_WITH_ID, "statement"), get.getValue().getUri().getPath());
    assertEquals(
        "backend_compute.all_purpose_cluster_compute.cluster_id=cluster+%2F%2B",
        get.getValue().getUri().getRawQuery());
    ArgumentCaptor<Request> chunks = ArgumentCaptor.forClass(Request.class);
    verify(apiClient, times(2)).execute(chunks.capture(), eq(ResultData.class));
    assertEquals(
        "row_offset=7&backend_compute.all_purpose_cluster_compute.cluster_id=cluster+%2F%2B",
        chunks.getAllValues().get(0).getUri().getRawQuery());
    assertEquals(
        "row_offset=0&backend_compute.all_purpose_cluster_compute.cluster_id=cluster+%2F%2B",
        chunks.getAllValues().get(1).getUri().getRawQuery());
    for (Request request : chunks.getAllValues()) {
      assertNull(request.getBodyString());
    }
    ArgumentCaptor<Request> changes = ArgumentCaptor.forClass(Request.class);
    verify(apiClient, times(2)).execute(changes.capture(), eq(Void.class));
    Request cancel = changes.getAllValues().get(0);
    Request close = changes.getAllValues().get(1);
    assertEquals(Request.POST, cancel.getMethod());
    assertEquals(String.format(CANCEL_STATEMENT_PATH_WITH_ID, "statement"), cancel.getUrl());
    assertEquals(
        "cluster /+",
        new ObjectMapper()
            .readTree(cancel.getBodyString())
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
    assertEquals(Request.DELETE, close.getMethod());
    assertEquals(String.format(STATEMENT_PATH_WITH_ID, "statement"), close.getUri().getPath());
    assertNull(close.getBodyString());
    assertEquals(
        "backend_compute.all_purpose_cluster_compute.cluster_id=cluster+%2F%2B",
        close.getUri().getRawQuery());
  }

  @Test
  void apcChunkFetchLogsRedactStatementIdWhileRequestsKeepIt() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    String rawId = "private-apc-metadata-chunk-id";
    StatementId id = StatementId.deserialize("s|a|org|cluster|" + rawId);
    when(apiClient.execute(any(Request.class), eq(ResultData.class))).thenReturn(new ResultData());

    try (LogCapture logs = new LogCapture()) {
      client.getResultChunks(id, 2, 7);
      client.getResultChunksData(id, 3, 0);
      assertTrue(logs.getOutput().contains("getResultChunks(statementId=[REDACTED]"));
      assertTrue(logs.getOutput().contains("getResultChunksData(statementId=[REDACTED]"));
      assertFalse(logs.getOutput().contains(rawId));
    }

    ArgumentCaptor<Request> requests = ArgumentCaptor.forClass(Request.class);
    verify(apiClient, times(2)).execute(requests.capture(), eq(ResultData.class));
    for (Request request : requests.getAllValues()) {
      assertTrue(request.getUri().getPath().contains(rawId));
    }
  }

  @ParameterizedTest
  @CsvSource({"'', false", "'', true", "'EnableDirectResults=0;', false"})
  public void testApcExecutePollUsesJsonTargetAndBodylessGet(String directMode, boolean realtime)
      throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "EnableSQLExecHybridResults=0;" + directMode, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .realtimeSession(realtime)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.RUNNING)));
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    client.executeStatement(
        "select 1", cluster, Map.of(), StatementType.QUERY, session, null, null);

    ArgumentCaptor<Request> execute = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(execute.capture(), eq(ExecuteStatementResponse.class));
    assertEquals(STATEMENT_PATH, execute.getValue().getUrl());
    JsonNode executeBody = new ObjectMapper().readTree(execute.getValue().getBodyString());
    assertEquals("session", executeBody.path("session_id").asText());
    assertEquals("ARROW_BATCH", executeBody.path("format").asText());
    assertEquals("INLINE_OR_EXTERNAL_LINKS", executeBody.path("disposition").asText());
    assertFalse(executeBody.has("statements"));
    assertFalse(executeBody.has("parameters"));
    if (directMode.isEmpty()) {
      assertFalse(executeBody.hasNonNull("wait_timeout"));
    } else {
      assertEquals("0s", executeBody.path("wait_timeout").asText());
    }
    assertEquals(
        "cluster",
        executeBody
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
    assertFalse(executeBody.has("warehouse_id"));

    ArgumentCaptor<Request> poll = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(poll.capture(), eq(GetStatementResponse.class));
    assertEquals(Request.GET, poll.getValue().getMethod());
    assertNull(poll.getValue().getBodyString());
    assertEquals(
        "backend_compute.all_purpose_cluster_compute.cluster_id=cluster",
        poll.getValue().getUri().getRawQuery());
    assertFalse(poll.getValue().getUrl().contains("/status"));
  }

  @Test
  public void testApcNativeBatchSerializesOrderedParameterSetsWithoutTopLevelParameters()
      throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42250L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getComputeResource()).thenReturn(cluster);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));
    List<BatchParameterSet> sets =
        List.of(
            BatchParameterSet.from(Map.of(1, getSqlParam(1, "first", DatabricksTypeUtil.STRING))),
            BatchParameterSet.from(Map.of(1, getSqlParam(1, "second", DatabricksTypeUtil.STRING))));

    assertArrayEquals(
        new long[] {java.sql.Statement.SUCCESS_NO_INFO, java.sql.Statement.SUCCESS_NO_INFO},
        client.executeNativeBatch("INSERT INTO t VALUES (?)", sets, session, null));
    ArgumentCaptor<Request> execute = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(execute.capture(), eq(ExecuteStatementResponse.class));
    JsonNode body = new ObjectMapper().readTree(execute.getValue().getBodyString());
    assertFalse(body.has("parameters"));
    assertFalse(body.has("statements"));
    assertEquals(2, body.path("parameter_sets").size());
    assertEquals(
        "first",
        body.path("parameter_sets").get(0).path("parameters").get(0).path("value").asText());
    assertEquals(
        "second",
        body.path("parameter_sets").get(1).path("parameters").get(0).path("value").asText());
  }

  @Test
  public void testNativeBatchRequiresLiveV10WithDriverCode() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(new AllPurposeCluster("org", "cluster"))
                .negotiatedProtocolVersion(42249L)
                .liveProtocolVersion(true)
                .build());

    java.sql.SQLFeatureNotSupportedException error =
        assertThrows(
            java.sql.SQLFeatureNotSupportedException.class,
            () -> client.executeNativeBatch("INSERT INTO t VALUES (?)", List.of(), session, null));
    assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(), error.getSQLState());
    assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.getCode(), error.getErrorCode());
    verifyNoInteractions(apiClient);
  }

  @Test
  public void testNativeBatchRejectsMismatchedSetsBeforeRequest() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(new AllPurposeCluster("org", "cluster"))
                .negotiatedProtocolVersion(42250L)
                .liveProtocolVersion(true)
                .build());
    List<BatchParameterSet> sets =
        List.of(
            BatchParameterSet.from(Map.of(1, getSqlParam(1, 1, DatabricksTypeUtil.INT))),
            BatchParameterSet.from(Map.of(2, getSqlParam(2, 2, DatabricksTypeUtil.INT))));

    assertThrows(
        SQLException.class,
        () -> client.executeNativeBatch("INSERT INTO t VALUES (?)", sets, session, null));
    verifyNoInteractions(apiClient);
  }

  @Test
  public void testWarehouseSubmitPreservesSdkExceptionForSyncAndAsync() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(warehouse)
                .build());
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any())).thenReturn("{}");
    DatabricksException sdkError = new DatabricksException("warehouse submit failed");
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenThrow(sdkError);

    assertSame(
        sdkError,
        assertThrows(
            DatabricksException.class,
            () ->
                client.executeStatement(
                    "SELECT 1", warehouse, Map.of(), StatementType.QUERY, session, null, null)));
    assertSame(
        sdkError,
        assertThrows(
            DatabricksException.class,
            () -> client.executeStatementAsync("SELECT 1", warehouse, Map.of(), session, null)));
  }

  @Test
  public void testWarehousePollAndResultPreserveSdkException() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(warehouse)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any())).thenReturn("{}");
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.PENDING)));
    DatabricksException sdkError = new DatabricksException("warehouse poll failed");
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class))).thenThrow(sdkError);

    assertSame(
        sdkError,
        assertThrows(
            DatabricksException.class,
            () ->
                client.executeStatement(
                    "SELECT 1", warehouse, Map.of(), StatementType.QUERY, session, null, null)));
    assertSame(
        sdkError,
        assertThrows(
            DatabricksException.class,
            () ->
                client.getStatementResult(
                    StatementId.forCurrentSession("statement", info), session, null)));
  }

  @Test
  public void testNativeBatchCleanupFailurePreservesSuccessfulCounts() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42250L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getComputeResource()).thenReturn(cluster);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));
    when(apiClient.execute(any(Request.class), eq(Void.class)))
        .thenThrow(new IOException("cleanup failed"));

    long[] counts =
        client.executeNativeBatch(
            "INSERT INTO t VALUES (?)",
            List.of(
                BatchParameterSet.from(
                    Map.of(1, getSqlParam(1, "secret", DatabricksTypeUtil.STRING)))),
            session,
            null);

    assertArrayEquals(new long[] {java.sql.Statement.SUCCESS_NO_INFO}, counts);
    verify(apiClient).execute(any(Request.class), eq(Void.class));
  }

  @Test
  public void testNativeBatchSucceedsWhenAggregateResultCannotBeDecoded() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42250L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getComputeResource()).thenReturn(cluster);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    // A manifest without a format cannot be decoded into a result set.
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
                .setManifest(new ResultManifest().setTotalRowCount(1L))
                .setResult(new ResultData().setRowCount(1L)));

    long[] counts =
        client.executeNativeBatch(
            "INSERT INTO t VALUES (?)",
            List.of(
                BatchParameterSet.from(Map.of(1, getSqlParam(1, "v", DatabricksTypeUtil.STRING)))),
            session,
            null);

    assertArrayEquals(new long[] {java.sql.Statement.SUCCESS_NO_INFO}, counts);
    verify(apiClient).execute(any(Request.class), eq(Void.class));
  }

  @Test
  public void testApcResponseDebugBodyHidesEchoedBindingAndPreservesWireBody() throws Exception {
    Request request =
        new RedactingRequest(
            Request.POST,
            "https://example.com" + STATEMENT_PATH,
            "{\"backend_compute\":{},\"statement\":\"SELECT ?\",\"parameters\":[]}");
    String echoed = "{\"error\":{\"message\":\"bound-secret\"}}";
    Response raw = new Response(request, 400, "Bad Request", Map.of(), echoed);

    Response protectedResponse = DatabricksSdkClient.redactApcResponseDebugBody(request, raw);

    assertFalse(protectedResponse.getDebugBody().contains("bound-secret"));
    assertEquals(
        echoed, new String(protectedResponse.getBody().readAllBytes(), StandardCharsets.UTF_8));
    assertEquals(400, protectedResponse.getStatusCode());
    assertSame(
        raw, DatabricksSdkClient.redactApcResponseDebugBody(new Request(Request.GET, "/"), raw));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = {42247L, 42248L})
  public void testApcDirectClientParametersFollowLiveVersion(Long version) throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(version)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    client.executeStatement(
        "SELECT ? AS value, '?' AS literal /* ? */",
        cluster,
        Map.of(1, getSqlParam(1, "x';--\\tail", DatabricksTypeUtil.STRING)),
        StatementType.QUERY,
        session,
        null,
        null);
    ArgumentCaptor<Request> execute = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(execute.capture(), eq(ExecuteStatementResponse.class));
    JsonNode body = new ObjectMapper().readTree(execute.getValue().getBodyString());
    if (version != null && version >= 42248L) {
      assertEquals("SELECT ? AS value, '?' AS literal /* ? */", body.path("statement").asText());
      assertEquals(1, body.path("parameters").size());
    } else {
      assertEquals(
          "SELECT 'x'';--\\\\tail' AS value, '?' AS literal /* ? */",
          body.path("statement").asText());
      assertFalse(body.has("parameters"));
    }
    assertFalse(body.has("parameter_sets"));
  }

  @Test
  public void testApcNativeFailuresRedactBindingsAndClassifyRawError() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42250L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getComputeResource()).thenReturn(cluster);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    String secret = "native-bound-secret";
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("failed")
                .setStatus(
                    new StatementStatus()
                        .setState(StatementState.FAILED)
                        .setSqlState("XXUCC")
                        .setError(
                            new ServiceError()
                                .setMessage(
                                    "[UC_CLIENT_EXCEPTION] Failed to contact the Unity Catalog "
                                        + "server. HTTP/1.1 504 Gateway Timeout, "
                                        + "DEADLINE_EXCEEDED: "
                                        + secret))));
    Map<Integer, ImmutableSqlParameter> bindings =
        Map.of(1, getSqlParam(1, secret, DatabricksTypeUtil.STRING));

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException single =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  client.executeStatement(
                      "SELECT ?", cluster, bindings, StatementType.QUERY, session, null, null));
      assertEquals("08S01", single.getSQLState());
      assertFalse(single.getMessage().contains(secret));

      DatabricksSQLException batch =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  client.executeNativeBatch(
                      "INSERT INTO t VALUES (?)",
                      List.of(BatchParameterSet.from(bindings)),
                      session,
                      null));
      assertEquals("08S01", batch.getSQLState());
      assertFalse(batch.getMessage().contains(secret));
      assertFalse(logs.getOutput().contains(secret));
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = {42247L, 42248L})
  public void testApcHttpExecutionErrorRedactsBoundValueWithoutParentStatement(Long version)
      throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(version)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    String secret = "http-echo-secret";
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenThrow(new DatabricksError("FAILED", "server echoed " + secret, 400));

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  client.executeStatement(
                      "SELECT ?",
                      cluster,
                      Map.of(1, getSqlParam(1, secret, DatabricksTypeUtil.STRING)),
                      StatementType.METADATA,
                      session,
                      null,
                      null));

      assertEquals("FAILED", error.getSQLState());
      assertEquals(400, error.getErrorCode());
      assertFalse(error.getMessage().contains(secret));
      DatabricksError cause = assertInstanceOf(DatabricksError.class, error.getCause());
      assertEquals("FAILED", cause.getErrorCode());
      assertEquals(400, cause.getStatusCode());
      assertFalse(cause.getMessage().contains(secret));
      assertFalse(logs.getOutput().contains(secret));
    }
  }

  @Test
  public void testApcNativePollFailureDoesNotExposeBoundValueInCause() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42248L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any())).thenReturn("{}");
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.PENDING)));
    String secret = "native-poll-secret";
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenThrow(
            new RuntimeException(
                "retry wrapper echoed " + secret,
                new DatabricksException("server echoed " + secret)));

    DatabricksSQLException error =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                client.executeStatement(
                    "SELECT ?",
                    cluster,
                    Map.of(1, getSqlParam(1, secret, DatabricksTypeUtil.STRING)),
                    StatementType.QUERY,
                    session,
                    null,
                    null));

    assertEquals(DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(), error.getSQLState());
    assertEquals(DatabricksDriverErrorCode.SDK_CLIENT_ERROR.getCode(), error.getErrorCode());
    assertFalse(error.getMessage().contains(secret));
    assertNull(error.getCause());
    ArgumentCaptor<Request> cleanup = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(cleanup.capture(), eq(Void.class));
    assertEquals(Request.DELETE, cleanup.getValue().getMethod());
    assertEquals(
        String.format(STATEMENT_PATH_WITH_ID, "statement"), cleanup.getValue().getUri().getPath());
  }

  @Test
  void apcPollPreservesServerErrorCodeAndStatusWithoutEchoedValues() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("private-org", "private-cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("private-session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("private-session");
    when(apiClient.serialize(any())).thenReturn("{}");
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("private-statement")
                .setStatus(new StatementStatus().setState(StatementState.PENDING)));
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenThrow(
            new DatabricksError(
                "PERMISSION_DENIED", "private-org private-cluster private-bound-value", 403));

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException failure =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  client.executeStatement(
                      "SELECT 1", cluster, Map.of(), StatementType.QUERY, session, null, null));
      assertEquals("PERMISSION_DENIED", failure.getSQLState());
      assertEquals(403, failure.getErrorCode());
      DatabricksError cause = assertInstanceOf(DatabricksError.class, failure.getCause());
      assertEquals(403, cause.getStatusCode());
      for (String secret :
          List.of("private-org", "private-cluster", "private-session", "private-bound-value")) {
        assertFalse(failure.getMessage().contains(secret));
        assertFalse(cause.getMessage().contains(secret));
        assertFalse(logs.getOutput().contains(secret));
      }
    }
    verify(apiClient).execute(any(Request.class), eq(Void.class));
  }

  @Test
  void apcInlineOnlyResultFailureDoesNotReturnPartialResultSet() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "EnableQueryResultDownload=0;TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("private-session")
            .computeResource(new AllPurposeCluster("private-org", "private-cluster"))
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("private-session");
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatementId("private-statement")
                .setStatus(
                    new StatementStatus()
                        .setState(StatementState.FAILED)
                        .setSqlState("22000")
                        .setError(
                            new ServiceError()
                                .setErrorCode(ServiceErrorCode.BAD_REQUEST)
                                .setMessage("cannot serve all rows inline: private-cluster")))
                .setResult(new ResultData()));

    DatabricksSQLException failure =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                client.getStatementResult(
                    StatementId.forCurrentSession("private-statement", info), session, null));

    assertEquals("22000", failure.getSQLState());
    assertTrue(failure.getMessage().contains("BAD_REQUEST"));
    assertFalse(failure.getMessage().contains("private-cluster"));
  }

  @Test
  void apcStatementFailureUsesSafeCodeWhenServerCodeIsMissing() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("private-session")
            .computeResource(new AllPurposeCluster("private-org", "private-cluster"))
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("private-session");
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatementId("private-statement")
                .setStatus(
                    new StatementStatus()
                        .setState(StatementState.FAILED)
                        .setSqlState("22000")
                        .setError(new ServiceError().setMessage("private-secret"))));

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException failure =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  client.getStatementResult(
                      StatementId.forCurrentSession("private-statement", info), session, null));
      assertTrue(failure.getMessage().contains("SERVER_ERROR"));
      assertFalse(failure.getMessage().contains("private-secret"));
      assertFalse(logs.getOutput().contains("private-secret"));
    }
  }

  @Test
  public void testApcPreparedPollIoFailureClosesAssignedOperation() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42248L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any())).thenReturn("{}");
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.PENDING)));
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenThrow(new IOException("poll failed"));
    DatabricksPreparedStatement prepared = mock(DatabricksPreparedStatement.class);
    when(prepared.getStatement()).thenReturn(mock(java.sql.Statement.class));

    assertThrows(
        DatabricksSQLException.class,
        () ->
            client.executeStatement(
                "SELECT ?",
                cluster,
                Map.of(1, getSqlParam(1, "bound", DatabricksTypeUtil.STRING)),
                StatementType.QUERY,
                session,
                prepared,
                null));

    verify(prepared).setStatementId(StatementId.forCurrentSession("statement", info));
    verify(prepared).closeServerOperation();
  }

  @Test
  public void testApcAsyncResultHttpFailureRedactsBoundValue() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42248L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("session");
    String secret = "async-bound-secret";
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenThrow(new DatabricksError("FAILED", "server echoed " + secret, 400));
    DatabricksPreparedStatement prepared = mock(DatabricksPreparedStatement.class);

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  client.getStatementResult(
                      StatementId.forCurrentSession("statement", info), session, prepared));
      assertEquals("FAILED", error.getSQLState());
      assertEquals(400, error.getErrorCode());
      assertFalse(error.getMessage().contains(secret));
      DatabricksError cause = assertInstanceOf(DatabricksError.class, error.getCause());
      assertEquals(400, cause.getStatusCode());
      assertFalse(cause.getMessage().contains(secret));
      assertFalse(logs.getOutput().contains(secret));
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = StatementState.class,
      names = {"SUCCEEDED", "FAILED", "CANCELED"})
  public void testApcMetadataCompletionRedactsStatementId(StatementState state) throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("session");
    if (state == StatementState.SUCCEEDED) {
      when(session.getConnectionContext()).thenReturn(context);
    }
    ResultManifest successManifest =
        new ResultManifest()
            .setFormat(SqlExecResultFormat.ARROW_STREAM)
            .setTotalChunkCount(0L)
            .setSchema(new ResultSchema().setColumns(List.of()).setColumnCount(0L));
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatus(new StatementStatus().setState(state).setSqlState("42000"))
                .setManifest(state == StatementState.SUCCEEDED ? successManifest : null)
                .setResult(state == StatementState.SUCCEEDED ? new ResultData() : null));
    String rawId = "private-metadata-statement-id";

    try (LogCapture logs = new LogCapture()) {
      if (state == StatementState.SUCCEEDED) {
        DatabricksResultSet result =
            client.getStatementResult(
                StatementId.forCurrentSession(rawId, info), session, null, StatementType.METADATA);
        assertEquals(rawId, result.getStatementId());
        assertTrue(logs.getOutput().contains("Empty result (total_chunk_count=0)"));
      } else {
        DatabricksSQLException error =
            assertThrows(
                DatabricksSQLException.class,
                () ->
                    client.getStatementResult(
                        StatementId.forCurrentSession(rawId, info),
                        session,
                        null,
                        StatementType.METADATA));
        assertFalse(error.getMessage().contains(rawId));
        assertTrue(error.getMessage().contains("[REDACTED]"));
      }
      assertFalse(logs.getOutput().contains(rawId));
      assertTrue(logs.getOutput().contains("[REDACTED]"));
    }
  }

  @Test
  public void testApcAsyncSubmitHttpFailureRedactsBoundValue() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(42248L)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(apiClient.serialize(any())).thenReturn("{}");
    String secret = "async-submit-secret";
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenThrow(new DatabricksException("server echoed " + secret));

    try (LogCapture logs = new LogCapture()) {
      DatabricksSQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  client.executeStatementAsync(
                      "SELECT ?",
                      cluster,
                      Map.of(1, getSqlParam(1, secret, DatabricksTypeUtil.STRING)),
                      session,
                      null));
      assertEquals(DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(), error.getSQLState());
      assertEquals(DatabricksDriverErrorCode.SDK_CLIENT_ERROR.getCode(), error.getErrorCode());
      assertFalse(error.getMessage().contains(secret));
      assertNull(error.getCause());
      assertFalse(logs.getOutput().contains(secret));
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = {42247L})
  public void testApcProcedureMetadataDowngradeOmitsParameters(Long version) throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .negotiatedProtocolVersion(version)
            .liveProtocolVersion(true)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getComputeResource()).thenReturn(cluster);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenThrow(new IOException("secret from server response"));
    DatabricksMetadataQueryClient metadata = new DatabricksMetadataQueryClient(client);

    SQLException proceduresError =
        assertThrows(
            SQLException.class,
            () -> metadata.listProcedures(session, "main", "s';--\\tail", "p'%"));
    SQLException columnsError =
        assertThrows(
            SQLException.class,
            () -> metadata.listProcedureColumns(session, "main", "s';--\\tail", "p'%", "c\\_"));
    assertFalse(proceduresError.getMessage().contains("secret from server response"));
    assertFalse(columnsError.getMessage().contains("secret from server response"));

    ArgumentCaptor<Request> requests = ArgumentCaptor.forClass(Request.class);
    verify(apiClient, times(2)).execute(requests.capture(), eq(ExecuteStatementResponse.class));
    JsonNode procedures =
        new ObjectMapper().readTree(requests.getAllValues().get(0).getBodyString());
    JsonNode columns = new ObjectMapper().readTree(requests.getAllValues().get(1).getBodyString());
    for (JsonNode body : List.of(procedures, columns)) {
      String sql = body.path("statement").asText();
      assertTrue(sql.contains("LIKE 's'';--\\\\tail'"));
      assertTrue(sql.contains("LIKE 'p''%'"));
      assertFalse(sql.contains("LIKE ?"));
      assertFalse(body.has("parameters"));
      assertFalse(body.has("parameter_sets"));
      assertEquals("ARROW_BATCH", body.path("format").asText());
      assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
    }
    assertTrue(columns.path("statement").asText().contains("LIKE 'c\\\\_'"));
    for (Request request : requests.getAllValues()) {
      assertFalse(request.getHeaders().containsKey("X-Databricks-Metadata-Operation-Type"));
      assertFalse(request.getHeaders().containsKey("x-databricks-sea-can-run-fully-sync"));
    }
  }

  private static Stream<Arguments> apcShowOperations() {
    return Stream.of(
        Arguments.of(MetadataOperationType.GET_CATALOGS, "SHOW CATALOGS"),
        Arguments.of(MetadataOperationType.GET_SCHEMAS, "SHOW SCHEMAS IN `main`"),
        Arguments.of(MetadataOperationType.GET_TABLES, "SHOW TABLES IN CATALOG `main`"),
        Arguments.of(MetadataOperationType.GET_COLUMNS, "SHOW COLUMNS IN CATALOG `main`"),
        Arguments.of(MetadataOperationType.GET_FUNCTIONS, "SHOW FUNCTIONS IN CATALOG `main`"),
        Arguments.of(
            MetadataOperationType.GET_PRIMARY_KEYS,
            "SHOW KEYS IN CATALOG `main` IN SCHEMA `s` IN TABLE `t`"),
        Arguments.of(
            MetadataOperationType.GET_CROSS_REFERENCE,
            "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `s` IN TABLE `t`"));
  }

  @ParameterizedTest
  @MethodSource("apcShowOperations")
  void apcShowMetadataRequestKeepsFormatAndOperationHeaderAcrossSyncModes(
      MetadataOperationType operation, String sql) throws Exception {
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    DatabricksSession session = mock(DatabricksSession.class);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("one")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)),
            new ExecuteStatementResponse()
                .setStatementId("two")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)),
            new ExecuteStatementResponse()
                .setStatementId("three")
                .setStatus(new StatementStatus().setState(StatementState.RUNNING)));

    for (int mode = 0; mode < 3; mode++) {
      String sync = mode == 1 ? "0" : "1";
      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(
              JDBC_URL
                  + "EnableSeaSyncMetadata="
                  + sync
                  + ";EnableSQLExecHybridResults=0;EnableThriftNativeMetadata=1;TelemetryLogLevel=OFF;",
              new Properties());
      when(session.getConnectionContext()).thenReturn(context);
      DatabricksSdkClient client =
          new DatabricksSdkClient(context, statementExecutionService, apiClient);
      if (mode == 2) {
        client.executeStatementAsync(
            sql, cluster, Map.of(), session, null, StatementType.METADATA, operation);
      } else {
        client.executeStatement(
            sql, cluster, Map.of(), StatementType.METADATA, session, null, operation);
      }
    }

    ArgumentCaptor<Request> captured = ArgumentCaptor.forClass(Request.class);
    verify(apiClient, times(3)).execute(captured.capture(), eq(ExecuteStatementResponse.class));
    for (int mode = 0; mode < 3; mode++) {
      Request request = captured.getAllValues().get(mode);
      JsonNode body = new ObjectMapper().readTree(request.getBodyString());
      assertEquals(sql, body.path("statement").asText());
      assertEquals("ARROW_STREAM", body.path("format").asText());
      assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
      assertEquals(
          operation.getHeaderValue(),
          request.getHeaders().get("X-Databricks-Metadata-Operation-Type"));
      assertEquals("true", request.getHeaders().get("X-Databricks-Require-Thrift-Native-Metadata"));
      assertEquals(
          mode == 0,
          "true".equals(request.getHeaders().get("x-databricks-sea-can-run-fully-sync")));
    }
  }

  @Test
  void apcV8ProcedureSelectsUseNativeParametersAndOrdinaryQueryFormat() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "EnableSQLExecHybridResults=0;TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(cluster)
                .negotiatedProtocolVersion(42248L)
                .liveProtocolVersion(true)
                .build());
    when(session.getSessionId()).thenReturn("session");
    when(session.getComputeResource()).thenReturn(cluster);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenThrow(new IOException("captured request"));
    DatabricksMetadataQueryClient metadata = new DatabricksMetadataQueryClient(client);

    assertThrows(SQLException.class, () -> metadata.listProcedures(session, "main", "s%", "p%"));
    assertThrows(
        SQLException.class, () -> metadata.listProcedureColumns(session, "main", "s%", "p%", "c%"));

    ArgumentCaptor<Request> captured = ArgumentCaptor.forClass(Request.class);
    verify(apiClient, times(2)).execute(captured.capture(), eq(ExecuteStatementResponse.class));
    for (Request request : captured.getAllValues()) {
      JsonNode body = new ObjectMapper().readTree(request.getBodyString());
      assertTrue(body.path("statement").asText().contains("LIKE ?"));
      assertTrue(body.path("parameters").size() > 0);
      assertEquals("ARROW_BATCH", body.path("format").asText());
      assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
      assertFalse(request.getHeaders().containsKey("X-Databricks-Metadata-Operation-Type"));
      assertFalse(request.getHeaders().containsKey("x-databricks-sea-can-run-fully-sync"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"SELECT current_catalog()", "SELECT current_catalog(), current_schema()"})
  void apcInternalMetadataSelectHasQueryFormatAndNoMetadataHeaders(String sql) throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "EnableSQLExecHybridResults=0;TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(cluster)
                .build());
    when(session.getSessionId()).thenReturn("session");
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    client.executeStatement(sql, cluster, Map.of(), StatementType.METADATA, session, null, null);

    ArgumentCaptor<Request> captured = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(captured.capture(), eq(ExecuteStatementResponse.class));
    JsonNode body = new ObjectMapper().readTree(captured.getValue().getBodyString());
    assertEquals(sql, body.path("statement").asText());
    assertEquals("ARROW_BATCH", body.path("format").asText());
    assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
    assertFalse(
        captured.getValue().getHeaders().containsKey("X-Databricks-Metadata-Operation-Type"));
    assertFalse(
        captured.getValue().getHeaders().containsKey("x-databricks-sea-can-run-fully-sync"));
  }

  @Test
  void typedApcMetadataCompletionRetainsMetadataUpdateCountSemantics() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + "TelemetryLogLevel=OFF;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    StatementId id = StatementId.forCurrentSession("statement", info);
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("session");
    when(session.getConnectionContext()).thenReturn(context);
    when(apiClient.execute(any(Request.class), eq(GetStatementResponse.class)))
        .thenReturn(
            new GetStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    DatabricksResultSet result =
        client.getStatementResult(id, session, null, StatementType.METADATA);
    assertEquals(StatementState.SUCCEEDED, result.getStatementStatus().getState());
    assertEquals(0, result.getUpdateCount());
  }

  @Test
  public void testWarehouseEmptyBindingsStillSerializeEmptyParametersArray() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(warehouse)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    client.executeStatement(
        "SELECT 1", warehouse, Map.of(), StatementType.QUERY, session, null, null);
    ArgumentCaptor<Request> execute = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(execute.capture(), eq(ExecuteStatementResponse.class));
    JsonNode body = new ObjectMapper().readTree(execute.getValue().getBodyString());
    assertTrue(body.has("parameters"));
    assertEquals(0, body.path("parameters").size());
    assertFalse(body.has("parameter_sets"));
  }

  @Test
  public void testApcAsyncExecuteUsesArrowBatchAndZeroWaitWithHybridResultsDisabled()
      throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            JDBC_URL + "EnableSQLExecHybridResults=0;", new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(apiClient.serialize(any()))
        .thenAnswer(invocation -> new ObjectMapper().writeValueAsString(invocation.getArgument(0)));
    when(apiClient.execute(any(Request.class), eq(ExecuteStatementResponse.class)))
        .thenReturn(
            new ExecuteStatementResponse()
                .setStatementId("statement")
                .setStatus(new StatementStatus().setState(StatementState.RUNNING)));

    client.executeStatementAsync("select 1", cluster, Map.of(), session, null);

    ArgumentCaptor<Request> request = ArgumentCaptor.forClass(Request.class);
    verify(apiClient).execute(request.capture(), eq(ExecuteStatementResponse.class));
    JsonNode body = new ObjectMapper().readTree(request.getValue().getBodyString());
    assertEquals("ARROW_BATCH", body.path("format").asText());
    assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
    assertEquals("session", body.path("session_id").asText());
    assertEquals("0s", body.path("wait_timeout").asText());
    assertFalse(body.has("statements"));
  }

  @Test
  public void testApcHeartbeatDoesNotCallStatusEndpoint() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, apiClient);
    StatementId id =
        StatementId.forCurrentSession(
            "statement",
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(new AllPurposeCluster("org", "cluster"))
                .build());
    assertThrows(SQLFeatureNotSupportedException.class, () -> client.checkStatementAlive(id));
    verifyNoInteractions(apiClient);
  }

  @Test
  public void testApcWireRequestsThroughFakeTransport() throws Exception {
    String presignedUrl = "https://storage.example.com/chunk?sig=a%2Bb%2Fc&part=2";
    Map<String, String> presignedHeaders =
        Map.of("x-amz-server-side-encryption-customer-algorithm", "AES256");
    List<Request> sent = new ArrayList<>();
    HttpClient transport =
        request -> {
          String path = request.getUri().getPath();
          if (path.equals("/oidc/.well-known/oauth-authorization-server")) {
            return new Response(request, 200, "OK", Map.of(), "{}");
          }
          // Snapshot the bytes visible to the transport before the SDK log view is redacted.
          sent.add(new Request(request.getMethod(), request.getUrl(), request.getBodyString()));
          String body;
          if (path.equals(SESSION_PATH)) {
            body = "{\"session_id\":\"session\",\"negotiated_protocol_version\":8}";
          } else if (path.endsWith("/result/chunks/2")) {
            body =
                new ObjectMapper()
                    .writeValueAsString(
                        Map.of(
                            "external_links",
                            List.of(
                                Map.of(
                                    "external_link", presignedUrl,
                                    "http_headers", presignedHeaders,
                                    "chunk_index", 2,
                                    "row_offset", 7,
                                    "row_count", 1))));
          } else if (path.equals(STATEMENT_PATH) && Request.POST.equals(request.getMethod())) {
            body = "{\"statement_id\":\"statement\",\"status\":{\"state\":\"PENDING\"}}";
          } else if (path.endsWith("/cancel") || Request.DELETE.equals(request.getMethod())) {
            body = "{}";
          } else {
            body = "{\"status\":{\"state\":\"CANCELED\"}}";
          }
          return new Response(request, 200, "OK", Map.of(), body);
        };
    ApiClient realApiClient =
        DatabricksSdkClient.buildApiClient(
            new DatabricksConfig()
                .setHost("https://example.com")
                .setAuthType("pat")
                .setToken("test-token")
                .setHttpClient(transport),
            null,
            true);
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, statementExecutionService, realApiClient);
    AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
    ImmutableSessionInfo info = client.createSession(cluster, null, null, null);
    assertEquals(8L, info.nativeProtocolVersion());
    StatementId id = StatementId.forCurrentSession("statement", info);
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    assertThrows(
        DatabricksSQLException.class,
        () ->
            client.executeStatement(
                "select 1", cluster, Map.of(), StatementType.QUERY, session, null, null));
    assertThrows(DatabricksSQLException.class, () -> client.getStatementResult(id, session, null));
    ChunkLinkFetchResult links = client.getResultChunks(id, 2, 7);
    client.getResultChunksData(id, 3, 9);
    client.cancelStatement(id);
    client.closeStatement(id);
    client.deleteSession(info);

    assertEquals(10, sent.size());
    assertEquals(Request.GET, sent.get(2).getMethod());
    assertEquals(Request.DELETE, sent.get(3).getMethod());
    assertEquals(
        String.format(STATEMENT_PATH_WITH_ID, "statement"), sent.get(3).getUri().getPath());
    sent.remove(3);
    assertEquals(SESSION_PATH, sent.get(0).getUri().getPath());
    assertFalse(new ObjectMapper().readTree(sent.get(0).getBodyString()).has("warehouse_id"));
    assertEquals(
        "cluster",
        new ObjectMapper()
            .readTree(sent.get(0).getBodyString())
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
    assertEquals(Request.POST, sent.get(1).getMethod());
    assertEquals(STATEMENT_PATH, sent.get(1).getUri().getPath());
    assertEquals(
        "cluster",
        new ObjectMapper()
            .readTree(sent.get(1).getBodyString())
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
    for (int i : new int[] {2, 3, 4, 5, 7}) {
      assertEquals(i == 7 ? Request.DELETE : Request.GET, sent.get(i).getMethod());
      assertNull(sent.get(i).getBodyString());
    }
    String computeQuery = "backend_compute.all_purpose_cluster_compute.cluster_id=cluster";
    for (int i : new int[] {2, 3, 7}) {
      assertEquals(
          String.format(STATEMENT_PATH_WITH_ID, "statement"), sent.get(i).getUri().getPath());
      assertEquals(computeQuery, sent.get(i).getUri().getRawQuery());
    }
    assertEquals(Request.DELETE, sent.get(7).getMethod());
    assertFalse(sent.stream().anyMatch(request -> request.getUri().getPath().endsWith("/status")));
    assertEquals(String.format(RESULT_CHUNK_PATH, "statement", 2), sent.get(4).getUri().getPath());
    assertEquals("row_offset=7&" + computeQuery, sent.get(4).getUri().getRawQuery());
    assertEquals(String.format(RESULT_CHUNK_PATH, "statement", 3), sent.get(5).getUri().getPath());
    assertEquals("row_offset=9&" + computeQuery, sent.get(5).getUri().getRawQuery());
    assertEquals(1, links.getChunkLinks().size());
    ExternalLink link = links.getChunkLinks().get(0);
    assertEquals(presignedUrl, link.getExternalLink());
    assertEquals(presignedHeaders, link.getHttpHeaders());
    assertFalse(link.getExternalLink().contains("backend_compute"));
    assertEquals(
        "cluster",
        new ObjectMapper()
            .readTree(sent.get(6).getBodyString())
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
    assertEquals(
        "cluster",
        new ObjectMapper()
            .readTree(sent.get(8).getBodyString())
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
  }
}
