package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import java.sql.BatchUpdateException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.stubbing.OngoingStubbing;

class ApcParameterCapabilitiesTest {
  private static final AllPurposeCluster CLUSTER = new AllPurposeCluster("org", "cluster");
  private static final String INSERT = "INSERT INTO t VALUES (?)";
  private static final long[] FAILED_PAIR = {Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED};

  static List<Long> protocolVersions() {
    List<Long> versions = new ArrayList<>();
    versions.add(42247L);
    versions.add(null);
    versions.add(42248L);
    versions.add(42249L);
    return versions;
  }

  static List<Long> fallbackVersions() {
    List<Long> versions = protocolVersions();
    versions.add(42250L);
    return versions;
  }

  private static boolean nativeParameters(Long version) {
    return version != null && version >= 42248L;
  }

  private TestConnection testConnection(Long version) throws Exception {
    return testConnection(version, true);
  }

  private TestConnection testConnection(Long version, boolean live) throws Exception {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    IDatabricksSession session = mock(IDatabricksSession.class);
    IDatabricksClient client = mock(IDatabricksClient.class);
    DatabricksConnection connection = mock(DatabricksConnection.class);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .sessionId("session")
            .computeResource(CLUSTER)
            .clientType(DatabricksClientType.SEA)
            .negotiatedProtocolVersion(version)
            .liveProtocolVersion(live)
            .build();
    lenient().when(connection.getConnectionContext()).thenReturn(context);
    lenient().when(connection.getSession()).thenReturn(session);
    lenient().when(context.getNonRowcountQueryPrefixes()).thenReturn(List.of());
    lenient().when(context.getMaxBatchSize()).thenReturn(100);
    lenient().when(context.getTelemetryLogLevel()).thenReturn(TelemetryLogLevel.OFF);
    lenient().when(context.isNativeBatchingEnabled()).thenReturn(true);
    lenient().when(context.isBatchedInsertsEnabled()).thenReturn(true);
    lenient().when(session.getSessionInfo()).thenReturn(info);
    lenient().when(session.getComputeResource()).thenReturn(CLUSTER);
    lenient().when(session.getDatabricksClient()).thenReturn(client);
    return new TestConnection(connection, context, client);
  }

  private static IDatabricksStatementInternal matching(IDatabricksStatementInternal statement) {
    return statement == null ? ArgumentMatchers.<IDatabricksStatementInternal>any() : eq(statement);
  }

  /** Stubs {@code executeStatement}; a null statement or prefix matches any. */
  private static OngoingStubbing<DatabricksResultSet> whenExecute(
      TestConnection test,
      IDatabricksComputeResource compute,
      String sqlPrefix,
      StatementType type,
      IDatabricksStatementInternal statement)
      throws Exception {
    return when(
        test.client.executeStatement(
            sqlPrefix == null ? anyString() : startsWith(sqlPrefix),
            eq(compute),
            anyMap(),
            eq(type),
            any(),
            matching(statement),
            isNull()));
  }

  private static OngoingStubbing<DatabricksResultSet> whenQuery(
      TestConnection test, IDatabricksStatementInternal statement) throws Exception {
    return whenExecute(test, CLUSTER, null, StatementType.QUERY, statement);
  }

  private static OngoingStubbing<DatabricksResultSet> whenUpdate(
      TestConnection test, IDatabricksStatementInternal statement) throws Exception {
    return whenExecute(test, CLUSTER, null, StatementType.UPDATE, statement);
  }

  private static final class Executed {
    final List<String> sql = new ArrayList<>();
    final List<Map<Integer, ImmutableSqlParameter>> bindings = new ArrayList<>();
  }

  @SuppressWarnings("unchecked")
  private static Executed verifyExecuted(
      TestConnection test,
      int times,
      IDatabricksComputeResource compute,
      StatementType type,
      IDatabricksStatementInternal statement)
      throws Exception {
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map<Integer, ImmutableSqlParameter>> bindings =
        ArgumentCaptor.forClass(Map.class);
    verify(test.client, times(times))
        .executeStatement(
            sql.capture(),
            eq(compute),
            bindings.capture(),
            eq(type),
            any(),
            matching(statement),
            isNull());
    Executed executed = new Executed();
    executed.sql.addAll(sql.getAllValues());
    executed.bindings.addAll(bindings.getAllValues());
    return executed;
  }

  private static Executed verifyQueryExecuted(
      TestConnection test, IDatabricksStatementInternal statement) throws Exception {
    return verifyExecuted(test, 1, CLUSTER, StatementType.QUERY, statement);
  }

  private static void verifyNoSingleRequest(TestConnection test) throws Exception {
    verify(test.client, never())
        .executeStatement(anyString(), any(), anyMap(), any(), any(), any(), any());
  }

  private static void verifyNoNativeBatch(TestConnection test) throws Exception {
    verify(test.client, never()).executeNativeBatch(anyString(), anyList(), any(), any());
  }

  private static void addIntBatch(DatabricksPreparedStatement prepared, int... values)
      throws SQLException {
    for (int value : values) {
      prepared.setInt(1, value);
      prepared.addBatch();
    }
  }

  private static void captureWarnings(List<LogRecord> warnings, Executable body) throws Throwable {
    Logger logger = Logger.getLogger(DatabricksPreparedStatement.class.getName());
    Level previousLevel = logger.getLevel();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            if (record.getLevel() == Level.WARNING) {
              warnings.add(record);
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(handler);
    logger.setLevel(Level.WARNING);
    try {
      body.execute();
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }
  }

  @ParameterizedTest
  @MethodSource("protocolVersions")
  void boundMetadataUsesBindingFreeDescribeAndLeavesOriginalReady(Long version) throws Exception {
    TestConnection test = testConnection(version);
    String sql = "SELECT ? AS value, 'it\\'s ?' AS literal /* ? */";
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, sql);
    prepared.setObject(1, "hello", Types.VARCHAR);
    DatabricksResultSet describeResult = mock(DatabricksResultSet.class);
    when(describeResult.next()).thenReturn(true, false);
    when(describeResult.getString(1)).thenReturn("value");
    when(describeResult.getString(2)).thenReturn("string");
    whenExecute(test, CLUSTER, "DESCRIBE QUERY", StatementType.QUERY, null)
        .thenReturn(describeResult);

    ResultSetMetaData metadata = prepared.getMetaData();
    assertEquals(1, metadata.getColumnCount());
    Executed describe = verifyExecuted(test, 1, CLUSTER, StatementType.QUERY, null);
    assertEquals(
        "DESCRIBE QUERY SELECT '?' AS value, 'it\\'s ?' AS literal /* ? */", describe.sql.get(0));
    assertTrue(describe.bindings.get(0).isEmpty());

    clearInvocations(test.client);
    DatabricksResultSet originalResult = mock(DatabricksResultSet.class);
    whenQuery(test, prepared).thenReturn(originalResult);
    assertSame(originalResult, prepared.executeQuery());
    Executed executed = verifyQueryExecuted(test, prepared);
    if (nativeParameters(version)) {
      assertEquals(sql, executed.sql.get(0));
      assertEquals("hello", executed.bindings.get(0).get(1).value());
    } else {
      assertEquals("SELECT 'hello' AS value, 'it\\'s ?' AS literal /* ? */", executed.sql.get(0));
      assertTrue(executed.bindings.get(0).isEmpty());
    }
  }

  @Test
  void apcFallbackBatchClosesOperationWhenPollingFails() throws Exception {
    TestConnection test = testConnection(42248L);
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, INSERT);
    addIntBatch(prepared, 1, 2);
    StatementId operation = new StatementId("polling-operation");
    whenUpdate(test, prepared)
        .thenAnswer(
            invocation -> {
              prepared.setStatementId(operation);
              throw new SQLException("poll failed", "08S01");
            });

    BatchUpdateException failure =
        assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);

    assertArrayEquals(new long[0], failure.getLargeUpdateCounts());
    assertEquals("08S01", failure.getSQLState());
    verify(test.client).closeStatement(operation);
    verifyExecuted(test, 1, CLUSTER, StatementType.UPDATE, prepared);
    assertFalse(prepared.isClosed());
  }

  @ParameterizedTest
  @MethodSource("fallbackVersions")
  void fallbackBatchFailureReportsOnlyExecutedSetsAndPreservesSqlError(Long version)
      throws Exception {
    TestConnection test = testConnection(version);
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, INSERT);
    addIntBatch(prepared, 1, 2, 3);
    DatabricksResultSet first = mock(DatabricksResultSet.class);
    when(first.getUpdateCount()).thenReturn(7L);
    whenUpdate(test, prepared)
        .thenReturn(first)
        .thenThrow(new SQLException("sensitive server detail", "40001", 1287));

    BatchUpdateException failure =
        assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);

    assertArrayEquals(new long[] {7}, failure.getLargeUpdateCounts());
    assertArrayEquals(new int[] {7}, failure.getUpdateCounts());
    assertEquals("40001", failure.getSQLState());
    assertEquals(1287, failure.getErrorCode());
    assertFalse(failure.getMessage().contains("sensitive server detail"));
    assertNull(failure.getCause());
    verifyExecuted(test, 2, CLUSTER, StatementType.UPDATE, prepared);
    verifyNoNativeBatch(test);
  }

  @Test
  void v10NativeBatchReportsUnknownPerSetCountsAndMarksAllFailedOnError() throws Exception {
    TestConnection test = testConnection(42250L);
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, INSERT);
    when(test.client.supportsNativeBatchExecution()).thenReturn(true);
    addIntBatch(prepared, 1, 2);
    when(test.client.executeNativeBatch(eq(INSERT), anyList(), any(), eq(prepared)))
        .thenReturn(new long[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO});
    prepared.closeOnCompletion();
    assertArrayEquals(
        new int[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO}, prepared.executeBatch());
    assertFalse(prepared.isClosed());
    assertArrayEquals(new long[0], prepared.executeLargeBatch());
    verifyNoSingleRequest(test);

    addIntBatch(prepared, 3, 4);
    when(test.client.executeNativeBatch(eq(INSERT), anyList(), any(), eq(prepared)))
        .thenThrow(new SQLException("native failed"));
    BatchUpdateException error =
        assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);
    assertArrayEquals(FAILED_PAIR, error.getLargeUpdateCounts());

    doThrow(new IllegalStateException("transport failed"))
        .when(test.client)
        .executeNativeBatch(eq(INSERT), anyList(), any(), eq(prepared));
    BatchUpdateException unchecked =
        assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);
    assertArrayEquals(FAILED_PAIR, unchecked.getLargeUpdateCounts());
  }

  @Test
  void nativeBatchProcessesJdbcEscapesAndHonorsEscapeSetting() throws Exception {
    TestConnection test = testConnection(42250L);
    String escaped = "INSERT INTO t VALUES (?, {d '2024-01-01'})";
    DatabricksPreparedStatement prepared =
        new DatabricksPreparedStatement(test.connection, escaped);
    prepared.setEscapeProcessing(true);
    when(test.client.supportsNativeBatchExecution()).thenReturn(true);
    when(test.client.executeNativeBatch(anyString(), anyList(), any(), eq(prepared)))
        .thenReturn(new long[] {Statement.SUCCESS_NO_INFO});
    addIntBatch(prepared, 1);
    prepared.executeLargeBatch();
    verify(test.client)
        .executeNativeBatch(
            eq("INSERT INTO t VALUES (?, DATE '2024-01-01')"), anyList(), any(), eq(prepared));

    prepared.setEscapeProcessing(false);
    addIntBatch(prepared, 2);
    prepared.executeLargeBatch();
    verify(test.client).executeNativeBatch(eq(escaped), anyList(), any(), eq(prepared));
  }

  @ParameterizedTest
  @MethodSource("protocolVersions")
  void batchResetsKeepApcPlaceholderCount(Long version) throws Exception {
    TestConnection test = testConnection(version);
    String sql = "SELECT 'it\\'s ?' AS literal, ? AS bound";
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, sql);
    prepared.setInt(1, 1);
    assertEquals(1, prepared.getParameterMetaData().getParameterCount());

    prepared.addBatch();
    prepared.setInt(1, 2);
    assertEquals(1, prepared.getParameterMetaData().getParameterCount());

    prepared.clearBatch();
    prepared.setInt(1, 3);
    assertEquals(1, prepared.getParameterMetaData().getParameterCount());
    whenQuery(test, prepared).thenReturn(mock(DatabricksResultSet.class));
    prepared.executeQuery();
    assertEquals(
        nativeParameters(version) ? sql : "SELECT 'it\\'s ?' AS literal, 3 AS bound",
        verifyQueryExecuted(test, prepared).sql.get(0));
  }

  @Test
  void nativeBatchRejectsEverySetWithInvalidBindingPositions() throws Exception {
    for (String sql : List.of("INSERT INTO t VALUES (?)", "INSERT INTO t VALUES (?, ?)")) {
      TestConnection test = testConnection(42250L);
      DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, sql);
      when(test.client.supportsNativeBatchExecution()).thenReturn(true);
      boolean twoPlaceholders = sql.contains("?, ?");
      prepared.setInt(1, 1);
      if (twoPlaceholders) {
        prepared.setInt(2, 2);
      }
      prepared.addBatch();
      prepared.setInt(twoPlaceholders ? 1 : 2, 3);
      prepared.addBatch();

      BatchUpdateException error =
          assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);
      assertArrayEquals(FAILED_PAIR, error.getLargeUpdateCounts());
      verifyNoNativeBatch(test);
      verifyNoSingleRequest(test);
    }
  }

  @Test
  void savedVersionDoesNotGrantNativeParameters() throws Exception {
    TestConnection test = testConnection(42250L, false);
    DatabricksPreparedStatement prepared =
        new DatabricksPreparedStatement(test.connection, "SELECT ?");
    prepared.setString(1, "saved");
    whenQuery(test, prepared).thenReturn(mock(DatabricksResultSet.class));

    prepared.executeQuery();

    Executed executed = verifyQueryExecuted(test, prepared);
    assertEquals("SELECT 'saved'", executed.sql.get(0));
    assertTrue(executed.bindings.get(0).isEmpty());
  }

  @Test
  void downgradeEmitsDistinctTelemetryReasonForMissingAndOldVersions() throws Throwable {
    List<LogRecord> warnings = new ArrayList<>();
    captureWarnings(
        warnings,
        () -> {
          for (Long version : protocolVersions().subList(0, 2)) {
            warnings.clear();
            String reason = version == null ? "missing version" : "below V8";
            TestConnection test = testConnection(version);
            DatabricksPreparedStatement prepared =
                new DatabricksPreparedStatement(test.connection, "SELECT ?");
            prepared.setString(1, "safe");
            whenQuery(test, prepared).thenReturn(mock(DatabricksResultSet.class));
            try (MockedStatic<TelemetryHelper> telemetry = mockStatic(TelemetryHelper.class)) {
              // Repeated executions must report the downgrade once per statement.
              for (int i = 0; i < 3; i++) {
                prepared.executeQuery();
              }
              telemetry.verify(
                  () ->
                      TelemetryHelper.exportFailureLog(
                          test.context, "APC_PARAMETER_DOWNGRADE", reason, TelemetryLogLevel.WARN),
                  times(1));
            }
            assertEquals(1, warnings.size());
            assertEquals("APC parameter downgrade: " + reason, warnings.get(0).getMessage());
          }
        });
  }

  @Test
  void unsupportedNativeBatchPathsEmitDriverErrorCode() {
    IDatabricksClient defaultClient = mock(IDatabricksClient.class, CALLS_REAL_METHODS);
    DatabricksThriftServiceClient thriftClient =
        mock(DatabricksThriftServiceClient.class, CALLS_REAL_METHODS);
    for (IDatabricksClient client : List.of(defaultClient, thriftClient)) {
      SQLFeatureNotSupportedException error =
          assertThrows(
              SQLFeatureNotSupportedException.class,
              () -> client.executeNativeBatch(INSERT, List.of(), null, null));
      assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(), error.getSQLState());
      assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.getCode(), error.getErrorCode());
    }
  }

  @Test
  void v10WithoutOptInOrSupportedExecutorFallsBackToIndividualRequests() throws Exception {
    for (boolean optIn : List.of(false, true)) {
      TestConnection test = testConnection(42250L);
      when(test.context.isNativeBatchingEnabled()).thenReturn(optIn);
      when(test.client.supportsNativeBatchExecution()).thenReturn(false);
      DatabricksPreparedStatement prepared =
          new DatabricksPreparedStatement(test.connection, INSERT);
      addIntBatch(prepared, 1, 2);
      DatabricksResultSet result = mock(DatabricksResultSet.class);
      when(result.getUpdateCount()).thenReturn(1L);
      whenUpdate(test, prepared).thenReturn(result);

      assertArrayEquals(new long[] {1, 1}, prepared.executeLargeBatch());
      verifyExecuted(test, 2, CLUSTER, StatementType.UPDATE, prepared);
      verifyNoNativeBatch(test);
    }
  }

  private static class TestConnection {
    final DatabricksConnection connection;
    final IDatabricksConnectionContext context;
    final IDatabricksClient client;

    TestConnection(
        DatabricksConnection connection,
        IDatabricksConnectionContext context,
        IDatabricksClient client) {
      this.connection = connection;
      this.context = context;
      this.client = client;
    }
  }
}
