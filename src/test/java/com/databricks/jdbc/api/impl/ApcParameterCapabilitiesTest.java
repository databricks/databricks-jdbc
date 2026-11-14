package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.jdbc.telemetry.latency.DatabricksMetricsTimedProcessor;
import java.sql.BatchUpdateException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class ApcParameterCapabilitiesTest {
  private static final AllPurposeCluster CLUSTER = new AllPurposeCluster("org", "cluster");
  private static final String INSERT = "INSERT INTO t VALUES (?)";

  static List<Long> protocolVersions() {
    List<Long> versions = new ArrayList<>();
    versions.add(42247L);
    versions.add(null);
    versions.add(42248L);
    versions.add(42249L);
    return versions;
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

  @ParameterizedTest
  @MethodSource("protocolVersions")
  void boundMetadataUsesBindingFreeDescribeAndLeavesOriginalReady(Long version) throws Exception {
    TestConnection test = testConnection(version);
    String sql = "SELECT ? AS value, '?' AS literal /* ? */";
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, sql);
    prepared.setObject(1, "hello", Types.VARCHAR);
    DatabricksResultSet describeResult = mock(DatabricksResultSet.class);
    when(describeResult.next()).thenReturn(true, false);
    when(describeResult.getString(1)).thenReturn("value");
    when(describeResult.getString(2)).thenReturn("string");
    when(test.client.executeStatement(
            startsWith("DESCRIBE QUERY"),
            eq(CLUSTER),
            anyMap(),
            eq(StatementType.QUERY),
            any(),
            any(),
            isNull()))
        .thenReturn(describeResult);

    ResultSetMetaData metadata = prepared.getMetaData();
    assertEquals(1, metadata.getColumnCount());
    ArgumentCaptor<String> describeSql = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<Integer, ImmutableSqlParameter>> describeBindings =
        ArgumentCaptor.forClass(Map.class);
    verify(test.client)
        .executeStatement(
            describeSql.capture(),
            eq(CLUSTER),
            describeBindings.capture(),
            eq(StatementType.QUERY),
            any(),
            any(),
            isNull());
    assertEquals(
        "DESCRIBE QUERY SELECT '?' AS value, '?' AS literal /* ? */", describeSql.getValue());
    assertTrue(describeBindings.getValue().isEmpty());

    clearInvocations(test.client);
    DatabricksResultSet originalResult = mock(DatabricksResultSet.class);
    when(test.client.executeStatement(
            anyString(),
            eq(CLUSTER),
            anyMap(),
            eq(StatementType.QUERY),
            any(),
            eq(prepared),
            isNull()))
        .thenReturn(originalResult);
    assertSame(originalResult, prepared.executeQuery());
    ArgumentCaptor<String> executedSql = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<Integer, ImmutableSqlParameter>> executedBindings =
        ArgumentCaptor.forClass(Map.class);
    verify(test.client)
        .executeStatement(
            executedSql.capture(),
            eq(CLUSTER),
            executedBindings.capture(),
            eq(StatementType.QUERY),
            any(),
            eq(prepared),
            isNull());
    if (version != null && version >= 42248) {
      assertEquals(sql, executedSql.getValue());
      assertEquals("hello", executedBindings.getValue().get(1).value());
    } else {
      assertEquals("SELECT 'hello' AS value, '?' AS literal /* ? */", executedSql.getValue());
      assertTrue(executedBindings.getValue().isEmpty());
    }
  }

  @ParameterizedTest
  @MethodSource("protocolVersions")
  void preparedBatchUsesPerSetFallbackAndCorrectParameterMode(Long version) throws Exception {
    TestConnection test = testConnection(version);
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, INSERT);
    for (String value : List.of("a' ; --", "b\\c")) {
      prepared.setString(1, value);
      prepared.addBatch();
    }
    DatabricksResultSet first = mock(DatabricksResultSet.class);
    DatabricksResultSet second = mock(DatabricksResultSet.class);
    when(first.getUpdateCount()).thenReturn(2L);
    when(second.getUpdateCount()).thenReturn(3L);
    when(test.client.executeStatement(
            anyString(),
            eq(CLUSTER),
            anyMap(),
            eq(StatementType.UPDATE),
            any(),
            eq(prepared),
            isNull()))
        .thenReturn(first, second);

    assertArrayEquals(new long[] {2, 3}, prepared.executeLargeBatch());
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<Integer, ImmutableSqlParameter>> bindings =
        ArgumentCaptor.forClass(Map.class);
    verify(test.client, times(2))
        .executeStatement(
            sql.capture(),
            eq(CLUSTER),
            bindings.capture(),
            eq(StatementType.UPDATE),
            any(),
            eq(prepared),
            isNull());
    if (version != null && version >= 42248) {
      assertEquals(List.of(INSERT, INSERT), sql.getAllValues());
      assertEquals("a' ; --", bindings.getAllValues().get(0).get(1).value());
    } else {
      assertEquals("INSERT INTO t VALUES ('a'' ; --')", sql.getAllValues().get(0));
      assertEquals("INSERT INTO t VALUES ('b\\\\c')", sql.getAllValues().get(1));
      assertTrue(bindings.getAllValues().stream().allMatch(Map::isEmpty));
    }
    verify(test.client, never()).executeNativeBatch(anyString(), anyList(), any(), any());
  }

  @Test
  void v10NativeBatchReportsUnknownPerSetCountsAndMarksAllFailedOnError() throws Exception {
    TestConnection test = testConnection(42250L);
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, INSERT);
    when(test.client.supportsNativeBatchExecution()).thenReturn(true);
    for (int value : List.of(1, 2)) {
      prepared.setInt(1, value);
      prepared.addBatch();
    }
    when(test.client.executeNativeBatch(eq(INSERT), anyList(), any(), eq(prepared)))
        .thenReturn(new long[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO});
    assertArrayEquals(
        new int[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO}, prepared.executeBatch());
    verify(test.client, never())
        .executeStatement(anyString(), any(), anyMap(), any(), any(), any(), any());

    prepared.setInt(1, 3);
    prepared.addBatch();
    prepared.setInt(1, 4);
    prepared.addBatch();
    when(test.client.executeNativeBatch(eq(INSERT), anyList(), any(), eq(prepared)))
        .thenThrow(new SQLException("native failed"));
    BatchUpdateException error =
        assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);
    assertArrayEquals(
        new long[] {Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
        error.getLargeUpdateCounts());

    doThrow(new IllegalStateException("transport failed"))
        .when(test.client)
        .executeNativeBatch(eq(INSERT), anyList(), any(), eq(prepared));
    BatchUpdateException unchecked =
        assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);
    assertArrayEquals(
        new long[] {Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
        unchecked.getLargeUpdateCounts());
  }

  @Test
  void complexNativeBindingsFailBeforeSingleOrBatchRequests() throws Exception {
    TestConnection test = testConnection(42250L);
    DatabricksPreparedStatement query =
        new DatabricksPreparedStatement(test.connection, "SELECT ?");
    query.setObject(1, new Object[] {1}, Types.ARRAY);
    DatabricksSQLException single = assertThrows(DatabricksSQLException.class, query::executeQuery);
    assertEquals("UNSUPPORTED_APC_PARAMETER", single.getSQLState());
    assertEquals(1045, single.getErrorCode());

    DatabricksPreparedStatement batch = new DatabricksPreparedStatement(test.connection, INSERT);
    batch.setInt(1, 1);
    batch.addBatch();
    batch.setObject(1, "struct", Types.STRUCT);
    batch.addBatch();
    BatchUpdateException error = assertThrows(BatchUpdateException.class, batch::executeLargeBatch);
    assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER.name(), error.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER.getCode(), error.getErrorCode());
    assertArrayEquals(
        new long[] {Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
        error.getLargeUpdateCounts());
    verify(test.client, never())
        .executeStatement(anyString(), any(), anyMap(), any(), any(), any(), any());
    verify(test.client, never()).executeNativeBatch(anyString(), anyList(), any(), any());
  }

  @Test
  void savedVersionDoesNotGrantNativeParameters() throws Exception {
    TestConnection test = testConnection(42250L, false);
    DatabricksPreparedStatement prepared =
        new DatabricksPreparedStatement(test.connection, "SELECT ?");
    prepared.setString(1, "saved");
    when(test.client.executeStatement(
            anyString(),
            eq(CLUSTER),
            anyMap(),
            eq(StatementType.QUERY),
            any(),
            eq(prepared),
            isNull()))
        .thenReturn(mock(DatabricksResultSet.class));

    prepared.executeQuery();
    verify(test.client)
        .executeStatement(
            eq("SELECT 'saved'"),
            eq(CLUSTER),
            eq(Map.of()),
            eq(StatementType.QUERY),
            any(),
            eq(prepared),
            isNull());
  }

  @Test
  void downgradeEmitsDistinctTelemetryReasonForMissingAndOldVersions() throws Exception {
    for (Long version : protocolVersions().subList(0, 2)) {
      TestConnection test = testConnection(version);
      DatabricksPreparedStatement prepared =
          new DatabricksPreparedStatement(test.connection, "SELECT ?");
      prepared.setString(1, "safe");
      when(test.client.executeStatement(
              anyString(),
              eq(CLUSTER),
              anyMap(),
              eq(StatementType.QUERY),
              any(),
              eq(prepared),
              isNull()))
          .thenReturn(mock(DatabricksResultSet.class));
      try (MockedStatic<TelemetryHelper> telemetry = mockStatic(TelemetryHelper.class)) {
        prepared.executeQuery();
        telemetry.verify(
            () ->
                TelemetryHelper.exportFailureLog(
                    test.context,
                    "APC_PARAMETER_DOWNGRADE",
                    version == null ? "missing version" : "below V8",
                    TelemetryLogLevel.WARN));
      }
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
      prepared.setInt(1, 1);
      prepared.addBatch();
      prepared.setInt(1, 2);
      prepared.addBatch();
      DatabricksResultSet result = mock(DatabricksResultSet.class);
      when(result.getUpdateCount()).thenReturn(1L);
      when(test.client.executeStatement(
              eq(INSERT),
              eq(CLUSTER),
              anyMap(),
              eq(StatementType.UPDATE),
              any(),
              eq(prepared),
              isNull()))
          .thenReturn(result);

      assertArrayEquals(new long[] {1, 1}, prepared.executeLargeBatch());
      verify(test.client, times(2))
          .executeStatement(
              eq(INSERT),
              eq(CLUSTER),
              anyMap(),
              eq(StatementType.UPDATE),
              any(),
              eq(prepared),
              isNull());
      verify(test.client, never()).executeNativeBatch(anyString(), anyList(), any(), any());
    }
  }

  @Test
  void nativeBatchCallTraversesSessionClientProxy() throws Exception {
    TestConnection test = testConnection(42250L);
    IDatabricksClient proxy = DatabricksMetricsTimedProcessor.createProxy(test.client);
    when(test.connection.getSession().getDatabricksClient()).thenReturn(proxy);
    when(test.client.supportsNativeBatchExecution()).thenReturn(true);
    when(test.client.executeNativeBatch(eq(INSERT), anyList(), any(), any()))
        .thenReturn(new long[] {Statement.SUCCESS_NO_INFO});
    DatabricksPreparedStatement prepared = new DatabricksPreparedStatement(test.connection, INSERT);
    prepared.setInt(1, 7);
    prepared.addBatch();

    assertArrayEquals(new long[] {Statement.SUCCESS_NO_INFO}, prepared.executeLargeBatch());
    verify(test.client).executeNativeBatch(eq(INSERT), anyList(), any(), eq(prepared));
  }

  @Test
  void statementBatchExecutesApcCommandsInOrderAndStopsOnFailure() throws Exception {
    TestConnection test = testConnection(42250L);
    DatabricksStatement statement = new DatabricksStatement(test.connection);
    statement.addBatch("INSERT INTO t VALUES (1)");
    statement.addBatch("INSERT INTO t VALUES (2)");
    statement.addBatch("INSERT INTO t VALUES (3)");
    DatabricksResultSet first = mock(DatabricksResultSet.class);
    DatabricksResultSet second = mock(DatabricksResultSet.class);
    DatabricksResultSet third = mock(DatabricksResultSet.class);
    when(first.getUpdateCount()).thenReturn(1L);
    when(second.getUpdateCount()).thenReturn(2L);
    when(third.getUpdateCount()).thenReturn(3L);
    when(test.client.executeStatement(
            anyString(),
            eq(CLUSTER),
            anyMap(),
            eq(StatementType.SQL),
            any(),
            eq(statement),
            isNull()))
        .thenReturn(first, second, third);
    assertArrayEquals(new long[] {1, 2, 3}, statement.executeLargeBatch());
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(test.client, times(3))
        .executeStatement(
            sql.capture(),
            eq(CLUSTER),
            anyMap(),
            eq(StatementType.SQL),
            any(),
            eq(statement),
            isNull());
    assertEquals(
        List.of("INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)", "INSERT INTO t VALUES (3)"),
        sql.getAllValues());

    clearInvocations(test.client);
    statement.addBatch("INSERT INTO t VALUES (1)");
    statement.addBatch("INSERT INTO t VALUES (2)");
    statement.addBatch("INSERT INTO t VALUES (3)");
    when(test.client.executeStatement(
            anyString(),
            eq(CLUSTER),
            anyMap(),
            eq(StatementType.SQL),
            any(),
            eq(statement),
            isNull()))
        .thenReturn(first)
        .thenThrow(new SQLException("second failed"));
    BatchUpdateException failure =
        assertThrows(BatchUpdateException.class, statement::executeLargeBatch);
    assertArrayEquals(new long[] {1}, failure.getLargeUpdateCounts());
    verify(test.client, times(2))
        .executeStatement(
            anyString(),
            eq(CLUSTER),
            anyMap(),
            eq(StatementType.SQL),
            any(),
            eq(statement),
            isNull());
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
