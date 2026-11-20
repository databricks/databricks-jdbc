package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.MetadataOperationType;
import com.databricks.jdbc.common.MetadataResultConstants;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.MetadataAsyncHandle;
import com.databricks.jdbc.dbclient.MetadataAsyncRequest;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.OngoingStubbing;

class DatabricksMetadataAsyncTest {
  private final IDatabricksClient client = mock(IDatabricksClient.class);
  private final IDatabricksSession session = mock(IDatabricksSession.class);
  private final IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
  private final AllPurposeCluster cluster = new AllPurposeCluster("org", "cluster");
  private final DatabricksResultSet pending = mock(DatabricksResultSet.class);
  private final DatabricksResultSet completed = mock(DatabricksResultSet.class);
  private DatabricksMetadataQueryClient metadata;

  @BeforeEach
  void setUp() throws SQLException {
    when(client.getConnectionContext()).thenReturn(context);
    when(context.getEnableMultipleCatalogSupport()).thenReturn(true);
    when(session.getComputeResource()).thenReturn(cluster);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(cluster)
                .build());
    when(session.getCurrentCatalogAndSchema()).thenReturn(new String[] {"main", "s"});
    when(pending.getStatementId()).thenReturn("statement");
    when(client.executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class)))
        .thenReturn(pending);
    metadata = new DatabricksMetadataQueryClient(client);
  }

  private static Stream<Arguments> showRequests() {
    return Stream.of(
        Arguments.of(MetadataAsyncRequest.forCatalogs(), "SHOW CATALOGS"),
        Arguments.of(
            MetadataAsyncRequest.forSchemas("main", "s%"), "SHOW SCHEMAS IN `main` LIKE 's*'"),
        Arguments.of(
            MetadataAsyncRequest.forTables("main", "s%", "t%", new String[] {"TABLE"}),
            "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 's*' LIKE 't*'"),
        Arguments.of(
            MetadataAsyncRequest.forColumns("main", "s%", "t%", "c%"),
            "SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 's*' TABLE LIKE 't*' LIKE 'c*'"),
        Arguments.of(
            MetadataAsyncRequest.forFunctions("main", "s%", "f%"),
            "SHOW FUNCTIONS IN CATALOG `main` SCHEMA LIKE 's*' LIKE 'f*'"),
        Arguments.of(
            MetadataAsyncRequest.forPrimaryKeys("main", "s", "t"),
            "SHOW KEYS IN CATALOG `main` IN SCHEMA `s` IN TABLE `t`"),
        Arguments.of(
            MetadataAsyncRequest.forImportedKeys("main", "s", "t"),
            "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `s` IN TABLE `t`"),
        Arguments.of(
            MetadataAsyncRequest.forCrossReferences("main", "s", "p", "main", "s", "t"),
            "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `s` IN TABLE `t`"));
  }

  private static Stream<MetadataAsyncRequest> invalidEmptyCatalogKeyRequests() {
    return Stream.of(
        MetadataAsyncRequest.forPrimaryKeys("", "s", null),
        MetadataAsyncRequest.forImportedKeys("", "s", null),
        MetadataAsyncRequest.forCrossReferences("main", "s", "p", "", "s", ""),
        MetadataAsyncRequest.forCrossReferences("", "s", null, "main", "s", "t"));
  }

  @ParameterizedTest
  @MethodSource("invalidEmptyCatalogKeyRequests")
  void emptyCatalogKeyArgumentsFailLikeSynchronousMetadata(MetadataAsyncRequest request)
      throws SQLException {
    SQLException synchronous;
    switch (request.getMethod()) {
      case PRIMARY_KEYS:
        synchronous =
            assertThrows(
                SQLException.class, () -> metadata.listPrimaryKeys(session, "", "s", null));
        break;
      case IMPORTED_KEYS:
        synchronous =
            assertThrows(
                SQLException.class, () -> metadata.listImportedKeys(session, "", "s", null));
        break;
      case CROSS_REFERENCES:
        synchronous =
            assertThrows(
                SQLException.class,
                () ->
                    metadata.listCrossReferences(
                        session,
                        request.getParentCatalog(),
                        request.getParentSchema(),
                        request.getParentTable(),
                        request.getCatalog(),
                        request.getSchema(),
                        request.getTable()));
        break;
      default:
        throw new AssertionError(request.getMethod());
    }
    SQLException asynchronous =
        assertThrows(SQLException.class, () -> metadata.executeMetadataAsync(request, session));
    assertEquals(synchronous.getSQLState(), asynchronous.getSQLState());
    assertEquals(synchronous.getErrorCode(), asynchronous.getErrorCode());
    verify(client, never())
        .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
  }

  @ParameterizedTest
  @MethodSource("showRequests")
  void submitsEveryShowWithTypedHandleAndConvertsNativeRows(
      MetadataAsyncRequest request, String expectedSql) throws SQLException {
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(request, session);
    assertEquals(request.getOperation(), handle.getOperation());
    assertEquals(request.getMethod(), handle.getRequest().getMethod());
    assertEquals("statement", handle.getStatementId().toSQLExecStatementId());
    verify(client)
        .executeStatementAsync(
            eq(expectedSql),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            eq(request.getOperation()));

    List<Object> nativeRow = rowFor(request.getMethod());
    DatabricksResultSet nativeResult = nativeRows(List.of(nativeRow));
    pollReturns(nativeResult);
    DatabricksResultSet jdbc = metadata.getMetadataExecutionResult(handle, session);
    assertEquals(0, jdbc.getUpdateCount());
    assertEquals(nativeRow.size(), jdbc.getMetaData().getColumnCount());
    assertTrue(jdbc.next());
    assertEquals(nativeRow.get(0), jdbc.getObject(1));
    assertFalse(jdbc.next());
    verify(nativeResult).close();
    verify(client).closeStatement(handle.getStatementId());
  }

  @Test
  void warehouseAsyncCompletionDoesNotCloseServerOperation() throws SQLException {
    StatementId id =
        StatementId.forCurrentSession(
            "warehouse-operation",
            ImmutableSessionInfo.builder()
                .clientType(DatabricksClientType.SEA)
                .sessionId("session")
                .computeResource(new Warehouse("warehouse"))
                .build());
    MetadataAsyncHandle handle =
        new MetadataAsyncHandle(id, MetadataAsyncRequest.forCatalogs(), null, null);
    DatabricksResultSet source = nativeRows(List.of(List.of("main")));
    when(client.getStatementResult(eq(id), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(source);

    DatabricksResultSet result = metadata.getMetadataExecutionResult(handle, session);

    assertTrue(result.next());
    assertEquals("main", result.getString(1));
    verify(source).close();
    verify(client, never()).closeStatement(any(StatementId.class));
  }

  @Test
  void resolvedNullCatalogTableSyntaxErrorPropagatesWhenMultipleCatalogsDisabled()
      throws SQLException {
    when(context.getEnableMultipleCatalogSupport()).thenReturn(false);
    when(session.getCurrentCatalog()).thenReturn("main");
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(
            MetadataAsyncRequest.forTables(null, "s%", "t%", new String[] {"TABLE"}), session);
    assertEquals("main", handle.getResolvedCatalog());
    DatabricksResultSet failed = parseFailure();
    pollReturns(failed);

    SQLException error =
        assertThrows(
            SQLException.class, () -> metadata.getMetadataExecutionResult(handle, session));
    assertEquals("42601", error.getSQLState());
    verify(client).closeStatement(handle.getStatementId());
    verify(failed).close();
  }

  private static List<Object> rowFor(MetadataAsyncRequest.Method method) {
    switch (method) {
      case CATALOGS:
        return List.of("main");
      case SCHEMAS:
        return List.of("s", "main");
      case TABLES:
        return new ArrayList<>(
            Arrays.asList("main", "s", "t", "TABLE", null, null, null, null, null, null));
      case COLUMNS:
        return metadataRow(false, "main");
      case FUNCTIONS:
        return new ArrayList<>(Arrays.asList("main", "s", "f", null, 1, "f"));
      case PRIMARY_KEYS:
        return List.of("main", "s", "t", "id", 1, "pk");
      case IMPORTED_KEYS:
      case CROSS_REFERENCES:
        return new ArrayList<>(
            Arrays.asList("main", "s", "p", "id", "main", "s", "t", "pid", 1, 3, 3, "fk", "pk", 7));
      default:
        throw new AssertionError(method);
    }
  }

  @Test
  void rejectsProcedureSelectsBeforeSubmitting() throws SQLException {
    SQLFeatureNotSupportedException error =
        assertThrows(
            SQLFeatureNotSupportedException.class,
            () ->
                metadata.executeMetadataAsync(
                    MetadataAsyncRequest.forOperation(MetadataOperationType.GET_PROCEDURES),
                    session));
    assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(), error.getSQLState());
    assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.getCode(), error.getErrorCode());
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () ->
            metadata.executeMetadataAsync(
                MetadataAsyncRequest.forOperation(MetadataOperationType.GET_PROCEDURE_COLUMNS),
                session));
    verify(client, never())
        .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
  }

  @Test
  void pollsMetadataCompletionAtConfiguredAsyncInterval() throws SQLException {
    when(context.getAsyncExecPollInterval()).thenReturn(200);
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forCatalogs(), session);
    DatabricksResultSet stillRunning = mock(DatabricksResultSet.class);
    when(stillRunning.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.RUNNING));
    DatabricksResultSet finished = nativeRows(List.of(List.of("main")));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(stillRunning, finished);

    long start = System.nanoTime();
    metadata.getMetadataExecutionResult(handle, session);
    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

    // Only a lower bound is asserted: the default 50ms poll would finish well under this.
    assertTrue(elapsedMillis >= 150, "polled faster than configured interval: " + elapsedMillis);
    verify(client, times(2))
        .getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA));
  }

  @Test
  void crossReferencesResolveNullCatalogAndSchemaLikeSynchronousWhenMultipleCatalogsDisabled()
      throws SQLException {
    when(context.getEnableMultipleCatalogSupport()).thenReturn(false);
    when(session.getCurrentCatalog()).thenReturn("main");
    when(client.executeStatement(
            anyString(), any(), anyMap(), any(), any(), any(), any(MetadataOperationType.class)))
        .thenThrow(new DatabricksSQLException("syntax", "42601"));

    metadata.listCrossReferences(session, null, null, "p", null, null, "t");
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(
            MetadataAsyncRequest.forCrossReferences(null, null, "p", null, null, "t"), session);

    org.mockito.ArgumentCaptor<String> syncSql = org.mockito.ArgumentCaptor.forClass(String.class);
    verify(client)
        .executeStatement(
            syncSql.capture(),
            any(),
            anyMap(),
            any(),
            any(),
            any(),
            any(MetadataOperationType.class));
    org.mockito.ArgumentCaptor<String> asyncSql = org.mockito.ArgumentCaptor.forClass(String.class);
    verify(client)
        .executeStatementAsync(
            asyncSql.capture(),
            any(),
            anyMap(),
            any(),
            any(),
            any(),
            any(MetadataOperationType.class));
    assertEquals(
        "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `s` IN TABLE `t`", syncSql.getValue());
    assertEquals(syncSql.getValue(), asyncSql.getValue());
    assertEquals("main", handle.getResolvedCatalog());
  }

  @Test
  void metadataTimeoutCancelsActiveStatement() throws SQLException {
    when(context.getMetadataOperationTimeout()).thenReturn(1);
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forCatalogs(), session);
    pollReturns(waiting());
    assertTimeout(() -> metadata.getMetadataExecutionResult(handle, session));
    verify(client).cancelStatement(handle.getStatementId());
  }

  @ParameterizedTest
  @CsvSource({"FAILED,42000", "FAILED,", "CANCELED,"})
  void nullCatalogNonSyntaxFailureOrCancellationDoesNotFanOut(StatementState state, String sqlState)
      throws SQLException {
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(
            MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%"), session);
    pollReturns(withState(state, sqlState));
    assertThrows(SQLException.class, () -> metadata.getMetadataExecutionResult(handle, session));
    verifySubmissions(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void nullCatalogSyntaxErrorFansOutAsyncAndCombinesRows(boolean schemas) throws SQLException {
    MetadataAsyncRequest original = fanOutRequest(schemas);
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(original, session);
    submitReturns(pendingWithId("catalogs"), pendingWithId("first"), pendingWithId("second"));
    pollReturns(
        parseFailure(),
        nativeRows(List.of(List.of("first"), List.of("second"))),
        nativeRows(List.of(metadataRow(schemas, "first"))),
        nativeRows(List.of(metadataRow(schemas, "second"))));

    DatabricksResultSet result = metadata.getMetadataExecutionResult(handle, session);
    List<String> catalogs = new ArrayList<>();
    int catalogColumn = schemas ? 2 : 1;
    while (result.next()) {
      catalogs.add(result.getString(catalogColumn));
    }
    assertEquals(List.of("first", "second"), catalogs);
    assertEquals(3, handle.getFallbackStatementIds().size());
    verify(client).closeStatement(handle.getStatementId());
    for (StatementId id : handle.getFallbackStatementIds()) {
      verify(client).closeStatement(id);
    }
    verifySubmissions(4);
  }

  private static Stream<Arguments> skippedCatalogSubmitErrors() {
    SQLException generic = new SQLException("first catalog submit failed", "42000");
    SQLException notFound =
        new DatabricksSQLException(
            "[SCHEMA_NOT_FOUND] The schema cannot be found. SQLSTATE: 42704", "42704");
    return Stream.of(
        Arguments.of(true, generic), Arguments.of(false, generic), Arguments.of(true, notFound));
  }

  @ParameterizedTest
  @MethodSource("skippedCatalogSubmitErrors")
  void catalogSubmissionFailureSkipsCatalogWithoutPollingItAndContinues(
      boolean schemas, SQLException submitError) throws SQLException {
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(fanOutRequest(schemas), session);
    submitReturns(pendingWithId("catalogs"), submitError, pendingWithId("second"));
    pollReturns(
        parseFailure(),
        nativeRows(List.of(List.of("first"), List.of("second"))),
        nativeRows(List.of(metadataRow(schemas, "second"))));

    DatabricksResultSet result = metadata.getMetadataExecutionResult(handle, session);
    assertTrue(result.next());
    assertEquals("second", result.getString(schemas ? 2 : 1));
    assertFalse(result.next());
    assertEquals(2, handle.getFallbackStatementIds().size());
    verifySubmissions(4);
    verify(client, times(3))
        .getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void catalogSubmissionCancellationOrTimeoutPropagates(boolean timeout) throws SQLException {
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forSchemas(null, "s%"), session);
    SQLException submitError =
        timeout
            ? new DatabricksTimeoutException(
                "metadata timeout", null, DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR)
            : new SQLException(
                "metadata cancelled",
                com.databricks.jdbc.common.DatabricksJdbcConstants.OPERATION_CANCELLED_SQLSTATE);
    submitReturns(pendingWithId("catalogs"), submitError);
    pollReturns(parseFailure(), nativeRows(List.of(List.of("first"))));

    assertSame(
        submitError,
        assertThrows(
            SQLException.class, () -> metadata.getMetadataExecutionResult(handle, session)));
    assertEquals(1, handle.getFallbackStatementIds().size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void emptyCatalogFallbackReturnsEmptyJdbcMetadata(boolean schemas) throws SQLException {
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(fanOutRequest(schemas), session);
    submitReturns(pendingWithId("catalogs"));
    DatabricksResultSet emptyCatalogs = mock(DatabricksResultSet.class);
    ResultSetMetaData emptyMetadata = mock(ResultSetMetaData.class);
    when(emptyCatalogs.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.SUCCEEDED));
    when(emptyCatalogs.isThriftNativeMetadataResult()).thenReturn(true);
    when(emptyCatalogs.getMetaData()).thenReturn(emptyMetadata);
    when(emptyMetadata.getColumnCount()).thenReturn(1);
    when(emptyCatalogs.next()).thenReturn(false);
    pollReturns(parseFailure(), emptyCatalogs);

    DatabricksResultSet result = metadata.getMetadataExecutionResult(handle, session);
    assertFalse(result.next());
    assertEquals(
        schemas
            ? MetadataResultConstants.SCHEMA_COLUMNS.size()
            : MetadataResultConstants.COLUMN_COLUMNS.size(),
        result.getMetaData().getColumnCount());
    assertEquals(1, handle.getFallbackStatementIds().size());
    verifySubmissions(2);
  }

  @Test
  void fallbackCancellationPropagates() throws SQLException {
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forSchemas(null, "s%"), session);
    submitReturns(pendingWithId("catalogs"), pendingWithId("child"));
    pollReturns(
        parseFailure(),
        nativeRows(List.of(List.of("main"))),
        withState(StatementState.CANCELED, null));
    SQLException error =
        assertThrows(
            SQLException.class, () -> metadata.getMetadataExecutionResult(handle, session));
    assertEquals(
        com.databricks.jdbc.common.DatabricksJdbcConstants.OPERATION_CANCELLED_SQLSTATE,
        error.getSQLState());
    assertEquals(2, handle.getFallbackStatementIds().size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void interruptedFallbackPollingPropagatesInsteadOfReturningPartialRows(boolean schemas)
      throws SQLException {
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(fanOutRequest(schemas), session);
    submitReturns(pendingWithId("catalogs"), pendingWithId("first"), pendingWithId("second"));
    DatabricksResultSet interrupting = mock(DatabricksResultSet.class);
    when(interrupting.getStatementStatus())
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return new StatementStatus().setState(StatementState.PENDING);
            });
    pollReturns(
        parseFailure(),
        nativeRows(List.of(List.of("first"), List.of("second"))),
        nativeRows(List.of(metadataRow(schemas, "first"))),
        interrupting);

    try {
      DatabricksSQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () -> metadata.getMetadataExecutionResult(handle, session));
      assertEquals(DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED.name(), error.getSQLState());
      assertEquals(
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED.getCode(), error.getErrorCode());
      assertInstanceOf(InterruptedException.class, error.getCause());
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(3, handle.getFallbackStatementIds().size());
      verifySubmissions(4);
    } finally {
      Thread.interrupted();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fanOutTimeoutCancelsOnlyTheActiveChildStatement(boolean parseErrorAtSubmission)
      throws SQLException {
    when(context.getMetadataOperationTimeout()).thenReturn(1);
    MetadataAsyncRequest request = MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%");
    DatabricksResultSet catalogRows = nativeRows(List.of(List.of("main")));
    StatementId child;
    if (parseErrorAtSubmission) {
      submitReturns(
          new DatabricksSQLException("syntax error at or near \"ALL\"", "42601"),
          pendingWithId("catalogs"),
          pendingWithId("child"));
      pollReturns(catalogRows, waiting());
      assertTimeout(() -> metadata.executeMetadataAsync(request, session));
      child = StatementId.forCurrentSession("child", session.getSessionInfo());
    } else {
      MetadataAsyncHandle handle = metadata.executeMetadataAsync(request, session);
      submitReturns(pendingWithId("catalogs"), pendingWithId("child"));
      pollReturns(parseFailure(), catalogRows, waiting());
      assertTimeout(() -> metadata.getMetadataExecutionResult(handle, session));
      child = handle.getFallbackStatementIds().get(1);
    }
    verify(client).cancelStatement(child);
    verify(client, times(1)).cancelStatement(any(StatementId.class));
  }

  @Test
  void submissionParseErrorFansOutAndClosesFanOutOperations() throws SQLException {
    submitReturns(
        new DatabricksSQLException("syntax error at or near \"ALL\"", "42601"),
        pendingWithId("catalogs"),
        pendingWithId("first"));
    pollReturns(
        nativeRows(List.of(List.of("first"))), nativeRows(List.of(metadataRow(true, "first"))));

    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forSchemas(null, "s%"), session);
    assertNull(handle.getStatementId());
    DatabricksResultSet result = metadata.getMetadataExecutionResult(handle, session);
    assertTrue(result.next());
    assertEquals("first", result.getString(2));
    assertFalse(result.next());
    verify(client)
        .closeStatement(StatementId.forCurrentSession("catalogs", session.getSessionInfo()));
    verify(client).closeStatement(StatementId.forCurrentSession("first", session.getSessionInfo()));
  }

  @Test
  void submissionObjectNotFoundReturnsEmptyResult() throws SQLException {
    submitReturns(
        new DatabricksSQLException(
            "[SCHEMA_NOT_FOUND] The schema cannot be found. SQLSTATE: 42704", "42704"));
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forSchemas("main", "s%"), session);
    assertFalse(metadata.getMetadataExecutionResult(handle, session).next());
  }

  private static MetadataAsyncRequest fanOutRequest(boolean schemas) {
    return schemas
        ? MetadataAsyncRequest.forSchemas(null, "s%")
        : MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%");
  }

  private static void assertTimeout(Executable action) {
    DatabricksTimeoutException error = assertThrows(DatabricksTimeoutException.class, action);
    assertEquals(DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR.name(), error.getSQLState());
    assertEquals(DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR.getCode(), error.getErrorCode());
  }

  /** Each outcome is either a result set or a Throwable thrown by that submission. */
  private void submitReturns(Object... outcomes) throws SQLException {
    OngoingStubbing<DatabricksResultSet> stub =
        when(
            client.executeStatementAsync(
                anyString(),
                eq(cluster),
                anyMap(),
                eq(session),
                isNull(),
                eq(StatementType.METADATA),
                any(MetadataOperationType.class)));
    for (Object outcome : outcomes) {
      stub =
          outcome instanceof Throwable
              ? stub.thenThrow((Throwable) outcome)
              : stub.thenReturn((DatabricksResultSet) outcome);
    }
  }

  private void pollReturns(DatabricksResultSet first, DatabricksResultSet... rest)
      throws SQLException {
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(first, rest);
  }

  private void verifySubmissions(int count) throws SQLException {
    verify(client, times(count))
        .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
  }

  private static DatabricksResultSet withState(StatementState state, String sqlState) {
    DatabricksResultSet result = mock(DatabricksResultSet.class);
    when(result.getStatementStatus())
        .thenReturn(new StatementStatus().setState(state).setSqlState(sqlState));
    return result;
  }

  private static DatabricksResultSet parseFailure() {
    return withState(StatementState.FAILED, "42601");
  }

  private static DatabricksResultSet waiting() {
    return withState(StatementState.PENDING, null);
  }

  private DatabricksResultSet pendingWithId(String id) {
    DatabricksResultSet result = mock(DatabricksResultSet.class);
    when(result.getStatementId()).thenReturn(id);
    return result;
  }

  private static List<Object> metadataRow(boolean schemas, String catalog) {
    if (schemas) {
      return List.of("s_name", catalog);
    }
    List<Object> row =
        new ArrayList<>(Arrays.asList(new Object[MetadataResultConstants.COLUMN_COLUMNS.size()]));
    row.set(0, catalog);
    row.set(1, "s_name");
    row.set(2, "t_name");
    row.set(3, "c_name");
    row.set(4, java.sql.Types.VARCHAR);
    row.set(5, "STRING");
    row.set(10, 1);
    row.set(16, 0);
    return row;
  }

  private static DatabricksResultSet nativeRows(List<List<Object>> rows) throws SQLException {
    DatabricksResultSet result = mock(DatabricksResultSet.class);
    ResultSetMetaData metadata = mock(ResultSetMetaData.class);
    when(result.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.SUCCEEDED));
    when(result.isThriftNativeMetadataResult()).thenReturn(true);
    when(result.getMetaData()).thenReturn(metadata);
    when(metadata.getColumnCount()).thenReturn(rows.get(0).size());
    final int[] current = {-1};
    when(result.next())
        .thenAnswer(
            invocation -> {
              current[0]++;
              return current[0] < rows.size();
            });
    when(result.getObject(anyInt()))
        .thenAnswer(
            invocation -> rows.get(current[0]).get((Integer) invocation.getArgument(0) - 1));
    return result;
  }
}
