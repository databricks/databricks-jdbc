package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksSession;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

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
  void submitsEveryShowWithTypedHandleAndConvertsNativeResult(
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

    ResultSetMetaData resultMetadata = mock(ResultSetMetaData.class);
    when(completed.getMetaData()).thenReturn(resultMetadata);
    when(completed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.SUCCEEDED));
    when(completed.isThriftNativeMetadataResult()).thenReturn(true);
    when(completed.next()).thenReturn(false);
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(completed);
    DatabricksResultSet jdbc = metadata.getMetadataExecutionResult(handle, session);
    assertNotNull(jdbc);
    assertEquals(0, jdbc.getUpdateCount());
    assertFalse(jdbc.next());
  }

  @ParameterizedTest
  @MethodSource("showRequests")
  void nativeRowsForEveryShowUseTheJdbcMetadataLayout(
      MetadataAsyncRequest request, String ignoredSql) throws SQLException {
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(request, session);
    List<Object> nativeRow = rowFor(request.getMethod());
    DatabricksResultSet nativeResult = nativeRows(List.of(nativeRow));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(nativeResult);

    DatabricksResultSet jdbc = metadata.getMetadataExecutionResult(handle, session);
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
    when(completed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(completed);

    SQLException error =
        assertThrows(
            SQLException.class, () -> metadata.getMetadataExecutionResult(handle, session));
    assertEquals("42601", error.getSQLState());
    verify(client).closeStatement(handle.getStatementId());
    verify(completed).close();
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
  void sessionMetadataClientSubmitsAndCompletesThroughInterface() throws SQLException {
    DatabricksSdkClient sdk = mock(DatabricksSdkClient.class);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.SEA)
            .sessionId("session")
            .computeResource(cluster)
            .build();
    when(context.getClientType()).thenReturn(DatabricksClientType.SEA);
    when(context.getComputeResource()).thenReturn(cluster);
    when(sdk.getConnectionContext()).thenReturn(context);
    when(sdk.createSession(eq(cluster), isNull(), isNull(), anyMap())).thenReturn(info);
    DatabricksSession realSession = new DatabricksSession(context, sdk);
    realSession.open();
    DatabricksResultSet submitted = pendingWithId("submitted");
    when(sdk.executeStatementAsync(
            eq("SHOW CATALOGS"),
            eq(cluster),
            anyMap(),
            eq(realSession),
            isNull(),
            eq(StatementType.METADATA),
            eq(MetadataOperationType.GET_CATALOGS)))
        .thenReturn(submitted);
    DatabricksResultSet nativeResult = mock(DatabricksResultSet.class);
    ResultSetMetaData nativeMetadata = mock(ResultSetMetaData.class);
    when(nativeResult.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.SUCCEEDED));
    when(nativeResult.isThriftNativeMetadataResult()).thenReturn(true);
    when(nativeResult.getMetaData()).thenReturn(nativeMetadata);
    when(nativeMetadata.getColumnCount()).thenReturn(1);
    when(nativeResult.next()).thenReturn(false);
    when(sdk.getStatementResult(
            any(StatementId.class), eq(realSession), isNull(), eq(StatementType.METADATA)))
        .thenReturn(nativeResult);

    MetadataAsyncHandle handle =
        realSession
            .getDatabricksMetadataClient()
            .executeMetadataAsync(MetadataAsyncRequest.forCatalogs(), realSession);
    DatabricksResultSet jdbc =
        realSession.getDatabricksMetadataClient().getMetadataExecutionResult(handle, realSession);
    assertEquals("submitted", handle.getStatementId().toSQLExecStatementId());
    assertEquals("TABLE_CAT", jdbc.getMetaData().getColumnName(1));
    assertEquals(0, jdbc.getUpdateCount());
    assertFalse(jdbc.next());
  }

  @Test
  void propagatesFailedCompletionWithoutCatalogFanout() throws SQLException {
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forSchemas("main", "s%"), session);
    when(completed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(completed);
    DatabricksSQLException error =
        assertThrows(
            DatabricksSQLException.class,
            () -> metadata.getMetadataExecutionResult(handle, session));
    assertEquals("42601", error.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED.getCode(), error.getErrorCode());
    verify(client, times(1))
        .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
  }

  @Test
  void cancellationPropagatesWithoutCatalogFanout() throws SQLException {
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forSchemas(null, "s%"), session);
    when(completed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.CANCELED));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(completed);
    assertThrows(SQLException.class, () -> metadata.getMetadataExecutionResult(handle, session));
    verify(client, times(1))
        .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
  }

  @Test
  void metadataTimeoutCancelsActiveStatement() throws SQLException {
    when(context.getMetadataOperationTimeout()).thenReturn(1);
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forCatalogs(), session);
    when(completed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.PENDING));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(completed);
    DatabricksTimeoutException error =
        assertThrows(
            DatabricksTimeoutException.class,
            () -> metadata.getMetadataExecutionResult(handle, session));
    assertEquals(DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR.name(), error.getSQLState());
    assertEquals(DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR.getCode(), error.getErrorCode());
    verify(client).cancelStatement(handle.getStatementId());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"42000"})
  void nullCatalogNonSyntaxOrMissingStateDoesNotFanOut(String state) throws SQLException {
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(
            MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%"), session);
    when(completed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState(state));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(completed);
    assertThrows(SQLException.class, () -> metadata.getMetadataExecutionResult(handle, session));
    verify(client, times(1))
        .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void nullCatalogSyntaxErrorFansOutAsyncAndCombinesRows(boolean schemas) throws SQLException {
    MetadataAsyncRequest original =
        schemas
            ? MetadataAsyncRequest.forSchemas(null, "s%")
            : MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%");
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(original, session);
    DatabricksResultSet catalogPending = pendingWithId("catalogs");
    DatabricksResultSet firstPending = pendingWithId("first");
    DatabricksResultSet secondPending = pendingWithId("second");
    when(client.executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class)))
        .thenReturn(catalogPending, firstPending, secondPending);

    DatabricksResultSet failed = mock(DatabricksResultSet.class);
    when(failed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    DatabricksResultSet catalogRows = nativeRows(List.of(List.of("first"), List.of("second")));
    List<List<Object>> firstRows = List.of(metadataRow(schemas, "first"));
    List<List<Object>> secondRows = List.of(metadataRow(schemas, "second"));
    DatabricksResultSet firstResult = nativeRows(firstRows);
    DatabricksResultSet secondResult = nativeRows(secondRows);
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(failed, catalogRows, firstResult, secondResult);

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
    verify(client, times(4))
        .executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class));
    String expected =
        schemas
            ? "SHOW SCHEMAS IN `%s` LIKE 's*'"
            : "SHOW COLUMNS IN CATALOG `%s` SCHEMA LIKE 's*' TABLE LIKE 't*' LIKE 'c*'";
    for (String catalog : List.of("first", "second")) {
      verify(client)
          .executeStatementAsync(
              eq(String.format(expected, catalog)),
              eq(cluster),
              anyMap(),
              eq(session),
              isNull(),
              eq(StatementType.METADATA),
              eq(original.getOperation()));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void catalogSubmissionFailureSkipsCatalogAndContinues(boolean schemas) throws SQLException {
    MetadataAsyncRequest request =
        schemas
            ? MetadataAsyncRequest.forSchemas(null, "s%")
            : MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%");
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(request, session);
    DatabricksResultSet catalogPending = pendingWithId("catalogs");
    DatabricksResultSet secondPending = pendingWithId("second");
    when(client.executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class)))
        .thenReturn(catalogPending)
        .thenThrow(new SQLException("first catalog submit failed", "42000"))
        .thenReturn(secondPending);
    DatabricksResultSet failed = mock(DatabricksResultSet.class);
    when(failed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    DatabricksResultSet catalogRows = nativeRows(List.of(List.of("first"), List.of("second")));
    DatabricksResultSet secondRows = nativeRows(List.of(metadataRow(schemas, "second")));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(failed, catalogRows, secondRows);

    DatabricksResultSet result = metadata.getMetadataExecutionResult(handle, session);
    assertTrue(result.next());
    assertEquals("second", result.getString(schemas ? 2 : 1));
    assertFalse(result.next());
    assertEquals(2, handle.getFallbackStatementIds().size());
    verify(client, times(4))
        .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
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
    DatabricksResultSet catalogPending = pendingWithId("catalogs");
    when(client.executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class)))
        .thenReturn(catalogPending)
        .thenThrow(submitError);
    DatabricksResultSet failed = mock(DatabricksResultSet.class);
    when(failed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    DatabricksResultSet catalogRows = nativeRows(List.of(List.of("first")));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(failed, catalogRows);

    assertSame(
        submitError,
        assertThrows(
            SQLException.class, () -> metadata.getMetadataExecutionResult(handle, session)));
    assertEquals(1, handle.getFallbackStatementIds().size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void emptyCatalogFallbackReturnsEmptyJdbcMetadata(boolean schemas) throws SQLException {
    MetadataAsyncRequest request =
        schemas
            ? MetadataAsyncRequest.forSchemas(null, "s%")
            : MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%");
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(request, session);
    DatabricksResultSet catalogPending = pendingWithId("catalogs");
    when(client.executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class)))
        .thenReturn(catalogPending);
    DatabricksResultSet failed = mock(DatabricksResultSet.class);
    when(failed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    DatabricksResultSet emptyCatalogs = mock(DatabricksResultSet.class);
    ResultSetMetaData emptyMetadata = mock(ResultSetMetaData.class);
    when(emptyCatalogs.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.SUCCEEDED));
    when(emptyCatalogs.isThriftNativeMetadataResult()).thenReturn(true);
    when(emptyCatalogs.getMetaData()).thenReturn(emptyMetadata);
    when(emptyMetadata.getColumnCount()).thenReturn(1);
    when(emptyCatalogs.next()).thenReturn(false);
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(failed, emptyCatalogs);

    DatabricksResultSet result = metadata.getMetadataExecutionResult(handle, session);
    assertFalse(result.next());
    assertEquals(
        schemas
            ? MetadataResultConstants.SCHEMA_COLUMNS.size()
            : MetadataResultConstants.COLUMN_COLUMNS.size(),
        result.getMetaData().getColumnCount());
    assertEquals(1, handle.getFallbackStatementIds().size());
    verify(client, times(2))
        .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
  }

  @Test
  void fallbackCancellationPropagates() throws SQLException {
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(MetadataAsyncRequest.forSchemas(null, "s%"), session);
    DatabricksResultSet catalogPending = pendingWithId("catalogs");
    DatabricksResultSet childPending = pendingWithId("child");
    when(client.executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class)))
        .thenReturn(catalogPending, childPending);
    DatabricksResultSet failed = mock(DatabricksResultSet.class);
    when(failed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    DatabricksResultSet cancelled = mock(DatabricksResultSet.class);
    when(cancelled.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.CANCELED));
    DatabricksResultSet catalogRows = nativeRows(List.of(List.of("main")));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(failed, catalogRows, cancelled);
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
    MetadataAsyncRequest request =
        schemas
            ? MetadataAsyncRequest.forSchemas(null, "s%")
            : MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%");
    MetadataAsyncHandle handle = metadata.executeMetadataAsync(request, session);
    DatabricksResultSet catalogPending = pendingWithId("catalogs");
    DatabricksResultSet firstPending = pendingWithId("first");
    DatabricksResultSet secondPending = pendingWithId("second");
    when(client.executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class)))
        .thenReturn(catalogPending, firstPending, secondPending);
    DatabricksResultSet failed = mock(DatabricksResultSet.class);
    when(failed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    DatabricksResultSet catalogRows = nativeRows(List.of(List.of("first"), List.of("second")));
    DatabricksResultSet firstRows = nativeRows(List.of(metadataRow(schemas, "first")));
    DatabricksResultSet waiting = mock(DatabricksResultSet.class);
    when(waiting.getStatementStatus())
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return new StatementStatus().setState(StatementState.PENDING);
            });
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(failed, catalogRows, firstRows, waiting);

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
      verify(client, times(4))
          .executeStatementAsync(anyString(), any(), anyMap(), any(), any(), any(), any());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void fallbackTimeoutCancelsChildStatement() throws SQLException {
    when(context.getMetadataOperationTimeout()).thenReturn(1);
    MetadataAsyncHandle handle =
        metadata.executeMetadataAsync(
            MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%"), session);
    DatabricksResultSet catalogPending = pendingWithId("catalogs");
    DatabricksResultSet childPending = pendingWithId("child");
    when(client.executeStatementAsync(
            anyString(),
            eq(cluster),
            anyMap(),
            eq(session),
            isNull(),
            eq(StatementType.METADATA),
            any(MetadataOperationType.class)))
        .thenReturn(catalogPending, childPending);
    DatabricksResultSet failed = mock(DatabricksResultSet.class);
    when(failed.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.FAILED).setSqlState("42601"));
    DatabricksResultSet catalogRows = nativeRows(List.of(List.of("main")));
    DatabricksResultSet waiting = mock(DatabricksResultSet.class);
    when(waiting.getStatementStatus())
        .thenReturn(new StatementStatus().setState(StatementState.PENDING));
    when(client.getStatementResult(
            any(StatementId.class), eq(session), isNull(), eq(StatementType.METADATA)))
        .thenReturn(failed, catalogRows, waiting);
    DatabricksTimeoutException error =
        assertThrows(
            DatabricksTimeoutException.class,
            () -> metadata.getMetadataExecutionResult(handle, session));
    assertEquals(DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR.name(), error.getSQLState());
    assertEquals(DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR.getCode(), error.getErrorCode());
    verify(client).cancelStatement(handle.getFallbackStatementIds().get(1));
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
