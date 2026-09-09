package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksBatchUpdateException;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PreparedStatementBatchExecutorTest {

  private static final String INSERT_SQL = "INSERT INTO target (id, name) VALUES (?, ?)";
  private static final String UPDATE_SQL = "UPDATE target SET name = ? WHERE id = ?";

  @Mock private DatabricksConnection connection;
  @Mock private IDatabricksConnectionContext connectionContext;
  @Mock private PreparedStatementBatchExecutor.StatementExecutor statementExecutor;
  @Mock private PreparedStatementBatchExecutor.NativeBatchExecutor nativeBatchExecutor;
  @Mock private DatabricksResultSet firstResultSet;
  @Mock private DatabricksResultSet secondResultSet;

  @Test
  void emptyBatchDoesNotExecuteStatements() throws Exception {
    PreparedStatementBatchExecutor executor = newExecutor(INSERT_SQL, false);

    assertArrayEquals(new long[0], executor.executeBatch(List.of()));
    verifyNoInteractions(connection, statementExecutor);
  }

  @Test
  void disabledBatchedInsertsExecuteEachParameterSetIndividually() throws Exception {
    setBatchedInsertsEnabled(false);
    List<BatchParameterSet> batch = createBatch(2);
    when(statementExecutor.execute(eq(INSERT_SQL), anyMap(), eq(StatementType.UPDATE), eq(false)))
        .thenReturn(firstResultSet, secondResultSet);
    when(firstResultSet.getUpdateCount()).thenReturn(3L);
    when(secondResultSet.getUpdateCount()).thenReturn(5L);

    long[] counts = newExecutor(INSERT_SQL, false, nativeBatchExecutor).executeBatch(batch);

    assertArrayEquals(new long[] {3, 5}, counts);
    verify(statementExecutor)
        .execute(INSERT_SQL, batch.get(0).getParameterBindings(), StatementType.UPDATE, false);
    verify(statementExecutor)
        .execute(INSERT_SQL, batch.get(1).getParameterBindings(), StatementType.UPDATE, false);
    verify(nativeBatchExecutor, never()).isSupported();
  }

  @Test
  void ineligibleSqlFallsBackToIndividualExecution() throws Exception {
    setBatchedInsertsEnabled(true);
    when(connectionContext.isNativeBatchingEnabled()).thenReturn(true);
    List<BatchParameterSet> batch = createBatch(1);
    when(statementExecutor.execute(eq(UPDATE_SQL), anyMap(), eq(StatementType.UPDATE), eq(false)))
        .thenReturn(firstResultSet);
    when(firstResultSet.getUpdateCount()).thenReturn(7L);

    long[] counts = newExecutor(UPDATE_SQL, false, nativeBatchExecutor).executeBatch(batch);

    assertArrayEquals(new long[] {7}, counts);
    verify(statementExecutor)
        .execute(UPDATE_SQL, batch.get(0).getParameterBindings(), StatementType.UPDATE, false);
    verify(nativeBatchExecutor, never()).isSupported();
  }

  @Test
  void nativeBatchingHandsOrderedParameterSetsToNativeExecutor() throws Exception {
    when(connection.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isNativeBatchingEnabled()).thenReturn(true);
    when(nativeBatchExecutor.isSupported()).thenReturn(true);
    List<BatchParameterSet> batch = createBatch(2);
    when(nativeBatchExecutor.execute(INSERT_SQL, batch)).thenReturn(new long[] {2, 3});

    long[] counts = newExecutor(INSERT_SQL, false, nativeBatchExecutor).executeBatch(batch);

    assertArrayEquals(new long[] {2, 3}, counts);
    assertEquals(List.of(1, 2), indexes(batch.get(0)));
    assertEquals(List.of(1, 2), indexes(batch.get(1)));
    verify(nativeBatchExecutor).execute(INSERT_SQL, batch);
    verifyNoInteractions(statementExecutor);
  }

  @Test
  void unsupportedNativeExecutorFallsBackToLegacyExecution() throws Exception {
    setBatchedInsertsEnabled(false);
    when(connectionContext.isNativeBatchingEnabled()).thenReturn(true);
    when(nativeBatchExecutor.isSupported()).thenReturn(false);
    List<BatchParameterSet> batch = createBatch(1);
    when(statementExecutor.execute(eq(INSERT_SQL), anyMap(), eq(StatementType.UPDATE), eq(false)))
        .thenReturn(firstResultSet);
    when(firstResultSet.getUpdateCount()).thenReturn(6L);

    long[] counts = newExecutor(INSERT_SQL, false, nativeBatchExecutor).executeBatch(batch);

    assertArrayEquals(new long[] {6}, counts);
    verify(nativeBatchExecutor, never()).execute(anyString(), eq(batch));
  }

  @Test
  void eligibleInsertIsRewrittenWithFlattenedParameters() throws Exception {
    setBatchedInsertsEnabled(true);
    List<BatchParameterSet> batch = createBatch(2);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<Integer, ImmutableSqlParameter>> parametersCaptor =
        ArgumentCaptor.forClass(Map.class);
    when(statementExecutor.execute(
            sqlCaptor.capture(), parametersCaptor.capture(), eq(StatementType.UPDATE), eq(false)))
        .thenReturn(firstResultSet);

    long[] counts = newExecutor(INSERT_SQL, false).executeBatch(batch);

    assertArrayEquals(new long[] {1, 1}, counts);
    assertEquals("INSERT INTO target (`id`, `name`) VALUES (?, ?), (?, ?)", sqlCaptor.getValue());
    assertEquals(4, parametersCaptor.getValue().size());
    assertEquals(1, parametersCaptor.getValue().get(1).cardinal());
    assertEquals(2, parametersCaptor.getValue().get(2).cardinal());
    assertEquals(1, parametersCaptor.getValue().get(3).cardinal());
    assertEquals(2, parametersCaptor.getValue().get(4).cardinal());
  }

  @Test
  void parameterizedRewriteUsesTheExisting256ParameterChunkLimit() throws Exception {
    setBatchedInsertsEnabled(true);
    when(statementExecutor.execute(anyString(), anyMap(), eq(StatementType.UPDATE), eq(false)))
        .thenReturn(firstResultSet);

    long[] counts = newExecutor(INSERT_SQL, false).executeBatch(createBatch(129));

    assertEquals(129, counts.length);
    verify(statementExecutor)
        .execute(eq(multiRowInsert(128)), anyMap(), eq(StatementType.UPDATE), eq(false));
    verify(statementExecutor)
        .execute(eq(multiRowInsert(1)), anyMap(), eq(StatementType.UPDATE), eq(false));
  }

  @Test
  void interpolatedRewriteUsesConfiguredBatchInsertSize() throws Exception {
    setBatchedInsertsEnabled(true);
    when(connectionContext.getBatchInsertSize()).thenReturn(2);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<Integer, ImmutableSqlParameter>> parametersCaptor =
        ArgumentCaptor.forClass(Map.class);
    when(statementExecutor.execute(
            sqlCaptor.capture(), parametersCaptor.capture(), eq(StatementType.UPDATE), eq(false)))
        .thenReturn(firstResultSet);

    long[] counts = newExecutor(INSERT_SQL, true).executeBatch(createBatch(3));

    assertArrayEquals(new long[] {1, 1, 1}, counts);
    assertEquals(2, sqlCaptor.getAllValues().size());
    assertEquals(
        "INSERT INTO target (`id`, `name`) VALUES (1, 'name-1'), (2, 'name-2')",
        sqlCaptor.getAllValues().get(0));
    assertEquals(
        "INSERT INTO target (`id`, `name`) VALUES (3, 'name-3')", sqlCaptor.getAllValues().get(1));
    assertTrue(parametersCaptor.getAllValues().stream().allMatch(Map::isEmpty));
  }

  @Test
  void rewrittenBatchFailureMarksEveryParameterSetFailed() throws Exception {
    setBatchedInsertsEnabled(true);
    when(statementExecutor.execute(anyString(), anyMap(), eq(StatementType.UPDATE), eq(false)))
        .thenThrow(new SQLException("rewrite failed"));

    DatabricksBatchUpdateException exception =
        assertThrows(
            DatabricksBatchUpdateException.class,
            () -> newExecutor(INSERT_SQL, false).executeBatch(createBatch(3)));

    assertArrayEquals(
        new long[] {Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
        exception.getLargeUpdateCounts());
  }

  @Test
  void individualFailurePreservesEarlierCountAndMarksRemainingSetsFailed() throws Exception {
    setBatchedInsertsEnabled(false);
    when(statementExecutor.execute(eq(INSERT_SQL), anyMap(), eq(StatementType.UPDATE), eq(false)))
        .thenReturn(firstResultSet)
        .thenThrow(new SQLException("individual failed"));
    when(firstResultSet.getUpdateCount()).thenReturn(4L);

    DatabricksBatchUpdateException exception =
        assertThrows(
            DatabricksBatchUpdateException.class,
            () -> newExecutor(INSERT_SQL, false).executeBatch(createBatch(3)));

    assertArrayEquals(
        new long[] {4, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
        exception.getLargeUpdateCounts());
  }

  private PreparedStatementBatchExecutor newExecutor(String sql, boolean interpolateParameters) {
    return new PreparedStatementBatchExecutor(
        sql, connection, interpolateParameters, statementExecutor);
  }

  private PreparedStatementBatchExecutor newExecutor(
      String sql,
      boolean interpolateParameters,
      PreparedStatementBatchExecutor.NativeBatchExecutor nativeExecutor) {
    return new PreparedStatementBatchExecutor(
        sql, connection, interpolateParameters, statementExecutor, nativeExecutor);
  }

  private void setBatchedInsertsEnabled(boolean enabled) {
    when(connection.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isBatchedInsertsEnabled()).thenReturn(enabled);
  }

  private List<BatchParameterSet> createBatch(int rowCount) {
    List<BatchParameterSet> batch = new ArrayList<>();
    for (int row = 1; row <= rowCount; row++) {
      DatabricksParameterMetaData parameterMetaData = new DatabricksParameterMetaData(INSERT_SQL);
      parameterMetaData.put(1, parameter(1, row, ColumnInfoTypeName.INT));
      parameterMetaData.put(2, parameter(2, "name-" + row, ColumnInfoTypeName.STRING));
      batch.add(BatchParameterSet.from(parameterMetaData.getParameterBindings()));
    }
    return batch;
  }

  private ImmutableSqlParameter parameter(
      int cardinal, Object value, ColumnInfoTypeName columnInfoTypeName) {
    return ImmutableSqlParameter.builder()
        .cardinal(cardinal)
        .value(value)
        .type(columnInfoTypeName)
        .build();
  }

  private String multiRowInsert(int rows) {
    return "INSERT INTO target (`id`, `name`) VALUES "
        + String.join(", ", java.util.Collections.nCopies(rows, "(?, ?)"));
  }

  private List<Integer> indexes(BatchParameterSet parameterSet) {
    return parameterSet.getParameters().stream()
        .map(ImmutableSqlParameter::cardinal)
        .collect(java.util.stream.Collectors.toList());
  }
}
