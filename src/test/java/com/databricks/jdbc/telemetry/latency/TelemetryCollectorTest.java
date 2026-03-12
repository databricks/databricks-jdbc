package com.databricks.jdbc.telemetry.latency;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.StatementTelemetryDetails;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import com.databricks.jdbc.model.telemetry.latency.OperationType;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;

public class TelemetryCollectorTest {
  private final TelemetryCollector handler = new TelemetryCollector();
  private static final String TEST_STATEMENT_ID = "test-statement-id";

  @BeforeEach
  void setUp() {
    DatabricksThreadContextHolder.setStatementId(TEST_STATEMENT_ID);
  }

  @AfterEach
  void tearDown() {
    handler.exportAllPendingTelemetryDetails();
    DatabricksThreadContextHolder.setStatementId((String) null);
  }

  @Test
  void testRecordChunkDownloadLatency_CreatesAndUpdatesDetails() {
    String statementId = TEST_STATEMENT_ID;
    handler.recordChunkDownloadLatency(statementId, 0, 100);
    handler.recordChunkDownloadLatency(statementId, 1, 200);
    ChunkDetails details = handler.getOrCreateTelemetryDetails(statementId).getChunkDetails();
    assertNotNull(details);
    assertEquals(100L, details.getInitialChunkLatencyMillis());
    assertEquals(200L, details.getSlowestChunkLatencyMillis());
    assertEquals(300L, details.getSumChunksDownloadTimeMillis());
  }

  @Test
  void testRecordChunkDownloadLatency_WithNullStatementId_DoesNothing() {
    handler.recordChunkDownloadLatency(null, 0, 100);
    assertNull(handler.getOrCreateTelemetryDetails(null));
  }

  @ParameterizedTest
  @CsvSource({"idA,0", "idB,1", "idC,2"})
  void testRecordChunkIteration_Accumulates(String statementId, long chunkIndex) {
    handler.recordChunkDownloadLatency(statementId, 0, 50); // ensure entry exists
    handler.recordChunkIteration(statementId, chunkIndex);
    assertEquals(
        1L,
        handler
            .getOrCreateTelemetryDetails(statementId)
            .getChunkDetails()
            .getTotalChunksIterated());
    handler.recordChunkIteration(statementId, chunkIndex + 1);
    assertEquals(
        2L,
        handler
            .getOrCreateTelemetryDetails(statementId)
            .getChunkDetails()
            .getTotalChunksIterated());
  }

  @Test
  void testRecordOperationLatency_WithCloseOperation() {
    String methodName = "closeStatement";
    long latency = 100L;

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      handler.recordOperationLatency(latency, methodName);

      mockedStatic.verify(
          () ->
              TelemetryHelper.exportTelemetryLog(
                  any(StatementTelemetryDetails.class), any(TelemetryLogLevel.class)));
    }
  }

  @ParameterizedTest
  @EnumSource(OperationType.class)
  void testIsCloseOperation(OperationType operationType) {
    boolean expected =
        operationType == OperationType.CLOSE_STATEMENT
            || operationType == OperationType.CANCEL_STATEMENT
            || operationType == OperationType.DELETE_SESSION;
    assertEquals(expected, handler.isCloseOperation(operationType));
  }

  @Test
  void testReturnsNullIfStatementIdIsNull() {
    assertNull(handler.getOrCreateTelemetryDetails(null));
  }

  @Test
  void testCreatesNewTelemetryDetailsIfAbsent() {
    String statementId = TEST_STATEMENT_ID;
    StatementTelemetryDetails details = handler.getOrCreateTelemetryDetails(statementId);

    assertNotNull(details);
    assertEquals(statementId, details.getStatementId());
    assertSame(details, handler.getOrCreateTelemetryDetails(statementId));
  }

  @Test
  void testReturnsExistingTelemetryDetailsIfPresent() {
    String statementId = TEST_STATEMENT_ID;
    handler.recordGetOperationStatus(statementId, 1000L);
    assertNotNull(handler.getOrCreateTelemetryDetails(statementId));
    StatementTelemetryDetails existing = handler.getOrCreateTelemetryDetails(statementId);
    StatementTelemetryDetails result = handler.getOrCreateTelemetryDetails(statementId);
    assertSame(existing, result);
    assertEquals(statementId, result.getStatementId());
  }
}
