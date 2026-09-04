package com.databricks.jdbc.telemetry.latency;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.StatementTelemetryDetails;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import com.databricks.jdbc.model.telemetry.latency.OperationType;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;

public class TelemetryCollectorTest {
  private static final String TEST_STATEMENT_ID = "test-statement-id";
  private final IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
  private final TelemetryCollector handler = new TelemetryCollector(mockContext);

  @BeforeEach
  void setUp() {
    DatabricksThreadContextHolder.setStatementId(TEST_STATEMENT_ID);
    when(mockContext.getTelemetryLogLevel()).thenReturn(TelemetryLogLevel.OFF);
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
                  eq(mockContext),
                  any(StatementTelemetryDetails.class),
                  any(TelemetryLogLevel.class)));
    }
  }

  @Test
  void testCancelExportsAccumulatedPollingDetailsAndClearsTracker() {
    handler.recordGetOperationStatus(TEST_STATEMENT_ID, 1000L);
    handler.recordGetOperationStatus(TEST_STATEMENT_ID, 250L);
    StatementTelemetryDetails pendingDetails =
        handler.getOrCreateTelemetryDetails(TEST_STATEMENT_ID);
    DatabricksThreadContextHolder.setStatementId("different-statement-id");

    handler.recordOperationLatency(TEST_STATEMENT_ID, 100L, "cancelStatement");

    JsonNode operationDetail = new ObjectMapper().valueToTree(pendingDetails.getOperationDetail());
    assertEquals(2L, operationDetail.get("n_operation_status_calls").asLong());
    assertEquals(1250L, operationDetail.get("operation_status_latency_millis").asLong());
    assertEquals("CANCEL_STATEMENT", operationDetail.get("operation_type").asText());
    assertEquals(100L, pendingDetails.getOperationLatencyMillis());
    assertFalse(handler.isTelemetryCollected(TEST_STATEMENT_ID));
  }

  @Test
  void testCollectorStoresConnectionContext() {
    assertSame(mockContext, handler.getConnectionContext());
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
