package com.databricks.jdbc.telemetry.latency;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.model.telemetry.StatementTelemetryDetails;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class TelemetryCollectorTest {
  private final TelemetryCollector handler = TelemetryCollector.getInstance();

  @AfterEach
  public void tearDown() throws Exception {
    handler.exportAllPendingTelemetryDetails();
  }

  @Test
  void testRecordChunkDownloadLatency_CreatesAndUpdatesDetails() {
    String statementId = "test-statement-id";
    handler.recordChunkDownloadLatency(statementId, 0, 100);
    handler.recordChunkDownloadLatency(statementId, 1, 200);
    ChunkDetails details = handler.getTelemetryDetails(statementId).getChunkDetails();
    assertNotNull(details);
    assertEquals(100L, details.getInitialChunkLatencyMillis());
    assertEquals(200L, details.getSlowestChunkLatencyMillis());
    assertEquals(300L, details.getSumChunksDownloadTimeMillis());
  }

  @ParameterizedTest
  @CsvSource({"idA,0", "idB,1", "idC,2"})
  void testRecordChunkIteration_Accumulates(String statementId, long chunkIndex) {
    handler.recordChunkDownloadLatency(statementId, 0, 50); // ensure entry exists
    handler.recordChunkIteration(statementId, chunkIndex);
    assertEquals(
        1L, handler.getTelemetryDetails(statementId).getChunkDetails().getTotalChunksIterated());
    handler.recordChunkIteration(statementId, chunkIndex + 1);
    assertEquals(
        2L, handler.getTelemetryDetails(statementId).getChunkDetails().getTotalChunksIterated());
  }

  @Test
  void testExportAllPendingTelemetryDetails_ClearsTrackers() {
    String statementId1 = "export-statement-1";
    String statementId2 = "export-statement-2";
    handler.recordChunkDownloadLatency(statementId1, 0, 100);
    handler.recordChunkDownloadLatency(statementId2, 0, 200);
    try (MockedStatic<TelemetryHelper> mocked = mockStatic(TelemetryHelper.class)) {
      handler.exportAllPendingTelemetryDetails();
      ArgumentCaptor<StatementTelemetryDetails> captor =
          ArgumentCaptor.forClass(StatementTelemetryDetails.class);
      mocked.verify(() -> TelemetryHelper.exportTelemetryLog(captor.capture()), times(2));
      assertTrue(
          captor.getAllValues().stream()
              .anyMatch(details -> statementId1.equals(details.getStatementId())));
      assertTrue(
          captor.getAllValues().stream()
              .anyMatch(details -> statementId2.equals(details.getStatementId())));
    }
    assertNull(handler.getTelemetryDetails(statementId1));
    assertNull(handler.getTelemetryDetails(statementId2));
  }
}
