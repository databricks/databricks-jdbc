package com.databricks.jdbc.telemetry.latency;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkLatencyHandlerTest {

  @Mock private StatementId mockStatementId;

  private ChunkLatencyHandler handler;

  @BeforeEach
  void setUp() {
    handler = ChunkLatencyHandler.getInstance();
    // Clear any existing state
    handler.getAllPendingChunkDetails(); // This clears the internal map
  }

  @AfterEach
  void tearDown() {
    // Clean up any remaining state
    handler.getAllPendingChunkDetails();
  }

  @Test
  void testGetInstance_ReturnsSameInstance() {
    ChunkLatencyHandler instance1 = ChunkLatencyHandler.getInstance();
    ChunkLatencyHandler instance2 = ChunkLatencyHandler.getInstance();
    assertSame(instance1, instance2);
  }

  @Test
  void testInitializeStatement_ValidStatementId() {
    when(mockStatementId.toString()).thenReturn("test-statement-1");

    handler.initializeStatement(mockStatementId, 5);

    ChunkDetails details = handler.getChunkDetails("test-statement-1");
    assertNotNull(details);
    assertEquals(5L, details.getTotalChunksPresent());
    assertEquals(0L, details.getTotalChunksIterated());
    assertEquals(0L, details.getSumChunksDownloadTimeMillis());
  }

  @Test
  void testInitializeStatement_NullStatementId() {
    // Should not throw exception
    assertDoesNotThrow(() -> handler.initializeStatement(null, 5));

    // Should not create any entries
    Map<String, ChunkDetails> pending = handler.getAllPendingChunkDetails();
    assertTrue(pending.isEmpty());
  }

  @Test
  void testRecordChunkDownloadLatency_NullStatementId() {
    // Should not throw exception
    assertDoesNotThrow(() -> handler.recordChunkDownloadLatency(null, 0, 100));

    // Should not create any entries
    Map<String, ChunkDetails> pending = handler.getAllPendingChunkDetails();
    assertTrue(pending.isEmpty());
  }

  @Test
  void testRecordChunkDownloadLatency_FirstChunk() {
    String statementId = "test-statement-1";

    handler.recordChunkDownloadLatency(statementId, 0, 150);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNotNull(details);
    assertEquals(150L, details.getInitialChunkLatencyMillis());
    assertEquals(150L, details.getSlowestChunkLatencyMillis());
    assertEquals(150L, details.getSumChunksDownloadTimeMillis());
  }

  @Test
  void testRecordChunkDownloadLatency_SubsequentChunks() {
    String statementId = "test-statement-1";

    // Record first chunk
    handler.recordChunkDownloadLatency(statementId, 0, 100);
    // Record second chunk with higher latency
    handler.recordChunkDownloadLatency(statementId, 1, 200);
    // Record third chunk with lower latency
    handler.recordChunkDownloadLatency(statementId, 2, 75);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNotNull(details);
    assertEquals(100L, details.getInitialChunkLatencyMillis()); // First chunk only
    assertEquals(200L, details.getSlowestChunkLatencyMillis()); // Highest latency
    assertEquals(375L, details.getSumChunksDownloadTimeMillis()); // Sum of all
  }

  @Test
  void testRecordChunkDownloadLatency_CreatesNewChunkDetailsIfNotExists() {
    String statementId = "test-statement-1";

    handler.recordChunkDownloadLatency(statementId, 1, 100);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNotNull(details);
    assertEquals(0L, details.getTotalChunksPresent()); // Default when auto-created
    assertNull(details.getInitialChunkLatencyMillis()); // Not first chunk
    assertEquals(100L, details.getSlowestChunkLatencyMillis());
    assertEquals(100L, details.getSumChunksDownloadTimeMillis());
  }

  @Test
  void testRecordChunkIteration_NullStatementId() {
    // Should not throw exception
    assertDoesNotThrow(() -> handler.recordChunkIteration(null, 0));

    // Should not create any entries
    Map<String, ChunkDetails> pending = handler.getAllPendingChunkDetails();
    assertTrue(pending.isEmpty());
  }

  @Test
  void testRecordChunkIteration_ValidStatementId() {
    String statementId = "test-statement-1";
    when(mockStatementId.toString()).thenReturn(statementId);

    // Initialize statement first
    handler.initializeStatement(mockStatementId, 3);

    // Record iterations
    handler.recordChunkIteration(statementId, 0);
    handler.recordChunkIteration(statementId, 1);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNotNull(details);
    assertEquals(2L, details.getTotalChunksIterated());
  }

  @Test
  void testRecordChunkIteration_NonExistentStatementId() {
    String statementId = "non-existent-statement";

    // Should not throw exception
    assertDoesNotThrow(() -> handler.recordChunkIteration(statementId, 0));

    // Should not create new entry
    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNull(details);
  }

  @Test
  void testGetChunkDetails_NullStatementId() {
    ChunkDetails details = handler.getChunkDetails(null);
    assertNull(details);
  }

  @Test
  void testGetChunkDetails_ValidStatementId() {
    String statementId = "test-statement-1";
    when(mockStatementId.toString()).thenReturn(statementId);

    handler.initializeStatement(mockStatementId, 5);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNotNull(details);
    assertEquals(5L, details.getTotalChunksPresent());

    // Should still be available after get (not cleaned up)
    ChunkDetails detailsAgain = handler.getChunkDetails(statementId);
    assertNotNull(detailsAgain);
    assertSame(details, detailsAgain);
  }

  @Test
  void testGetChunkDetailsAndCleanup_NullStatementId() {
    ChunkDetails details = handler.getChunkDetailsAndCleanup(null);
    assertNull(details);
  }

  @Test
  void testGetChunkDetailsAndCleanup_ValidStatementId() {
    String statementId = "test-statement-1";
    when(mockStatementId.toString()).thenReturn(statementId);

    handler.initializeStatement(mockStatementId, 5);

    ChunkDetails details = handler.getChunkDetailsAndCleanup(statementId);
    assertNotNull(details);
    assertEquals(5L, details.getTotalChunksPresent());

    // Should be cleaned up after get
    ChunkDetails detailsAgain = handler.getChunkDetails(statementId);
    assertNull(detailsAgain);
  }

  @Test
  void testGetChunkDetailsAndCleanup_NonExistentStatementId() {
    String statementId = "non-existent-statement";

    ChunkDetails details = handler.getChunkDetailsAndCleanup(statementId);
    assertNull(details);
  }

  @Test
  void testClearStatement_NullStatementId() {
    // Should not throw exception
    assertDoesNotThrow(() -> handler.clearStatement(null));
  }

  @Test
  void testClearStatement_ValidStatementId() {
    String statementId = "test-statement-1";
    when(mockStatementId.toString()).thenReturn(statementId);

    handler.initializeStatement(mockStatementId, 5);

    // Verify it exists
    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNotNull(details);

    // Clear it
    handler.clearStatement(mockStatementId);

    // Verify it's gone
    ChunkDetails detailsAfterClear = handler.getChunkDetails(statementId);
    assertNull(detailsAfterClear);
  }

  @Test
  void testGetAllPendingChunkDetails_EmptyTrackers() {
    Map<String, ChunkDetails> pendingDetails = handler.getAllPendingChunkDetails();
    assertTrue(pendingDetails.isEmpty());
  }

  @Test
  void testGetAllPendingChunkDetails_WithTrackers() {
    String statementId1 = "test-statement-1";
    String statementId2 = "test-statement-2";

    StatementId mockStatementId1 = mock(StatementId.class);
    StatementId mockStatementId2 = mock(StatementId.class);
    when(mockStatementId1.toString()).thenReturn(statementId1);
    when(mockStatementId2.toString()).thenReturn(statementId2);

    handler.initializeStatement(mockStatementId1, 3);
    handler.initializeStatement(mockStatementId2, 5);

    // Verify data exists before calling getAllPendingChunkDetails
    assertNotNull(handler.getChunkDetails(statementId1));
    assertNotNull(handler.getChunkDetails(statementId2));

    Map<String, ChunkDetails> pendingDetails = handler.getAllPendingChunkDetails();

    assertEquals(2, pendingDetails.size());
    assertTrue(pendingDetails.containsKey(statementId1));
    assertTrue(pendingDetails.containsKey(statementId2));
    assertEquals(3L, pendingDetails.get(statementId1).getTotalChunksPresent());
    assertEquals(5L, pendingDetails.get(statementId2).getTotalChunksPresent());

    // Verify trackers are cleared after getting all pending details
    Map<String, ChunkDetails> pendingDetailsAfter = handler.getAllPendingChunkDetails();
    assertTrue(pendingDetailsAfter.isEmpty());
  }

  @Test
  void testComplexScenario() {
    String statementId = "test-statement-complex";
    StatementId mockStatementIdComplex = mock(StatementId.class);
    when(mockStatementIdComplex.toString()).thenReturn(statementId);

    // Initialize statement
    handler.initializeStatement(mockStatementIdComplex, 10);

    // Record some chunk download latencies
    handler.recordChunkDownloadLatency(statementId, 0, 100); // First chunk
    handler.recordChunkDownloadLatency(statementId, 1, 250); // Slowest chunk
    handler.recordChunkDownloadLatency(statementId, 2, 150); // Regular chunk

    // Record some chunk iterations
    handler.recordChunkIteration(statementId, 0);
    handler.recordChunkIteration(statementId, 1);
    handler.recordChunkIteration(statementId, 2);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNotNull(details);

    // Verify all metrics are correctly calculated
    assertEquals(10L, details.getTotalChunksPresent());
    assertEquals(3L, details.getTotalChunksIterated());
    assertEquals(100L, details.getInitialChunkLatencyMillis());
    assertEquals(250L, details.getSlowestChunkLatencyMillis());
    assertEquals(500L, details.getSumChunksDownloadTimeMillis());

    // Clean up
    ChunkDetails cleanedDetails = handler.getChunkDetailsAndCleanup(statementId);
    assertNotNull(cleanedDetails);
    assertSame(details, cleanedDetails);

    // Verify cleanup
    ChunkDetails afterCleanup = handler.getChunkDetails(statementId);
    assertNull(afterCleanup);
  }

  @Test
  void testConcurrentAccess() {
    String statementId1 = "concurrent-statement-1";
    String statementId2 = "concurrent-statement-2";

    // This test verifies that the ConcurrentHashMap is used correctly
    // and doesn't throw ConcurrentModificationException

    handler.recordChunkDownloadLatency(statementId1, 0, 100);
    handler.recordChunkDownloadLatency(statementId2, 0, 200);

    ChunkDetails details1 = handler.getChunkDetails(statementId1);
    ChunkDetails details2 = handler.getChunkDetails(statementId2);

    assertNotNull(details1);
    assertNotNull(details2);
    assertEquals(100L, details1.getSlowestChunkLatencyMillis());
    assertEquals(200L, details2.getSlowestChunkLatencyMillis());

    // Get all pending details should return both
    Map<String, ChunkDetails> allPending = handler.getAllPendingChunkDetails();
    assertEquals(2, allPending.size());
  }

  @Test
  void testSlowstChunkLatencyUpdate() {
    String statementId = "test-slowest-chunk";

    // Record chunks with increasing latency
    handler.recordChunkDownloadLatency(statementId, 0, 50);
    handler.recordChunkDownloadLatency(statementId, 1, 100);
    handler.recordChunkDownloadLatency(statementId, 2, 75);
    handler.recordChunkDownloadLatency(statementId, 3, 200); // New slowest
    handler.recordChunkDownloadLatency(statementId, 4, 25);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertNotNull(details);
    assertEquals(50L, details.getInitialChunkLatencyMillis()); // First chunk
    assertEquals(200L, details.getSlowestChunkLatencyMillis()); // Slowest chunk
    assertEquals(450L, details.getSumChunksDownloadTimeMillis()); // Sum: 50+100+75+200+25
  }

  @Test
  void testSumChunksDownloadTimeAccumulation() {
    String statementId = "test-sum-chunks";

    // Record multiple chunks and verify sum accumulation
    handler.recordChunkDownloadLatency(statementId, 0, 10);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertEquals(10L, details.getSumChunksDownloadTimeMillis());

    handler.recordChunkDownloadLatency(statementId, 1, 20);
    assertEquals(30L, details.getSumChunksDownloadTimeMillis());

    handler.recordChunkDownloadLatency(statementId, 2, 15);
    assertEquals(45L, details.getSumChunksDownloadTimeMillis());
  }

  @Test
  void testChunkIterationAccumulation() {
    String statementId = "test-iteration-accumulation";
    StatementId mockStatementIdIter = mock(StatementId.class);
    when(mockStatementIdIter.toString()).thenReturn(statementId);

    // Initialize statement
    handler.initializeStatement(mockStatementIdIter, 5);

    ChunkDetails details = handler.getChunkDetails(statementId);
    assertEquals(0L, details.getTotalChunksIterated());

    // Record iterations one by one
    handler.recordChunkIteration(statementId, 0);
    assertEquals(1L, details.getTotalChunksIterated());

    handler.recordChunkIteration(statementId, 1);
    assertEquals(2L, details.getTotalChunksIterated());

    handler.recordChunkIteration(statementId, 2);
    assertEquals(3L, details.getTotalChunksIterated());
  }
}
