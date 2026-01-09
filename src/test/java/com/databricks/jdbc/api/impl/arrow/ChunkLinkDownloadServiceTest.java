package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ChunkLinkDownloadServiceTest {

  private static final long TOTAL_CHUNKS = 5;
  private static final long NEXT_BATCH_START_INDEX = 1;
  private final ExternalLink linkForChunkIndex_1 =
      createExternalLink("test-url", 1L, Collections.emptyMap(), "2025-02-16T00:00:00Z");
  private final ExternalLink linkForChunkIndex_2 =
      createExternalLink("test-url", 2L, Collections.emptyMap(), "2025-02-16T00:00:00Z");
  private final ExternalLink linkForChunkIndex_3 =
      createExternalLink("test-url", 3L, Collections.emptyMap(), "2025-02-16T00:00:00Z");
  private final ExternalLink linkForChunkIndex_4 =
      createExternalLink("test-url", 4L, Collections.emptyMap(), "2025-02-16T00:00:00Z");
  @Mock private IDatabricksSession mockSession;

  @Mock private IDatabricksClient mockClient;

  @Mock private StatementId mockStatementId;

  @Mock private ConcurrentMap<Long, ArrowResultChunk> mockChunkMap;

  @BeforeEach
  void setUp() {
    when(mockSession.getConnectionContext()).thenReturn(mock(IDatabricksConnectionContext.class));
    lenient().when(mockChunkMap.get(anyLong())).thenReturn(null);
  }

  @Test
  void testGetLinkForChunk_Success()
      throws DatabricksSQLException, InterruptedException, ExecutionException, TimeoutException {
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);

    // Mock the response to link requests
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L), anyLong()))
        .thenReturn(buildChunkLinkFetchResult(Collections.singletonList(linkForChunkIndex_1)));

    long chunkIndex = 1L;
    when(mockChunkMap.get(chunkIndex)).thenReturn(mock(ArrowResultChunk.class));

    ChunkLinkDownloadService<ArrowResultChunk> service =
        new ChunkLinkDownloadService<>(
            mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);

    // Trigger the download chain
    CompletableFuture<ExternalLink> future = service.getLinkForChunk(chunkIndex);
    ExternalLink result = future.get(1, TimeUnit.SECONDS);
    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(500);

    assertEquals(linkForChunkIndex_1, result);
    verify(mockClient).getResultChunks(mockStatementId, NEXT_BATCH_START_INDEX, 0L);
  }

  @Test
  void testGetLinkForChunk_AfterShutdown() throws ExecutionException, InterruptedException {
    ChunkLinkDownloadService<ArrowResultChunk> service =
        new ChunkLinkDownloadService<>(
            mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);
    service.shutdown();

    CompletableFuture<ExternalLink> future = service.getLinkForChunk(1L);
    ExecutionException exception =
        assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
    assertInstanceOf(DatabricksValidationException.class, exception.getCause());
    assertTrue(exception.getCause().getMessage().contains("shutdown"));
  }

  @Test
  void testGetLinkForChunk_InvalidIndex() throws ExecutionException, InterruptedException {
    ChunkLinkDownloadService<ArrowResultChunk> service =
        new ChunkLinkDownloadService<>(
            mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);
    CompletableFuture<ExternalLink> future = service.getLinkForChunk(TOTAL_CHUNKS + 1);
    ExecutionException exception =
        assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
    assertInstanceOf(DatabricksValidationException.class, exception.getCause());
    assertTrue(exception.getCause().getMessage().contains("exceeds total chunks"));
  }

  @Test
  void testGetLinkForChunk_ClientError()
      throws DatabricksSQLException, ExecutionException, InterruptedException {
    long chunkIndex = 1L;
    DatabricksSQLException expectedError =
        new DatabricksSQLException("Test error", DatabricksDriverErrorCode.INVALID_STATE);
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);
    // Mock an error in response to the link request
    when(mockClient.getResultChunks(eq(mockStatementId), anyLong(), anyLong()))
        .thenThrow(expectedError);
    when(mockChunkMap.get(chunkIndex)).thenReturn(mock(ArrowResultChunk.class));

    ChunkLinkDownloadService<ArrowResultChunk> service =
        new ChunkLinkDownloadService<>(
            mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);

    CompletableFuture<ExternalLink> future = service.getLinkForChunk(chunkIndex);

    ExecutionException exception =
        assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
    assertEquals(expectedError, exception.getCause());
  }

  @Test
  void testAutoTriggerForSEAClient() throws DatabricksSQLException, InterruptedException {
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);
    // Mock the response to link requests
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L), anyLong()))
        .thenReturn(buildChunkLinkFetchResult(Collections.singletonList(linkForChunkIndex_1)));
    // Download chain will be triggered immediately in the constructor
    when(mockSession.getConnectionContext().getClientType()).thenReturn(DatabricksClientType.SEA);

    long chunkIndex = 1L;
    when(mockChunkMap.get(chunkIndex)).thenReturn(mock(ArrowResultChunk.class));

    new ChunkLinkDownloadService<>(
        mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);

    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(500);

    verify(mockClient).getResultChunks(mockStatementId, NEXT_BATCH_START_INDEX, 0L);
  }

  @Test
  void testHandleExpiredLinks()
      throws DatabricksSQLException, ExecutionException, InterruptedException, TimeoutException {
    when(mockSession.getConnectionContext().getClientType()).thenReturn(DatabricksClientType.SEA);
    // Create an expired link for chunk index 1
    ExternalLink expiredLinkForChunkIndex_1 =
        createExternalLink("test-url", 1L, Collections.emptyMap(), "2020-02-14T00:00:00Z");
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);

    // Mock the response to link requests. Return the expired link for chunk index 1
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L), anyLong()))
        .thenReturn(
            buildChunkLinkFetchResult(Collections.singletonList(expiredLinkForChunkIndex_1)));

    long chunkIndex = 1L;
    ArrowResultChunk mockChunk = mock(ArrowResultChunk.class);
    when(mockChunk.getStatus()).thenReturn(ChunkStatus.PENDING);
    when(mockChunkMap.get(chunkIndex)).thenReturn(mockChunk);

    ChunkLinkDownloadService<ArrowResultChunk> service =
        new ChunkLinkDownloadService<>(
            mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);

    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(500);

    // Mock a new valid link for chunk index 1
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L), anyLong()))
        .thenReturn(buildChunkLinkFetchResult(Collections.singletonList(linkForChunkIndex_1)));
    // Try to get the link for chunk index 1. Download chain will be re-triggered because the link
    // is expired
    CompletableFuture<ExternalLink> future = service.getLinkForChunk(chunkIndex);
    ExternalLink result = future.get(1, TimeUnit.SECONDS);
    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(500);

    assertEquals(linkForChunkIndex_1, result);
    verify(mockClient, times(2)).getResultChunks(mockStatementId, chunkIndex, 0L);
  }

  @Test
  void testBatchDownloadChaining()
      throws DatabricksSQLException, ExecutionException, InterruptedException, TimeoutException {
    // Use a far future date to ensure links are never considered expired
    String farFutureExpiration = Instant.now().plus(10, ChronoUnit.MINUTES).toString();

    ExternalLink linkForChunkIndex_1 =
        createExternalLink("test-url", 1L, Collections.emptyMap(), farFutureExpiration);
    ExternalLink linkForChunkIndex_2 =
        createExternalLink("test-url", 2L, Collections.emptyMap(), farFutureExpiration);
    ExternalLink linkForChunkIndex_3 =
        createExternalLink("test-url", 3L, Collections.emptyMap(), farFutureExpiration);
    ExternalLink linkForChunkIndex_4 =
        createExternalLink("test-url", 4L, Collections.emptyMap(), farFutureExpiration);
    ExternalLink linkForChunkIndex_5 =
        createExternalLink("test-url", 5L, Collections.emptyMap(), farFutureExpiration);
    ExternalLink linkForChunkIndex_6 =
        createExternalLink("test-url", 6L, Collections.emptyMap(), farFutureExpiration);

    ArrowResultChunk mockChunk = mock(ArrowResultChunk.class);
    when(mockChunkMap.get(anyLong())).thenReturn(mockChunk);
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);
    // Mock the links for the first batch. The link futures for both chunks will be completed at the
    // same time
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L), anyLong()))
        .thenReturn(
            buildChunkLinkFetchResult(Arrays.asList(linkForChunkIndex_1, linkForChunkIndex_2)));
    // Mock the links for the second batch.
    when(mockClient.getResultChunks(eq(mockStatementId), eq(3L), anyLong()))
        .thenReturn(
            buildChunkLinkFetchResult(Arrays.asList(linkForChunkIndex_3, linkForChunkIndex_4)));
    // Mock the links for the third batch.
    when(mockClient.getResultChunks(eq(mockStatementId), eq(5L), anyLong()))
        .thenReturn(
            buildChunkLinkFetchResult(Arrays.asList(linkForChunkIndex_5, linkForChunkIndex_6)));

    ChunkLinkDownloadService<ArrowResultChunk> service =
        new ChunkLinkDownloadService<>(
            mockSession, mockStatementId, 7, mockChunkMap, NEXT_BATCH_START_INDEX);

    // Trigger the download chain
    CompletableFuture<ExternalLink> future1 = service.getLinkForChunk(1L);
    CompletableFuture<ExternalLink> future2 = service.getLinkForChunk(2L);
    CompletableFuture<ExternalLink> future3 = service.getLinkForChunk(3L);
    CompletableFuture<ExternalLink> future4 = service.getLinkForChunk(4L);
    CompletableFuture<ExternalLink> future5 = service.getLinkForChunk(5L);
    CompletableFuture<ExternalLink> future6 = service.getLinkForChunk(6L);

    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(2000);

    ExternalLink result1 = future1.get(10, TimeUnit.SECONDS);
    ExternalLink result2 = future2.get(10, TimeUnit.SECONDS);
    ExternalLink result3 = future3.get(10, TimeUnit.SECONDS);
    ExternalLink result4 = future4.get(10, TimeUnit.SECONDS);
    ExternalLink result5 = future5.get(10, TimeUnit.SECONDS);
    ExternalLink result6 = future6.get(10, TimeUnit.SECONDS);

    assertEquals(linkForChunkIndex_1, result1);
    assertEquals(linkForChunkIndex_2, result2);
    assertEquals(linkForChunkIndex_3, result3);
    assertEquals(linkForChunkIndex_4, result4);
    assertEquals(linkForChunkIndex_5, result5);
    assertEquals(linkForChunkIndex_6, result6);
    // Verify the request for first batch
    verify(mockClient, times(1)).getResultChunks(mockStatementId, 1L, 0L);
    // Verify the request for second batch
    verify(mockClient, times(1)).getResultChunks(mockStatementId, 3L, 0L);
    // Verify the request for third batch
    verify(mockClient, times(1)).getResultChunks(mockStatementId, 5L, 0L);
  }

  @Test
  void testUpfrontFetchedLinks_FuturesCompletedInConstructor()
      throws ExecutionException, InterruptedException, TimeoutException {
    when(mockSession.getConnectionContext().getClientType())
        .thenReturn(DatabricksClientType.THRIFT);

    // Create links for upfront-fetched chunks
    ExternalLink link0 =
        createExternalLink("url-0", 0L, Collections.emptyMap(), "2025-02-16T00:00:00Z");
    ExternalLink link1 =
        createExternalLink("url-1", 1L, Collections.emptyMap(), "2025-02-16T00:00:00Z");
    ExternalLink link2 =
        createExternalLink("url-2", 2L, Collections.emptyMap(), "2025-02-16T00:00:00Z");

    // Create mock chunks with links already set
    ArrowResultChunk mockChunk0 = mock(ArrowResultChunk.class);
    ArrowResultChunk mockChunk1 = mock(ArrowResultChunk.class);
    ArrowResultChunk mockChunk2 = mock(ArrowResultChunk.class);

    ArrowResultChunk mockChunk3 = mock(ArrowResultChunk.class);
    ArrowResultChunk mockChunk4 = mock(ArrowResultChunk.class);

    when(mockChunk0.getChunkLink()).thenReturn(link0);
    when(mockChunk1.getChunkLink()).thenReturn(link1);
    when(mockChunk2.getChunkLink()).thenReturn(link2);

    when(mockChunkMap.get(0L)).thenReturn(mockChunk0);
    when(mockChunkMap.get(1L)).thenReturn(mockChunk1);
    when(mockChunkMap.get(2L)).thenReturn(mockChunk2);
    lenient().when(mockChunkMap.get(3L)).thenReturn(mockChunk3);
    lenient().when(mockChunkMap.get(4L)).thenReturn(mockChunk4);

    // Create service with nextBatchStartIndex = 3 (meaning chunks 0, 1, 2 were upfront-fetched)
    long nextBatchStartIndex = 3L;
    ChunkLinkDownloadService<ArrowResultChunk> service =
        new ChunkLinkDownloadService<>(
            mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, nextBatchStartIndex);

    // Verify that futures for chunks 0, 1, 2 are already completed
    CompletableFuture<ExternalLink> future0 = service.getLinkFutureForTest(0L);
    CompletableFuture<ExternalLink> future1 = service.getLinkFutureForTest(1L);
    CompletableFuture<ExternalLink> future2 = service.getLinkFutureForTest(2L);

    assertTrue(future0.isDone(), "Future for chunk 0 should be completed");
    assertTrue(future1.isDone(), "Future for chunk 1 should be completed");
    assertTrue(future2.isDone(), "Future for chunk 2 should be completed");

    // Verify the futures contain the correct links
    assertEquals(link0, future0.get(100, TimeUnit.MILLISECONDS));
    assertEquals(link1, future1.get(100, TimeUnit.MILLISECONDS));
    assertEquals(link2, future2.get(100, TimeUnit.MILLISECONDS));

    // Verify that futures for chunks 3, 4 are not completed
    CompletableFuture<ExternalLink> future3 = service.getLinkFutureForTest(3L);
    CompletableFuture<ExternalLink> future4 = service.getLinkFutureForTest(4L);

    assertFalse(future3.isDone(), "Future for chunk 3 should not be completed");
    assertFalse(future4.isDone(), "Future for chunk 4 should not be completed");
  }

  private ExternalLink createExternalLink(
      String url, long chunkIndex, Map<String, String> headers, String expiration) {
    ExternalLink link = new ExternalLink();
    link.setExternalLink(url);
    link.setChunkIndex(chunkIndex);
    link.setHttpHeaders(headers);
    link.setExpiration(expiration);
    link.setRowOffset(chunkIndex * 100L);
    link.setRowCount(100L);

    return link;
  }

  /**
   * Helper method to build ChunkLinkFetchResult from a list of ExternalLinks. This mimics the
   * behavior of the SEA client's buildChunkLinkFetchResult method.
   */
  private ChunkLinkFetchResult buildChunkLinkFetchResult(List<ExternalLink> links) {
    if (links == null || links.isEmpty()) {
      return ChunkLinkFetchResult.endOfStream();
    }

    ExternalLink lastLink = links.get(links.size() - 1);
    boolean hasMore = lastLink.getNextChunkIndex() != null;
    long nextFetchIndex = hasMore ? lastLink.getNextChunkIndex() : -1;
    long nextRowOffset = lastLink.getRowOffset() + lastLink.getRowCount();

    return ChunkLinkFetchResult.of(links, hasMore, nextFetchIndex, nextRowOffset);
  }
}
