package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  @Mock private Map<Long, ArrowResultChunk> mockChunkMap;

  @BeforeEach
  void setUp() {
    when(mockSession.getConnectionContext()).thenReturn(mock(IDatabricksConnectionContext.class));
  }

  @Test
  void testGetLinkForChunk_Success()
      throws DatabricksSQLException, InterruptedException, ExecutionException, TimeoutException {
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);

    // Mock the response to link requests
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_1));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(2L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_2));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(3L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_3));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(4L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_4));

    long chunkIndex = 1L;
    when(mockChunkMap.get(chunkIndex)).thenReturn(mock(ArrowResultChunk.class));

    ChunkLinkDownloadService service =
        new ChunkLinkDownloadService(
            mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);

    // Trigger the download chain
    CompletableFuture<ExternalLink> future = service.getLinkForChunk(chunkIndex);
    ExternalLink result = future.get(1, TimeUnit.SECONDS);
    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(500);

    assertEquals(linkForChunkIndex_1, result);
    verify(mockClient).getResultChunks(mockStatementId, NEXT_BATCH_START_INDEX);
  }

  @Test
  void testGetLinkForChunk_AfterShutdown() throws ExecutionException, InterruptedException {
    ChunkLinkDownloadService service =
        new ChunkLinkDownloadService(
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
    ChunkLinkDownloadService service =
        new ChunkLinkDownloadService(
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
    when(mockClient.getResultChunks(eq(mockStatementId), anyLong())).thenThrow(expectedError);
    when(mockChunkMap.get(chunkIndex)).thenReturn(mock(ArrowResultChunk.class));

    ChunkLinkDownloadService service =
        new ChunkLinkDownloadService(
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
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_1));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(2L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_2));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(3L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_3));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(4L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_4));
    // Download chain will be triggered immediately in the constructor
    when(mockSession.getConnectionContext().getClientType()).thenReturn(DatabricksClientType.SEA);

    new ChunkLinkDownloadService(
        mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);

    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(500);

    verify(mockClient).getResultChunks(mockStatementId, NEXT_BATCH_START_INDEX);
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
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L)))
        .thenReturn(Collections.singletonList(expiredLinkForChunkIndex_1));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(2L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_2));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(3L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_3));
    when(mockClient.getResultChunks(eq(mockStatementId), eq(4L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_4));

    long chunkIndex = 1L;
    ArrowResultChunk mockChunk = mock(ArrowResultChunk.class);
    when(mockChunk.getStatus()).thenReturn(ArrowResultChunk.ChunkStatus.PENDING);
    when(mockChunkMap.get(chunkIndex)).thenReturn(mockChunk);

    ChunkLinkDownloadService service =
        new ChunkLinkDownloadService(
            mockSession, mockStatementId, TOTAL_CHUNKS, mockChunkMap, NEXT_BATCH_START_INDEX);

    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(500);

    // Mock a new valid link for chunk index 1
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L)))
        .thenReturn(Collections.singletonList(linkForChunkIndex_1));
    // Try to get the link for chunk index 1. Download chain will be re-triggered because the link
    // is expired
    CompletableFuture<ExternalLink> future = service.getLinkForChunk(chunkIndex);
    ExternalLink result = future.get(1, TimeUnit.SECONDS);
    // Sleep to allow the service to complete the download pipeline
    TimeUnit.MILLISECONDS.sleep(500);

    assertEquals(linkForChunkIndex_1, result);
    verify(mockClient, times(2)).getResultChunks(mockStatementId, chunkIndex);
  }

  @Test
  void testBatchDownloadChaining()
      throws DatabricksSQLException, ExecutionException, InterruptedException, TimeoutException {
    ExternalLink linkForChunkIndex_5 =
        createExternalLink("test-url", 5L, Collections.emptyMap(), "2025-02-16T00:00:00Z");
    ExternalLink linkForChunkIndex_6 =
        createExternalLink("test-url", 6L, Collections.emptyMap(), "2025-02-16T00:00:00Z");
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);
    // Mock the links for the first batch. The link futures for both chunks will be completed at the
    // same time
    when(mockClient.getResultChunks(eq(mockStatementId), eq(1L)))
        .thenReturn(Arrays.asList(linkForChunkIndex_1, linkForChunkIndex_2));
    // Mock the links for the second batch.
    when(mockClient.getResultChunks(eq(mockStatementId), eq(3L)))
        .thenReturn(Arrays.asList(linkForChunkIndex_3, linkForChunkIndex_4));
    // Mock the links for the third batch.
    when(mockClient.getResultChunks(eq(mockStatementId), eq(5L)))
        .thenReturn(Arrays.asList(linkForChunkIndex_5, linkForChunkIndex_6));

    ChunkLinkDownloadService service =
        new ChunkLinkDownloadService(
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

    ExternalLink result1 = future1.get(1, TimeUnit.SECONDS);
    ExternalLink result2 = future2.get(1, TimeUnit.SECONDS);
    ExternalLink result3 = future3.get(1, TimeUnit.SECONDS);
    ExternalLink result4 = future4.get(1, TimeUnit.SECONDS);
    ExternalLink result5 = future5.get(1, TimeUnit.SECONDS);
    ExternalLink result6 = future6.get(1, TimeUnit.SECONDS);

    assertEquals(linkForChunkIndex_1, result1);
    assertEquals(linkForChunkIndex_2, result2);
    assertEquals(linkForChunkIndex_3, result3);
    assertEquals(linkForChunkIndex_4, result4);
    assertEquals(linkForChunkIndex_5, result5);
    assertEquals(linkForChunkIndex_6, result6);
    // Verify the request for first batch
    verify(mockClient, times(1)).getResultChunks(mockStatementId, 1L);
    // Verify the request for second batch
    verify(mockClient, times(1)).getResultChunks(mockStatementId, 3L);
    // Verify the request for third batch
    verify(mockClient, times(1)).getResultChunks(mockStatementId, 5L);
  }

  private ExternalLink createExternalLink(
      String url, long chunkIndex, Map<String, String> headers, String expiration) {
    ExternalLink link = new ExternalLink();
    link.setExternalLink(url);
    link.setChunkIndex(chunkIndex);
    link.setHttpHeaders(headers);
    link.setExpiration(expiration);

    return link;
  }
}
