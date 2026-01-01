package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkDownloadTaskTest {
  @Mock ArrowResultChunk chunk;
  @Mock IDatabricksHttpClient httpClient;
  @Mock RemoteChunkProvider remoteChunkProvider;
  @Mock ChunkLinkDownloadService<ArrowResultChunk> chunkLinkDownloadService;
  private ChunkDownloadTask chunkDownloadTask;
  private CompletableFuture<Void> downloadFuture;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    downloadFuture = new CompletableFuture<>();
    chunkDownloadTask =
        new ChunkDownloadTask(chunk, httpClient, remoteChunkProvider, chunkLinkDownloadService);
  }

  @Test
  void testRetryLogicWithSocketException() throws Exception {
    when(chunk.getChunkReadyFuture()).thenReturn(downloadFuture);
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    when(chunk.getChunkIndex()).thenReturn(7L);
    when(remoteChunkProvider.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    DatabricksParsingException throwableError =
        new DatabricksParsingException(
            "Connection reset",
            new SocketException("Connection reset"),
            DatabricksDriverErrorCode.INVALID_STATE);

    // Simulate SocketException for the first two attempts, then succeed
    doThrow(throwableError)
        .doThrow(throwableError)
        .doNothing()
        .when(chunk)
        .downloadData(httpClient, CompressionCodec.NONE, 0.1);

    chunkDownloadTask.call();

    verify(chunk, times(3)).downloadData(httpClient, CompressionCodec.NONE, 0.1);
    assertTrue(downloadFuture.isDone());
    assertDoesNotThrow(() -> downloadFuture.get());
  }

  @Test
  void testRetryLogicExhaustedWithSocketException() throws Exception {
    when(chunk.getChunkReadyFuture()).thenReturn(downloadFuture);
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    when(chunk.getChunkIndex()).thenReturn(7L);
    when(remoteChunkProvider.getCompressionCodec()).thenReturn(CompressionCodec.NONE);

    // Simulate SocketException for all attempts
    doThrow(
            new DatabricksParsingException(
                "Connection reset",
                new SocketException("Connection reset"),
                DatabricksDriverErrorCode.INVALID_STATE))
        .when(chunk)
        .downloadData(httpClient, CompressionCodec.NONE, 0.1);

    assertThrows(DatabricksSQLException.class, () -> chunkDownloadTask.call());
    verify(chunk, times(ChunkDownloadTask.MAX_RETRIES))
        .downloadData(httpClient, CompressionCodec.NONE, 0.1);
    assertTrue(downloadFuture.isDone());
    ExecutionException executionException =
        assertThrows(ExecutionException.class, () -> downloadFuture.get());
    assertInstanceOf(DatabricksSQLException.class, executionException.getCause());
  }

  @Test
  void testRetryLogicWithRealChunkAndStatusTransitions() throws Exception {
    StatementId statementId = new StatementId("test-statement-123");
    ExternalLink mockExternalLink = mock(ExternalLink.class);
    when(mockExternalLink.getExternalLink()).thenReturn("https://test-url.com/chunk");
    when(mockExternalLink.getHttpHeaders()).thenReturn(Collections.emptyMap());
    when(mockExternalLink.getExpiration()).thenReturn("2025-12-31T23:59:59Z");

    ArrowResultChunk realChunk =
        ArrowResultChunk.builder()
            .withStatementId(statementId)
            .withChunkInfo(createMockBaseChunkInfo(7L, 100L, 0L))
            .withChunkStatus(ChunkStatus.URL_FETCHED)
            .withChunkReadyTimeoutSeconds(30)
            .build();

    // Set the chunk link after creation
    realChunk.setChunkLink(mockExternalLink);

    // Mock HTTP client and responses
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    HttpEntity mockEntity = mock(HttpEntity.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getEntity()).thenReturn(mockEntity);

    // Track status changes by wrapping the real chunk
    List<ChunkStatus> statusHistory = new ArrayList<>();
    ArrowResultChunk spiedChunk = spy(realChunk);
    doAnswer(
            invocation -> {
              ChunkStatus status = invocation.getArgument(0);
              statusHistory.add(status);
              // Call the real setStatus method
              invocation.callRealMethod();
              return null;
            })
        .when(spiedChunk)
        .setStatus(any(ChunkStatus.class));

    // Mock the initializeData method to avoid Arrow parsing issues
    doAnswer(
            invocation -> {
              // Simulate successful data initialization
              spiedChunk.setStatus(ChunkStatus.PROCESSING_SUCCEEDED);
              return null;
            })
        .when(spiedChunk)
        .initializeData(any(InputStream.class));

    // Create a valid Arrow stream for successful response
    byte[] validArrowData = createValidArrowStreamData();

    // Mock HTTP client to fail twice, then succeed
    AtomicInteger httpCallCount = new AtomicInteger(0);
    when(httpClient.execute(any(HttpGet.class), eq(true)))
        .thenAnswer(
            invocation -> {
              int callNumber = httpCallCount.incrementAndGet();
              if (callNumber <= 2) {
                // First two calls throw SocketException
                throw new SocketException("Connection reset by peer");
              } else {
                // Third call succeeds
                when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(validArrowData));
                return mockResponse;
              }
            });

    when(remoteChunkProvider.getCompressionCodec()).thenReturn(CompressionCodec.NONE);

    // Mock chunkLinkDownloadService.getLinkForChunk() in case the chunk link becomes invalid
    CompletableFuture<ExternalLink> linkFuture =
        CompletableFuture.completedFuture(mockExternalLink);
    when(chunkLinkDownloadService.getLinkForChunk(anyLong())).thenReturn(linkFuture);

    // Create task with the spied chunk
    ChunkDownloadTask task =
        new ChunkDownloadTask(
            spiedChunk, httpClient, remoteChunkProvider, chunkLinkDownloadService);

    // Execute the task
    assertDoesNotThrow(task::call);

    // Verify HTTP client was called 3 times (2 failures + 1 success)
    verify(httpClient, times(3)).execute(any(HttpGet.class), eq(true));

    // Verify status progression includes link refetching:
    // URL_FETCHED -> DOWNLOAD_FAILED -> DOWNLOAD_RETRY -> URL_FETCHED (refetch) ->
    // DOWNLOAD_FAILED -> DOWNLOAD_RETRY -> URL_FETCHED (refetch) ->
    // DOWNLOAD_SUCCEEDED -> PROCESSING_SUCCEEDED
    assertEquals(9, statusHistory.size());
    assertEquals(ChunkStatus.URL_FETCHED, statusHistory.get(0)); // Initial chunk link set
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, statusHistory.get(1));
    assertEquals(ChunkStatus.DOWNLOAD_RETRY, statusHistory.get(2));
    assertEquals(ChunkStatus.URL_FETCHED, statusHistory.get(3)); // Link refetched after retry
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, statusHistory.get(4));
    assertEquals(ChunkStatus.DOWNLOAD_RETRY, statusHistory.get(5));
    assertEquals(ChunkStatus.URL_FETCHED, statusHistory.get(6)); // Link refetched again
    assertEquals(ChunkStatus.DOWNLOAD_SUCCEEDED, statusHistory.get(7)); // Download success
    assertEquals(ChunkStatus.PROCESSING_SUCCEEDED, statusHistory.get(8)); // Processing success

    // The chunk should eventually reach PROCESSING_SUCCEEDED after parsing the Arrow data
    assertEquals(ChunkStatus.PROCESSING_SUCCEEDED, spiedChunk.getStatus());

    // Verify the future completed successfully
    CompletableFuture<Void> chunkFuture = spiedChunk.getChunkReadyFuture();
    assertTrue(chunkFuture.isDone());
    assertDoesNotThrow(() -> chunkFuture.get());

    // Verify initializeData was called once (on successful download)
    verify(spiedChunk, times(1)).initializeData(any(InputStream.class));
  }

  private BaseChunkInfo createMockBaseChunkInfo(long chunkIndex, long rowCount, long rowOffset) {
    BaseChunkInfo mockChunkInfo = mock(BaseChunkInfo.class);
    when(mockChunkInfo.getChunkIndex()).thenReturn(chunkIndex);
    when(mockChunkInfo.getRowCount()).thenReturn(rowCount);
    when(mockChunkInfo.getRowOffset()).thenReturn(rowOffset);
    return mockChunkInfo;
  }

  private byte[] createValidArrowStreamData() {
    return new byte[] {1, 2, 3, 4, 5};
  }
}
