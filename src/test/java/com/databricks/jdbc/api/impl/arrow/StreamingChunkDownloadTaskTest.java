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
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamingChunkDownloadTaskTest {

  private static final double CLOUD_FETCH_SPEED_THRESHOLD = 0.1;

  @Mock private ArrowResultChunk chunk;
  @Mock private IDatabricksHttpClient httpClient;
  @Mock private ChunkLinkFetcher linkFetcher;

  private StreamingChunkDownloadTask downloadTask;
  private CompletableFuture<Void> downloadFuture;

  @BeforeEach
  void setUp() {
    downloadFuture = new CompletableFuture<>();
    downloadTask =
        new StreamingChunkDownloadTask(
            chunk, httpClient, CompressionCodec.NONE, linkFetcher, CLOUD_FETCH_SPEED_THRESHOLD);
  }

  @Test
  void testSuccessfulDownloadOnFirstAttempt() throws Exception {
    when(chunk.getChunkReadyFuture()).thenReturn(downloadFuture);
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    when(chunk.getChunkIndex()).thenReturn(0L);

    // Mock successful download
    doNothing()
        .when(chunk)
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);

    downloadTask.call();

    verify(chunk, times(1))
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);
    verify(chunk, never()).setStatus(ChunkStatus.DOWNLOAD_RETRY);
    assertTrue(downloadFuture.isDone());
    assertDoesNotThrow(() -> downloadFuture.get());
  }

  @Test
  void testRetryLogicWithSocketException() throws Exception {
    when(chunk.getChunkReadyFuture()).thenReturn(downloadFuture);
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    when(chunk.getChunkIndex()).thenReturn(7L);

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
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);

    downloadTask.call();

    verify(chunk, times(3))
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);
    verify(chunk, times(2)).setStatus(ChunkStatus.DOWNLOAD_RETRY);
    assertTrue(downloadFuture.isDone());
    assertDoesNotThrow(() -> downloadFuture.get());
  }

  @Test
  void testRetryLogicExhaustedWithSocketException() throws Exception {
    when(chunk.getChunkReadyFuture()).thenReturn(downloadFuture);
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    when(chunk.getChunkIndex()).thenReturn(7L);

    // Simulate SocketException for all attempts
    doThrow(
            new DatabricksParsingException(
                "Connection reset",
                new SocketException("Connection reset"),
                DatabricksDriverErrorCode.INVALID_STATE))
        .when(chunk)
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);

    assertThrows(DatabricksSQLException.class, () -> downloadTask.call());

    // Should attempt MAX_RETRIES (5) times
    verify(chunk, times(5))
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);
    verify(chunk, times(1)).setStatus(ChunkStatus.DOWNLOAD_FAILED);
    assertTrue(downloadFuture.isDone());

    ExecutionException executionException =
        assertThrows(ExecutionException.class, () -> downloadFuture.get());
    assertInstanceOf(DatabricksSQLException.class, executionException.getCause());
  }

  @Test
  void testLinkRefreshWhenLinkIsInvalid() throws Exception {
    when(chunk.getChunkReadyFuture()).thenReturn(downloadFuture);
    when(chunk.getChunkIndex()).thenReturn(5L);
    when(chunk.getRowOffset()).thenReturn(100L);

    // First call: link is invalid, second call: link is valid
    when(chunk.isChunkLinkInvalid()).thenReturn(true, false);

    ExternalLink freshLink = mock(ExternalLink.class);
    when(linkFetcher.refetchLink(5L, 100L)).thenReturn(freshLink);

    doNothing()
        .when(chunk)
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);

    downloadTask.call();

    // Verify link was refreshed
    verify(linkFetcher, times(1)).refetchLink(5L, 100L);
    verify(chunk, times(1)).setChunkLink(freshLink);
    verify(chunk, times(1))
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);
    assertTrue(downloadFuture.isDone());
    assertDoesNotThrow(() -> downloadFuture.get());
  }

  @Test
  void testLinkRefreshFailureAndRetry() throws Exception {
    when(chunk.getChunkReadyFuture()).thenReturn(downloadFuture);
    when(chunk.getChunkIndex()).thenReturn(5L);
    when(chunk.getRowOffset()).thenReturn(100L);
    when(chunk.isChunkLinkInvalid()).thenReturn(true, true, false);

    ExternalLink freshLink = mock(ExternalLink.class);
    when(linkFetcher.refetchLink(5L, 100L)).thenReturn(freshLink);

    // First download fails with IOException, retry succeeds
    doThrow(new SocketException("Connection reset"))
        .doNothing()
        .when(chunk)
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);

    downloadTask.call();

    // Should refresh link twice (once per attempt)
    verify(linkFetcher, times(2)).refetchLink(5L, 100L);
    verify(chunk, times(2)).setChunkLink(freshLink);
    verify(chunk, times(2))
        .downloadData(httpClient, CompressionCodec.NONE, CLOUD_FETCH_SPEED_THRESHOLD);
    verify(chunk, times(1)).setStatus(ChunkStatus.DOWNLOAD_RETRY);
    assertTrue(downloadFuture.isDone());
    assertDoesNotThrow(() -> downloadFuture.get());
  }

  @Test
  void testStatusTransitionsDuringRetries() throws Exception {
    StatementId statementId = new StatementId("test-statement-123");
    ExternalLink mockExternalLink = mock(ExternalLink.class);
    when(mockExternalLink.getExternalLink()).thenReturn("https://test-url.com/chunk");
    when(mockExternalLink.getHttpHeaders()).thenReturn(Collections.emptyMap());
    when(mockExternalLink.getExpiration()).thenReturn("2025-12-31T23:59:59Z");

    ArrowResultChunk realChunk =
        ArrowResultChunk.builder()
            .withStatementId(statementId)
            .withChunkMetadata(7L, 100L, 0L)
            .withChunkStatus(ChunkStatus.URL_FETCHED)
            .withChunkReadyTimeoutSeconds(30)
            .build();

    realChunk.setChunkLink(mockExternalLink);

    // Track status changes
    List<ChunkStatus> statusHistory = new ArrayList<>();
    ArrowResultChunk spiedChunk = spy(realChunk);
    doAnswer(
            invocation -> {
              ChunkStatus status = invocation.getArgument(0);
              statusHistory.add(status);
              invocation.callRealMethod();
              return null;
            })
        .when(spiedChunk)
        .setStatus(any(ChunkStatus.class));

    // Mock the initializeData method to avoid Arrow parsing issues
    doAnswer(
            invocation -> {
              spiedChunk.setStatus(ChunkStatus.PROCESSING_SUCCEEDED);
              return null;
            })
        .when(spiedChunk)
        .initializeData(any(InputStream.class));

    // Mock HTTP client to fail twice, then succeed
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    HttpEntity mockEntity = mock(HttpEntity.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getEntity()).thenReturn(mockEntity);

    byte[] validArrowData = new byte[] {1, 2, 3, 4, 5};
    AtomicInteger httpCallCount = new AtomicInteger(0);
    when(httpClient.execute(any(HttpGet.class), eq(true)))
        .thenAnswer(
            invocation -> {
              int callNumber = httpCallCount.incrementAndGet();
              if (callNumber <= 2) {
                throw new SocketException("Connection reset by peer");
              } else {
                when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(validArrowData));
                return mockResponse;
              }
            });

    // Mock linkFetcher.refetchLink() in case the chunk link becomes invalid during retries
    when(linkFetcher.refetchLink(anyLong(), anyLong())).thenReturn(mockExternalLink);

    // Create task with the spied chunk
    StreamingChunkDownloadTask task =
        new StreamingChunkDownloadTask(
            spiedChunk,
            httpClient,
            CompressionCodec.NONE,
            linkFetcher,
            CLOUD_FETCH_SPEED_THRESHOLD);

    // Execute the task
    assertDoesNotThrow(task::call);

    // Verify HTTP client was called 3 times (2 failures + 1 success)
    verify(httpClient, times(3)).execute(any(HttpGet.class), eq(true));

    // Verify status progression includes retries
    assertTrue(statusHistory.contains(ChunkStatus.DOWNLOAD_FAILED));
    assertTrue(statusHistory.contains(ChunkStatus.DOWNLOAD_RETRY));
    assertTrue(statusHistory.contains(ChunkStatus.DOWNLOAD_SUCCEEDED));
    assertEquals(ChunkStatus.PROCESSING_SUCCEEDED, spiedChunk.getStatus());

    // Verify the future completed successfully
    CompletableFuture<Void> chunkFuture = spiedChunk.getChunkReadyFuture();
    assertTrue(chunkFuture.isDone());
    assertDoesNotThrow(() -> chunkFuture.get());
  }
}
