package com.databricks.jdbc.api.impl.arrow.incubator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.arrow.ChunkStatus;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import java.io.IOException;
import java.net.SocketException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ArrowResultChunkV2Test {

  @Mock private IDatabricksHttpClient mockHttpClient;
  @Mock private TSparkArrowResultLink mockThriftChunkInfo;
  private ArrowResultChunkV2 chunk;

  @BeforeEach
  void setUp() {
    // Setup mock thrift chunk info
    when(mockThriftChunkInfo.getRowCount()).thenReturn(100L);
    when(mockThriftChunkInfo.getStartRowOffset()).thenReturn(0L);
    when(mockThriftChunkInfo.getExpiryTime())
        .thenReturn(Instant.now().plusSeconds(300).toEpochMilli());
    when(mockThriftChunkInfo.getFileLink()).thenReturn("https://test-url.com");

    // Create chunk instance using builder with thrift info
    chunk =
        ArrowResultChunkV2.builder()
            .statementId("test-statement-123")
            .withThriftChunkInfo(1L, mockThriftChunkInfo)
            .build();
  }

  @Test
  void testSuccessfulDownload() throws InterruptedException {
    byte[] mockData = "test data".getBytes();
    setupSuccessfulDownload(mockData);

    chunk.downloadData(mockHttpClient, CompressionCodec.NONE);

    // Wait a bit for async processing to complete
    TimeUnit.SECONDS.sleep(1);

    // Assert
    verify(mockHttpClient).executeAsync(any(), any(), any());

    // Successful download but processing failed because of invalid arrow data
    assertEquals(ChunkStatus.PROCESSING_FAILED, chunk.getStatus());
  }

  @Test
  void testRetryOnSocketException() throws InterruptedException {
    setupFailedDownloadWithRetry(new SocketException("Connection reset"));

    chunk.downloadData(mockHttpClient, CompressionCodec.NONE);

    // Wait a bit for retry
    TimeUnit.SECONDS.sleep(2);

    // Assert
    verify(mockHttpClient, times(2)).executeAsync(any(), any(), any());
    assertEquals(ChunkStatus.DOWNLOAD_RETRY, chunk.getStatus());
  }

  @Test
  void testMaxRetriesExceeded() throws InterruptedException {
    setupFailedDownloadWithMaxRetries(new SocketException("Connection reset"));

    chunk.downloadData(mockHttpClient, CompressionCodec.NONE);

    // Wait for all retries to complete
    TimeUnit.SECONDS.sleep(5);

    // Assert
    verify(mockHttpClient, times(3)).executeAsync(any(), any(), any());
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
  }

  @Test
  void testNonRetryableError() {
    setupFailedDownload(new IOException("Non-retryable error"));

    chunk.downloadData(mockHttpClient, CompressionCodec.NONE);

    // Assert
    verify(mockHttpClient, times(1)).executeAsync(any(), any(), any());
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
  }

  @Test
  void testChunkLinkInvalid() {
    // Create chunk with soon-to-expire link
    when(mockThriftChunkInfo.getExpiryTime())
        .thenReturn(Instant.now().minusSeconds(120).toEpochMilli());
    ArrowResultChunkV2 expiredChunk =
        ArrowResultChunkV2.builder()
            .statementId("test-statement-123")
            .withThriftChunkInfo(1L, mockThriftChunkInfo)
            .build();

    assertTrue(expiredChunk.isChunkLinkInvalid());
  }

  @Test
  void testReleaseChunk() throws InterruptedException {
    byte[] mockData = "test data".getBytes();
    setupSuccessfulDownload(mockData);
    chunk.downloadData(mockHttpClient, CompressionCodec.NONE);

    // Wait a bit for async processing to complete
    TimeUnit.SECONDS.sleep(1);

    boolean result = chunk.releaseChunk();

    assertTrue(result);
    assertEquals(ChunkStatus.CHUNK_RELEASED, chunk.getStatus());
  }

  @Test
  void testDoubleRelease() throws InterruptedException {
    byte[] mockData = "test data".getBytes();
    setupSuccessfulDownload(mockData);
    chunk.downloadData(mockHttpClient, CompressionCodec.NONE);

    // Wait a bit for async processing to complete
    TimeUnit.SECONDS.sleep(1);

    chunk.releaseChunk();

    boolean result = chunk.releaseChunk();

    assertFalse(result);
    assertEquals(ChunkStatus.CHUNK_RELEASED, chunk.getStatus());
  }

  @Test
  void testCancellation() {
    doAnswer(
            invocation -> {
              FutureCallback<byte[]> callback = invocation.getArgument(2);
              callback.cancelled();
              return null;
            })
        .when(mockHttpClient)
        .executeAsync(any(), any(), any());

    chunk.downloadData(mockHttpClient, CompressionCodec.NONE);

    assertEquals(ChunkStatus.CANCELLED, chunk.getStatus());
  }

  // Helper methods for setting up mock behaviors
  private void setupSuccessfulDownload(byte[] data) {
    doAnswer(
            invocation -> {
              FutureCallback<byte[]> callback = invocation.getArgument(2);
              callback.completed(data);
              return null;
            })
        .when(mockHttpClient)
        .executeAsync(
            any(AsyncRequestProducer.class),
            any(AsyncResponseConsumer.class),
            any(FutureCallback.class));
  }

  private void setupFailedDownload(Exception error) {
    doAnswer(
            invocation -> {
              FutureCallback<byte[]> callback = invocation.getArgument(2);
              callback.failed(error);
              return null;
            })
        .when(mockHttpClient)
        .executeAsync(
            any(AsyncRequestProducer.class),
            any(AsyncResponseConsumer.class),
            any(FutureCallback.class));
  }

  private void setupFailedDownloadWithRetry(Exception error) {
    doAnswer(
            invocation -> {
              FutureCallback<byte[]> callback = invocation.getArgument(2);
              callback.failed(error);
              return null;
            })
        .when(mockHttpClient)
        .executeAsync(
            any(AsyncRequestProducer.class),
            any(AsyncResponseConsumer.class),
            any(FutureCallback.class));
  }

  private void setupFailedDownloadWithMaxRetries(Exception error) {
    doAnswer(
            invocation -> {
              FutureCallback<byte[]> callback = invocation.getArgument(2);
              callback.failed(error);
              return null;
            })
        .when(mockHttpClient)
        .executeAsync(
            any(AsyncRequestProducer.class),
            any(AsyncResponseConsumer.class),
            any(FutureCallback.class));
  }
}
