package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.net.SocketException;
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
  private ChunkDownloadTask chunkDownloadTask;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    chunkDownloadTask = new ChunkDownloadTask(chunk, httpClient, remoteChunkProvider);
  }

  @Test
  void testRetryLogicWithSocketException() throws Exception {
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
        .downloadData(httpClient, CompressionCodec.NONE);

    chunkDownloadTask.call();

    verify(chunk, times(3)).downloadData(httpClient, CompressionCodec.NONE);
    verify(remoteChunkProvider, times(1)).downloadProcessed(7L);
  }

  @Test
  void testRetryLogicExhaustedWithSocketException() throws Exception {
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
        .downloadData(httpClient, CompressionCodec.NONE);

    assertThrows(DatabricksSQLException.class, () -> chunkDownloadTask.call());
    verify(chunk, times(ChunkDownloadTask.MAX_RETRIES))
        .downloadData(httpClient, CompressionCodec.NONE);
    verify(remoteChunkProvider, times(1)).downloadProcessed(7L);
  }
}
