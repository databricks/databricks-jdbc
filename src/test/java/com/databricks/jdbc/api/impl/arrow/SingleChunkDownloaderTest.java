package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.net.SocketException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SingleChunkDownloaderTest {
  @Mock ArrowResultChunk chunk;
  @Mock IDatabricksHttpClient httpClient;
  @Mock ChunkDownloader chunkDownloader;
  private SingleChunkDownloader singleChunkDownloader;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    singleChunkDownloader = new SingleChunkDownloader(chunk, httpClient, chunkDownloader);
  }

  @Test
  void testRetryLogicWithSocketException() throws Exception {
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    when(chunk.getChunkIndex()).thenReturn(7L);

    // Simulate SocketException for the first two attempts, then succeed
    doThrow(
            new DatabricksParsingException(
                "Connection reset", new SocketException("Connection reset")))
        .doThrow(
            new DatabricksParsingException(
                "Connection reset", new SocketException("Connection reset")))
        .doNothing()
        .when(chunk)
        .downloadData(httpClient);

    singleChunkDownloader.call();

    verify(chunk, times(3)).downloadData(httpClient);
    verify(chunkDownloader, times(1)).downloadProcessed(7L);
  }

  @Test
  void testRetryLogicExhaustedWithSocketException() throws Exception {
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    when(chunk.getChunkIndex()).thenReturn(7L);

    // Simulate SocketException for all attempts
    doThrow(
            new DatabricksParsingException(
                "Connection reset", new SocketException("Connection reset")))
        .when(chunk)
        .downloadData(httpClient);

    assertThrows(DatabricksSQLException.class, () -> singleChunkDownloader.call());
    verify(chunk, times(SingleChunkDownloader.MAX_RETRIES)).downloadData(httpClient);
    verify(chunkDownloader, times(1)).downloadProcessed(7L);
  }
}
