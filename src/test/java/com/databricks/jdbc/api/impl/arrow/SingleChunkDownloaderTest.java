package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SingleChunkDownloaderTest {
  @Mock ArrowResultChunk chunk;
  @Mock IDatabricksHttpClient httpClient;
  @Mock ChunkDownloader chunkDownloader;

  @Test
  void testCallThrowsError() throws DatabricksSQLException, IOException {
    SingleChunkDownloader singleChunkDownloader =
        new SingleChunkDownloader(chunk, httpClient, chunkDownloader);
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    doThrow(new DatabricksHttpException(TEST_STRING)).when(chunk).downloadData(httpClient);
    Assertions.assertDoesNotThrow(() -> singleChunkDownloader.call());

    doThrow(new DatabricksParsingException(TEST_STRING)).when(chunk).downloadData(httpClient);
    Assertions.assertDoesNotThrow(() -> singleChunkDownloader.call());

    doThrow(new IOException(TEST_STRING)).when(chunk).downloadData(httpClient);
    Assertions.assertDoesNotThrow(() -> singleChunkDownloader.call());
  }
}
