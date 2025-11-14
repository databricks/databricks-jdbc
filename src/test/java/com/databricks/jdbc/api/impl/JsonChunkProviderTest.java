package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JsonChunkProviderTest {

  @Mock private IDatabricksSession mockSession;
  @Mock private IDatabricksClient mockClient;
  @Mock private StatementId mockStatementId;

  private ResultManifest resultManifest;
  private ResultData initialResultData;

  @BeforeEach
  void setUp() {
    when(mockStatementId.toString()).thenReturn("test-statement-id");

    // Setup test data
    resultManifest = new ResultManifest();
    initialResultData = new ResultData();
  }

  @Test
  void testSingleChunkProvider() throws DatabricksSQLException {
    // Setup single chunk scenario
    resultManifest.setTotalChunkCount(1L);

    Collection<Collection<String>> dataArray =
        Arrays.asList(Arrays.asList("row1col1", "row1col2"), Arrays.asList("row2col1", "row2col2"));
    initialResultData.setDataArray(dataArray);
    initialResultData.setChunkIndex(0L);

    // Create provider
    JsonChunkProvider provider =
        new JsonChunkProvider(resultManifest, initialResultData, mockStatementId, mockSession);

    // Verify results
    List<List<Object>> allData = provider.getAllData();
    assertEquals(2, allData.size());
    assertEquals(1, provider.getChunkCount());

    assertEquals("row1col1", allData.get(0).get(0));
    assertEquals("row1col2", allData.get(0).get(1));
    assertEquals("row2col1", allData.get(1).get(0));
    assertEquals("row2col2", allData.get(1).get(1));

    // Verify no additional API calls were made for single chunk
    verify(mockClient, never()).getResultChunksData(any(), anyLong());
  }

  @Test
  void testMultiChunkProvider() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);
    // Setup multi-chunk scenario
    resultManifest.setTotalChunkCount(3L);

    // First chunk data (chunk 0)
    Collection<Collection<String>> chunk0Data =
        Arrays.asList(
            Arrays.asList("chunk0row1col1", "chunk0row1col2"),
            Arrays.asList("chunk0row2col1", "chunk0row2col2"));
    initialResultData.setDataArray(chunk0Data);
    initialResultData.setChunkIndex(0L);

    // Second chunk data (chunk 1)
    ResultData chunk1Data = new ResultData();
    Collection<Collection<String>> chunk1DataArray =
        Arrays.asList(Arrays.asList("chunk1row1col1", "chunk1row1col2"));
    chunk1Data.setDataArray(chunk1DataArray);
    chunk1Data.setChunkIndex(1L);

    // Third chunk data (chunk 2)
    ResultData chunk2Data = new ResultData();
    Collection<Collection<String>> chunk2DataArray =
        Arrays.asList(
            Arrays.asList("chunk2row1col1", "chunk2row1col2"),
            Arrays.asList("chunk2row2col1", "chunk2row2col2"),
            Arrays.asList("chunk2row3col1", "chunk2row3col2"));
    chunk2Data.setDataArray(chunk2DataArray);
    chunk2Data.setChunkIndex(2L);

    // Mock client responses
    when(mockClient.getResultChunksData(mockStatementId, 1L)).thenReturn(chunk1Data);
    when(mockClient.getResultChunksData(mockStatementId, 2L)).thenReturn(chunk2Data);

    // Create provider
    JsonChunkProvider provider =
        new JsonChunkProvider(resultManifest, initialResultData, mockStatementId, mockSession);

    // Verify results
    List<List<Object>> allData = provider.getAllData();
    assertEquals(6, allData.size()); // 2 + 1 + 3 = 6 total rows
    assertEquals(3, provider.getChunkCount());

    // Verify chunk 0 data
    assertEquals("chunk0row1col1", allData.get(0).get(0));
    assertEquals("chunk0row1col2", allData.get(0).get(1));
    assertEquals("chunk0row2col1", allData.get(1).get(0));
    assertEquals("chunk0row2col2", allData.get(1).get(1));

    // Verify chunk 1 data
    assertEquals("chunk1row1col1", allData.get(2).get(0));
    assertEquals("chunk1row1col2", allData.get(2).get(1));

    // Verify chunk 2 data
    assertEquals("chunk2row1col1", allData.get(3).get(0));
    assertEquals("chunk2row1col2", allData.get(3).get(1));
    assertEquals("chunk2row2col1", allData.get(4).get(0));
    assertEquals("chunk2row2col2", allData.get(4).get(1));
    assertEquals("chunk2row3col1", allData.get(5).get(0));
    assertEquals("chunk2row3col2", allData.get(5).get(1));

    // Verify API calls were made for chunks 1 and 2
    verify(mockClient).getResultChunksData(mockStatementId, 1L);
    verify(mockClient).getResultChunksData(mockStatementId, 2L);
    verify(mockClient, never())
        .getResultChunksData(mockStatementId, 0L); // First chunk not fetched via API
  }

  @Test
  void testProviderClose() throws DatabricksSQLException {
    resultManifest.setTotalChunkCount(1L);
    initialResultData.setDataArray(Arrays.asList(Arrays.asList("test")));

    JsonChunkProvider provider =
        new JsonChunkProvider(resultManifest, initialResultData, mockStatementId, mockSession);

    assertFalse(provider.isClosed());
    assertEquals(1, provider.getAllData().size());

    provider.close();

    assertTrue(provider.isClosed());
    assertEquals(0, provider.getAllData().size());
  }

  @Test
  void testNullDataHandling() throws DatabricksSQLException {
    resultManifest.setTotalChunkCount(1L);
    initialResultData.setDataArray(null);

    JsonChunkProvider provider =
        new JsonChunkProvider(resultManifest, initialResultData, mockStatementId, mockSession);

    assertEquals(0, provider.getAllData().size());
    assertEquals(1, provider.getChunkCount());
    assertTrue(provider.getAllData().isEmpty());
  }

  @Test
  void testEmptyDataHandling() throws DatabricksSQLException {
    resultManifest.setTotalChunkCount(1L);
    initialResultData.setDataArray(Collections.emptyList());

    JsonChunkProvider provider =
        new JsonChunkProvider(resultManifest, initialResultData, mockStatementId, mockSession);

    assertEquals(0, provider.getAllData().size());
    assertEquals(1, provider.getChunkCount());
    assertTrue(provider.getAllData().isEmpty());
  }

  @Test
  void testNullRowHandling() throws DatabricksSQLException {
    resultManifest.setTotalChunkCount(1L);

    // Data array with null rows
    Collection<Collection<String>> dataArray =
        Arrays.asList(
            Arrays.asList("row1col1", "row1col2"),
            null, // Null row
            Arrays.asList("row3col1", "row3col2"));
    initialResultData.setDataArray(dataArray);

    JsonChunkProvider provider =
        new JsonChunkProvider(resultManifest, initialResultData, mockStatementId, mockSession);

    List<List<Object>> allData = provider.getAllData();
    assertEquals(3, allData.size());

    // First row
    assertEquals("row1col1", allData.get(0).get(0));
    assertEquals("row1col2", allData.get(0).get(1));

    // Null row becomes empty list
    assertTrue(allData.get(1).isEmpty());

    // Third row
    assertEquals("row3col1", allData.get(2).get(0));
    assertEquals("row3col2", allData.get(2).get(1));
  }

  @Test
  void testChunkFetchFailure() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);
    // Setup multi-chunk scenario
    resultManifest.setTotalChunkCount(2L);

    Collection<Collection<String>> chunk0Data =
        Arrays.asList(Arrays.asList("chunk0row1col1", "chunk0row1col2"));
    initialResultData.setDataArray(chunk0Data);
    initialResultData.setChunkIndex(0L);

    // Mock client to throw exception for chunk 1
    DatabricksSQLException chunkException =
        new DatabricksSQLException("Network error", DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(mockClient.getResultChunksData(mockStatementId, 1L)).thenThrow(chunkException);

    // Creating provider should throw exception
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> {
              new JsonChunkProvider(
                  resultManifest, initialResultData, mockStatementId, mockSession);
            });

    assertTrue(exception.getMessage().contains("Failed to fetch chunk 1"));
    assertEquals(
        DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.toString(), exception.getSQLState());
    assertSame(chunkException, exception.getCause());
  }

  @Test
  void testZeroChunkCount() throws DatabricksSQLException {
    resultManifest.setTotalChunkCount(0L);
    initialResultData.setDataArray(Arrays.asList(Arrays.asList("test")));

    JsonChunkProvider provider =
        new JsonChunkProvider(resultManifest, initialResultData, mockStatementId, mockSession);

    // Should still process the initial data even with 0 chunk count
    assertEquals(1, provider.getAllData().size());
    assertEquals(0, provider.getChunkCount());

    // No additional API calls should be made
    verify(mockClient, never()).getResultChunksData(any(), anyLong());
  }

  @Test
  void testChunkDataWithNullDataArray() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockClient);
    resultManifest.setTotalChunkCount(2L);
    initialResultData.setDataArray(Arrays.asList(Arrays.asList("chunk0")));

    // Mock client to return chunk with null data array
    ResultData chunk1Data = new ResultData();
    chunk1Data.setDataArray(null);
    when(mockClient.getResultChunksData(mockStatementId, 1L)).thenReturn(chunk1Data);

    JsonChunkProvider provider =
        new JsonChunkProvider(resultManifest, initialResultData, mockStatementId, mockSession);

    // Should handle null data array gracefully
    assertEquals(1, provider.getAllData().size()); // Only initial chunk data
    assertEquals("chunk0", provider.getAllData().get(0).get(0));
  }
}
