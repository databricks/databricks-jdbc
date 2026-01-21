package com.databricks.jdbc.api.impl.streaming;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.ColumnarRowView;
import com.databricks.jdbc.api.impl.thrift.ThriftBatchFetcher;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for streaming-specific behavior that doesn't exist in the lazy implementations. These tests
 * cover the sliding window, batch release, and prefetch error handling.
 */
@ExtendWith(MockitoExtension.class)
public class ThriftStreamingProviderTest {

  @Mock private ThriftBatchFetcher batchFetcher;

  @Test
  void testSingleBatchNoMoreRows() throws DatabricksSQLException {
    TFetchResultsResp response = createResponseWithStringData(2, false);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, response, 3, 30);

    try {
      // Move to first batch
      assertTrue(provider.hasNextBatch());
      assertTrue(provider.nextBatch());

      // Initial batch has data
      StreamingBatch<ColumnarRowView> batch = provider.getCurrentBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertFalse(batch.hasMoreRows());

      // No more batches since hasMoreRows was false
      assertFalse(provider.hasNextBatch());
      assertFalse(provider.nextBatch());

      // Should be at end of stream
      assertTrue(provider.isEndOfStreamReached());
    } finally {
      provider.close();
    }
  }

  @Test
  void testSlidingWindowBoundsMemory() throws DatabricksSQLException, InterruptedException {
    int maxBatchesInMemory = 3;

    // Initial batch
    TFetchResultsResp initialResponse = createResponseWithStringData(2, true);

    // Subsequent batches
    TFetchResultsResp batch2 = createResponseWithStringData(2, true);
    TFetchResultsResp batch3 = createResponseWithStringData(2, true);
    TFetchResultsResp batch4 = createResponseWithStringData(2, true);
    TFetchResultsResp batch5 = createResponseWithStringData(2, false);

    when(batchFetcher.fetchNextBatch())
        .thenReturn(batch2)
        .thenReturn(batch3)
        .thenReturn(batch4)
        .thenReturn(batch5);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, maxBatchesInMemory, 30);

    try {
      // Let prefetch thread run a bit
      Thread.sleep(100);

      // Provider should never have more than maxBatchesInMemory batches
      assertTrue(
          provider.getBatchesInMemory() <= maxBatchesInMemory,
          "Batches in memory ("
              + provider.getBatchesInMemory()
              + ") should not exceed max ("
              + maxBatchesInMemory
              + ")");

      // Consume batches and verify memory is bounded
      provider.nextBatch(); // batch 0
      Thread.sleep(50);
      assertTrue(provider.getBatchesInMemory() <= maxBatchesInMemory);

      provider.nextBatch(); // batch 1 (releases batch 0)
      Thread.sleep(50);
      assertTrue(provider.getBatchesInMemory() <= maxBatchesInMemory);

      provider.nextBatch(); // batch 2 (releases batch 1)
      Thread.sleep(50);
      assertTrue(provider.getBatchesInMemory() <= maxBatchesInMemory);

    } finally {
      provider.close();
    }
  }

  @Test
  void testEmptyBatchesSkippedByNextBatch() throws DatabricksSQLException {
    // Initial response with empty batch
    TFetchResultsResp emptyBatch = createEmptyResponse(true);

    // Second batch also empty
    TFetchResultsResp emptyBatch2 = createEmptyResponse(true);

    // Third batch has data
    TFetchResultsResp dataBatch = createResponseWithStringData(3, false);

    when(batchFetcher.fetchNextBatch()).thenReturn(emptyBatch2).thenReturn(dataBatch);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, emptyBatch, 3, 30);

    try {
      // nextBatch should skip empty batches and return when it finds data
      assertTrue(provider.nextBatch());

      StreamingBatch<ColumnarRowView> batch = provider.getCurrentBatch();
      assertNotNull(batch);
      assertTrue(batch.getRowCount() > 0);
      assertEquals(3, batch.getRowCount());
    } finally {
      provider.close();
    }
  }

  @Test
  void testPrefetchErrorPropagated() throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(2, true);

    DatabricksSQLException fetchError =
        new DatabricksSQLException("Network failure", DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(batchFetcher.fetchNextBatch()).thenThrow(fetchError);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      // Move to initial batch
      provider.nextBatch();

      // Give prefetch thread time to fail
      Thread.sleep(100);

      // Next call should propagate the error
      DatabricksSQLException thrown =
          assertThrows(DatabricksSQLException.class, provider::nextBatch);
      assertTrue(thrown.getMessage().contains("Prefetch failed"));
    } finally {
      provider.close();
    }
  }

  @Test
  void testCloseStopsPrefetchAndClosesFetcher()
      throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(2, true);
    TFetchResultsResp batch2 = createResponseWithStringData(2, false);

    when(batchFetcher.fetchNextBatch()).thenReturn(batch2);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    // Let prefetch run
    Thread.sleep(100);

    assertTrue(provider.getBatchesInMemory() > 0);

    provider.close();

    // Verify fetcher was closed
    verify(batchFetcher).close();

    // After close, hasNextBatch should return false
    assertFalse(provider.hasNextBatch());
  }

  @Test
  void testNullInputValidation() {
    TFetchResultsResp validResponse = createResponseWithStringData(1, false);

    // Null response
    assertThrows(
        IllegalArgumentException.class,
        () -> ThriftStreamingProvider.forColumnar(batchFetcher, null, 3, 30));

    // Null fetcher
    assertThrows(
        IllegalArgumentException.class,
        () -> ThriftStreamingProvider.forColumnar(null, validResponse, 3, 30));
  }

  // ==================== Helper Methods ====================

  private TFetchResultsResp createEmptyResponse(boolean hasMoreRows) {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = hasMoreRows;
    TRowSet emptyRowSet = new TRowSet();
    emptyRowSet.setColumns(Collections.emptyList());
    response.setResults(emptyRowSet);
    return response;
  }

  private TFetchResultsResp createResponseWithStringData(int rowCount, boolean hasMoreRows) {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = hasMoreRows;

    TRowSet rowSet = new TRowSet();
    List<TColumn> columns = new ArrayList<>();

    TColumn column = new TColumn();
    TStringColumn stringCol = new TStringColumn();
    List<String> values = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      values.add("value_" + i);
    }
    stringCol.setValues(values);
    column.setStringVal(stringCol);
    columns.add(column);

    rowSet.setColumns(columns);
    response.setResults(rowSet);
    return response;
  }
}
