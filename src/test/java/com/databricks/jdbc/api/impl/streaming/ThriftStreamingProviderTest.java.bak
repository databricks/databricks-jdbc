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

    // The prefetch thread starts during construction and may fail before or after
    // the first nextBatch() call. We need to handle both cases.
    DatabricksSQLException caughtException = null;
    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      // Consume batches until we hit the error
      while (provider.nextBatch()) {
        // Keep iterating - error will be thrown when we need a batch that failed to prefetch
      }
    } catch (DatabricksSQLException e) {
      caughtException = e;
    } finally {
      provider.close();
    }

    // Verify we caught the expected error
    assertNotNull(caughtException, "Expected DatabricksSQLException to be thrown");
    assertTrue(
        caughtException.getMessage().contains("Prefetch failed"),
        "Exception should contain 'Prefetch failed': " + caughtException.getMessage());
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

  @Test
  void testWaitForBatchCreationTimeout() throws DatabricksSQLException {
    // Initial batch with hasMoreRows=true so prefetch thread starts
    TFetchResultsResp initialResponse = createResponseWithStringData(1, true);

    // Mock fetcher to block indefinitely (simulating slow server)
    when(batchFetcher.fetchNextBatch())
        .thenAnswer(
            inv -> {
              Thread.sleep(10000); // Sleep longer than timeout
              return createResponseWithStringData(1, false);
            });

    // Use very short timeout (1 second) to trigger timeout quickly
    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 1);

    try {
      // Consume initial batch
      provider.nextBatch();

      // Try to get next batch - should timeout waiting for batch creation
      DatabricksSQLException thrown =
          assertThrows(DatabricksSQLException.class, provider::nextBatch);
      assertTrue(
          thrown.getMessage().contains("Timeout") || thrown.getMessage().contains("timeout"),
          "Expected timeout error but got: " + thrown.getMessage());
    } finally {
      provider.close();
    }
  }

  @Test
  void testWaitForBatchInterrupted() throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(1, true);

    // Mock fetcher to block
    when(batchFetcher.fetchNextBatch())
        .thenAnswer(
            inv -> {
              Thread.sleep(10000);
              return createResponseWithStringData(1, false);
            });

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    // Start a thread that will try to get next batch
    Thread testThread =
        new Thread(
            () -> {
              try {
                provider.nextBatch(); // Initial batch
                provider.nextBatch(); // Will block waiting for batch 1
              } catch (DatabricksSQLException e) {
                // Expected - interrupted
                assertTrue(
                    e.getMessage().contains("Interrupt") || e.getMessage().contains("interrupt"));
              }
            });

    try {
      testThread.start();
      Thread.sleep(100); // Let the thread start waiting

      // Interrupt the waiting thread
      testThread.interrupt();
      testThread.join(2000);

      assertFalse(testThread.isAlive(), "Test thread should have terminated");
    } finally {
      provider.close();
    }
  }

  @Test
  void testGetCurrentBatchWaitsForReady() throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(2, true);
    TFetchResultsResp batch2 = createResponseWithStringData(2, false);

    // Add a small delay to batch fetching
    when(batchFetcher.fetchNextBatch())
        .thenAnswer(
            inv -> {
              Thread.sleep(50);
              return batch2;
            });

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      // Move to initial batch
      assertTrue(provider.nextBatch());
      StreamingBatch<ColumnarRowView> batch = provider.getCurrentBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());

      // Move to next batch - should wait for it to be ready
      assertTrue(provider.nextBatch());
      batch = provider.getCurrentBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
    } finally {
      provider.close();
    }
  }

  @Test
  void testCloseWhileWaiting() throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(1, true);

    // Mock fetcher to block indefinitely
    when(batchFetcher.fetchNextBatch())
        .thenAnswer(
            inv -> {
              Thread.sleep(10000);
              return createResponseWithStringData(1, false);
            });

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    // Start a thread that will wait for next batch
    Thread waitingThread =
        new Thread(
            () -> {
              try {
                provider.nextBatch();
                provider.nextBatch(); // Will block
              } catch (DatabricksSQLException e) {
                // Expected after close
              }
            });

    try {
      waitingThread.start();
      Thread.sleep(100); // Let thread start waiting

      // Close should unblock the waiting thread
      provider.close();

      waitingThread.join(2000);
      assertFalse(waitingThread.isAlive(), "Waiting thread should have terminated after close");
    } finally {
      if (waitingThread.isAlive()) {
        waitingThread.interrupt();
      }
    }
  }

  @Test
  void testGetCurrentBatchBeforeFirstBatch() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createResponseWithStringData(2, false);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      // Before calling nextBatch(), currentBatchIndex is -1
      // getCurrentBatch should return null
      StreamingBatch<ColumnarRowView> batch = provider.getCurrentBatch();
      assertNull(batch, "getCurrentBatch should return null before first nextBatch()");
    } finally {
      provider.close();
    }
  }

  @Test
  void testGetCurrentBatchAfterNextBatch() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createResponseWithStringData(2, false);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      // Move to first batch
      assertTrue(provider.nextBatch());

      // getCurrentBatch should return the batch
      StreamingBatch<ColumnarRowView> batch = provider.getCurrentBatch();
      assertNotNull(batch);
      assertEquals(0, batch.getBatchIndex());
      assertEquals(2, batch.getRowCount());
      assertTrue(batch.isReady());
    } finally {
      provider.close();
    }
  }

  @Test
  void testGetCurrentBatchWithPrefetchError() throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(1, true);

    DatabricksSQLException fetchError =
        new DatabricksSQLException("Server error", DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(batchFetcher.fetchNextBatch()).thenThrow(fetchError);

    // The prefetch thread starts during construction and may fail at any point.
    DatabricksSQLException caughtException = null;
    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      // Move to initial batch
      provider.nextBatch();

      // Let prefetch thread fail
      Thread.sleep(100);

      // Either nextBatch or getCurrentBatch should propagate the prefetch error
      while (provider.nextBatch()) {
        provider.getCurrentBatch();
      }
    } catch (DatabricksSQLException e) {
      caughtException = e;
    } finally {
      provider.close();
    }

    // Verify we caught the expected error
    assertNotNull(caughtException, "Expected DatabricksSQLException to be thrown");
    assertTrue(
        caughtException.getMessage().contains("Prefetch failed"),
        "Exception should contain 'Prefetch failed': " + caughtException.getMessage());
  }

  @Test
  void testGetCurrentBatchMultipleCalls() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createResponseWithStringData(2, false);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      provider.nextBatch();

      // Multiple calls to getCurrentBatch should return same batch
      StreamingBatch<ColumnarRowView> batch1 = provider.getCurrentBatch();
      StreamingBatch<ColumnarRowView> batch2 = provider.getCurrentBatch();
      StreamingBatch<ColumnarRowView> batch3 = provider.getCurrentBatch();

      assertSame(batch1, batch2);
      assertSame(batch2, batch3);
    } finally {
      provider.close();
    }
  }

  @Test
  void testBatchReadyTimeout() throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(1, true);

    // Create a batch that will never become ready by making fetcher very slow
    when(batchFetcher.fetchNextBatch())
        .thenAnswer(
            inv -> {
              // Simulate a batch that takes too long
              Thread.sleep(5000);
              return createResponseWithStringData(1, false);
            });

    // Use 1 second timeout
    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 1);

    try {
      // Consume initial batch
      provider.nextBatch();

      // Try to move to next batch - should timeout
      DatabricksSQLException thrown =
          assertThrows(DatabricksSQLException.class, provider::nextBatch);
      assertTrue(
          thrown.getMessage().toLowerCase().contains("timeout"),
          "Expected timeout error: " + thrown.getMessage());
    } finally {
      provider.close();
    }
  }

  @Test
  void testTotalRowsFetched() throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(3, true);
    TFetchResultsResp batch2 = createResponseWithStringData(5, false);

    when(batchFetcher.fetchNextBatch()).thenReturn(batch2);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      // Initial batch has at least 3 rows (prefetch may have already fetched more)
      assertTrue(provider.getTotalRowsFetched() >= 3);

      // Let prefetch run
      Thread.sleep(100);

      // After prefetch, should have 8 total rows (3 + 5)
      assertEquals(8, provider.getTotalRowsFetched());
    } finally {
      provider.close();
    }
  }

  @Test
  void testBatchesInMemoryCount() throws DatabricksSQLException, InterruptedException {
    TFetchResultsResp initialResponse = createResponseWithStringData(1, true);
    TFetchResultsResp batch2 = createResponseWithStringData(1, true);
    TFetchResultsResp batch3 = createResponseWithStringData(1, false);

    when(batchFetcher.fetchNextBatch()).thenReturn(batch2).thenReturn(batch3);

    ThriftStreamingProvider<ColumnarRowView> provider =
        ThriftStreamingProvider.forColumnar(batchFetcher, initialResponse, 3, 30);

    try {
      // Initial: at least 1 batch in memory (prefetch may have already fetched more)
      assertTrue(provider.getBatchesInMemory() >= 1);

      // Let prefetch run
      Thread.sleep(100);

      // After prefetch of 2 more batches (3 total), should be within bounds
      assertTrue(provider.getBatchesInMemory() >= 1);
      assertTrue(provider.getBatchesInMemory() <= 3);
    } finally {
      provider.close();
    }
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
