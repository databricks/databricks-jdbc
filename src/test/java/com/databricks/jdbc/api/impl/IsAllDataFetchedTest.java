package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.arrow.*;
import org.junit.jupiter.api.Test;

/**
 * Tests for isAllDataFetched() across all IExecutionResult and ChunkProvider implementations.
 * Verifies that heartbeat stops proactively when all data has been fetched from the server.
 */
public class IsAllDataFetchedTest {

  // =========================================================================
  // IExecutionResult default
  // =========================================================================

  @Test
  void testDefaultIsAllDataFetched_returnsFalse() {
    // The default implementation returns false (conservative)
    IExecutionResult result = mock(IExecutionResult.class, CALLS_REAL_METHODS);
    assertFalse(result.isAllDataFetched());
  }

  // =========================================================================
  // InlineJsonResult — always true
  // =========================================================================

  @Test
  void testInlineJsonResult_alwaysTrue() {
    // InlineJsonResult fetches all data at construction
    InlineJsonResult result = new InlineJsonResult(new Object[][] {{1, "a"}, {2, "b"}});
    assertTrue(result.isAllDataFetched());
  }

  // =========================================================================
  // InlineChunkProvider — always true
  // =========================================================================

  @Test
  void testInlineChunkProvider_alwaysTrue() {
    InlineChunkProvider provider = mock(InlineChunkProvider.class, CALLS_REAL_METHODS);
    assertTrue(provider.isAllDataFetched());
  }

  // =========================================================================
  // AbstractRemoteChunkProvider — based on nextChunkToDownload vs chunkCount
  // =========================================================================

  @Test
  void testRemoteChunkProvider_notAllDownloaded() {
    // Create a concrete subclass mock to test the base class logic
    AbstractRemoteChunkProvider<?> provider =
        mock(AbstractRemoteChunkProvider.class, CALLS_REAL_METHODS);
    // Default fields are 0, so nextChunkToDownload (0) < chunkCount (0) is false
    // Let's set chunkCount > nextChunkToDownload
    try {
      java.lang.reflect.Field chunkCountField =
          AbstractRemoteChunkProvider.class.getDeclaredField("chunkCount");
      chunkCountField.setAccessible(true);
      chunkCountField.set(provider, 5L);

      java.lang.reflect.Field nextField =
          AbstractRemoteChunkProvider.class.getDeclaredField("nextChunkToDownload");
      nextField.setAccessible(true);
      nextField.set(provider, 2L);

      assertFalse(provider.isAllDataFetched());

      // Now set all downloaded
      nextField.set(provider, 5L);
      assertTrue(provider.isAllDataFetched());

      // More than needed (edge case)
      nextField.set(provider, 7L);
      assertTrue(provider.isAllDataFetched());
    } catch (Exception e) {
      fail("Reflection failed: " + e.getMessage());
    }
  }

  // =========================================================================
  // StreamingChunkProvider — based on endOfStreamReached
  // =========================================================================

  @Test
  void testStreamingChunkProvider_beforeEndOfStream() {
    StreamingChunkProvider provider = mock(StreamingChunkProvider.class, CALLS_REAL_METHODS);
    // Default endOfStreamReached is false
    assertFalse(provider.isAllDataFetched());
  }

  // =========================================================================
  // LazyThriftResult — based on hasMoreRows
  // =========================================================================

  @Test
  void testLazyThriftResult_isAllDataFetched_delegatesToIsCompletelyFetched() {
    LazyThriftResult result = mock(LazyThriftResult.class);
    // isAllDataFetched delegates to isCompletelyFetched
    doReturn(false).when(result).isCompletelyFetched();
    doCallRealMethod().when(result).isAllDataFetched();
    assertFalse(result.isAllDataFetched());

    doReturn(true).when(result).isCompletelyFetched();
    assertTrue(result.isAllDataFetched());
  }

  // =========================================================================
  // ChunkProvider default
  // =========================================================================

  @Test
  void testChunkProviderDefault_returnsFalse() {
    ChunkProvider provider = mock(ChunkProvider.class, CALLS_REAL_METHODS);
    assertFalse(provider.isAllDataFetched());
  }
}
