package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamingChunkProviderTest {

  private static final StatementId STATEMENT_ID = new StatementId("test-statement-id");
  private static final int MAX_CHUNKS_IN_MEMORY = 4;
  private static final int LINK_PREFETCH_WINDOW = 10;
  private static final int CHUNK_READY_TIMEOUT_SECONDS = 5;
  private static final double CLOUD_FETCH_SPEED_THRESHOLD = 0.1;

  // Use a far future date to ensure links are never considered expired
  private static final String FAR_FUTURE_EXPIRATION =
      Instant.now().plus(1, ChronoUnit.HOURS).toString();

  @Mock private ChunkLinkFetcher mockLinkFetcher;
  @Mock private IDatabricksHttpClient mockHttpClient;
  @Mock private CloseableHttpResponse mockHttpResponse;
  @Mock private HttpEntity mockHttpEntity;
  @Mock private StatusLine mockStatusLine;

  private StreamingChunkProvider provider;

  // Valid Arrow IPC bytes - created once and reused
  private static final byte[] validArrowData;

  static {
    validArrowData = createArrowData();
  }

  // HTTP client tracking state - reset by setupHttpClientWithTracking()
  private AtomicInteger concurrentDownloads;
  private AtomicInteger maxConcurrentDownloads;
  private Set<Integer> downloadedChunkIndices;
  private Set<String> downloadedUrls;
  private CountDownLatch downloadsStartedLatch;

  @BeforeEach
  void setUp() throws Exception {
    // Default HTTP setup with tracking - tests can call setupHttpClientWithTracking() to customize
    setupHttpClientWithTracking(10, 40);
  }

  @AfterEach
  void tearDown() {
    if (provider != null && !provider.isClosed()) {
      provider.close();
    }
  }

  // ==================== Test Infrastructure ====================

  /**
   * Creates valid Arrow IPC format bytes with a simple schema. This data can be used to simulate
   * successful chunk downloads.
   */
  private static byte[] createArrowData() {
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {

      // Create a simple schema with one int column
      try (IntVector intVector = new IntVector("test_column", allocator)) {
        intVector.allocateNew(1);
        intVector.set(0, 42);
        intVector.setValueCount(1);

        try (VectorSchemaRoot root = VectorSchemaRoot.of(intVector);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
      }

      return out.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test Arrow data", e);
    }
  }

  /**
   * Sets up the mock HTTP client with tracking capabilities. Tracks concurrent downloads, max
   * concurrent downloads, which chunks were downloaded, and download start times.
   *
   * @param expectedDownloads Number of downloads to wait for via {@link #downloadsStartedLatch}
   * @param downloadDelayMs Simulated download delay in milliseconds
   */
  private void setupHttpClientWithTracking(int expectedDownloads, long downloadDelayMs)
      throws Exception {
    // Initialize tracking state
    concurrentDownloads = new AtomicInteger(0);
    maxConcurrentDownloads = new AtomicInteger(0);
    downloadedChunkIndices = Collections.synchronizedSet(new HashSet<>());
    downloadedUrls = Collections.synchronizedSet(new HashSet<>());
    downloadsStartedLatch = new CountDownLatch(expectedDownloads);

    // Setup response chain (lenient since not all tests use HTTP)
    lenient().when(mockStatusLine.getStatusCode()).thenReturn(200);
    lenient().when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
    lenient().when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    lenient()
        .when(mockHttpEntity.getContent())
        .thenAnswer(inv -> new ByteArrayInputStream(validArrowData));
    lenient().when(mockHttpEntity.getContentLength()).thenReturn((long) validArrowData.length);

    // Setup execute with tracking
    lenient()
        .when(mockHttpClient.execute(any(HttpUriRequest.class), anyBoolean()))
        .thenAnswer(
            invocation -> {
              int current = concurrentDownloads.incrementAndGet();
              maxConcurrentDownloads.updateAndGet(max -> Math.max(max, current));

              // Extract chunk index from URL (format: http://test-url/chunk-N)
              HttpUriRequest request = invocation.getArgument(0);
              String url = request.getURI().toString();
              int chunkIndex = Integer.parseInt(url.substring(url.lastIndexOf('-') + 1));

              downloadedChunkIndices.add(chunkIndex);
              downloadedUrls.add(url);
              downloadsStartedLatch.countDown();

              try {
                TimeUnit.MILLISECONDS.sleep(downloadDelayMs);
              } finally {
                concurrentDownloads.decrementAndGet();
              }

              return mockHttpResponse;
            });
  }

  /** Waits for a specific chunk to start downloading. */
  private boolean waitForChunkDownload(int chunkIndex, long timeout, TimeUnit unit)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
    while (System.currentTimeMillis() < deadline) {
      if (downloadedChunkIndices.contains(chunkIndex)) {
        return true;
      }
      TimeUnit.MILLISECONDS.sleep(10);
    }
    return false;
  }

  /**
   * Sets up the mock HTTP client to fail for specific chunks while succeeding for others.
   *
   * @param failingChunkIndices Set of chunk indices that should fail with DatabricksHttpException
   * @param downloadDelayMs Simulated download delay for successful chunks
   */
  private void setupHttpClientWithSelectiveFailure(
      Set<Integer> failingChunkIndices, long downloadDelayMs) throws Exception {
    // Use lenient() for response mocks since they won't be used if all chunks fail
    lenient().when(mockStatusLine.getStatusCode()).thenReturn(200);
    lenient().when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
    lenient().when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    lenient()
        .when(mockHttpEntity.getContent())
        .thenAnswer(inv -> new ByteArrayInputStream(validArrowData));
    lenient().when(mockHttpEntity.getContentLength()).thenReturn((long) validArrowData.length);

    when(mockHttpClient.execute(any(HttpUriRequest.class), anyBoolean()))
        .thenAnswer(
            invocation -> {
              HttpUriRequest request = invocation.getArgument(0);
              String url = request.getURI().toString();
              int chunkIndex = Integer.parseInt(url.substring(url.lastIndexOf('-') + 1));

              if (failingChunkIndices.contains(chunkIndex)) {
                throw new DatabricksHttpException(
                    "Simulated failure for chunk " + chunkIndex,
                    DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR);
              }

              TimeUnit.MILLISECONDS.sleep(downloadDelayMs);
              return mockHttpResponse;
            });
  }

  // ==================== Helper Methods ====================

  private ExternalLink createExternalLink(
      long chunkIndex, long rowCount, Long nextChunkIndex, String expiration) {
    ExternalLink link = new ExternalLink();
    link.setExternalLink("http://test-url/chunk-" + chunkIndex);
    link.setChunkIndex(chunkIndex);
    link.setHttpHeaders(Collections.emptyMap());
    link.setExpiration(expiration);
    link.setRowOffset(chunkIndex * rowCount);
    link.setRowCount(rowCount);
    link.setNextChunkIndex(nextChunkIndex);
    return link;
  }

  /**
   * Creates a batch of links for chunks in range [startIndex, startIndex + count). The last link's
   * nextChunkIndex is set based on hasMore parameter.
   *
   * @param startIndex The starting chunk index for this batch
   * @param count Number of chunks in this batch
   * @param rowsPerChunk Rows per chunk
   * @param hasMore If true, last link points to next chunk; if false, last link has null
   *     nextChunkIndex
   * @param expiredChunkIndices Optional set of chunk indices that should have expired links
   * @return ChunkLinkFetchResult for the batch
   */
  private ChunkLinkFetchResult createLinkBatch(
      int startIndex,
      int count,
      long rowsPerChunk,
      boolean hasMore,
      Set<Integer> expiredChunkIndices) {
    List<ExternalLink> links = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      int chunkIndex = startIndex + i;
      boolean isLastInBatch = (i == count - 1);
      Long nextChunkIndex;
      if (isLastInBatch) {
        nextChunkIndex = hasMore ? (long) (chunkIndex + 1) : null;
      } else {
        nextChunkIndex = (long) (chunkIndex + 1);
      }

      String expiration =
          expiredChunkIndices.contains(chunkIndex) ? getExpiredExpiration() : FAR_FUTURE_EXPIRATION;

      links.add(createExternalLink(chunkIndex, rowsPerChunk, nextChunkIndex, expiration));
    }

    if (links.isEmpty()) {
      return ChunkLinkFetchResult.endOfStream();
    }

    ExternalLink lastLink = links.get(links.size() - 1);
    long nextFetchIndex = hasMore ? lastLink.getNextChunkIndex() : -1;
    long nextRowOffset = lastLink.getRowOffset() + lastLink.getRowCount();

    return ChunkLinkFetchResult.of(links, hasMore, nextFetchIndex, nextRowOffset);
  }

  /** Returns an expiration time that is already expired (1 hour in the past). */
  private String getExpiredExpiration() {
    return Instant.now().minus(1, ChronoUnit.HOURS).toString();
  }

  /** Returns an expiration time that is about to expire (within the 60-second buffer). */
  private String getAboutToExpireExpiration() {
    return Instant.now().plus(30, ChronoUnit.SECONDS).toString();
  }

  private StreamingChunkProvider createProvider(
      ChunkLinkFetchResult initialLinks, int linkPrefetchWindow, int maxChunksInMemory)
      throws DatabricksParsingException {
    return new StreamingChunkProvider(
        mockLinkFetcher,
        mockHttpClient,
        CompressionCodec.NONE,
        STATEMENT_ID,
        maxChunksInMemory,
        linkPrefetchWindow,
        CHUNK_READY_TIMEOUT_SECONDS,
        CLOUD_FETCH_SPEED_THRESHOLD,
        null, // connectionContext - not needed for tests
        initialLinks);
  }

  // ==================== Category 1: Basic Iteration / Happy Path ====================

  @Nested
  @DisplayName("Category 1: Basic Iteration / Happy Path")
  class BasicIterationTests {

    @Test
    @DisplayName("Should iterate through all chunks in correct order")
    void testIterateThroughAllChunks() throws Exception {
      // Setup: 5 chunks provided upfront, no more to fetch
      int totalChunks = 5;
      long rowsPerChunk = 100L;
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, totalChunks, rowsPerChunk, false, Collections.emptySet());

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Verify we can iterate through all chunks
      for (int i = 0; i < totalChunks; i++) {
        assertTrue(provider.hasNextChunk(), "Should have next chunk at index " + i);
        assertTrue(provider.next(), "next() should return true at index " + i);
        assertNotNull(provider.getChunk(), "getChunk() should return chunk at index " + i);
      }

      // After consuming all chunks
      assertFalse(provider.hasNextChunk(), "Should not have more chunks after consuming all");
      assertFalse(provider.next(), "next() should return false after all chunks consumed");
    }

    @Test
    @DisplayName("Should handle empty result (end of stream immediately)")
    void testEmptyResult() throws Exception {
      // Setup: End of stream from the start - no initial links
      ChunkLinkFetchResult initialLinks = ChunkLinkFetchResult.endOfStream();

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Verify empty result behavior
      assertFalse(provider.hasNextChunk(), "Should not have any chunks for empty result");
      assertFalse(provider.next(), "next() should return false for empty result");
      assertEquals(
          0, provider.getChunkCount(), "Chunk count should be 0 after end of stream reached");
    }

    @Test
    @DisplayName("Should handle single chunk result")
    void testSingleChunk() throws Exception {
      // Setup: Single chunk, no more
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 1, 100L, false, Collections.emptySet());

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Verify single chunk iteration
      assertTrue(provider.hasNextChunk(), "Should have one chunk");
      assertTrue(provider.next(), "next() should return true for first chunk");
      assertNotNull(provider.getChunk(), "getChunk() should return the single chunk");

      // After consuming the single chunk
      assertFalse(provider.hasNextChunk(), "Should not have more chunks after single chunk");
      assertFalse(provider.next(), "next() should return false after single chunk");
    }

    @Test
    @DisplayName("Should handle all links provided upfront (no additional fetching needed)")
    void testInitialLinksOnly() throws Exception {
      // Setup: All 3 chunks provided initially, no more to fetch
      int totalChunks = 3;
      long rowsPerChunk = 50L;
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, totalChunks, rowsPerChunk, false, Collections.emptySet());

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Iterate through all chunks
      int consumedChunks = 0;
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk(), "getChunk() should return chunk");
        consumedChunks++;
        if (consumedChunks > totalChunks + 1) {
          fail("Consumed more chunks than expected - possible infinite loop");
        }
      }

      assertEquals(
          totalChunks, consumedChunks, "Should have consumed exactly " + totalChunks + " chunks");

      // Verify no additional fetch calls were made (only initial links used)
      verify(mockLinkFetcher, never()).fetchLinks(anyLong(), anyLong());
    }

    @Test
    @DisplayName("Should return null from getChunk() before first next() call")
    void testGetChunkBeforeNext() throws Exception {
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 3, 100L, false, Collections.emptySet());

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // getChunk() before next() should return null (currentChunkIndex is -1)
      assertNull(provider.getChunk(), "getChunk() should return null before first next() call");

      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }
    }

    @Test
    @DisplayName("Should track row count correctly across multiple chunks")
    void testRowCountTracking() throws Exception {
      // Setup: 5 chunks with 100 rows each
      int totalChunks = 5;
      long rowsPerChunk = 100L;
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, totalChunks, rowsPerChunk, false, Collections.emptySet());

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Allow time for initialization
      TimeUnit.MILLISECONDS.sleep(100);

      // Total rows should be sum of all chunks
      long expectedTotalRows = totalChunks * rowsPerChunk;
      assertEquals(
          expectedTotalRows,
          provider.getRowCount(),
          "Row count should match sum of all chunk rows");

      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }
    }

    @Test
    @DisplayName("Should fetch additional link batches and iterate through all chunks")
    void testMultipleLinkBatchFetching() throws Exception {
      // Setup:
      // - Initial links: chunks 0-2 (with hasMore=true)
      // - Batch 1 fetch: chunks 3-5 (with hasMore=true)
      // - Batch 2 fetch: chunks 6-8 (with hasMore=true)
      // - Batch 3 fetch: chunks 9-10 (with hasMore=false, end of stream)
      long rowsPerChunk = 100L;

      // Initial links (chunks 0, 1, 2) indicating more data available
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 3, rowsPerChunk, true, Collections.emptySet());

      // Mock the link fetcher to return subsequent batches
      // Batch 1: chunks 3, 4, 5
      ChunkLinkFetchResult batch1 =
          createLinkBatch(3, 3, rowsPerChunk, true, Collections.emptySet());
      // Batch 2: chunks 6, 7, 8
      ChunkLinkFetchResult batch2 =
          createLinkBatch(6, 3, rowsPerChunk, true, Collections.emptySet());
      // Batch 3: chunks 9, 10 (final batch)
      ChunkLinkFetchResult batch3 =
          createLinkBatch(9, 2, rowsPerChunk, false, Collections.emptySet());

      // Configure mock to return batches based on the requested chunk index
      // Note: startRowOffset is ignored by SEA-style fetching, we match on chunkIndex
      // Simulate network latency for link fetching
      when(mockLinkFetcher.fetchLinks(eq(3L), anyLong()))
          .thenAnswer(
              invocation -> {
                TimeUnit.MILLISECONDS.sleep(40);
                return batch1;
              });
      when(mockLinkFetcher.fetchLinks(eq(6L), anyLong()))
          .thenAnswer(
              invocation -> {
                TimeUnit.MILLISECONDS.sleep(40);
                return batch2;
              });
      when(mockLinkFetcher.fetchLinks(eq(9L), anyLong()))
          .thenAnswer(
              invocation -> {
                TimeUnit.MILLISECONDS.sleep(40);
                return batch3;
              });

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Iterate through all 11 chunks (0-10)
      int totalExpectedChunks = 11;
      int consumedChunks = 0;

      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk(), "Chunk should not be null at index " + consumedChunks);

        consumedChunks++;
        if (consumedChunks > totalExpectedChunks + 1) {
          fail("Consumed more chunks than expected - possible infinite loop");
        }
      }

      // Verify all chunks were consumed
      assertEquals(
          totalExpectedChunks,
          consumedChunks,
          "Should have consumed exactly " + totalExpectedChunks + " chunks");

      // Verify the link fetcher was called for each additional batch
      verify(mockLinkFetcher).fetchLinks(eq(3L), anyLong());
      verify(mockLinkFetcher).fetchLinks(eq(6L), anyLong());
      verify(mockLinkFetcher).fetchLinks(eq(9L), anyLong());

      // Verify total row count
      long expectedTotalRows = totalExpectedChunks * rowsPerChunk;
      assertEquals(
          expectedTotalRows, provider.getRowCount(), "Total row count should match all chunks");

      // Verify chunk count is known after end of stream
      assertEquals(
          totalExpectedChunks,
          provider.getChunkCount(),
          "Chunk count should be known after end of stream");
    }

    @Test
    @DisplayName(
        "Should fetch additional link batches using row offset (Thrift-style) and iterate through all chunks")
    void testMultipleLinkBatchFetchingWithRowOffset() throws Exception {
      // Setup similar to SEA test but mocking based on rowOffset (Thrift behavior):
      // - Initial links: chunks 0-2 (rows 0-299, nextRowOffset=300)
      // - Batch 1 fetch at rowOffset 300: chunks 3-5 (rows 300-599, nextRowOffset=600)
      // - Batch 2 fetch at rowOffset 600: chunks 6-8 (rows 600-899, nextRowOffset=900)
      // - Batch 3 fetch at rowOffset 900: chunks 9-10 (rows 900-1099, end of stream)
      long rowsPerChunk = 100L;

      // Initial links (chunks 0, 1, 2) indicating more data available
      // Row offsets: chunk 0 -> 0, chunk 1 -> 100, chunk 2 -> 200
      // nextRowOffset after this batch = 300
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 3, rowsPerChunk, true, Collections.emptySet());

      // Batch 1: chunks 3, 4, 5 (row offsets 300, 400, 500, nextRowOffset=600)
      ChunkLinkFetchResult batch1 =
          createLinkBatch(3, 3, rowsPerChunk, true, Collections.emptySet());
      // Batch 2: chunks 6, 7, 8 (row offsets 600, 700, 800, nextRowOffset=900)
      ChunkLinkFetchResult batch2 =
          createLinkBatch(6, 3, rowsPerChunk, true, Collections.emptySet());
      // Batch 3: chunks 9, 10 (row offsets 900, 1000, end of stream)
      ChunkLinkFetchResult batch3 =
          createLinkBatch(9, 2, rowsPerChunk, false, Collections.emptySet());

      // Configure mock to return batches based on the requested row offset (Thrift-style)
      // Thrift uses rowOffset for FETCH_ABSOLUTE continuation
      // Simulate network latency for link fetching
      when(mockLinkFetcher.fetchLinks(anyLong(), eq(300L)))
          .thenAnswer(
              invocation -> {
                TimeUnit.MILLISECONDS.sleep(40);
                return batch1;
              });
      when(mockLinkFetcher.fetchLinks(anyLong(), eq(600L)))
          .thenAnswer(
              invocation -> {
                TimeUnit.MILLISECONDS.sleep(40);
                return batch2;
              });
      when(mockLinkFetcher.fetchLinks(anyLong(), eq(900L)))
          .thenAnswer(
              invocation -> {
                TimeUnit.MILLISECONDS.sleep(40);
                return batch3;
              });

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Iterate through all 11 chunks (0-10)
      int totalExpectedChunks = 11;
      int consumedChunks = 0;

      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk(), "Chunk should not be null at index " + consumedChunks);

        consumedChunks++;
        if (consumedChunks > totalExpectedChunks + 1) {
          fail("Consumed more chunks than expected - possible infinite loop");
        }
      }

      // Verify all chunks were consumed
      assertEquals(
          totalExpectedChunks,
          consumedChunks,
          "Should have consumed exactly " + totalExpectedChunks + " chunks");

      // Verify the link fetcher was called with correct row offsets
      verify(mockLinkFetcher).fetchLinks(anyLong(), eq(300L));
      verify(mockLinkFetcher).fetchLinks(anyLong(), eq(600L));
      verify(mockLinkFetcher).fetchLinks(anyLong(), eq(900L));

      // Verify total row count
      long expectedTotalRows = totalExpectedChunks * rowsPerChunk;
      assertEquals(
          expectedTotalRows, provider.getRowCount(), "Total row count should match all chunks");

      // Verify chunk count is known after end of stream
      assertEquals(
          totalExpectedChunks,
          provider.getChunkCount(),
          "Chunk count should be known after end of stream");
    }
  }

  // ==================== Category 2: Link Prefetch Window Behavior ====================

  @Nested
  @DisplayName("Category 2: Link Prefetch Window Behavior")
  class LinkPrefetchWindowTests {

    @Test
    @DisplayName("Should prefetch links up to currentChunkIndex + linkPrefetchWindow")
    void testLinkPrefetchWindowAdvancesCorrectly() throws Exception {
      // Setup: Small prefetch window of 5, with many chunks available
      // Initial: chunks 0-2, then batches of 3 chunks each
      int prefetchWindow = 5;
      long rowsPerChunk = 100L;

      // Initial links (chunks 0, 1, 2)
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 3, rowsPerChunk, true, Collections.emptySet());

      // Track which batches are fetched
      AtomicInteger fetchCallCount = new AtomicInteger(0);

      // Setup batches - each batch returns 3 chunks
      // Batch at index 3: chunks 3, 4, 5
      ChunkLinkFetchResult batch1 =
          createLinkBatch(3, 3, rowsPerChunk, true, Collections.emptySet());
      // Batch at index 6: chunks 6, 7, 8
      ChunkLinkFetchResult batch2 =
          createLinkBatch(6, 3, rowsPerChunk, true, Collections.emptySet());
      // Batch at index 9: chunks 9, 10, 11
      ChunkLinkFetchResult batch3 =
          createLinkBatch(9, 3, rowsPerChunk, true, Collections.emptySet());
      // Batch at index 12: chunks 12, 13, 14 (end)
      ChunkLinkFetchResult batch4 =
          createLinkBatch(12, 3, rowsPerChunk, false, Collections.emptySet());

      when(mockLinkFetcher.fetchLinks(eq(3L), anyLong()))
          .thenAnswer(
              inv -> {
                fetchCallCount.incrementAndGet();
                return batch1;
              });
      when(mockLinkFetcher.fetchLinks(eq(6L), anyLong()))
          .thenAnswer(
              inv -> {
                fetchCallCount.incrementAndGet();
                return batch2;
              });
      when(mockLinkFetcher.fetchLinks(eq(9L), anyLong()))
          .thenAnswer(
              inv -> {
                fetchCallCount.incrementAndGet();
                return batch3;
              });
      when(mockLinkFetcher.fetchLinks(eq(12L), anyLong()))
          .thenAnswer(
              inv -> {
                fetchCallCount.incrementAndGet();
                return batch4;
              });

      provider = createProvider(initialLinks, prefetchWindow, MAX_CHUNKS_IN_MEMORY);

      // Wait for prefetch thread to work
      TimeUnit.MILLISECONDS.sleep(200);

      // With currentChunkIndex=-1 and prefetchWindow=5, target is -1+5=4
      // Initial links give us chunks 0-2, so prefetch should fetch batch1 (chunks 3-5)
      // After batch1, nextLinkFetchIndex=6 which is > target(4), so prefetch pauses
      // Verify batch1 was fetched (chunks 3-5 now available)
      verify(mockLinkFetcher).fetchLinks(eq(3L), anyLong());

      // Consume first chunk (currentChunkIndex becomes 0)
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Wait for prefetch to potentially advance
      TimeUnit.MILLISECONDS.sleep(200);

      // With currentChunkIndex=0 and prefetchWindow=5, target is 0+5=5
      // nextLinkFetchIndex=6 is still > target(5), so no new fetch yet
      verifyNoMoreInteractions(mockLinkFetcher);

      // Consume more chunks to advance the window
      for (int i = 1; i <= 3; i++) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }

      // Now currentChunkIndex=3, target=3+5=8
      // nextLinkFetchIndex=6 <= target(8), so batch2 should be fetched
      TimeUnit.MILLISECONDS.sleep(200);
      verify(mockLinkFetcher).fetchLinks(eq(6L), anyLong());

      // Continue consuming to verify prefetch continues advancing
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }

      // All batches should have been fetched by the end
      verify(mockLinkFetcher).fetchLinks(eq(9L), anyLong());
      verify(mockLinkFetcher).fetchLinks(eq(12L), anyLong());
    }

    @Test
    @DisplayName("Should pause prefetch when window is full (caught up)")
    void testPrefetchPausesWhenWindowFull() throws Exception {
      // Setup: prefetchWindow=3, initial links give chunks 0-4 (5 chunks)
      // Since initial links exceed the window, no additional fetch should happen
      // until consumer advances
      int prefetchWindow = 3;
      long rowsPerChunk = 100L;

      // Initial links: chunks 0-4 (more than prefetchWindow)
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 5, rowsPerChunk, true, Collections.emptySet());

      // Next batch would be at index 5 (not expected to be called in this test)
      ChunkLinkFetchResult batch1 =
          createLinkBatch(5, 3, rowsPerChunk, false, Collections.emptySet());
      lenient().when(mockLinkFetcher.fetchLinks(eq(5L), anyLong())).thenReturn(batch1);

      provider = createProvider(initialLinks, prefetchWindow, MAX_CHUNKS_IN_MEMORY);

      // Wait to ensure prefetch thread has time to act
      TimeUnit.MILLISECONDS.sleep(300);

      // With currentChunkIndex=-1 and prefetchWindow=3, target is -1+3=2
      // Initial links set nextLinkFetchIndex=5, which is > target(2)
      // So prefetch should NOT fetch batch1 yet
      verify(mockLinkFetcher, never()).fetchLinks(anyLong(), anyLong());

      // Consume chunk 0 - currentChunkIndex becomes 0, target becomes 0+3=3
      // nextLinkFetchIndex=5 is still > target(3), no fetch
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      TimeUnit.MILLISECONDS.sleep(200);
      verify(mockLinkFetcher, never()).fetchLinks(anyLong(), anyLong());

      // Consume chunk 1 - currentChunkIndex becomes 1, target becomes 1+3=4
      // nextLinkFetchIndex=5 is still > target(4), no fetch
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      TimeUnit.MILLISECONDS.sleep(200);
      verify(mockLinkFetcher, never()).fetchLinks(anyLong(), anyLong());

      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }
    }

    @Test
    @DisplayName("Should resume prefetch when consumer advances past threshold")
    void testPrefetchResumesWhenConsumerAdvances() throws Exception {
      // Setup: prefetchWindow=2, initial links give chunks 0-3 (4 chunks)
      // Prefetch starts paused because nextLinkFetchIndex(4) > target(-1+2=1)
      // As consumer advances, prefetch should resume
      int prefetchWindow = 2;
      long rowsPerChunk = 100L;

      // Initial links: chunks 0-3
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 4, rowsPerChunk, true, Collections.emptySet());

      // Batches for subsequent fetches
      ChunkLinkFetchResult batch1 =
          createLinkBatch(4, 2, rowsPerChunk, true, Collections.emptySet());
      ChunkLinkFetchResult batch2 =
          createLinkBatch(6, 2, rowsPerChunk, false, Collections.emptySet());

      CountDownLatch batch1Fetched = new CountDownLatch(1);
      when(mockLinkFetcher.fetchLinks(eq(4L), anyLong()))
          .thenAnswer(
              inv -> {
                batch1Fetched.countDown();
                return batch1;
              });
      // batch2 may not be fetched within this test's scope
      lenient().when(mockLinkFetcher.fetchLinks(eq(6L), anyLong())).thenReturn(batch2);

      provider = createProvider(initialLinks, prefetchWindow, MAX_CHUNKS_IN_MEMORY);

      // Initially paused - no fetch calls
      TimeUnit.MILLISECONDS.sleep(200);
      verify(mockLinkFetcher, never()).fetchLinks(anyLong(), anyLong());

      // Consume chunks 0, 1 - currentChunkIndex becomes 1, target becomes 1+2=3
      // nextLinkFetchIndex=4 is still > target(3), no fetch yet
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      TimeUnit.MILLISECONDS.sleep(200);
      verify(mockLinkFetcher, never()).fetchLinks(anyLong(), anyLong());

      // Consume chunk 2 - currentChunkIndex becomes 2, target becomes 2+2=4
      // nextLinkFetchIndex=4 <= target(4), so fetch should resume!
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Wait for prefetch to resume and fetch batch1
      assertTrue(
          batch1Fetched.await(2, TimeUnit.SECONDS), "Prefetch should resume and fetch batch1");
      verify(mockLinkFetcher).fetchLinks(eq(4L), anyLong());

      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }
    }

    @Test
    @DisplayName("Should not over-fetch with small prefetch window (window=1)")
    void testSmallPrefetchWindow() throws Exception {
      // Setup: Very small prefetch window of 1
      // This tests that prefetch doesn't fetch more than necessary
      int prefetchWindow = 1;
      long rowsPerChunk = 100L;

      // Initial: only chunk 0
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 1, rowsPerChunk, true, Collections.emptySet());

      // Subsequent single-chunk batches
      ChunkLinkFetchResult batch1 =
          createLinkBatch(1, 1, rowsPerChunk, true, Collections.emptySet());
      ChunkLinkFetchResult batch2 =
          createLinkBatch(2, 1, rowsPerChunk, true, Collections.emptySet());
      ChunkLinkFetchResult batch3 =
          createLinkBatch(3, 1, rowsPerChunk, false, Collections.emptySet());

      AtomicInteger fetchCount = new AtomicInteger(0);

      when(mockLinkFetcher.fetchLinks(eq(1L), anyLong()))
          .thenAnswer(
              inv -> {
                fetchCount.incrementAndGet();
                return batch1;
              });
      when(mockLinkFetcher.fetchLinks(eq(2L), anyLong()))
          .thenAnswer(
              inv -> {
                fetchCount.incrementAndGet();
                return batch2;
              });
      when(mockLinkFetcher.fetchLinks(eq(3L), anyLong()))
          .thenAnswer(
              inv -> {
                fetchCount.incrementAndGet();
                return batch3;
              });

      provider = createProvider(initialLinks, prefetchWindow, MAX_CHUNKS_IN_MEMORY);

      // With prefetchWindow=1 and currentChunkIndex=-1, target=-1+1=0
      // Initial links set nextLinkFetchIndex=1, which is > target(0)
      // So initially no fetch
      TimeUnit.MILLISECONDS.sleep(200);
      assertEquals(0, fetchCount.get(), "Should not fetch ahead initially with window=1");

      // Consume chunk 0 - currentChunkIndex=0, target=0+1=1
      // nextLinkFetchIndex=1 <= target(1), fetch batch1
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      TimeUnit.MILLISECONDS.sleep(200);
      assertEquals(1, fetchCount.get(), "Should fetch exactly one batch after consuming chunk 0");

      // Consume chunk 1 - currentChunkIndex=1, target=1+1=2
      // nextLinkFetchIndex=2 <= target(2), fetch batch2
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      TimeUnit.MILLISECONDS.sleep(200);
      assertEquals(2, fetchCount.get(), "Should fetch one more batch after consuming chunk 1");

      // Consume chunk 2 - currentChunkIndex=2, target=2+1=3
      // nextLinkFetchIndex=3 <= target(3), fetch batch3
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      TimeUnit.MILLISECONDS.sleep(200);
      assertEquals(3, fetchCount.get(), "Should fetch final batch after consuming chunk 2");

      // Consume final chunk
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertFalse(provider.hasNextChunk());
    }

    @Test
    @DisplayName(
        "Should prefetch all available links with large window before consumption catches up")
    void testLargePrefetchWindow() throws Exception {
      // Setup: Large prefetch window (50), with only 10 total chunks
      // All links should be fetched before consumer even starts
      int prefetchWindow = 50;
      long rowsPerChunk = 100L;

      // Initial: chunks 0-2
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 3, rowsPerChunk, true, Collections.emptySet());

      // Subsequent batches
      ChunkLinkFetchResult batch1 =
          createLinkBatch(3, 3, rowsPerChunk, true, Collections.emptySet());
      ChunkLinkFetchResult batch2 =
          createLinkBatch(6, 4, rowsPerChunk, false, Collections.emptySet()); // final batch

      CountDownLatch batch1Fetched = new CountDownLatch(1);
      CountDownLatch batch2Fetched = new CountDownLatch(1);

      when(mockLinkFetcher.fetchLinks(eq(3L), anyLong()))
          .thenAnswer(
              inv -> {
                batch1Fetched.countDown();
                return batch1;
              });
      when(mockLinkFetcher.fetchLinks(eq(6L), anyLong()))
          .thenAnswer(
              inv -> {
                batch2Fetched.countDown();
                return batch2;
              });

      provider = createProvider(initialLinks, prefetchWindow, MAX_CHUNKS_IN_MEMORY);

      // With large prefetchWindow=50 and currentChunkIndex=-1, target=-1+50=49
      // All batches should be fetched immediately since nextLinkFetchIndex will always be <= target

      // Wait for both batches to be fetched
      assertTrue(
          batch1Fetched.await(2, TimeUnit.SECONDS),
          "Batch1 should be fetched immediately with large window");
      assertTrue(
          batch2Fetched.await(2, TimeUnit.SECONDS),
          "Batch2 should be fetched immediately with large window");

      // Verify both batches were fetched before we even consumed any chunks
      verify(mockLinkFetcher).fetchLinks(eq(3L), anyLong());
      verify(mockLinkFetcher).fetchLinks(eq(6L), anyLong());

      // Now consume all chunks - they should all be readily available
      int consumedCount = 0;
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
        consumedCount++;
      }

      assertEquals(10, consumedCount, "Should have consumed all 10 chunks");
      assertEquals(10, provider.getChunkCount(), "Chunk count should be 10");
    }
  }

  // ==================== Category 3: Download Window Behavior ====================

  @Nested
  @DisplayName("Category 3: Download Window Behavior")
  class DownloadWindowTests {

    @Test
    @DisplayName("Should never have more than maxChunksInMemory chunks downloading/downloaded")
    void testDownloadWindowRespectsMaxChunksInMemory() throws Exception {
      // Setup: maxChunksInMemory=2, with 6 chunks available
      // At any point, only 2 chunks should be in memory (downloading or downloaded)
      int maxChunksInMemory = 2;
      int prefetchWindow = 10; // Large enough to not limit link fetching
      long rowsPerChunk = 100L;

      // All 6 chunks provided upfront
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 6, rowsPerChunk, false, Collections.emptySet());

      // Use shared tracking with longer delay to better observe concurrency
      setupHttpClientWithTracking(1, 50);

      provider = createProvider(initialLinks, prefetchWindow, maxChunksInMemory);

      // Wait for at least one download to start
      assertTrue(
          downloadsStartedLatch.await(2, TimeUnit.SECONDS), "Downloads should start immediately");

      // Give some time for parallel downloads to be submitted
      TimeUnit.MILLISECONDS.sleep(200);

      // Consume all chunks
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }

      // Verify max concurrent downloads never exceeded maxChunksInMemory
      assertTrue(
          maxConcurrentDownloads.get() <= maxChunksInMemory,
          "Max concurrent downloads ("
              + maxConcurrentDownloads.get()
              + ") should not exceed maxChunksInMemory ("
              + maxChunksInMemory
              + ")");

      // Verify we processed all 6 chunks
      assertEquals(6, provider.getChunkCount());
    }

    @Test
    @DisplayName("Should trigger new downloads when chunks are consumed and released")
    void testDownloadWindowSlidesAsChunksConsumed() throws Exception {
      // Setup: maxChunksInMemory=2, with 5 chunks
      // Initially chunks 0,1 download. After consuming chunks, chunk 2 should start downloading.
      int maxChunksInMemory = 2;
      int prefetchWindow = 10;
      long rowsPerChunk = 100L;

      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 5, rowsPerChunk, false, Collections.emptySet());

      // Use shared tracking - wait for 2 initial downloads
      setupHttpClientWithTracking(2, 30);

      provider = createProvider(initialLinks, prefetchWindow, maxChunksInMemory);

      // Wait for initial downloads (chunks 0 and 1)
      assertTrue(
          downloadsStartedLatch.await(2, TimeUnit.SECONDS),
          "Chunks 0 and 1 should start downloading immediately");

      // Initially only chunks 0 and 1 should be downloading (maxChunksInMemory=2)
      TimeUnit.MILLISECONDS.sleep(100);
      assertTrue(
          downloadedChunkIndices.contains(0) && downloadedChunkIndices.contains(1),
          "Chunks 0 and 1 should be downloaded first");

      // Chunk 2 should NOT have started yet (window is full)
      assertFalse(
          waitForChunkDownload(2, 100, TimeUnit.MILLISECONDS),
          "Chunk 2 should not start until a slot is freed");

      // Consume chunk 0 - this releases it and should trigger chunk 2 download
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Chunk 2 should NOT have started yet (window is full and no chunk is released)
      assertFalse(
          waitForChunkDownload(2, 100, TimeUnit.MILLISECONDS),
          "Chunk 2 should not start until a slot is freed");

      // Consume chunk 1 to release more slots - chunk release happens on next() after getChunk()
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Now chunk 2 should start downloading (slot freed by releasing earlier chunks)
      assertTrue(
          waitForChunkDownload(2, 2, TimeUnit.SECONDS),
          "Chunk 2 should start after earlier chunks are consumed");

      // Consume remaining chunks
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }

      // All 5 chunks should have been downloaded
      assertEquals(5, downloadedChunkIndices.size(), "All 5 chunks should have been downloaded");
    }
  }

  // ==================== Category 4: Failure Scenarios - Link Fetching ====================

  @Nested
  @DisplayName("Category 4: Failure Scenarios - Link Fetching")
  class LinkFetchingFailureTests {

    @Test
    @DisplayName("Should propagate link fetch exception to consumer")
    void testLinkFetchFailure() throws Exception {
      // Setup: Initial batch has chunk 0, fetching chunk 1 fails
      int prefetchWindow = 5;
      long rowsPerChunk = 100L;

      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 1, rowsPerChunk, true, Collections.emptySet());

      // Link fetch for chunk 1 throws exception
      DatabricksSQLException fetchException =
          new DatabricksSQLException("Network error fetching links", "08000");
      when(mockLinkFetcher.fetchLinks(eq(1L), anyLong())).thenThrow(fetchException);

      setupHttpClientWithTracking(1, 30);

      provider = createProvider(initialLinks, prefetchWindow, MAX_CHUNKS_IN_MEMORY);

      // Consume chunk 0 successfully
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Attempting to get chunk 1 should fail - the error from prefetch should propagate
      // Either next() or getChunk() should throw or return indicating failure
      assertTrue(provider.next(), "next() succeeds as endOfStream not yet known");

      // getChunk() should throw because chunk 1 was never created due to fetch failure
      assertThrows(
          DatabricksSQLException.class,
          () -> provider.getChunk(),
          "getChunk() should throw when chunk creation failed due to link fetch error");
    }

    @Test
    @DisplayName("Should handle empty links returned unexpectedly mid-stream")
    void testLinkFetchReturnsEmptyUnexpectedly() throws Exception {
      // Setup: Initial batch has chunks 0-1 with hasMore=true, but next fetch returns empty
      int prefetchWindow = 5;
      long rowsPerChunk = 100L;

      // Initial links indicate more data available
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 2, rowsPerChunk, true, Collections.emptySet());

      // Next fetch unexpectedly returns empty (server-side issue)
      ChunkLinkFetchResult emptyResult = ChunkLinkFetchResult.endOfStream();
      when(mockLinkFetcher.fetchLinks(eq(2L), anyLong())).thenReturn(emptyResult);

      setupHttpClientWithTracking(2, 30);

      provider = createProvider(initialLinks, prefetchWindow, MAX_CHUNKS_IN_MEMORY);

      // Consume chunks 0 and 1 successfully
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // After consuming available chunks, provider should recognize end of stream
      // (empty fetch result sets endOfStreamReached)
      // Give prefetch time to process the empty result
      TimeUnit.MILLISECONDS.sleep(200);

      assertFalse(provider.hasNextChunk(), "Should have no more chunks after empty fetch result");
      assertEquals(2, provider.getChunkCount(), "Chunk count should be 2");
    }

    @Test
    @DisplayName("Should propagate runtime exception (NPE) from link fetcher to consumer")
    void testLinkFetchRuntimeException() throws Exception {
      // Setup: Initial batch has chunk 0, fetching chunk 1 throws NPE
      int prefetchWindow = 5;
      long rowsPerChunk = 100L;

      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 1, rowsPerChunk, true, Collections.emptySet());

      // Link fetch for chunk 1 throws NPE (simulating bug or unexpected null)
      when(mockLinkFetcher.fetchLinks(eq(1L), anyLong()))
          .thenThrow(new NullPointerException("Simulated NPE in link fetcher"));

      setupHttpClientWithTracking(1, 30);

      provider = createProvider(initialLinks, prefetchWindow, MAX_CHUNKS_IN_MEMORY);

      // Consume chunk 0 successfully
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Attempting to get chunk 1 should fail - the runtime exception should propagate
      assertTrue(provider.next(), "next() succeeds as endOfStream not yet known");

      // getChunk() should throw because chunk 1 was never created due to NPE
      assertThrows(
          Exception.class,
          () -> provider.getChunk(),
          "getChunk() should throw when chunk creation failed due to runtime exception");
    }
  }

  // ==================== Category 5: Failure Scenarios - Download ====================

  @Nested
  @DisplayName("Category 5: Failure Scenarios - Download")
  class DownloadFailureTests {

    @Test
    @DisplayName(
        "Should propagate failure for specific chunk while others succeed (Thrift-style batching)")
    void testPartialDownloadFailure() throws Exception {
      // Setup: Multiple batches fetched via Thrift-style row offsets
      // - Initial: chunks 0-2 (rows 0-299, nextRowOffset=300)
      // - Batch 1 at rowOffset 300: chunks 3-5 (rows 300-599, nextRowOffset=600)
      // - Batch 2 at rowOffset 600: chunks 6-8 (rows 600-899, nextRowOffset=900)
      // - Batch 3 at rowOffset 900: chunks 9-11 (rows 900-1199, end of stream)
      // Chunk 7 (in batch 2) will fail download
      long rowsPerChunk = 100L;
      int failingChunkIndex = 7;

      // Initial links (chunks 0, 1, 2)
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 3, rowsPerChunk, true, Collections.emptySet());

      // Subsequent batches using Thrift-style row offset continuation
      ChunkLinkFetchResult batch1 =
          createLinkBatch(3, 3, rowsPerChunk, true, Collections.emptySet()); // chunks 3-5
      ChunkLinkFetchResult batch2 =
          createLinkBatch(6, 3, rowsPerChunk, true, Collections.emptySet()); // chunks 6-8
      ChunkLinkFetchResult batch3 =
          createLinkBatch(9, 3, rowsPerChunk, false, Collections.emptySet()); // chunks 9-11 (final)

      // Configure mock link fetcher to return batches
      // StreamingChunkProvider calls fetchLinks(nextChunkIndex, nextRowOffset)
      // After initial (chunks 0-2): nextChunkIndex=3, nextRowOffset=300
      // After batch1 (chunks 3-5): nextChunkIndex=6, nextRowOffset=600
      // After batch2 (chunks 6-8): nextChunkIndex=9, nextRowOffset=900
      when(mockLinkFetcher.fetchLinks(eq(3L), eq(300L)))
          .thenAnswer(
              inv -> {
                TimeUnit.MILLISECONDS.sleep(20);
                return batch1;
              });
      when(mockLinkFetcher.fetchLinks(eq(6L), eq(600L)))
          .thenAnswer(
              inv -> {
                TimeUnit.MILLISECONDS.sleep(20);
                return batch2;
              });
      when(mockLinkFetcher.fetchLinks(eq(9L), eq(900L)))
          .thenAnswer(
              inv -> {
                TimeUnit.MILLISECONDS.sleep(20);
                return batch3;
              });

      // Setup HTTP client to fail only for chunk 7
      setupHttpClientWithSelectiveFailure(Set.of(failingChunkIndex), 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume chunks 0-6 successfully (chunks before the failing one)
      for (int i = 0; i < failingChunkIndex; i++) {
        assertTrue(provider.next(), "next() should succeed for chunk " + i);
        assertNotNull(provider.getChunk(), "Chunk " + i + " should be retrieved successfully");
      }

      // Verify link fetches occurred for batches 1 and 2 (batch 3 may or may not be fetched yet)
      verify(mockLinkFetcher).fetchLinks(eq(3L), eq(300L));
      verify(mockLinkFetcher).fetchLinks(eq(6L), eq(600L));

      // Chunk 7 should fail
      assertTrue(provider.next(), "next() should succeed for chunk 7 (chunk exists)");
      DatabricksSQLException exception =
          assertThrows(
              DatabricksSQLException.class,
              () -> provider.getChunk(),
              "Chunk 7 should fail due to download error");
      assertNotNull(exception, "Exception should be thrown for failing chunk");
    }
  }

  // ==================== Category 6: Link Expiry ====================

  @Nested
  @DisplayName("Category 6: Link Expiry")
  class LinkExpiryTests {

    @Test
    @DisplayName("Should refetch expired link before download (SEA-style using chunk index)")
    void testRefetchExpiredLinkSea() throws Exception {
      // Setup: Chunk 1 has an expired link, should trigger refetch using chunk index
      long rowsPerChunk = 100L;

      // Initial links: chunk 0 valid, chunk 1 expired, chunk 2 valid
      ChunkLinkFetchResult initialLinks = createLinkBatch(0, 3, rowsPerChunk, false, Set.of(1));

      // Create a fresh link for chunk 1 when refetch is called
      ExternalLink freshLinkForChunk1 =
          createExternalLink(1, rowsPerChunk, 2L, FAR_FUTURE_EXPIRATION);

      // SEA uses chunkIndex for refetch - mock refetchLink(chunkIndex=1, rowOffset=100)
      when(mockLinkFetcher.refetchLink(eq(1L), eq(100L))).thenReturn(freshLinkForChunk1);

      setupHttpClientWithTracking(3, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume chunk 0 successfully
      assertTrue(provider.next());
      assertNotNull(provider.getChunk(), "Chunk 0 should be retrieved successfully");

      // Consume chunk 1 - should trigger refetch due to expired link
      assertTrue(provider.next());
      assertNotNull(provider.getChunk(), "Chunk 1 should be retrieved after link refetch");

      // Verify refetchLink was called with the correct chunk index (SEA-style)
      verify(mockLinkFetcher).refetchLink(eq(1L), eq(100L));

      // Consume chunk 2 successfully
      assertTrue(provider.next());
      assertNotNull(provider.getChunk(), "Chunk 2 should be retrieved successfully");

      // Verify no more chunks
      assertFalse(provider.hasNextChunk());
    }

    @Test
    @DisplayName("Should refetch expired link before download (Thrift-style using row offset)")
    void testRefetchExpiredLinkThrift() throws Exception {
      // Setup: Similar to SEA test but emphasizing row offset for Thrift
      long rowsPerChunk = 100L;

      // Initial links: chunk 0 valid, chunk 1 expired (row offset 100), chunk 2 valid
      ChunkLinkFetchResult initialLinks = createLinkBatch(0, 3, rowsPerChunk, false, Set.of(1));

      // Create a fresh link for chunk 1 when refetch is called
      // Thrift-style: refetch uses row offset (100) to identify the chunk
      ExternalLink freshLinkForChunk1 =
          createExternalLink(1, rowsPerChunk, 2L, FAR_FUTURE_EXPIRATION);

      // Mock refetchLink - both SEA and Thrift call this method
      // SEA uses chunkIndex, Thrift uses rowOffset - the fetcher implementation decides
      when(mockLinkFetcher.refetchLink(eq(1L), eq(100L))).thenReturn(freshLinkForChunk1);

      setupHttpClientWithTracking(3, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume all chunks
      for (int i = 0; i < 3; i++) {
        assertTrue(provider.next(), "next() should succeed for chunk " + i);
        assertNotNull(provider.getChunk(), "Chunk " + i + " should be retrieved successfully");
      }

      // Verify refetchLink was called for the expired chunk
      // The call passes both chunkIndex and rowOffset - fetcher uses what it needs
      verify(mockLinkFetcher).refetchLink(eq(1L), eq(100L));

      assertFalse(provider.hasNextChunk());
    }

    @Test
    @DisplayName("Should handle multiple expired links in sequence")
    void testMultipleExpiredLinks() throws Exception {
      // Setup: Chunks 1 and 3 have expired links
      long rowsPerChunk = 100L;

      // Initial links: chunks 0, 2, 4 valid; chunks 1, 3 expired
      ChunkLinkFetchResult initialLinks = createLinkBatch(0, 5, rowsPerChunk, false, Set.of(1, 3));

      // Create fresh links for expired chunks
      ExternalLink freshLinkForChunk1 =
          createExternalLink(1, rowsPerChunk, 2L, FAR_FUTURE_EXPIRATION);
      ExternalLink freshLinkForChunk3 =
          createExternalLink(3, rowsPerChunk, 4L, FAR_FUTURE_EXPIRATION);

      when(mockLinkFetcher.refetchLink(eq(1L), eq(100L))).thenReturn(freshLinkForChunk1);
      when(mockLinkFetcher.refetchLink(eq(3L), eq(300L))).thenReturn(freshLinkForChunk3);

      setupHttpClientWithTracking(5, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume all 5 chunks
      for (int i = 0; i < 5; i++) {
        assertTrue(provider.next(), "next() should succeed for chunk " + i);
        assertNotNull(provider.getChunk(), "Chunk " + i + " should be retrieved successfully");
      }

      // Verify refetchLink was called for both expired chunks
      verify(mockLinkFetcher).refetchLink(eq(1L), eq(100L));
      verify(mockLinkFetcher).refetchLink(eq(3L), eq(300L));

      // Verify no refetch for valid chunks
      verify(mockLinkFetcher, never()).refetchLink(eq(0L), anyLong());
      verify(mockLinkFetcher, never()).refetchLink(eq(2L), anyLong());
      verify(mockLinkFetcher, never()).refetchLink(eq(4L), anyLong());

      assertFalse(provider.hasNextChunk());
      assertEquals(5, provider.getChunkCount());
    }

    @Test
    @DisplayName("Should handle link that expires within buffer window (about to expire)")
    void testLinkAboutToExpire() throws Exception {
      // Setup: Chunk 1 has a link that expires within the 60-second buffer
      // isChunkLinkInvalid() checks: expiryTime.minusSeconds(60).isBefore(now)
      // So a link expiring in 30 seconds is considered invalid
      long rowsPerChunk = 100L;

      List<ExternalLink> links = new ArrayList<>();
      // Chunk 0: valid
      links.add(createExternalLink(0, rowsPerChunk, 1L, FAR_FUTURE_EXPIRATION));
      // Chunk 1: about to expire (within 60-second buffer)
      links.add(createExternalLink(1, rowsPerChunk, 2L, getAboutToExpireExpiration()));
      // Chunk 2: valid
      links.add(createExternalLink(2, rowsPerChunk, null, FAR_FUTURE_EXPIRATION));

      ChunkLinkFetchResult initialLinks =
          ChunkLinkFetchResult.of(links, false, -1, 3 * rowsPerChunk);

      // Create fresh link for chunk 1
      ExternalLink freshLinkForChunk1 =
          createExternalLink(1, rowsPerChunk, 2L, FAR_FUTURE_EXPIRATION);

      when(mockLinkFetcher.refetchLink(eq(1L), eq(100L))).thenReturn(freshLinkForChunk1);

      setupHttpClientWithTracking(3, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume all chunks
      for (int i = 0; i < 3; i++) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk(), "Chunk " + i + " should be retrieved");
      }

      // Verify refetch was triggered for the about-to-expire link
      verify(mockLinkFetcher).refetchLink(eq(1L), eq(100L));

      assertFalse(provider.hasNextChunk());
    }

    @Test
    @DisplayName("Should propagate error when link refetch fails")
    void testRefetchLinkFailure() throws Exception {
      // Setup: Chunk 1 has expired link, and refetch fails
      long rowsPerChunk = 100L;

      ChunkLinkFetchResult initialLinks = createLinkBatch(0, 3, rowsPerChunk, false, Set.of(1));

      // Refetch fails with exception
      DatabricksSQLException refetchException =
          new DatabricksSQLException("Failed to refetch link", "08000");
      when(mockLinkFetcher.refetchLink(eq(1L), eq(100L))).thenThrow(refetchException);

      setupHttpClientWithTracking(3, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume chunk 0 successfully
      assertTrue(provider.next());
      assertNotNull(provider.getChunk(), "Chunk 0 should be retrieved successfully");

      // Chunk 1 should fail due to refetch failure
      assertTrue(provider.next(), "next() should succeed (chunk exists)");
      DatabricksSQLException exception =
          assertThrows(
              DatabricksSQLException.class,
              () -> provider.getChunk(),
              "getChunk() should fail when link refetch fails");

      assertNotNull(exception);
    }

    @Test
    @DisplayName("Should handle expired link in fetched batch (not initial links)")
    void testExpiredLinkInFetchedBatch() throws Exception {
      // Setup: Initial links are valid, but fetched batch contains expired link
      long rowsPerChunk = 100L;

      // Initial links: chunks 0-2, all valid, more to fetch
      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 3, rowsPerChunk, true, Collections.emptySet());

      // Fetched batch: chunks 3-5, chunk 4 has expired link
      ChunkLinkFetchResult fetchedBatch = createLinkBatch(3, 3, rowsPerChunk, false, Set.of(4));

      when(mockLinkFetcher.fetchLinks(eq(3L), anyLong())).thenReturn(fetchedBatch);

      // Fresh link for chunk 4
      ExternalLink freshLinkForChunk4 =
          createExternalLink(4, rowsPerChunk, 5L, FAR_FUTURE_EXPIRATION);
      when(mockLinkFetcher.refetchLink(eq(4L), eq(400L))).thenReturn(freshLinkForChunk4);

      setupHttpClientWithTracking(6, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume all 6 chunks
      for (int i = 0; i < 6; i++) {
        assertTrue(provider.next(), "next() should succeed for chunk " + i);
        assertNotNull(provider.getChunk(), "Chunk " + i + " should be retrieved");
      }

      // Verify fetchLinks was called for the second batch
      verify(mockLinkFetcher).fetchLinks(eq(3L), anyLong());

      // Verify refetch was called for chunk 4
      verify(mockLinkFetcher).refetchLink(eq(4L), eq(400L));

      assertFalse(provider.hasNextChunk());
      assertEquals(6, provider.getChunkCount());
    }

    @Test
    @DisplayName("Should handle all initial links being expired")
    void testAllInitialLinksExpired() throws Exception {
      // Setup: All initial links are expired
      long rowsPerChunk = 100L;

      ChunkLinkFetchResult initialLinks =
          createLinkBatch(0, 3, rowsPerChunk, false, Set.of(0, 1, 2));

      // Fresh links for all chunks
      ExternalLink freshLink0 = createExternalLink(0, rowsPerChunk, 1L, FAR_FUTURE_EXPIRATION);
      ExternalLink freshLink1 = createExternalLink(1, rowsPerChunk, 2L, FAR_FUTURE_EXPIRATION);
      ExternalLink freshLink2 = createExternalLink(2, rowsPerChunk, null, FAR_FUTURE_EXPIRATION);

      when(mockLinkFetcher.refetchLink(eq(0L), eq(0L))).thenReturn(freshLink0);
      when(mockLinkFetcher.refetchLink(eq(1L), eq(100L))).thenReturn(freshLink1);
      when(mockLinkFetcher.refetchLink(eq(2L), eq(200L))).thenReturn(freshLink2);

      setupHttpClientWithTracking(3, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume all chunks - each should trigger a refetch
      for (int i = 0; i < 3; i++) {
        assertTrue(provider.next(), "next() should succeed for chunk " + i);
        assertNotNull(provider.getChunk(), "Chunk " + i + " should be retrieved after refetch");
      }

      // Verify all links were refetched
      verify(mockLinkFetcher).refetchLink(eq(0L), eq(0L));
      verify(mockLinkFetcher).refetchLink(eq(1L), eq(100L));
      verify(mockLinkFetcher).refetchLink(eq(2L), eq(200L));

      assertFalse(provider.hasNextChunk());
    }

    @Test
    @DisplayName("Should handle refetch returning link with different URL")
    void testRefetchReturnsNewUrl() throws Exception {
      // Setup: Expired link gets refetched with a completely new URL
      // This tests that the new URL is used for download
      long rowsPerChunk = 100L;

      ChunkLinkFetchResult initialLinks = createLinkBatch(0, 2, rowsPerChunk, false, Set.of(1));

      // Fresh link with different URL
      ExternalLink freshLinkForChunk1 = new ExternalLink();
      freshLinkForChunk1.setExternalLink("http://new-url/chunk-1"); // Different URL
      freshLinkForChunk1.setChunkIndex(1L);
      freshLinkForChunk1.setHttpHeaders(Collections.emptyMap());
      freshLinkForChunk1.setExpiration(FAR_FUTURE_EXPIRATION);
      freshLinkForChunk1.setRowOffset(100L);
      freshLinkForChunk1.setRowCount(rowsPerChunk);
      freshLinkForChunk1.setNextChunkIndex(null);

      when(mockLinkFetcher.refetchLink(eq(1L), eq(100L))).thenReturn(freshLinkForChunk1);

      setupHttpClientWithTracking(2, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume both chunks
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Verify the new URL was used for chunk 1
      assertTrue(
          downloadedUrls.contains("http://new-url/chunk-1"),
          "Should have downloaded from the new URL after refetch");

      assertFalse(provider.hasNextChunk());
    }

    @Test
    @DisplayName("Should handle expired link with Thrift-style batch fetching continuation")
    void testExpiredLinkWithThriftBatchFetching() throws Exception {
      // Setup: Test Thrift-style batch fetching where expired links occur
      // across multiple fetched batches using row offset continuation
      long rowsPerChunk = 100L;

      // Initial links: chunks 0-2, chunk 1 expired
      ChunkLinkFetchResult initialLinks = createLinkBatch(0, 3, rowsPerChunk, true, Set.of(1));

      // Batch 1 (fetched via row offset 300): chunks 3-5, chunk 4 expired
      ChunkLinkFetchResult batch1 = createLinkBatch(3, 3, rowsPerChunk, false, Set.of(4));

      // Mock fetch using row offset (Thrift-style)
      when(mockLinkFetcher.fetchLinks(eq(3L), eq(300L))).thenReturn(batch1);

      // Fresh links for expired chunks
      ExternalLink freshLink1 = createExternalLink(1, rowsPerChunk, 2L, FAR_FUTURE_EXPIRATION);
      ExternalLink freshLink4 = createExternalLink(4, rowsPerChunk, 5L, FAR_FUTURE_EXPIRATION);

      when(mockLinkFetcher.refetchLink(eq(1L), eq(100L))).thenReturn(freshLink1);
      when(mockLinkFetcher.refetchLink(eq(4L), eq(400L))).thenReturn(freshLink4);

      setupHttpClientWithTracking(6, 30);

      provider = createProvider(initialLinks, LINK_PREFETCH_WINDOW, MAX_CHUNKS_IN_MEMORY);

      // Consume all 6 chunks
      for (int i = 0; i < 6; i++) {
        assertTrue(provider.next(), "next() should succeed for chunk " + i);
        assertNotNull(provider.getChunk(), "Chunk " + i + " should be retrieved");
      }

      // Verify Thrift-style fetch was called with row offset
      verify(mockLinkFetcher).fetchLinks(eq(3L), eq(300L));

      // Verify both expired links were refetched
      verify(mockLinkFetcher).refetchLink(eq(1L), eq(100L));
      verify(mockLinkFetcher).refetchLink(eq(4L), eq(400L));

      assertFalse(provider.hasNextChunk());
      assertEquals(6, provider.getChunkCount());
    }
  }
}
