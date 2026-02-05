package com.databricks.jdbc.api.impl.streaming;

import com.databricks.jdbc.api.impl.ColumnarRowView;
import com.databricks.jdbc.api.impl.arrow.ArrowResultChunk;
import com.databricks.jdbc.api.impl.thrift.ThriftBatchFetcher;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Type-safe streaming provider for Thrift-based results.
 *
 * <p>This generic provider works with any data type through pluggable processors. It provides
 * proactive prefetching with a sliding window to maximize throughput while bounding memory usage.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Type-safe: {@code getData()} returns the correct type without casting
 *   <li>Prefetching: Background thread fetches ahead of consumer
 *   <li>Memory-bounded: Sliding window limits batches in memory
 *   <li>Pluggable: Processors handle type-specific data conversion
 * </ul>
 *
 * @param <T> The type of data in each batch (ColumnarRowView or ArrowResultChunk)
 */
public class ThriftStreamingProvider<T> implements AutoCloseable {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ThriftStreamingProvider.class);
  private static final String PREFETCH_THREAD_NAME = "databricks-inline-prefetcher";

  // Configuration
  private final int maxBatchesInMemory;
  private final int batchReadyTimeoutSeconds;

  // Dependencies - note the generic processor type
  private final ThriftBatchFetcher batchFetcher;
  private final ThriftResponseProcessor<T> processor;

  // Type-safe batch storage
  private final ConcurrentMap<Long, StreamingBatch<T>> batches = new ConcurrentHashMap<>();

  // Position tracking
  private final AtomicLong currentBatchIndex = new AtomicLong(-1);
  private final AtomicLong highestFetchedBatchIndex = new AtomicLong(-1);
  private final AtomicLong nextBatchToFetch = new AtomicLong(1); // 0 is initial batch
  private final AtomicLong totalRowsFetched = new AtomicLong(0);
  private final AtomicLong nextRowOffset = new AtomicLong(0);

  // State and synchronization
  private volatile boolean endOfStreamReached = false;
  private volatile boolean closed = false;
  private volatile DatabricksSQLException prefetchError = null;
  private final ReentrantLock prefetchLock = new ReentrantLock();
  private final Condition batchAvailable = prefetchLock.newCondition();
  private final Condition consumerAdvanced = prefetchLock.newCondition();
  private final AtomicInteger batchesInMemory = new AtomicInteger(0);

  // Prefetch thread
  private final Thread prefetchThread;

  // ==================== Factory Methods ====================

  /**
   * Creates a streaming provider for Thrift Columnar results.
   *
   * @param fetcher The batch fetcher for retrieving data from server
   * @param initialResponse The initial Thrift fetch response
   * @param maxBatchesInMemory Maximum batches to keep in memory (sliding window)
   * @param timeoutSeconds Timeout waiting for batch to be ready
   * @return A type-safe provider that produces ColumnarRowView data
   * @throws DatabricksSQLException if initialization fails
   */
  public static ThriftStreamingProvider<ColumnarRowView> forColumnar(
      ThriftBatchFetcher fetcher,
      TFetchResultsResp initialResponse,
      int maxBatchesInMemory,
      int timeoutSeconds)
      throws DatabricksSQLException {
    return new ThriftStreamingProvider<>(
        fetcher,
        initialResponse,
        new ColumnarResponseProcessor(),
        maxBatchesInMemory,
        timeoutSeconds);
  }

  /**
   * Creates a streaming provider for Inline Arrow results.
   *
   * @param fetcher The batch fetcher for retrieving data from server
   * @param initialResponse The initial Thrift fetch response
   * @param statementId The statement ID for chunk creation
   * @param maxBatchesInMemory Maximum batches to keep in memory (sliding window)
   * @param timeoutSeconds Timeout waiting for batch to be ready
   * @return A type-safe provider that produces ArrowResultChunk data
   * @throws DatabricksSQLException if initialization fails
   */
  public static ThriftStreamingProvider<ArrowResultChunk> forInlineArrow(
      ThriftBatchFetcher fetcher,
      TFetchResultsResp initialResponse,
      StatementId statementId,
      int maxBatchesInMemory,
      int timeoutSeconds)
      throws DatabricksSQLException {
    return new ThriftStreamingProvider<>(
        fetcher,
        initialResponse,
        new InlineArrowResponseProcessor(statementId),
        maxBatchesInMemory,
        timeoutSeconds);
  }

  // ==================== Constructor ====================

  private ThriftStreamingProvider(
      ThriftBatchFetcher fetcher,
      TFetchResultsResp initialResponse,
      ThriftResponseProcessor<T> processor,
      int maxBatchesInMemory,
      int timeoutSeconds)
      throws DatabricksSQLException {

    // Validate required parameters
    if (initialResponse == null) {
      LOGGER.error("Cannot create ThriftStreamingProvider: initialResponse is null");
      throw new IllegalArgumentException("initialResponse cannot be null");
    }
    if (fetcher == null) {
      LOGGER.error("Cannot create ThriftStreamingProvider: fetcher is null");
      throw new IllegalArgumentException("fetcher cannot be null");
    }
    if (processor == null) {
      LOGGER.error("Cannot create ThriftStreamingProvider: processor is null");
      throw new IllegalArgumentException("processor cannot be null");
    }

    this.batchFetcher = fetcher;
    this.processor = processor;
    // We need at least 2 batches in memory to perform any kind of prefetching
    int effectiveMaxBatchesInMemory = Math.max(2, maxBatchesInMemory);
    if (maxBatchesInMemory < 2) {
      LOGGER.warn(
          "Configured maxBatchesInMemory={} is less than the minimum of 2; using 2 instead to enable prefetching.",
          maxBatchesInMemory);
    }
    this.maxBatchesInMemory = effectiveMaxBatchesInMemory;
    this.batchReadyTimeoutSeconds = timeoutSeconds;

    LOGGER.debug(
        "Creating ThriftStreamingProvider: maxBatches={}, timeout={}s, processor={}",
        this.maxBatchesInMemory,
        timeoutSeconds,
        processor.getClass().getSimpleName());

    // Process initial batch using the generic processor
    StreamingBatch<T> initialBatch = processor.processInitialResponse(initialResponse);
    batches.put(0L, initialBatch);
    highestFetchedBatchIndex.set(0);
    batchesInMemory.incrementAndGet();
    totalRowsFetched.addAndGet(initialBatch.getRowCount());
    nextRowOffset.set(initialBatch.getRowCount());

    LOGGER.debug(
        "Initial batch processed: rows={}, hasMoreRows={}",
        initialBatch.getRowCount(),
        initialBatch.hasMoreRows());

    if (!initialBatch.hasMoreRows()) {
      endOfStreamReached = true;
      LOGGER.debug("Single batch result - all data in initial response");
    }

    // Start prefetch thread
    this.prefetchThread = new Thread(this::prefetchLoop, PREFETCH_THREAD_NAME);
    this.prefetchThread.setDaemon(true);
    this.prefetchThread.start();

    notifyConsumerAdvanced();
  }

  // ==================== Public API ====================

  /**
   * Checks if there are more batches available.
   *
   * @return true if more batches may be available
   */
  public boolean hasNextBatch() {
    if (closed) return false;
    if (!endOfStreamReached) return true;
    return currentBatchIndex.get() < highestFetchedBatchIndex.get();
  }

  /**
   * Moves to the next batch. Releases the previous batch.
   *
   * <p>This method automatically skips empty batches (rowCount == 0), continuing to advance until a
   * non-empty batch is found or no more batches are available. This ensures consumers never see
   * empty batches and matches the behavior of lazy result implementations.
   *
   * @return true if moved to a non-empty batch, false if no more batches
   * @throws DatabricksSQLException if an error occurred during prefetch
   */
  public boolean nextBatch() throws DatabricksSQLException {
    if (closed) return false;

    checkPrefetchError();

    // Release previous batch (type-safe release via Consumer<T>)
    long prevIndex = currentBatchIndex.get();
    if (prevIndex >= 0) {
      releaseBatch(prevIndex);
    }

    // Keep advancing until we find a non-empty batch or run out of batches
    while (hasNextBatch()) {
      currentBatchIndex.incrementAndGet();
      notifyConsumerAdvanced();

      StreamingBatch<T> batch = getCurrentBatch();
      if (batch != null && batch.getRowCount() > 0) {
        return true; // Found a non-empty batch
      }

      // Empty batch - release it and try next
      LOGGER.debug("Skipping empty batch {}", currentBatchIndex.get());
      releaseBatch(currentBatchIndex.get());
    }

    return false; // No more non-empty batches available
  }

  /**
   * Gets the current batch with type-safe data access.
   *
   * <p>No casting required - returns {@code StreamingBatch<T>} with correctly typed data!
   *
   * @return The current batch, or null if before first batch
   * @throws DatabricksSQLException if the batch cannot be retrieved
   */
  public StreamingBatch<T> getCurrentBatch() throws DatabricksSQLException {
    long batchIdx = currentBatchIndex.get();
    if (batchIdx < 0) return null;

    checkPrefetchError();

    StreamingBatch<T> batch = batches.get(batchIdx);

    if (batch == null) {
      LOGGER.debug("Batch {} not yet available, waiting for prefetch", batchIdx);
      waitForBatchCreation(batchIdx);
      batch = batches.get(batchIdx);
    }

    if (batch == null) {
      LOGGER.error("Batch {} not found after waiting", batchIdx);
      throw new DatabricksSQLException(
          "Batch " + batchIdx + " not found after waiting",
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    // Wait for batch to be ready
    if (!batch.isReady()) {
      try {
        batch.waitUntilReady(batchReadyTimeoutSeconds);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted waiting for batch {}", batchIdx);
        throw new DatabricksSQLException(
            "Interrupted waiting for batch " + batchIdx,
            e,
            DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
      } catch (ExecutionException e) {
        LOGGER.error("Failed to fetch batch {}: {}", batchIdx, e.getCause().getMessage(), e);
        throw new DatabricksSQLException(
            "Failed to fetch batch " + batchIdx,
            e.getCause(),
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      } catch (TimeoutException e) {
        LOGGER.error(
            "Timeout waiting for batch {} (timeout: {}s)", batchIdx, batchReadyTimeoutSeconds);
        throw new DatabricksSQLException(
            "Timeout waiting for batch "
                + batchIdx
                + " (timeout: "
                + batchReadyTimeoutSeconds
                + "s)",
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      }
    }

    return batch;
  }

  /**
   * Gets total rows fetched so far.
   *
   * @return The total row count
   */
  public long getTotalRowsFetched() {
    return totalRowsFetched.get();
  }

  /**
   * Gets number of batches currently in memory.
   *
   * @return The batch count
   */
  public int getBatchesInMemory() {
    return batchesInMemory.get();
  }

  /**
   * Checks if the end of stream has been reached.
   *
   * @return true if all batches have been fetched from the server
   */
  public boolean isEndOfStreamReached() {
    return endOfStreamReached;
  }

  @Override
  public void close() {
    if (closed) return;

    LOGGER.debug("Closing ThriftStreamingProvider, total rows: {}", totalRowsFetched.get());
    closed = true;

    notifyConsumerAdvanced();
    notifyBatchAvailable();

    // Interrupt and wait for prefetch thread to terminate before releasing resources
    prefetchThread.interrupt();
    try {
      prefetchThread.join(5000); // Wait up to 5s for clean shutdown
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.debug("Interrupted while waiting for prefetch thread to terminate");
    }

    // Release all batches using type-safe release action
    // Continue releasing subsequent batches even if one fails
    for (StreamingBatch<T> batch : batches.values()) {
      try {
        batch.release();
      } catch (Exception e) {
        LOGGER.warn("Error releasing batch during close: {}", e.getMessage(), e);
      }
    }
    batches.clear();

    try {
      batchFetcher.close();
    } catch (Exception e) {
      LOGGER.warn("Error closing batchFetcher: {}", e.getMessage(), e);
    }
  }

  // ==================== Prefetch Logic ====================

  private void prefetchLoop() {
    LOGGER.debug("Prefetch thread started");

    while (!closed && !Thread.currentThread().isInterrupted()) {
      try {
        prefetchLock.lock();
        try {
          while (!closed && !endOfStreamReached && batchesInMemory.get() >= maxBatchesInMemory) {
            LOGGER.debug(
                "Prefetch waiting: batches={}/{}", batchesInMemory.get(), maxBatchesInMemory);
            consumerAdvanced.await();
          }
        } finally {
          prefetchLock.unlock();
        }

        if (closed || endOfStreamReached) break;

        fetchNextBatchInternal();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.debug("Prefetch thread interrupted");
        break;
      } catch (DatabricksSQLException e) {
        LOGGER.error("Prefetch error: {}", e.getMessage());
        prefetchError = e;
        notifyBatchAvailable();
        break;
      } catch (Exception e) {
        LOGGER.error("Unexpected prefetch error: {}", e.getMessage(), e);
        prefetchError =
            new DatabricksSQLException(
                "Unexpected prefetch error: " + e.getMessage(),
                e,
                DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        notifyBatchAvailable();
        break;
      }
    }

    LOGGER.debug("Prefetch thread exiting");
  }

  private void fetchNextBatchInternal() throws SQLException {
    long batchIndex = nextBatchToFetch.getAndIncrement();
    long rowOffset = nextRowOffset.get();

    LOGGER.debug("Fetching batch {} at offset {}", batchIndex, rowOffset);

    // Fetch from server
    TFetchResultsResp response = batchFetcher.fetchNextBatch();

    // Process using type-safe generic processor
    StreamingBatch<T> batch = processor.processResponse(response, batchIndex, rowOffset);

    // Update state
    batches.put(batchIndex, batch);
    batchesInMemory.incrementAndGet();
    highestFetchedBatchIndex.updateAndGet(cur -> Math.max(cur, batchIndex));
    totalRowsFetched.addAndGet(batch.getRowCount());
    nextRowOffset.addAndGet(batch.getRowCount());

    LOGGER.debug(
        "Batch {} ready: rows={}, hasMore={}",
        batchIndex,
        batch.getRowCount(),
        batch.hasMoreRows());

    if (!batch.hasMoreRows()) {
      endOfStreamReached = true;
      LOGGER.debug("End of stream at batch {}", batchIndex);
    }

    notifyBatchAvailable();
  }

  // ==================== Resource Management ====================

  private void releaseBatch(long batchIndex) {
    StreamingBatch<T> batch = batches.remove(batchIndex);
    if (batch != null) {
      // Decrement counter BEFORE release to prevent prefetch stall if release() throws
      batchesInMemory.decrementAndGet();
      try {
        batch.release(); // Uses type-safe Consumer<T> internally
      } catch (Exception e) {
        LOGGER.warn("Error releasing batch {}: {}", batchIndex, e.getMessage(), e);
      }
      LOGGER.debug("Released batch {}, batches in memory: {}", batchIndex, batchesInMemory.get());
      notifyConsumerAdvanced();
    }
  }

  private void waitForBatchCreation(long batchIndex) throws DatabricksSQLException {
    prefetchLock.lock();
    try {
      long waitStartTime = System.currentTimeMillis();
      long timeoutMillis = batchReadyTimeoutSeconds * 1000L;

      while (!closed && !batches.containsKey(batchIndex)) {
        checkPrefetchError();
        if (endOfStreamReached && batchIndex > highestFetchedBatchIndex.get()) {
          LOGGER.error(
              "Batch {} does not exist (highest fetched: {})",
              batchIndex,
              highestFetchedBatchIndex.get());
          throw new DatabricksSQLException(
              "Batch "
                  + batchIndex
                  + " does not exist (highest: "
                  + highestFetchedBatchIndex.get()
                  + ")",
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        // Check for timeout
        long elapsedMillis = System.currentTimeMillis() - waitStartTime;
        if (elapsedMillis >= timeoutMillis) {
          LOGGER.error(
              "Timeout waiting for batch {} to be created (timeout: {}s)",
              batchIndex,
              batchReadyTimeoutSeconds);
          throw new DatabricksSQLException(
              "Timeout waiting for batch "
                  + batchIndex
                  + " to be created (timeout: "
                  + batchReadyTimeoutSeconds
                  + "s)",
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        try {
          long remainingMillis = timeoutMillis - elapsedMillis;
          batchAvailable.await(remainingMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn("Interrupted waiting for batch {} creation", batchIndex);
          throw new DatabricksSQLException(
              "Interrupted waiting for batch",
              e,
              DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
        }
      }
    } finally {
      prefetchLock.unlock();
    }
  }

  private void checkPrefetchError() throws DatabricksSQLException {
    if (prefetchError != null) {
      LOGGER.error("Prefetch failed: {}", prefetchError.getMessage(), prefetchError);
      throw new DatabricksSQLException(
          "Prefetch failed: " + prefetchError.getMessage(),
          prefetchError,
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }
  }

  private void notifyConsumerAdvanced() {
    prefetchLock.lock();
    try {
      consumerAdvanced.signalAll();
    } finally {
      prefetchLock.unlock();
    }
  }

  private void notifyBatchAvailable() {
    prefetchLock.lock();
    try {
      batchAvailable.signalAll();
    } finally {
      prefetchLock.unlock();
    }
  }
}
