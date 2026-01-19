package com.databricks.jdbc.api.impl.thrift;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createColumnarView;

import com.databricks.jdbc.api.impl.ColumnarRowView;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Streaming batch provider that prefetches Thrift columnar batches proactively.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Configurable sliding window of batches in memory
 *   <li>Background prefetch thread fetches ahead of consumer
 *   <li>Non-blocking batch retrieval when prefetch keeps up
 *   <li>Graceful handling of slow consumers and fast producers
 * </ul>
 *
 * <p>The provider uses a single prefetch thread that sequentially fetches batches using Thrift's
 * FETCH_NEXT orientation. This ensures batches are always received and stored in the correct order.
 *
 * <p>Memory is bounded by the sliding window: only {@code maxBatchesInMemory} batches are kept in
 * memory at a time. As the consumer advances, old batches are released and the prefetch thread is
 * signaled to fetch more.
 */
public class ThriftBatchProvider implements AutoCloseable {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ThriftBatchProvider.class);
  private static final String PREFETCH_THREAD_NAME = "databricks-thrift-batch-prefetcher";
  private static final int DEFAULT_BATCH_READY_TIMEOUT_SECONDS = 300;

  // Configuration
  private final int maxBatchesInMemory;
  private final int batchReadyTimeoutSeconds;

  // Dependencies
  private final ThriftBatchFetcher batchFetcher;

  // Batch storage - indexed by batch number
  private final ConcurrentMap<Long, ThriftBatch> batches = new ConcurrentHashMap<>();

  // Position tracking
  private final AtomicLong currentBatchIndex = new AtomicLong(-1);
  private final AtomicLong highestFetchedBatchIndex = new AtomicLong(-1);
  private final AtomicLong nextBatchToFetch = new AtomicLong(1); // 0 is initial batch

  // Row tracking
  private final AtomicLong totalRowsFetched = new AtomicLong(0);
  private final AtomicLong nextRowOffset = new AtomicLong(0);

  // State
  private volatile boolean endOfStreamReached = false;
  private volatile boolean closed = false;
  private volatile DatabricksSQLException prefetchError = null;

  // Synchronization
  private final ReentrantLock prefetchLock = new ReentrantLock();
  private final Condition batchAvailable = prefetchLock.newCondition();
  private final Condition consumerAdvanced = prefetchLock.newCondition();

  // Executors
  private final ExecutorService prefetchExecutor;
  private final Thread prefetchThread;

  // Memory management
  private final AtomicInteger batchesInMemory = new AtomicInteger(0);

  /**
   * Creates a new ThriftBatchProvider with default timeout.
   *
   * @param batchFetcher Fetcher for retrieving batches from server
   * @param initialResponse The initial response containing first batch
   * @param maxBatchesInMemory Maximum batches to keep in memory (sliding window)
   * @throws DatabricksSQLException if the initial batch cannot be processed
   */
  public ThriftBatchProvider(
      ThriftBatchFetcher batchFetcher, TFetchResultsResp initialResponse, int maxBatchesInMemory)
      throws DatabricksSQLException {
    this(batchFetcher, initialResponse, maxBatchesInMemory, DEFAULT_BATCH_READY_TIMEOUT_SECONDS);
  }

  /**
   * Creates a new ThriftBatchProvider.
   *
   * @param batchFetcher Fetcher for retrieving batches from server
   * @param initialResponse The initial response containing first batch
   * @param maxBatchesInMemory Maximum batches to keep in memory (sliding window)
   * @param batchReadyTimeoutSeconds Timeout waiting for batch to be ready
   * @throws DatabricksSQLException if the initial batch cannot be processed
   */
  public ThriftBatchProvider(
      ThriftBatchFetcher batchFetcher,
      TFetchResultsResp initialResponse,
      int maxBatchesInMemory,
      int batchReadyTimeoutSeconds)
      throws DatabricksSQLException {

    this.batchFetcher = batchFetcher;
    this.maxBatchesInMemory = Math.max(2, maxBatchesInMemory); // At least 2 for prefetch to work
    this.batchReadyTimeoutSeconds = batchReadyTimeoutSeconds;

    LOGGER.info(
        "Creating ThriftBatchProvider: maxBatchesInMemory={}, batchReadyTimeoutSeconds={}",
        this.maxBatchesInMemory,
        batchReadyTimeoutSeconds);

    // Process initial batch (batch 0)
    processInitialBatch(initialResponse);

    // Create prefetch executor
    this.prefetchExecutor = createPrefetchExecutor();

    // Start prefetch thread
    this.prefetchThread = new Thread(this::prefetchLoop, PREFETCH_THREAD_NAME);
    this.prefetchThread.setDaemon(true);
    this.prefetchThread.start();

    // Notify prefetch thread to start
    notifyConsumerAdvanced();
  }

  // ==================== Public API ====================

  /**
   * Checks if there are more batches available.
   *
   * @return true if more batches exist or may exist
   */
  public boolean hasNextBatch() {
    if (closed) {
      return false;
    }
    if (!endOfStreamReached) {
      return true;
    }
    return currentBatchIndex.get() < highestFetchedBatchIndex.get();
  }

  /**
   * Moves to the next batch. Releases the previous batch to free memory.
   *
   * @return true if moved to next batch, false if no more batches
   * @throws DatabricksSQLException if an error occurred during prefetch
   */
  public boolean nextBatch() throws DatabricksSQLException {
    if (closed) {
      return false;
    }

    // Check for prefetch errors
    checkPrefetchError();

    // Release previous batch
    long prevIndex = currentBatchIndex.get();
    if (prevIndex >= 0) {
      releaseBatch(prevIndex);
    }

    if (!hasNextBatch()) {
      return false;
    }

    currentBatchIndex.incrementAndGet();
    notifyConsumerAdvanced();

    return true;
  }

  /**
   * Gets the current batch, waiting if necessary for it to be ready.
   *
   * @return The current batch, or null if before first batch
   * @throws DatabricksSQLException if the batch cannot be retrieved
   */
  public ThriftBatch getCurrentBatch() throws DatabricksSQLException {
    long batchIdx = currentBatchIndex.get();
    if (batchIdx < 0) {
      return null;
    }

    // Check for prefetch errors
    checkPrefetchError();

    ThriftBatch batch = batches.get(batchIdx);

    if (batch == null) {
      // Batch not yet created - wait for prefetch
      LOGGER.debug("Batch {} not yet available, waiting for prefetch", batchIdx);
      waitForBatchCreation(batchIdx);
      batch = batches.get(batchIdx);
    }

    if (batch == null) {
      throw new DatabricksSQLException(
          "Batch " + batchIdx + " not found after waiting",
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    // Wait for batch to be ready (data fetched)
    if (!batch.isReady()) {
      try {
        batch.waitUntilReady(batchReadyTimeoutSeconds);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabricksSQLException(
            "Interrupted waiting for batch " + batchIdx,
            e,
            DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
      } catch (ExecutionException e) {
        throw new DatabricksSQLException(
            "Failed to fetch batch " + batchIdx,
            e.getCause(),
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      } catch (TimeoutException e) {
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
   * @return the total row count
   */
  public long getTotalRowsFetched() {
    return totalRowsFetched.get();
  }

  /**
   * Gets number of batches currently in memory.
   *
   * @return the batch count
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
    if (closed) {
      return;
    }

    LOGGER.info("Closing ThriftBatchProvider, total rows fetched: {}", totalRowsFetched.get());
    closed = true;

    // Wake up waiting threads
    notifyConsumerAdvanced();
    notifyBatchAvailable();

    // Interrupt prefetch thread
    if (prefetchThread != null) {
      prefetchThread.interrupt();
    }

    // Shutdown executor
    if (prefetchExecutor != null) {
      prefetchExecutor.shutdownNow();
    }

    // Release all batches
    for (ThriftBatch batch : batches.values()) {
      batch.release();
    }
    batches.clear();

    // Close fetcher
    if (batchFetcher != null) {
      batchFetcher.close();
    }
  }

  // ==================== Prefetch Logic ====================

  private void prefetchLoop() {
    LOGGER.info(
        "[PREFETCH] Thread started - maxBatchesInMemory={}, timeoutSeconds={}",
        maxBatchesInMemory,
        batchReadyTimeoutSeconds);

    while (!closed && !Thread.currentThread().isInterrupted()) {
      try {
        // Wait if we have enough batches prefetched
        prefetchLock.lock();
        try {
          while (!closed && !endOfStreamReached && batchesInMemory.get() >= maxBatchesInMemory) {
            LOGGER.info(
                "[PREFETCH] Waiting for consumer - batchesInMemory={}/{}, currentConsumerBatch={}",
                batchesInMemory.get(),
                maxBatchesInMemory,
                currentBatchIndex.get());
            consumerAdvanced.await();
          }
        } finally {
          prefetchLock.unlock();
        }

        if (closed || endOfStreamReached) {
          LOGGER.info(
              "[PREFETCH] Exiting loop - closed={}, endOfStream={}", closed, endOfStreamReached);
          break;
        }

        // Fetch next batch
        fetchNextBatchInternal();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.info("[PREFETCH] Thread interrupted");
        break;
      } catch (DatabricksSQLException e) {
        LOGGER.error("[PREFETCH] Error: {}", e.getMessage());
        prefetchError = e;
        notifyBatchAvailable(); // Wake up waiting consumers
        break;
      } catch (Exception e) {
        LOGGER.error("[PREFETCH] Unexpected error: {}", e.getMessage(), e);
        prefetchError =
            new DatabricksSQLException(
                "Unexpected prefetch error: " + e.getMessage(),
                e,
                DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        notifyBatchAvailable();
        break;
      }
    }

    LOGGER.info(
        "[PREFETCH] Thread exiting - totalRowsFetched={}, batchesFetched={}",
        totalRowsFetched.get(),
        highestFetchedBatchIndex.get() + 1);
  }

  private void fetchNextBatchInternal() throws DatabricksSQLException {
    long batchIndex = nextBatchToFetch.getAndIncrement();
    long rowOffset = nextRowOffset.get();

    long fetchStartTime = System.currentTimeMillis();
    LOGGER.info(
        "[PREFETCH] Starting fetch for batch {} (rowOffset={}, batchesInMemory={})",
        batchIndex,
        rowOffset,
        batchesInMemory.get());

    // Create batch placeholder
    ThriftBatch batch = new ThriftBatch(batchIndex, rowOffset);
    batch.setFetching();
    batches.put(batchIndex, batch);
    batchesInMemory.incrementAndGet();

    // Notify that batch exists (even if not ready yet)
    notifyBatchAvailable();

    try {
      // Perform the actual fetch (blocking network call)
      TFetchResultsResp response = batchFetcher.fetchNextBatch();
      ColumnarRowView view = createColumnarView(response.getResults());

      long fetchDuration = System.currentTimeMillis() - fetchStartTime;

      // Update batch with data
      batch.setData(response, view);
      highestFetchedBatchIndex.updateAndGet(cur -> Math.max(cur, batchIndex));
      totalRowsFetched.addAndGet(view.getRowCount());
      nextRowOffset.addAndGet(view.getRowCount());

      LOGGER.info(
          "[PREFETCH] Batch {} ready - rows={}, fetchTime={}ms, totalRows={}, hasMore={}",
          batchIndex,
          view.getRowCount(),
          fetchDuration,
          totalRowsFetched.get(),
          response.hasMoreRows);

      // Check if end of stream
      if (!response.hasMoreRows) {
        endOfStreamReached = true;
        LOGGER.info(
            "[PREFETCH] End of stream at batch {} - totalRows={}",
            batchIndex,
            totalRowsFetched.get());
      }

      // Notify waiting consumers that batch is ready
      notifyBatchAvailable();

    } catch (DatabricksSQLException e) {
      batch.setError(e);
      batchesInMemory.decrementAndGet();
      batches.remove(batchIndex);
      throw e;
    }
  }

  private void processInitialBatch(TFetchResultsResp initialResponse)
      throws DatabricksSQLException {
    LOGGER.info("[INIT] Processing initial batch (batch 0)");

    ThriftBatch batch = new ThriftBatch(0, 0);
    ColumnarRowView view = createColumnarView(initialResponse.getResults());
    batch.setData(initialResponse, view);

    batches.put(0L, batch);
    highestFetchedBatchIndex.set(0);
    batchesInMemory.incrementAndGet();
    totalRowsFetched.addAndGet(view.getRowCount());
    nextRowOffset.set(view.getRowCount());

    LOGGER.info(
        "[INIT] Initial batch ready - rows={}, hasMoreRows={}",
        view.getRowCount(),
        initialResponse.hasMoreRows);

    if (!initialResponse.hasMoreRows) {
      endOfStreamReached = true;
      LOGGER.info("[INIT] Single batch result - all data in initial response");
    }
  }

  // ==================== Resource Management ====================

  private void releaseBatch(long batchIndex) {
    ThriftBatch batch = batches.remove(batchIndex);
    if (batch != null) {
      long rowsInBatch = batch.getRowCount();
      batch.release();
      int remaining = batchesInMemory.decrementAndGet();
      LOGGER.info(
          "[CONSUMER] Released batch {} ({} rows) - batchesInMemory={}/{}",
          batchIndex,
          rowsInBatch,
          remaining,
          maxBatchesInMemory);

      // Notify prefetch thread that there's room for more
      notifyConsumerAdvanced();
    }
  }

  private void waitForBatchCreation(long batchIndex) throws DatabricksSQLException {
    prefetchLock.lock();
    try {
      while (!closed && !batches.containsKey(batchIndex)) {
        // Check for errors
        checkPrefetchError();

        // Check if we're past end of stream
        if (endOfStreamReached && batchIndex > highestFetchedBatchIndex.get()) {
          throw new DatabricksSQLException(
              "Batch "
                  + batchIndex
                  + " does not exist (highest: "
                  + highestFetchedBatchIndex.get()
                  + ")",
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        try {
          batchAvailable.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
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
      throw new DatabricksSQLException(
          "Prefetch failed: " + prefetchError.getMessage(),
          prefetchError,
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }
  }

  // ==================== Synchronization ====================

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

  // ==================== Executor Creation ====================

  private ExecutorService createPrefetchExecutor() {
    ThreadFactory threadFactory =
        runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName(PREFETCH_THREAD_NAME + "-worker");
          thread.setDaemon(true);
          return thread;
        };

    return Executors.newSingleThreadExecutor(threadFactory);
  }
}
