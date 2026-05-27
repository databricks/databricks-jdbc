package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DecompressionUtil.decompress;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ChunkProvider for SEA inline Arrow results with rolling-window prefetch. Fetches chunks via
 * GetResultData with row_offset, using a background thread to prefetch ahead of the consumer.
 *
 * <p>Design mirrors {@link com.databricks.jdbc.api.impl.streaming.ThriftStreamingProvider
 * ThriftStreamingProvider} for consistency: uses Lock + Condition for signaling, ConcurrentHashMap
 * for indexed batch storage, and explicit batchesInMemory tracking for backpressure.
 *
 * <p>Used when UseBoundedSeaApi=1 AND EnableQueryResultDownload=0.
 */
class SeaInlineArrowChunkProvider implements ChunkProvider {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(SeaInlineArrowChunkProvider.class);
  private static final String PREFETCH_THREAD_NAME = "sea-inline-prefetcher";

  // Configuration
  private final int maxBatchesInMemory;
  private final int chunkReadyTimeoutSeconds;

  // Dependencies
  private final IDatabricksSession session;
  private final StatementId statementId;
  private final CompressionCodec compressionCodec;

  // Indexed chunk storage (mirrors ThriftStreamingProvider's ConcurrentHashMap<Long, Batch>)
  private final ConcurrentMap<Long, ArrowResultChunk> chunks = new ConcurrentHashMap<>();

  // Position tracking
  private final AtomicLong currentChunkIndex = new AtomicLong(-1);
  private final AtomicLong highestFetchedChunkIndex = new AtomicLong(-1);
  private final AtomicLong nextFetchChunkIndex = new AtomicLong(1); // 0 is initial chunk
  private final AtomicLong nextFetchRowOffset = new AtomicLong(0);
  private final AtomicLong totalRowCount = new AtomicLong(0);

  // State and synchronization (mirrors ThriftStreamingProvider's lock + conditions)
  private volatile boolean endOfStream = false;
  private volatile boolean closed = false;
  private volatile DatabricksSQLException prefetchError = null;
  private final ReentrantLock prefetchLock = new ReentrantLock();
  private final Condition chunkAvailable = prefetchLock.newCondition();
  private final Condition consumerAdvanced = prefetchLock.newCondition();
  private final AtomicInteger chunksInMemory = new AtomicInteger(0);

  // Prefetch thread
  private final Thread prefetchThread;

  SeaInlineArrowChunkProvider(
      ResultData initialResultData,
      ResultManifest resultManifest,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this.session = session;
    this.statementId = statementId;
    this.compressionCodec = resultManifest.getResultCompression();

    IDatabricksConnectionContext ctx = session.getConnectionContext();
    int configuredMax = ctx.getThriftMaxBatchesInMemory();
    // Need at least 2 to enable any prefetching (same as ThriftStreamingProvider)
    if (configuredMax < 2) {
      LOGGER.warn(
          "Configured maxBatchesInMemory={} is less than the minimum of 2; using 2 instead.",
          configuredMax);
    }
    this.maxBatchesInMemory = Math.max(2, configuredMax);
    this.chunkReadyTimeoutSeconds = ctx.getChunkReadyTimeoutSeconds();

    // Process initial chunk
    ArrowResultChunk firstChunk = processResultData(initialResultData);
    long rowCount = initialResultData.getRowCount() != null ? initialResultData.getRowCount() : 0;
    long rowOffset =
        initialResultData.getRowOffset() != null ? initialResultData.getRowOffset() : 0;
    totalRowCount.addAndGet(rowCount);
    nextFetchRowOffset.set(rowOffset + rowCount);

    // Store initial chunk
    chunks.put(0L, firstChunk);
    highestFetchedChunkIndex.set(0);
    chunksInMemory.incrementAndGet();

    // Determine if there are more chunks
    if (initialResultData.getNextChunkIndex() != null) {
      nextFetchChunkIndex.set(initialResultData.getNextChunkIndex());
    } else {
      endOfStream = true;
    }

    // Start prefetch thread (mirrors ThriftStreamingProvider)
    this.prefetchThread = new Thread(this::prefetchLoop, PREFETCH_THREAD_NAME);
    this.prefetchThread.setDaemon(true);
    this.prefetchThread.start();

    notifyConsumerAdvanced();

    LOGGER.debug(
        "SeaInlineArrowChunkProvider created: statement={}, maxBatches={}, endOfStream={}",
        statementId.toSQLExecStatementId(),
        maxBatchesInMemory,
        endOfStream);
  }

  @Override
  public boolean hasNextChunk() {
    if (closed) return false;
    if (!endOfStream) return true;
    return currentChunkIndex.get() < highestFetchedChunkIndex.get();
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (closed) return false;

    checkPrefetchError();

    // Release previous chunk
    long prevIndex = currentChunkIndex.get();
    if (prevIndex >= 0) {
      releaseChunk(prevIndex);
    }

    if (!hasNextChunk()) {
      return false;
    }

    long nextIndex = currentChunkIndex.incrementAndGet();
    notifyConsumerAdvanced();

    // Wait for the chunk to be available (mirrors ThriftStreamingProvider.getCurrentBatch)
    ArrowResultChunk chunk = chunks.get(nextIndex);
    if (chunk == null) {
      LOGGER.debug("Chunk {} not yet available, waiting for prefetch", nextIndex);
      waitForChunkCreation(nextIndex);
      chunk = chunks.get(nextIndex);
    }

    if (chunk == null) {
      LOGGER.error("Chunk {} not found after waiting", nextIndex);
      throw new DatabricksSQLException(
          "Chunk " + nextIndex + " not found after waiting",
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    return true;
  }

  @Override
  public ArrowResultChunk getChunk() {
    long idx = currentChunkIndex.get();
    if (idx < 0) return null;
    return chunks.get(idx);
  }

  @Override
  public void close() {
    if (closed) return;

    LOGGER.debug("Closing SeaInlineArrowChunkProvider, total rows: {}", totalRowCount.get());
    closed = true;

    notifyConsumerAdvanced();
    notifyChunkAvailable();

    // Interrupt and wait for prefetch thread (mirrors ThriftStreamingProvider)
    prefetchThread.interrupt();
    try {
      prefetchThread.join(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.debug("Interrupted while waiting for prefetch thread to terminate");
    }

    // Release all chunks
    for (ArrowResultChunk chunk : chunks.values()) {
      try {
        chunk.releaseChunk();
      } catch (Exception e) {
        LOGGER.warn("Error releasing chunk during close: {}", e.getMessage(), e);
      }
    }
    chunks.clear();
  }

  @Override
  public long getRowCount() {
    return totalRowCount.get();
  }

  @Override
  public long getChunkCount() {
    return currentChunkIndex.get() + 1;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  // ==================== Prefetch Logic ====================

  /** Background prefetch loop — mirrors ThriftStreamingProvider.prefetchLoop(). */
  private void prefetchLoop() {
    LOGGER.debug("Prefetch thread started");

    while (!closed && !Thread.currentThread().isInterrupted()) {
      try {
        // Wait until queue has room (backpressure from slow consumer)
        prefetchLock.lock();
        try {
          while (!closed && !endOfStream && chunksInMemory.get() >= maxBatchesInMemory) {
            LOGGER.debug(
                "Prefetch waiting: chunks={}/{}", chunksInMemory.get(), maxBatchesInMemory);
            consumerAdvanced.await();
          }
        } finally {
          prefetchLock.unlock();
        }

        if (closed || endOfStream) break;

        fetchNextChunkInternal();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.debug("Prefetch thread interrupted");
        break;
      } catch (DatabricksSQLException e) {
        LOGGER.error("Prefetch error: {}", e.getMessage());
        prefetchError = e;
        notifyChunkAvailable();
        break;
      } catch (Exception e) {
        LOGGER.error("Unexpected prefetch error: {}", e.getMessage(), e);
        prefetchError =
            new DatabricksSQLException(
                "Unexpected prefetch error: " + e.getMessage(),
                e,
                DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
        notifyChunkAvailable();
        break;
      }
    }

    LOGGER.debug("Prefetch thread exiting");
  }

  /**
   * Fetches a single chunk from the server — mirrors
   * ThriftStreamingProvider.fetchNextBatchInternal().
   */
  private void fetchNextChunkInternal() throws DatabricksSQLException {
    long chunkIndex = nextFetchChunkIndex.get();
    long rowOffset = nextFetchRowOffset.get();

    LOGGER.debug("Fetching inline chunk {} at offset {}", chunkIndex, rowOffset);

    DatabricksSdkClient client = (DatabricksSdkClient) session.getDatabricksClient();
    ResultData resultData = client.getResultChunksData(statementId, chunkIndex, rowOffset);

    ArrowResultChunk chunk = processResultData(resultData);
    long rowCount = resultData.getRowCount() != null ? resultData.getRowCount() : 0;

    // Store chunk and update state
    chunks.put(chunkIndex, chunk);
    chunksInMemory.incrementAndGet();
    highestFetchedChunkIndex.updateAndGet(cur -> Math.max(cur, chunkIndex));
    totalRowCount.addAndGet(rowCount);
    nextFetchRowOffset.addAndGet(rowCount);

    LOGGER.debug(
        "Chunk {} ready: rowCount={}, hasMore={}",
        chunkIndex,
        rowCount,
        resultData.getNextChunkIndex() != null);

    // Update continuation
    if (resultData.getNextChunkIndex() != null) {
      nextFetchChunkIndex.set(resultData.getNextChunkIndex());
    } else {
      endOfStream = true;
      LOGGER.debug("End of stream at chunk {}", chunkIndex);
    }

    notifyChunkAvailable();
  }

  // ==================== Resource Management ====================

  private void releaseChunk(long chunkIndex) {
    ArrowResultChunk chunk = chunks.remove(chunkIndex);
    if (chunk != null) {
      // Decrement counter BEFORE release to prevent prefetch stall if release throws
      chunksInMemory.decrementAndGet();
      try {
        chunk.releaseChunk();
      } catch (Exception e) {
        LOGGER.warn("Error releasing chunk {}: {}", chunkIndex, e.getMessage(), e);
      }
      LOGGER.debug("Released chunk {}, chunks in memory: {}", chunkIndex, chunksInMemory.get());
      notifyConsumerAdvanced();
    }
  }

  /**
   * Waits for a chunk to be created by the prefetch thread. Mirrors
   * ThriftStreamingProvider.waitForBatchCreation().
   */
  private void waitForChunkCreation(long chunkIndex) throws DatabricksSQLException {
    prefetchLock.lock();
    try {
      long waitStartTime = System.currentTimeMillis();
      long timeoutMillis = chunkReadyTimeoutSeconds * 1000L;

      while (!closed && !chunks.containsKey(chunkIndex)) {
        checkPrefetchError();
        if (endOfStream && chunkIndex > highestFetchedChunkIndex.get()) {
          LOGGER.error(
              "Chunk {} does not exist (highest fetched: {})",
              chunkIndex,
              highestFetchedChunkIndex.get());
          throw new DatabricksSQLException(
              "Chunk "
                  + chunkIndex
                  + " does not exist (highest: "
                  + highestFetchedChunkIndex.get()
                  + ")",
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        long elapsedMillis = System.currentTimeMillis() - waitStartTime;
        if (elapsedMillis >= timeoutMillis) {
          LOGGER.error(
              "Timeout waiting for chunk {} to be created (timeout: {}s)",
              chunkIndex,
              chunkReadyTimeoutSeconds);
          throw new DatabricksSQLException(
              "Timeout waiting for chunk "
                  + chunkIndex
                  + " to be created (timeout: "
                  + chunkReadyTimeoutSeconds
                  + "s)",
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        try {
          long remainingMillis = timeoutMillis - elapsedMillis;
          chunkAvailable.await(remainingMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn("Interrupted waiting for chunk {} creation", chunkIndex);
          throw new DatabricksSQLException(
              "Interrupted waiting for chunk",
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

  private void notifyChunkAvailable() {
    prefetchLock.lock();
    try {
      chunkAvailable.signalAll();
    } finally {
      prefetchLock.unlock();
    }
  }

  // ==================== Data Processing ====================

  /** Decompresses attachment bytes and creates an ArrowResultChunk. */
  private ArrowResultChunk processResultData(ResultData resultData) throws DatabricksSQLException {
    byte[] attachment = resultData.getAttachment();
    if (attachment == null || attachment.length == 0) {
      throw new DatabricksSQLException(
          "No inline Arrow data (attachment) in result",
          DatabricksDriverErrorCode.RESULT_SET_ERROR);
    }

    byte[] decompressedBytes =
        decompress(attachment, compressionCodec, "SEA inline Arrow chunk decompression");

    long rowCount = resultData.getRowCount() != null ? resultData.getRowCount() : 0;
    return ArrowResultChunk.builder()
        .withInputStream(new ByteArrayInputStream(decompressedBytes), rowCount)
        .build();
  }
}
