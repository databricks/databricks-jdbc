package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;
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
import javax.annotation.Nonnull;

/**
 * A streaming chunk provider that fetches chunk links proactively and downloads chunks in parallel.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>No dependency on total chunk count - streams until end of data
 *   <li>Proactive link prefetching with configurable window
 *   <li>Memory-bounded parallel downloads
 *   <li>Automatic link refresh on expiration
 * </ul>
 *
 * <p>This provider uses two key windows:
 *
 * <ul>
 *   <li>Link prefetch window: How many links to fetch ahead of consumption
 *   <li>Download window: How many chunks to keep in memory (downloading or ready)
 * </ul>
 */
public class StreamingChunkProvider implements ChunkProvider {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(StreamingChunkProvider.class);
  private static final String DOWNLOAD_THREAD_PREFIX = "databricks-jdbc-streaming-downloader-";
  private static final String PREFETCH_THREAD_NAME = "databricks-jdbc-link-prefetcher";

  // Configuration
  private final int linkPrefetchWindow;
  private final int maxChunksInMemory;
  private final int chunkReadyTimeoutSeconds;

  // Dependencies
  private final ChunkLinkFetcher linkFetcher;
  private final IDatabricksHttpClient httpClient;
  private final CompressionCodec compressionCodec;
  private final StatementId statementId;
  private final double cloudFetchSpeedThreshold;
  private final IDatabricksConnectionContext connectionContext;

  // Chunk storage
  private final ConcurrentMap<Long, ArrowResultChunk> chunks = new ConcurrentHashMap<>();

  // Position tracking
  // Using AtomicLong for single-writer variables to make thread-safety explicit:
  // - currentChunkIndex: written only by consumer thread
  // - highestKnownChunkIndex: written only by prefetch thread (after construction)
  // - nextDownloadIndex: written only under downloadLock, but AtomicLong for consistency
  private final AtomicLong currentChunkIndex = new AtomicLong(-1);
  private final AtomicLong highestKnownChunkIndex = new AtomicLong(-1);
  private volatile long nextLinkFetchIndex = 0;
  private volatile long nextRowOffsetToFetch = 0;
  private final AtomicLong nextDownloadIndex = new AtomicLong(0);

  // State flags
  private volatile boolean endOfStreamReached = false;
  private volatile boolean closed = false;
  private volatile DatabricksSQLException prefetchError = null;

  // Row tracking
  private final AtomicLong totalRowCount = new AtomicLong(0);

  // Synchronization for prefetch thread
  private final ReentrantLock prefetchLock = new ReentrantLock();
  private final Condition consumerAdvanced = prefetchLock.newCondition();
  private final Condition chunkCreated = prefetchLock.newCondition();

  // Synchronization for download coordination.
  // This lock is needed because triggerDownloads() is called from both the prefetch thread
  // (via fetchNextLinkBatch) and the consumer thread (via releaseChunk), and the download
  // logic reads multiple shared variables (chunksInMemory, nextDownloadIndex,
  // highestKnownChunkIndex)
  // that must be consistent within the loop.
  private final ReentrantLock downloadLock = new ReentrantLock();

  // Executors
  private final ExecutorService downloadExecutor;
  private final Thread linkPrefetchThread;

  // Track chunks currently in memory (for sliding window)
  private final AtomicInteger chunksInMemory = new AtomicInteger(0);

  /**
   * Creates a new StreamingChunkProvider.
   *
   * @param linkFetcher Fetcher for chunk links
   * @param httpClient HTTP client for downloads
   * @param compressionCodec Codec for decompressing chunk data
   * @param statementId Statement ID for logging and chunk creation
   * @param maxChunksInMemory Maximum chunks to keep in memory (download window)
   * @param linkPrefetchWindow How many links to fetch ahead
   * @param chunkReadyTimeoutSeconds Timeout waiting for chunk to be ready
   * @param cloudFetchSpeedThreshold Speed threshold for logging warnings
   * @param initialLinks Initial links provided with result data (avoids extra fetch), may be null
   */
  public StreamingChunkProvider(
      ChunkLinkFetcher linkFetcher,
      IDatabricksHttpClient httpClient,
      CompressionCodec compressionCodec,
      StatementId statementId,
      int maxChunksInMemory,
      int linkPrefetchWindow,
      int chunkReadyTimeoutSeconds,
      double cloudFetchSpeedThreshold,
      IDatabricksConnectionContext connectionContext,
      ChunkLinkFetchResult initialLinks)
      throws DatabricksParsingException {

    this.linkFetcher = linkFetcher;
    this.httpClient = httpClient;
    this.compressionCodec = compressionCodec;
    this.statementId = statementId;
    this.maxChunksInMemory = maxChunksInMemory;
    this.linkPrefetchWindow = linkPrefetchWindow;
    this.chunkReadyTimeoutSeconds = chunkReadyTimeoutSeconds;
    this.cloudFetchSpeedThreshold = cloudFetchSpeedThreshold;
    this.connectionContext = connectionContext;

    LOGGER.info(
        "Creating StreamingChunkProvider for statement {}: maxChunksInMemory={}, linkPrefetchWindow={}",
        statementId,
        maxChunksInMemory,
        linkPrefetchWindow);

    // Process initial links if provided
    processInitialLinks(initialLinks);

    // Create download executor
    this.downloadExecutor = createDownloadExecutor(maxChunksInMemory);

    // Start link prefetch thread
    this.linkPrefetchThread = new Thread(this::linkPrefetchLoop, PREFETCH_THREAD_NAME);
    this.linkPrefetchThread.setDaemon(true);
    this.linkPrefetchThread.start();

    // Trigger initial downloads and prefetch
    triggerDownloads();
    notifyConsumerAdvanced();
  }

  // ==================== ChunkProvider Interface ====================

  @Override
  public boolean hasNextChunk() {
    if (closed) {
      return false;
    }

    // If we haven't reached end of stream, there might be more
    if (!endOfStreamReached) {
      return true;
    }

    // We've reached end of stream - check if there are unconsumed chunks
    return currentChunkIndex.get() < highestKnownChunkIndex.get();
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (closed) {
      return false;
    }

    // Release previous chunk if any
    long prevIndex = currentChunkIndex.get();
    if (prevIndex >= 0) {
      releaseChunk(prevIndex);
    }

    if (!hasNextChunk()) {
      return false;
    }

    currentChunkIndex.incrementAndGet();

    // Notify prefetch thread that consumer advanced
    notifyConsumerAdvanced();

    return true;
  }

  @Override
  public AbstractArrowResultChunk getChunk() throws DatabricksSQLException {
    long chunkIdx = currentChunkIndex.get();
    if (chunkIdx < 0) {
      return null;
    }

    ArrowResultChunk chunk = chunks.get(chunkIdx);

    if (chunk == null) {
      // Chunk not yet created - wait for it
      LOGGER.debug("Chunk {} not yet available, waiting for prefetch", chunkIdx);
      waitForChunkCreation(chunkIdx);
      chunk = chunks.get(chunkIdx);
    }

    if (chunk == null) {
      throw new DatabricksSQLException(
          "Chunk " + chunkIdx + " not found after waiting",
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    // Wait for chunk to be ready (downloaded and processed)
    try {
      chunk.waitForChunkReady();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabricksSQLException(
          "Interrupted waiting for chunk " + chunkIdx,
          e,
          DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
    } catch (ExecutionException e) {
      throw new DatabricksSQLException(
          "Failed to prepare chunk " + chunkIdx,
          e.getCause(),
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    } catch (TimeoutException e) {
      throw new DatabricksSQLException(
          "Timeout waiting for chunk " + chunkIdx + " (timeout: " + chunkReadyTimeoutSeconds + "s)",
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    return chunk;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    LOGGER.info("Closing StreamingChunkProvider for statement {}", statementId);
    closed = true;

    // Wake up any waiting threads so they can exit
    notifyConsumerAdvanced();
    notifyChunkCreated();

    // Interrupt prefetch thread
    if (linkPrefetchThread != null) {
      linkPrefetchThread.interrupt();
    }

    // Shutdown download executor
    if (downloadExecutor != null) {
      downloadExecutor.shutdownNow();
    }

    // Release all chunks
    for (ArrowResultChunk chunk : chunks.values()) {
      try {
        chunk.releaseChunk();
      } catch (Exception e) {
        LOGGER.warn("Error releasing chunk: {}", e.getMessage());
      }
    }
    chunks.clear();

    // Close link fetcher
    if (linkFetcher != null) {
      linkFetcher.close();
    }
  }

  @Override
  public long getRowCount() {
    return totalRowCount.get();
  }

  /**
   * Returns the total chunk count only when all chunks have been discovered.
   *
   * <p>In streaming mode, the total chunk count is unknown until we reach the end of the stream.
   * This method returns -1 if chunks are still being discovered, and the actual count once all
   * chunks have been fetched.
   *
   * @return the total chunk count if all chunks have been discovered, or -1 if still streaming
   */
  @Override
  public long getChunkCount() {
    // In streaming mode, we don't know total chunks until end of stream
    if (endOfStreamReached) {
      return highestKnownChunkIndex.get() + 1;
    }
    return -1; // Unknown
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  // ==================== Link Prefetch Logic ====================

  private void linkPrefetchLoop() {
    LOGGER.debug("Link prefetch thread started for statement {}", statementId);

    while (!closed && !Thread.currentThread().isInterrupted()) {
      try {
        prefetchLock.lock();
        try {
          long targetIndex = currentChunkIndex.get() + linkPrefetchWindow;

          // Wait if we're caught up
          while (!endOfStreamReached && nextLinkFetchIndex > targetIndex) {
            if (closed) break;
            LOGGER.debug(
                "Prefetch caught up, waiting for consumer. next={}, target={}",
                nextLinkFetchIndex,
                targetIndex);
            consumerAdvanced.await();
            targetIndex = currentChunkIndex.get() + linkPrefetchWindow;
          }
        } finally {
          prefetchLock.unlock();
        }

        if (closed || endOfStreamReached) {
          break;
        }

        // Fetch next batch of links
        fetchNextLinkBatch();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.debug("Link prefetch thread interrupted");
        break;
      } catch (DatabricksSQLException e) {
        LOGGER.error("Error fetching links: {}", e.getMessage());
        prefetchError = e;
        notifyChunkCreated(); // Wake up any waiting consumer to check the error
        break;
      } catch (Exception e) {
        LOGGER.error("Unexpected error in link prefetch: {}", e.getMessage(), e);
        prefetchError =
            new DatabricksSQLException(
                "Unexpected error in link prefetch: " + e.getMessage(),
                e,
                DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        notifyChunkCreated(); // Wake up any waiting consumer to check the error
        break;
      }
    }

    LOGGER.debug("Link prefetch thread exiting for statement {}", statementId);
  }

  private void fetchNextLinkBatch() throws SQLException {
    if (endOfStreamReached || closed) {
      return;
    }

    LOGGER.debug(
        "Fetching links starting from index {}, row offset {} for statement {}",
        nextLinkFetchIndex,
        nextRowOffsetToFetch,
        statementId);

    ChunkLinkFetchResult result = linkFetcher.fetchLinks(nextLinkFetchIndex, nextRowOffsetToFetch);

    if (result.isEndOfStream()) {
      LOGGER.info("End of stream reached for statement {}", statementId);
      endOfStreamReached = true;
      return;
    }

    // Process received links - create chunks
    for (ExternalLink link : result.getChunkLinks()) {
      createChunkFromLink(link);
    }

    // Update next fetch positions
    if (result.hasMore()) {
      nextLinkFetchIndex = result.getNextFetchIndex();
      nextRowOffsetToFetch = result.getNextRowOffset();
    } else {
      endOfStreamReached = true;
      LOGGER.info("End of stream reached for statement {} (hasMore=false)", statementId);
    }

    // Trigger downloads for new chunks
    triggerDownloads();
  }

  /**
   * Processes initial links provided with the result data. This avoids an extra fetch call for
   * links the server already provided.
   *
   * @param initialLinks The initial links from ResultData, may be null
   */
  private void processInitialLinks(ChunkLinkFetchResult initialLinks)
      throws DatabricksParsingException {
    if (initialLinks == null) {
      LOGGER.debug("No initial links provided for statement {}", statementId);
      return;
    }

    LOGGER.info(
        "Processing {} initial links for statement {}",
        initialLinks.getChunkLinks().size(),
        statementId);

    for (ExternalLink link : initialLinks.getChunkLinks()) {
      createChunkFromLink(link);
    }

    // Set next fetch positions using unified API
    if (initialLinks.hasMore()) {
      nextLinkFetchIndex = initialLinks.getNextFetchIndex();
      nextRowOffsetToFetch = initialLinks.getNextRowOffset();
      LOGGER.debug(
          "Next fetch position set to chunk index {}, row offset {} from initial links",
          nextLinkFetchIndex,
          nextRowOffsetToFetch);
    } else {
      endOfStreamReached = true;
      LOGGER.info("End of stream reached from initial links for statement {}", statementId);
    }
  }

  /**
   * Creates a chunk from an external link and registers it for download.
   *
   * @param link The external link containing chunkIndex, rowCount, rowOffset, and download URL
   */
  private void createChunkFromLink(ExternalLink link) throws DatabricksParsingException {
    long chunkIndex = link.getChunkIndex();
    if (chunks.containsKey(chunkIndex)) {
      LOGGER.debug("Chunk {} already exists, skipping creation", chunkIndex);
      return;
    }

    long rowCount = link.getRowCount();
    long rowOffset = link.getRowOffset();

    ArrowResultChunk chunk =
        ArrowResultChunk.builder()
            .withStatementId(statementId)
            .withChunkMetadata(chunkIndex, rowCount, rowOffset)
            .withChunkReadyTimeoutSeconds(chunkReadyTimeoutSeconds)
            .withConnectionContext(connectionContext)
            .build();

    chunk.setChunkLink(link);
    chunks.put(chunkIndex, chunk);
    highestKnownChunkIndex.updateAndGet(current -> Math.max(current, chunkIndex));
    totalRowCount.addAndGet(rowCount);

    // Notify any waiting consumers that a chunk is available
    notifyChunkCreated();

    LOGGER.debug(
        "Created chunk {} with {} rows for statement {}", chunkIndex, rowCount, statementId);
  }

  // ==================== Download Coordination ====================

  private void triggerDownloads() {
    downloadLock.lock();
    try {
      long downloadIdx = nextDownloadIndex.get();
      while (!closed
          && chunksInMemory.get() < maxChunksInMemory
          && downloadIdx <= highestKnownChunkIndex.get()) {
        ArrowResultChunk chunk = chunks.get(downloadIdx);

        if (chunk == null) {
          // Chunk not yet created, wait for prefetch
          break;
        }

        // Only submit if not already downloading/downloaded
        ChunkStatus status = chunk.getStatus();
        if (status == ChunkStatus.PENDING || status == ChunkStatus.URL_FETCHED) {
          submitDownloadTask(chunk);
          chunksInMemory.incrementAndGet();
        }

        downloadIdx = nextDownloadIndex.incrementAndGet();
      }
    } finally {
      downloadLock.unlock();
    }
  }

  private void submitDownloadTask(ArrowResultChunk chunk) {
    LOGGER.debug("Submitting download task for chunk {}", chunk.getChunkIndex());

    StreamingChunkDownloadTask task =
        new StreamingChunkDownloadTask(
            chunk, httpClient, compressionCodec, linkFetcher, cloudFetchSpeedThreshold);

    downloadExecutor.submit(task);
  }

  // ==================== Resource Management ====================

  private void releaseChunk(long chunkIndex) {
    ArrowResultChunk chunk = chunks.get(chunkIndex);
    if (chunk != null && chunk.releaseChunk()) {
      chunks.remove(chunkIndex);
      chunksInMemory.decrementAndGet();

      LOGGER.debug("Released chunk {}, chunksInMemory={}", chunkIndex, chunksInMemory.get());

      // Trigger more downloads to fill the freed slot
      triggerDownloads();
    }
  }

  /**
   * Waits for a chunk to be created by the prefetch thread.
   *
   * <p>This method waits indefinitely for the chunk to be created, relying on the following exit
   * conditions:
   *
   * <ul>
   *   <li>Chunk is created (success)
   *   <li>Provider is closed
   *   <li>Prefetch thread encountered an error
   *   <li>End of stream reached and chunk doesn't exist
   *   <li>Thread is interrupted
   * </ul>
   *
   * <p>The overall timeout for chunk retrieval is enforced by {@link
   * ArrowResultChunk#waitForChunkReady()} which has a configurable timeout.
   */
  private void waitForChunkCreation(long chunkIndex) throws DatabricksSQLException {
    prefetchLock.lock();
    try {
      while (!closed && !chunks.containsKey(chunkIndex)) {
        // Check if prefetch thread encountered an error
        if (prefetchError != null) {
          throw new DatabricksSQLException(
              "Link prefetch failed: " + prefetchError.getMessage(),
              prefetchError,
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        long highestKnown = highestKnownChunkIndex.get();
        if (endOfStreamReached && chunkIndex > highestKnown) {
          throw new DatabricksSQLException(
              "Chunk " + chunkIndex + " does not exist (highest known: " + highestKnown + ")",
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        try {
          chunkCreated.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabricksSQLException(
              "Interrupted waiting for chunk creation",
              e,
              DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
        }
      }
    } finally {
      prefetchLock.unlock();
    }
  }

  // ==================== Synchronization Helpers ====================

  private void notifyConsumerAdvanced() {
    prefetchLock.lock();
    try {
      consumerAdvanced.signalAll();
    } finally {
      prefetchLock.unlock();
    }
  }

  private void notifyChunkCreated() {
    prefetchLock.lock();
    try {
      chunkCreated.signalAll();
    } finally {
      prefetchLock.unlock();
    }
  }

  // ==================== Executor Creation ====================

  private ExecutorService createDownloadExecutor(int poolSize) {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private final AtomicInteger threadCount = new AtomicInteger(1);

          @Override
          public Thread newThread(@Nonnull Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(DOWNLOAD_THREAD_PREFIX + threadCount.getAndIncrement());
            thread.setDaemon(true);
            return thread;
          }
        };

    return Executors.newFixedThreadPool(poolSize, threadFactory);
  }
}
