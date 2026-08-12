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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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

  /** Immutable holder for the next fetch position — ensures atomic reads of (index, rowOffset). */
  private static final class FetchPosition {
    final long chunkIndex;
    final long rowOffset;

    FetchPosition(long chunkIndex, long rowOffset) {
      this.chunkIndex = chunkIndex;
      this.rowOffset = rowOffset;
    }
  }

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

  // Ordered queues of chunk indices — populated by createChunkFromLink, consumed by
  // next() and triggerDownloads() respectively. Uses server-provided chunk indices
  // (which may not be sequential under bounded SEA API) instead of sequential counters.
  private final ConcurrentLinkedQueue<Long> consumptionOrder = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<Long> downloadOrder = new ConcurrentLinkedQueue<>();

  // Position tracking
  // Using AtomicLong for single-writer variables to make thread-safety explicit:
  // - currentChunkIndex: written only by consumer thread
  // - highestKnownChunkIndex: written only by prefetch thread (after construction)
  private volatile long currentChunkIndex = -1;
  private final AtomicLong consumedChunkCount = new AtomicLong(0);
  private final AtomicLong highestKnownChunkIndex = new AtomicLong(-1);
  // Bundled as an immutable pair for atomic reads/writes across threads.
  // The prefetch thread reads this (fetchNextLinkBatch) while the download thread
  // may update it (getRefreshedLink reconciliation). A volatile reference to an
  // immutable holder ensures both fields are always read consistently.
  private volatile FetchPosition nextFetchPosition = new FetchPosition(0, 0);

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
  // logic reads multiple shared variables (chunksInMemory, downloadOrder)
  // that must be consistent within the loop.
  private final ReentrantLock downloadLock = new ReentrantLock();

  // Synchronization for coalescing concurrent expired-link refreshes.
  // When multiple download threads detect expired links simultaneously, only one thread
  // performs the actual batch FetchResults RPC while others wait and reuse the result.
  private final Object refetchLock = new Object();

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
    return !consumptionOrder.isEmpty();
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (closed) {
      return false;
    }

    // Release previous chunk if any
    if (currentChunkIndex >= 0) {
      releaseChunk(currentChunkIndex);
    }

    if (!hasNextChunk()) {
      return false;
    }

    // Poll the next server chunk index from the consumption queue.
    // If the queue is empty but endOfStream is false, wait for prefetch to enqueue one.
    Long nextIndex = consumptionOrder.poll();
    if (nextIndex == null) {
      waitForNextChunkEnqueued();
      nextIndex = consumptionOrder.poll();
    }

    if (nextIndex == null) {
      if (endOfStreamReached) return false;
      throw new DatabricksSQLException(
          "No chunk available after waiting", DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    currentChunkIndex = nextIndex;
    consumedChunkCount.incrementAndGet();

    // Notify prefetch thread that consumer advanced
    notifyConsumerAdvanced();

    return true;
  }

  @Override
  public AbstractArrowResultChunk getChunk() throws DatabricksSQLException {
    long chunkIdx = currentChunkIndex;
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
      // Wait for download threads to finish error handling before releasing chunks.
      // After shutdownNow() interrupts threads, they exit their retry sleep and process
      // the error path (catch → finally → setStatus) in milliseconds. 3 seconds is a
      // conservative upper bound to avoid racing with that error handling path.
      try {
        if (!downloadExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
          LOGGER.warn("Download threads did not terminate within timeout");
        }
      } catch (InterruptedException e) {
        LOGGER.error(e, "Interrupted while waiting for download threads to terminate");
        Thread.currentThread().interrupt();
      }
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
      return consumedChunkCount.get() + consumptionOrder.size();
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
          long targetIndex = currentChunkIndex + linkPrefetchWindow;

          // Wait if we're caught up
          while (!endOfStreamReached && nextFetchPosition.chunkIndex > targetIndex) {
            if (closed) break;
            LOGGER.debug(
                "Prefetch caught up, waiting for consumer. next={}, target={}",
                nextFetchPosition.chunkIndex,
                targetIndex);
            consumerAdvanced.await();
            targetIndex = currentChunkIndex + linkPrefetchWindow;
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

    FetchPosition pos = nextFetchPosition;
    LOGGER.debug(
        "Fetching links starting from index {}, row offset {} for statement {}",
        pos.chunkIndex,
        pos.rowOffset,
        statementId);

    ChunkLinkFetchResult result = linkFetcher.fetchLinks(pos.chunkIndex, pos.rowOffset);

    if (result.isEndOfStream()) {
      LOGGER.info("End of stream reached for statement {}", statementId);
      endOfStreamReached = true;
      notifyChunkCreated(); // Wake consumer parked in waitForNextChunkEnqueued
      return;
    }

    // Process received links - create chunks
    for (ExternalLink link : result.getChunkLinks()) {
      createChunkFromLink(link);
    }

    // Update next fetch position atomically
    if (result.hasMore()) {
      nextFetchPosition = new FetchPosition(result.getNextFetchIndex(), result.getNextRowOffset());
    } else {
      endOfStreamReached = true;
      LOGGER.info("End of stream reached for statement {} (hasMore=false)", statementId);
      notifyChunkCreated(); // Wake consumer parked in waitForNextChunkEnqueued
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

    // Set next fetch position atomically
    if (initialLinks.hasMore()) {
      FetchPosition pos =
          new FetchPosition(initialLinks.getNextFetchIndex(), initialLinks.getNextRowOffset());
      nextFetchPosition = pos;
      LOGGER.debug(
          "Next fetch position set to chunk index {}, row offset {} from initial links",
          pos.chunkIndex,
          pos.rowOffset);
    } else {
      endOfStreamReached = true;
      notifyChunkCreated(); // Wake consumer parked in waitForNextChunkEnqueued
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

    // Atomic insert — if another thread already created this chunk, skip.
    // This is safe because createChunkFromLink can be called concurrently from
    // the prefetch thread (fetchNextLinkBatch) and download threads (getRefreshedLink).
    ArrowResultChunk existing = chunks.putIfAbsent(chunkIndex, chunk);
    if (existing != null) {
      LOGGER.debug("Chunk {} already exists, skipping creation", chunkIndex);
      return;
    }

    // Enqueue for consumption and download in server-provided order
    consumptionOrder.add(chunkIndex);
    downloadOrder.add(chunkIndex);
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
      while (!closed && chunksInMemory.get() < maxChunksInMemory) {
        Long downloadIdx = downloadOrder.peek();
        if (downloadIdx == null) {
          break;
        }

        ArrowResultChunk chunk = chunks.get(downloadIdx);
        if (chunk == null) {
          break;
        }

        // Only submit if not already downloading/downloaded
        ChunkStatus status = chunk.getStatus();
        if (status == ChunkStatus.PENDING || status == ChunkStatus.URL_FETCHED) {
          submitDownloadTask(chunk);
          chunksInMemory.incrementAndGet();
        }

        downloadOrder.poll();
      }
    } finally {
      downloadLock.unlock();
    }
  }

  /**
   * Coalesces concurrent expired-link refresh requests into a single batch RPC.
   *
   * <p>When multiple download threads detect expired links simultaneously, the first thread to
   * acquire the lock performs a single {@link ChunkLinkFetcher#fetchLinks} call from the lowest
   * expired chunk offset. The response refreshes all expired in-memory chunks. Subsequent threads
   * find their chunks already refreshed via the double-check pattern.
   *
   * <p>If the batch response does not include the requested chunk, falls back to a single {@link
   * ChunkLinkFetcher#refetchLink} call.
   *
   * @param chunkIndex The chunk index whose link has expired
   * @param rowOffset The row offset of the chunk (used by Thrift)
   * @return The refreshed ExternalLink
   * @throws SQLException if the refresh fails
   */
  ExternalLink getRefreshedLink(long chunkIndex, long rowOffset) throws SQLException {
    synchronized (refetchLock) {
      // Double-check: another thread may have already refreshed while we waited on the lock
      ArrowResultChunk targetChunk = chunks.get(chunkIndex);
      if (targetChunk != null && !targetChunk.isChunkLinkInvalid()) {
        return targetChunk.getChunkLink();
      }

      // Find the minimum expired chunk index among pre-download chunks.
      // Only called from StreamingChunkDownloadTask before download starts, so the
      // target chunk is always pre-download. We also skip chunks that have already been
      // downloaded (DOWNLOAD_IN_PROGRESS or later) since their links are no longer needed.
      long minExpiredIndex = Long.MAX_VALUE;
      long minExpiredRowOffset = 0;
      int expiredCount = 0;
      for (ArrowResultChunk c : chunks.values()) {
        ChunkStatus status = c.getStatus();
        if (status != ChunkStatus.PENDING
            && status != ChunkStatus.URL_FETCHED
            && status != ChunkStatus.DOWNLOAD_FAILED
            && status != ChunkStatus.DOWNLOAD_RETRY) {
          continue; // Already downloading or downloaded — link refresh not needed
        }
        if (c.isChunkLinkInvalid()) {
          expiredCount++;
          if (c.getChunkIndex() < minExpiredIndex) {
            minExpiredIndex = c.getChunkIndex();
            minExpiredRowOffset = c.getStartRowOffset();
          }
        }
      }

      if (minExpiredIndex == Long.MAX_VALUE) {
        // No expired chunks found — race condition resolved by another thread
        if (targetChunk != null) {
          return targetChunk.getChunkLink();
        }
        throw new DatabricksSQLException(
            "Chunk " + chunkIndex + " not found during link refresh",
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      }

      LOGGER.info(
          "Coalesced link refresh: fetching from chunk {} (row offset {}) for {} expired chunks",
          minExpiredIndex,
          minExpiredRowOffset,
          expiredCount);

      // Single batch FetchResults RPC from the lowest expired offset
      ChunkLinkFetchResult result = linkFetcher.fetchLinks(minExpiredIndex, minExpiredRowOffset);

      // Reconcile ALL links from the refresh response with local chunk state.
      boolean allRefreshChunksCreated = true;
      for (ExternalLink link : result.getChunkLinks()) {
        ArrowResultChunk c = chunks.get(link.getChunkIndex());
        if (c != null) {
          // Existing chunk: update link only for pre-download states.
          // DOWNLOADING stays as-is (download task owns the state machine).
          // DOWNLOADED/RELEASED/etc. stay as-is (bytes already in memory).
          ChunkStatus status = c.getStatus();
          if (status == ChunkStatus.PENDING
              || status == ChunkStatus.URL_FETCHED
              || status == ChunkStatus.DOWNLOAD_FAILED
              || status == ChunkStatus.DOWNLOAD_RETRY) {
            c.setChunkLink(link);
          }
        } else {
          // Server returned a chunk not yet in our map — create it.
          // Handles cases where refresh response includes chunks beyond
          // our current highestKnownChunkIndex.
          try {
            createChunkFromLink(link);
          } catch (Exception e) {
            LOGGER.warn(
                "Failed to create chunk {} from refresh response: {}",
                link.getChunkIndex(),
                e.getMessage());
            allRefreshChunksCreated = false;
          }
        }
      }

      // Update end-of-stream and prefetch position from refresh response.
      // Only advance past the refresh window if all chunks were successfully created —
      // otherwise the prefetch loop would skip the failed chunk, leaving a gap that
      // causes consumers to hang forever in waitForChunkCreation.
      if (!result.hasMore()) {
        endOfStreamReached = true;
        notifyChunkCreated(); // Wake consumer parked in waitForNextChunkEnqueued
      } else if (result.getNextFetchIndex() > nextFetchPosition.chunkIndex
          && allRefreshChunksCreated) {
        nextFetchPosition =
            new FetchPosition(result.getNextFetchIndex(), result.getNextRowOffset());
      }

      // Trigger downloads for any newly-created chunks
      triggerDownloads();

      // Check if our target chunk was refreshed by the batch
      targetChunk = chunks.get(chunkIndex);
      if (targetChunk != null && !targetChunk.isChunkLinkInvalid()) {
        return targetChunk.getChunkLink();
      }

      // Fallback: batch response did not include the requested chunk
      LOGGER.warn(
          "Batch refresh did not include chunk {}, falling back to single refetch", chunkIndex);
      ExternalLink fallbackLink = linkFetcher.refetchLink(chunkIndex, rowOffset);
      if (targetChunk != null) {
        targetChunk.setChunkLink(fallbackLink);
      }
      return fallbackLink;
    }
  }

  private void submitDownloadTask(ArrowResultChunk chunk) {
    LOGGER.debug("Submitting download task for chunk {}", chunk.getChunkIndex());

    StreamingChunkDownloadTask task =
        new StreamingChunkDownloadTask(
            chunk, httpClient, compressionCodec, this::getRefreshedLink, cloudFetchSpeedThreshold);

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

  /** Waits until the prefetch thread enqueues a chunk index into the consumption queue. */
  private void waitForNextChunkEnqueued() throws DatabricksSQLException {
    prefetchLock.lock();
    try {
      while (!closed && consumptionOrder.isEmpty() && !endOfStreamReached) {
        if (prefetchError != null) {
          throw new DatabricksSQLException(
              "Link prefetch failed: " + prefetchError.getMessage(),
              prefetchError,
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }
        try {
          chunkCreated.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
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
