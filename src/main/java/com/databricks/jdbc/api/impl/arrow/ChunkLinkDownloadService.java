package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.api.impl.arrow.ArrowResultChunk.SECONDS_BUFFER_FOR_EXPIRY;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A service that manages the downloading and refreshing of external links for chunked data
 * retrieval.
 *
 * <p>This service maintains a CompletableFuture for each chunk's external link.
 *
 * <p><b>Key Features:</b>
 *
 * <p><b>1. Download Pipeline:</b>
 *
 * <ul>
 *   <li>Automatically initiates a download chain when using SQL Execution API
 *   <li>Fetches links in batches, starting from a specified chunk index
 *   <li>Processes batches serially, with each new request starting from (last fetched index + 1)
 *   <li>Completes the corresponding futures as soon as links are received
 * </ul>
 *
 * <p><b>2. Link Expiration Handling:</b>
 *
 * <ul>
 *   <li>Monitors link expiration when chunks request their download links
 *   <li>When an expired link is detected (and its chunk hasn't been downloaded):
 *       <ul>
 *         <li>Finds the earliest chunk index with an expired link
 *         <li>Restarts the download chain from this index
 *       </ul>
 * </ul>
 *
 * <p><b>3. Correctness Guarantee:</b>
 *
 * <p>The service maintains correctness through two mechanisms:
 *
 * <ul>
 *   <li>Monotonically increasing request indexes
 *   <li>Server's guarantee of returning continuous series of chunk links
 * </ul>
 *
 * <p>This design ensures that no chunks are missed and links remain valid during the download
 * process.
 *
 * @param <T> The specific type of {@link AbstractArrowResultChunk} this service manages
 */
public class ChunkLinkDownloadService<T extends AbstractArrowResultChunk> {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ChunkLinkDownloadService.class);

  private final IDatabricksSession session;
  private final StatementId statementId;
  private final long totalChunks;
  private final Map<Long, CompletableFuture<ExternalLink>> chunkIndexToLinkFuture;
  private final AtomicLong nextBatchStartIndex;
  private final AtomicBoolean isDownloadInProgress;
  private final AtomicBoolean isDownloadChainStarted;
  private volatile boolean isShutdown;
  private volatile CompletableFuture<Void> currentDownloadTask;

  /**
   * Lock object to ensure thread-safe reset operations.
   *
   * <p>This lock is crucial for preventing race conditions during link expiration checks and reset
   * operations. Without it, multiple threads could simultaneously detect expired links and attempt
   * to reset the download chain, leading to duplicate or missed chunks.
   */
  private final Object resetLock = new Object();

  private final ConcurrentMap<Long, T> chunkIndexToChunksMap;

  public ChunkLinkDownloadService(
      IDatabricksSession session,
      StatementId statementId,
      long totalChunks,
      ConcurrentMap<Long, T> chunkIndexToChunksMap,
      long nextBatchStartIndex) {
    LOGGER.info(
        "Initializing ChunkLinkDownloadService for statement {} with total chunks: {}, starting at index: {}",
        statementId,
        totalChunks,
        nextBatchStartIndex);

    this.session = session;
    this.statementId = statementId;
    this.totalChunks = totalChunks;
    this.nextBatchStartIndex = new AtomicLong(nextBatchStartIndex);
    this.isDownloadInProgress = new AtomicBoolean(false);
    this.isDownloadChainStarted = new AtomicBoolean(false);
    this.isShutdown = false;

    this.chunkIndexToLinkFuture = new ConcurrentHashMap<>();
    for (long i = 0; i < totalChunks; i++) {
      this.chunkIndexToLinkFuture.put(i, new CompletableFuture<>());
    }

    this.chunkIndexToChunksMap = chunkIndexToChunksMap;

    // Complete futures for chunks that already have their links (upfront-fetched)
    if (nextBatchStartIndex > 0) {
      LOGGER.info("Completing futures for {} upfront-fetched links", nextBatchStartIndex);
      int completedCount = 0;
      for (long i = 0; i < Math.min(nextBatchStartIndex, totalChunks); i++) {
        T chunk = chunkIndexToChunksMap.get(i);
        if (chunk != null) {
          ExternalLink link = chunk.getChunkLink();
          if (link != null) {
            LOGGER.debug("Completing link future for chunk {} in constructor", i);
            chunkIndexToLinkFuture.get(i).complete(link);
            completedCount++;
          }
        }
      }
      LOGGER.info("Completed {} futures for upfront-fetched links", completedCount);
    }

    if (session.getConnectionContext().getClientType() == DatabricksClientType.SEA
        && isDownloadChainStarted.compareAndSet(false, true)) {
      // SEA doesn't give all chunk links, so better to trigger download chain as soon as possible
      LOGGER.info("Auto-triggering download chain for SEA client type");
      triggerNextBatchDownload();
    }
  }

  /**
   * Retrieves the external link for a specified chunk index.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>Checks if the service is operational
   *   <li>Validates the requested chunk index
   *   <li>Handles expired links if necessary
   *   <li>Initiates the download chain if not already started
   * </ul>
   *
   * @param chunkIndex The index of the chunk for which to get the link
   * @return A CompletableFuture containing the ExternalLink for the requested chunk
   * @throws ExecutionException If the link retrieval fails
   * @throws InterruptedException If the operation is interrupted
   */
  public CompletableFuture<ExternalLink> getLinkForChunk(long chunkIndex)
      throws ExecutionException, InterruptedException {
    if (isShutdown) {
      LOGGER.warn(
          "Attempt to get link for chunk {} while chunk download service is shutdown", chunkIndex);
      return createExceptionalFuture(
          new DatabricksValidationException("Chunk Link Download Service is shutdown"));
    }

    if (chunkIndex >= totalChunks) {
      LOGGER.error("Requested chunk index {} exceeds total chunks {}", chunkIndex, totalChunks);
      return createExceptionalFuture(
          new DatabricksValidationException("Chunk index exceeds total chunks"));
    }

    LOGGER.debug("Getting link for chunk {}", chunkIndex);
    handleExpiredLinksAndReset(chunkIndex);

    // Trigger first download if not already done
    if (isDownloadChainStarted.compareAndSet(false, true)) {
      LOGGER.info("Initiating first download chain for chunk {}", chunkIndex);
      triggerNextBatchDownload();
    }

    return chunkIndexToLinkFuture.get(chunkIndex);
  }

  /** Shuts down the service and cancels all pending operations. */
  public void shutdown() {
    LOGGER.info("Shutting down ChunkLinkDownloadService for statement {}", statementId);
    isShutdown = true;
    chunkIndexToLinkFuture.forEach(
        (index, future) -> {
          if (!future.isDone()) {
            LOGGER.debug("Completing future for chunk {} exceptionally due to shutdown", index);
            future.completeExceptionally(
                new DatabricksValidationException("Service was shut down"));
          }
        });
  }

  /**
   * Initiates the download of the next batch of chunk links.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>Checks if a download is already in progress
   *   <li>Validates if there are more chunks to download
   *   <li>Makes an async request to fetch the next batch of links
   *   <li>Updates futures with received links
   *   <li>Triggers the next batch download if more chunks remain
   * </ul>
   *
   * <p>If an error occurs during download, all pending futures are completed exceptionally.
   */
  private void triggerNextBatchDownload() {
    if (isShutdown || !isDownloadInProgress.compareAndSet(false, true)) {
      LOGGER.debug(
          "Skipping batch download - Service shutdown: {}, Download in progress: {}",
          isShutdown,
          isDownloadInProgress.get());
      return;
    }

    final long batchStartIndex = nextBatchStartIndex.get();
    if (batchStartIndex >= totalChunks) {
      LOGGER.info(
          "No more chunks to download. Current index: {}, Total chunks: {}",
          batchStartIndex,
          totalChunks);
      isDownloadInProgress.set(false);
      return;
    }

    // Calculate row offset for this batch
    final long batchStartRowOffset = getChunkStartRowOffset(batchStartIndex);

    LOGGER.info("Starting batch download from index {}", batchStartIndex);
    currentDownloadTask =
        CompletableFuture.runAsync(
            () -> {
              try {
                ChunkLinkFetchResult result =
                    session
                        .getDatabricksClient()
                        .getResultChunks(statementId, batchStartIndex, batchStartRowOffset);
                LOGGER.info(
                    "Retrieved {} links for batch starting at {} for statement id {}",
                    result.getChunkLinks().size(),
                    batchStartIndex,
                    statementId);

                // Complete futures for all chunks in this batch
                for (ExternalLink link : result.getChunkLinks()) {
                  CompletableFuture<ExternalLink> future =
                      chunkIndexToLinkFuture.get(link.getChunkIndex());
                  if (future != null) {
                    LOGGER.debug(
                        "Completing future for chunk {} for statement id {}",
                        link.getChunkIndex(),
                        statementId);
                    future.complete(link);
                  }
                }

                // Update next batch start index and trigger next batch
                if (!result.getChunkLinks().isEmpty()) {
                  long maxChunkIndex =
                      result.getChunkLinks().stream()
                          .mapToLong(ExternalLink::getChunkIndex)
                          .max()
                          .getAsLong();
                  nextBatchStartIndex.set(maxChunkIndex + 1);
                  LOGGER.debug("Updated next batch start index to {}", maxChunkIndex + 1);

                  // Mark current download as complete and trigger next batch
                  isDownloadInProgress.set(false);
                  if (maxChunkIndex + 1 < totalChunks) {
                    LOGGER.debug("Triggering next batch download");
                    triggerNextBatchDownload();
                  }
                }
              } catch (SQLException e) {
                // If the download fails, complete exceptionally all pending futures
                handleBatchDownloadError(batchStartIndex, e);
              }
            });
  }

  /**
   * Handles errors that occur during batch download.
   *
   * <p>Completes all pending futures exceptionally with the encountered error and resets the
   * download progress flag.
   */
  private void handleBatchDownloadError(long batchStartIndex, SQLException e) {
    LOGGER.error(
        e,
        "Failed to download links for batch starting at {} : {}",
        batchStartIndex,
        e.getMessage());

    // Complete exceptionally all pending futures
    chunkIndexToLinkFuture.forEach(
        (index, future) -> {
          if (!future.isDone()) {
            LOGGER.debug(
                "Completing future for chunk {} exceptionally due to batch download error", index);
            future.completeExceptionally(e);
          }
        });

    isDownloadInProgress.set(false);
  }

  /**
   * Creates a CompletableFuture that is already completed exceptionally with the given exception.
   */
  private CompletableFuture<ExternalLink> createExceptionalFuture(Exception e) {
    CompletableFuture<ExternalLink> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  /**
   * Handles expired links and resets the download chain if necessary.
   *
   * <p>This method performs the following steps:
   *
   * <ul>
   *   <li>Checks for expired links
   *   <li>Identifies the minimum chunk index with an expired link
   *   <li>Cancels any ongoing download task
   *   <li>Resets futures for all chunks from the minimum expired index
   *   <li>Initiates a new batch download from the minimum expired index
   * </ul>
   *
   * <p>The synchronization is critical because:
   *
   * <ul>
   *   <li>Multiple threads might detect expired links simultaneously
   *   <li>Without synchronization, we could have overlapping reset operation
   *   <li>This could lead to race conditions where some chunks are missed or downloaded multiple
   *       times
   *   <li>The lock ensures the entire check-and-reset operation is atomic
   * </ul>
   *
   * @param chunkIndex The chunk index that triggered the check
   * @throws ExecutionException If checking link expiration fails
   * @throws InterruptedException If the operation is interrupted
   */
  private void handleExpiredLinksAndReset(long chunkIndex)
      throws ExecutionException, InterruptedException {
    synchronized (resetLock) {
      if (isChunkLinkExpiredForPendingDownload(chunkIndex)) {
        LOGGER.info(
            "Detected expired link for chunk {}, re-triggering batch download from the smallest index with the expired link",
            chunkIndex);
        for (long i = 0; i < totalChunks; i++) {
          if (isChunkLinkExpiredForPendingDownload(i)) {
            LOGGER.info("Found the smallest index {} with the expired link, initiating reset", i);
            cancelCurrentDownloadTask();
            resetFuturesFromIndex(i);
            prepareNewBatchDownload(i);
            break;
          }
        }
      }
    }
  }

  /**
   * Checks if a chunk's link future is complete but the link has expired.
   *
   * <p>A link is considered expired if:
   *
   * <ul>
   *   <li>The future is complete
   *   <li>The link has passed its expiration time
   *   <li>The chunk data hasn't been successfully downloaded yet
   * </ul>
   *
   * @param chunkIndex The index of the chunk to check
   * @return true if the link is complete but expired, false otherwise
   * @throws ExecutionException If checking the future fails
   * @throws InterruptedException If the operation is interrupted
   */
  private boolean isChunkLinkExpiredForPendingDownload(long chunkIndex)
      throws ExecutionException, InterruptedException {
    CompletableFuture<ExternalLink> chunkFuture = chunkIndexToLinkFuture.get(chunkIndex);
    T chunk = chunkIndexToChunksMap.get(chunkIndex);

    return chunkFuture.isDone()
        && isChunkLinkExpired(chunkFuture.get())
        && chunk.getStatus() != ChunkStatus.DOWNLOAD_SUCCEEDED;
  }

  /** Cancels the current download task if it exists and is not done. Waits briefly for cleanup. */
  private void cancelCurrentDownloadTask() {
    if (currentDownloadTask != null && !currentDownloadTask.isDone()) {
      LOGGER.debug("Cancelling current download task");
      currentDownloadTask.cancel(true);
      try {
        currentDownloadTask.get(100, java.util.concurrent.TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // Expected - task was cancelled
        LOGGER.trace("Expected exception while cancelling download task: {}", e.getMessage());
      }
      currentDownloadTask = null;
    }
  }

  /**
   * Resets futures for all chunks starting from the given index.
   *
   * @param startIndex the index from which to start resetting futures
   */
  private void resetFuturesFromIndex(long startIndex) {
    LOGGER.info("Resetting futures from index {}", startIndex);
    for (long j = startIndex; j < totalChunks; j++) {
      CompletableFuture<ExternalLink> future = chunkIndexToLinkFuture.get(j);
      if (future != null && !future.isDone()) {
        LOGGER.debug("Cancelling future for chunk {}", j);
        future.cancel(true);
      }
      chunkIndexToLinkFuture.put(j, new CompletableFuture<>());
    }
  }

  /**
   * Prepares for a new batch download by setting appropriate flags and indices.
   *
   * @param startIndex the index from which to start the new batch
   */
  private void prepareNewBatchDownload(long startIndex) {
    LOGGER.info("Preparing new batch download from index {}", startIndex);
    nextBatchStartIndex.set(startIndex);
    isDownloadInProgress.set(false);
    isDownloadChainStarted.set(false);
  }

  /**
   * Gets the start row offset for a given chunk index.
   *
   * @param chunkIndex the chunk index to get the row offset for
   * @return the start row offset for the chunk
   */
  private long getChunkStartRowOffset(long chunkIndex) {
    T chunk = chunkIndexToChunksMap.get(chunkIndex);
    if (chunk == null) {
      // Should never happen.
      throw new IllegalStateException(
          "Chunk not found in map for index "
              + chunkIndex
              + ". "
              + "Total chunks: "
              + totalChunks
              + ", StatementId: "
              + statementId);
    }
    return chunk.getStartRowOffset();
  }

  private boolean isChunkLinkExpired(ExternalLink link) {
    if (link == null || link.getExpiration() == null) {
      LOGGER.warn("Link or expiration is null, assuming link is expired");
      return true;
    }

    // Skip expiry check when running against fake service (tests)
    if (DriverUtil.isRunningAgainstFake()) {
      return false;
    }

    Instant expirationWithBuffer =
        Instant.parse(link.getExpiration()).minusSeconds(SECONDS_BUFFER_FOR_EXPIRY);

    return expirationWithBuffer.isBefore(Instant.now());
  }

  /**
   * Returns the CompletableFuture for a specific chunk index for testing purposes.
   *
   * @param chunkIndex The index of the chunk
   * @return The CompletableFuture associated with the chunk index, or null if not found
   */
  @VisibleForTesting
  CompletableFuture<ExternalLink> getLinkFutureForTest(long chunkIndex) {
    return chunkIndexToLinkFuture.get(chunkIndex);
  }
}
