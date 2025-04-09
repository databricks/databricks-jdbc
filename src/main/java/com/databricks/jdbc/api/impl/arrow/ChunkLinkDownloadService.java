package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.api.impl.arrow.ArrowResultChunk.SECONDS_BUFFER_FOR_EXPIRY;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ExternalLink;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A service that manages the downloading and refreshing of external links for chunked data
 * retrieval.
 *
 * <p>This service maintains a CompletableFuture for each chunk's external link.
 *
 * <h3>Key Features:</h3>
 *
 * <h4>1. Download Pipeline:</h4>
 *
 * <ul>
 *   <li>Automatically initiates a download chain when using SQL Execution API
 *   <li>Fetches links in batches, starting from a specified chunk index
 *   <li>Processes batches serially, with each new request starting from (last fetched index + 1)
 *   <li>Completes the corresponding futures as soon as links are received
 * </ul>
 *
 * <h4>2. Link Expiration Handling:</h4>
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
 * <h4>3. Correctness Guarantee:</h4>
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
 */
public class ChunkLinkDownloadService {
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

  private final Map<Long, ArrowResultChunk> chunkIndexToChunksMap;

  public ChunkLinkDownloadService(
      IDatabricksSession session,
      StatementId statementId,
      long totalChunks,
      Map<Long, ArrowResultChunk> chunkIndexToChunksMap,
      long nextBatchStartIndex) {
    LOGGER.info(
        "Initializing ChunkLinkDownloadService for statement %s with total chunks: %d, starting at index: %d",
        statementId, totalChunks, nextBatchStartIndex);

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
          "Attempt to get link for chunk %d while chunk download service is shutdown", chunkIndex);
      return createExceptionalFuture(
          new DatabricksValidationException("Chunk Link Download Service is shutdown"));
    }

    if (chunkIndex >= totalChunks) {
      LOGGER.error("Requested chunk index %d exceeds total chunks %d", chunkIndex, totalChunks);
      return createExceptionalFuture(
          new DatabricksValidationException("Chunk index exceeds total chunks"));
    }

    LOGGER.debug("Getting link for chunk %d", chunkIndex);
    handleExpiredLinksAndReset(chunkIndex);

    // Trigger first download if not already done
    if (isDownloadChainStarted.compareAndSet(false, true)) {
      LOGGER.info("Initiating first download chain for chunk %d", chunkIndex);
      triggerNextBatchDownload();
    }

    return chunkIndexToLinkFuture.get(chunkIndex);
  }

  /** Shuts down the service and cancels all pending operations. */
  public void shutdown() {
    LOGGER.info("Shutting down ChunkLinkDownloadService for statement %s", statementId);
    isShutdown = true;
    chunkIndexToLinkFuture.forEach(
        (index, future) -> {
          if (!future.isDone()) {
            LOGGER.debug("Completing future for chunk %d exceptionally due to shutdown", index);
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
          "Skipping batch download - Service shutdown: %s, Download in progress: %s",
          isShutdown, isDownloadInProgress.get());
      return;
    }

    final long batchStartIndex = nextBatchStartIndex.get();
    if (batchStartIndex >= totalChunks) {
      LOGGER.info(
          "No more chunks to download. Current index: %d, Total chunks: %d",
          batchStartIndex, totalChunks);
      isDownloadInProgress.set(false);
      return;
    }

    LOGGER.info("Starting batch download from index %d", batchStartIndex);
    currentDownloadTask =
        CompletableFuture.runAsync(
            () -> {
              try {
                Collection<ExternalLink> links =
                    session.getDatabricksClient().getResultChunks(statementId, batchStartIndex);
                LOGGER.info(
                    "Retrieved %d links for batch starting at %d for statement id %s",
                    links.size(), batchStartIndex, statementId);

                // Complete futures for all chunks in this batch
                for (ExternalLink link : links) {
                  CompletableFuture<ExternalLink> future =
                      chunkIndexToLinkFuture.get(link.getChunkIndex());
                  if (future != null) {
                    LOGGER.debug(
                        "Completing future for chunk %d for statement id %s",
                        link.getChunkIndex(), statementId);
                    future.complete(link);
                  }
                }

                // Update next batch start index and trigger next batch
                if (!links.isEmpty()) {
                  long maxChunkIndex =
                      links.stream().mapToLong(ExternalLink::getChunkIndex).max().getAsLong();
                  nextBatchStartIndex.set(maxChunkIndex + 1);
                  LOGGER.debug("Updated next batch start index to %d", maxChunkIndex + 1);

                  // Mark current download as complete and trigger next batch
                  isDownloadInProgress.set(false);
                  if (maxChunkIndex + 1 < totalChunks) {
                    LOGGER.debug("Triggering next batch download");
                    triggerNextBatchDownload();
                  }
                }
              } catch (DatabricksSQLException e) {
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
  private void handleBatchDownloadError(long batchStartIndex, DatabricksSQLException e) {
    LOGGER.error(
        e,
        "Failed to download links for batch starting at %d: %s",
        batchStartIndex,
        e.getMessage());

    // Complete exceptionally all pending futures
    chunkIndexToLinkFuture.forEach(
        (index, future) -> {
          if (!future.isDone()) {
            LOGGER.debug(
                "Completing future for chunk %d exceptionally due to batch download error", index);
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
            "Detected expired link for chunk %d, re-triggering batch download from the smallest index with the expired link",
            chunkIndex);
        for (long i = 1; i < totalChunks; i++) {
          if (isChunkLinkExpiredForPendingDownload(i)) {
            LOGGER.info("Found the smallest index %d with the expired link, initiating reset", i);
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
    ArrowResultChunk chunk = chunkIndexToChunksMap.get(chunkIndex);

    return chunkFuture.isDone()
        && isChunkLinkExpired(chunkFuture.get())
        && chunk.getStatus() != ArrowResultChunk.ChunkStatus.DOWNLOAD_SUCCEEDED;
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
        LOGGER.trace("Expected exception while cancelling download task: %s", e.getMessage());
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
    LOGGER.info("Resetting futures from index %d", startIndex);
    for (long j = startIndex; j < totalChunks; j++) {
      CompletableFuture<ExternalLink> future = chunkIndexToLinkFuture.get(j);
      if (future != null && !future.isDone()) {
        LOGGER.debug("Cancelling future for chunk %d", j);
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
    LOGGER.info("Preparing new batch download from index %d", startIndex);
    nextBatchStartIndex.set(startIndex);
    isDownloadInProgress.set(false);
    isDownloadChainStarted.set(false);
  }

  private boolean isChunkLinkExpired(ExternalLink link) {
    if (link == null || link.getExpiration() == null) {
      LOGGER.warn("Link or expiration is null, assuming link is expired");
      return true;
    }
    Instant expirationWithBuffer =
        Instant.parse(link.getExpiration()).minusSeconds(SECONDS_BUFFER_FOR_EXPIRY);

    return expirationWithBuffer.isBefore(Instant.now());
  }
}
